// Demonstrates why the ring buffer's single-producer rule is a correctness
// constraint rather than a style preference.
//
//   --mode=broken  two producer threads share one SpscQueue. Both advance the
//                  same write index and write the same slot, so messages are
//                  torn, lost, or the ring wedges. Built with -fsanitize=thread
//                  this reports a data race on the index and the slot storage.
//
//   --mode=fixed   each producer owns a private SpscQueue and a single sequencer
//                  thread is the only writer to the downstream queue. Same two
//                  sources, one owner per queue, no race.
//
// This mirrors the real pipeline: multicast and TCP recovery both feed the
// sequencer, and only the sequencer writes to the ring the book reads.

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "llte/args.hpp"
#include "llte/clock.hpp"
#include "llte/spsc_queue.hpp"

namespace {

constexpr std::size_t kQueueCapacity = 1024;

// Corrupted indexes can leave the ring looking permanently full, or leave the
// producers and consumer crawling forward together without either ever reaching
// its exit condition. How long that takes varies wildly run to run, so the whole
// demo works to a wall-clock deadline instead of a spin count. Livelock is one of
// the failure modes being shown; hanging is not.
constexpr std::uint64_t kDeadlineCheckInterval = 4096;

struct Item {
    std::uint64_t producer;
    std::uint64_t sequence;
    std::uint64_t checksum;
};

constexpr std::uint64_t checksum_for(std::uint64_t producer, std::uint64_t sequence) {
    return (producer * 0x9e3779b97f4a7c15ULL) ^ (sequence * 0xbf58476d1ce4e5b9ULL);
}

using Queue = llte::SpscQueue<Item, kQueueCapacity>;

struct Verdict {
    std::uint64_t sent = 0;
    std::uint64_t received = 0;
    std::uint64_t corrupted = 0;
    std::uint64_t out_of_order = 0;
    std::uint64_t stalled_pushes = 0;
    bool hit_time_budget = false;
};

template <typename QueueType>
bool push_bounded(QueueType& queue, const Item& item, std::uint64_t deadline_ns) {
    for (std::uint64_t spins = 0;; ++spins) {
        if (queue.try_push(item)) {
            return true;
        }
        if (spins % kDeadlineCheckInterval == 0 && llte::now_ns() > deadline_ns) {
            return false;
        }
    }
}

class Verifier {
public:
    explicit Verifier(int producer_count) : next_expected_(producer_count, 0) {}

    void check(const Item& item) {
        received_ += 1;
        if (item.producer >= next_expected_.size() ||
            item.checksum != checksum_for(item.producer, item.sequence)) {
            corrupted_ += 1;
            return;
        }
        if (item.sequence != next_expected_[item.producer]) {
            out_of_order_ += 1;
        }
        next_expected_[item.producer] = item.sequence + 1;
    }

    std::uint64_t received() const { return received_; }

    void publish(Verdict& verdict) const {
        verdict.received = received_;
        verdict.corrupted = corrupted_;
        verdict.out_of_order = out_of_order_;
    }

private:
    std::vector<std::uint64_t> next_expected_;
    std::uint64_t received_ = 0;
    std::uint64_t corrupted_ = 0;
    std::uint64_t out_of_order_ = 0;
};

Verdict run_broken(int producer_count, std::uint64_t per_producer, std::uint64_t deadline_ns) {
    Queue queue;
    std::atomic<int> finished{0};
    std::atomic<std::uint64_t> stalled{0};
    std::atomic<bool> gave_up{false};

    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    for (int id = 0; id < producer_count; ++id) {
        producers.emplace_back([&, id] {
            for (std::uint64_t i = 0; i < per_producer; ++i) {
                const Item item{static_cast<std::uint64_t>(id), i,
                                checksum_for(static_cast<std::uint64_t>(id), i)};
                // Two threads calling try_push on one SpscQueue: they race on the
                // write index and can land on the same slot.
                if (!push_bounded(queue, item, deadline_ns)) {
                    stalled.fetch_add(per_producer - i, std::memory_order_relaxed);
                    break;
                }
            }
            finished.fetch_add(1, std::memory_order_release);
        });
    }

    // Inconsistent indexes also let the consumer re-read slots that were never
    // published, so cap it at the number actually sent. Past that point the
    // stream is already provably broken and the extra reads say nothing new.
    const std::uint64_t expected_total = static_cast<std::uint64_t>(producer_count) * per_producer;
    Verifier verifier(producer_count);
    std::thread consumer([&] {
        Item item{};
        std::uint64_t spins = 0;
        while (verifier.received() < expected_total) {
            if (queue.try_pop(item)) {
                verifier.check(item);
            } else if (finished.load(std::memory_order_acquire) == producer_count &&
                       queue.empty()) {
                return;
            }
            if (++spins % kDeadlineCheckInterval == 0 && llte::now_ns() > deadline_ns) {
                gave_up.store(true, std::memory_order_release);
                return;
            }
        }
    });

    for (auto& producer : producers) {
        producer.join();
    }
    consumer.join();

    Verdict verdict;
    verdict.sent = static_cast<std::uint64_t>(producer_count) * per_producer;
    verdict.stalled_pushes = stalled.load(std::memory_order_relaxed);
    verdict.hit_time_budget = gave_up.load(std::memory_order_acquire);
    verifier.publish(verdict);
    return verdict;
}

Verdict run_fixed(int producer_count, std::uint64_t per_producer, std::uint64_t deadline_ns) {
    // One queue per producer, so every queue still has exactly one writer.
    std::vector<std::unique_ptr<Queue>> inbound;
    inbound.reserve(producer_count);
    for (int i = 0; i < producer_count; ++i) {
        inbound.push_back(std::make_unique<Queue>());
    }
    Queue downstream;

    std::atomic<int> finished{0};
    std::atomic<bool> sequencer_done{false};
    std::atomic<std::uint64_t> stalled{0};

    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    for (int id = 0; id < producer_count; ++id) {
        producers.emplace_back([&, id] {
            for (std::uint64_t i = 0; i < per_producer; ++i) {
                const Item item{static_cast<std::uint64_t>(id), i,
                                checksum_for(static_cast<std::uint64_t>(id), i)};
                if (!push_bounded(*inbound[id], item, deadline_ns)) {
                    stalled.fetch_add(1, std::memory_order_relaxed);
                }
            }
            finished.fetch_add(1, std::memory_order_release);
        });
    }

    // Sole consumer of each inbound queue and sole producer of the downstream one.
    std::thread sequencer([&] {
        Item item{};
        for (;;) {
            bool moved = false;
            for (auto& queue : inbound) {
                if (queue->try_pop(item)) {
                    if (!push_bounded(downstream, item, deadline_ns)) {
                        stalled.fetch_add(1, std::memory_order_relaxed);
                    }
                    moved = true;
                }
            }
            if (moved || finished.load(std::memory_order_acquire) != producer_count) {
                continue;
            }
            bool drained = true;
            for (auto& queue : inbound) {
                drained = drained && queue->empty();
            }
            if (drained) {
                sequencer_done.store(true, std::memory_order_release);
                return;
            }
        }
    });

    Verifier verifier(producer_count);
    std::thread consumer([&] {
        Item item{};
        for (;;) {
            if (downstream.try_pop(item)) {
                verifier.check(item);
            } else if (sequencer_done.load(std::memory_order_acquire) && downstream.empty()) {
                return;
            }
        }
    });

    for (auto& producer : producers) {
        producer.join();
    }
    sequencer.join();
    consumer.join();

    Verdict verdict;
    verdict.sent = static_cast<std::uint64_t>(producer_count) * per_producer;
    verdict.stalled_pushes = stalled.load(std::memory_order_relaxed);
    verifier.publish(verdict);
    return verdict;
}

}  // namespace

int main(int argc, char** argv) {
    const llte::Args args(argc, argv);
    if (args.has("help")) {
        std::printf(
            "usage: spsc_race_demo [--mode broken|fixed] [--producers N] [--messages N]\n"
            "                      [--time-budget-ms N]\n");
        return 0;
    }

    const std::string mode = args.str("mode", "fixed");
    const int producers = static_cast<int>(args.integer("producers", 2));
    const auto per_producer = static_cast<std::uint64_t>(args.integer("messages", 200000));
    const auto budget_ms = static_cast<std::uint64_t>(args.integer("time-budget-ms", 5000));

    std::printf("spsc_race_demo: mode=%s producers=%d messages/producer=%llu budget=%llums\n",
                mode.c_str(), producers, static_cast<unsigned long long>(per_producer),
                static_cast<unsigned long long>(budget_ms));

    const std::uint64_t deadline_ns = llte::now_ns() + budget_ms * 1'000'000ULL;
    const Verdict verdict = mode == "broken"
                                ? run_broken(producers, per_producer, deadline_ns)
                                : run_fixed(producers, per_producer, deadline_ns);

    const std::uint64_t lost =
        verdict.sent > verdict.received ? verdict.sent - verdict.received : 0;
    std::printf("sent=%llu received=%llu lost=%llu corrupted=%llu out_of_order=%llu\n",
                static_cast<unsigned long long>(verdict.sent),
                static_cast<unsigned long long>(verdict.received),
                static_cast<unsigned long long>(lost),
                static_cast<unsigned long long>(verdict.corrupted),
                static_cast<unsigned long long>(verdict.out_of_order));
    std::printf("stalled_pushes=%llu hit_time_budget=%s\n",
                static_cast<unsigned long long>(verdict.stalled_pushes),
                verdict.hit_time_budget ? "yes" : "no");

    const bool clean = lost == 0 && verdict.corrupted == 0 && verdict.out_of_order == 0 &&
                       verdict.stalled_pushes == 0 && !verdict.hit_time_budget;
    std::printf("result: %s\n", clean ? "stream intact" : "STREAM DAMAGED");
    // The broken mode is expected to fail; reporting it as success would defeat
    // the point of the demo.
    return clean ? 0 : 1;
}
