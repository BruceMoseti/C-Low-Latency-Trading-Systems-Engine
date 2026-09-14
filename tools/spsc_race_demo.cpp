// Demonstrates why the ring buffer's single-producer rule is a correctness
// constraint rather than a style preference.
//
//   --mode=broken  two producer threads share one SpscQueue. Both advance the
//                  same write index and write the same slot, so messages are
//                  torn or lost. Built with -fsanitize=thread this reports a
//                  data race on the queue's index and storage.
//
//   --mode=fixed   each producer owns a private SpscQueue and a single sequencer
//                  thread is the only writer to the downstream queue. Same two
//                  sources, one owner per queue, no race.
//
// This mirrors the real pipeline: multicast and TCP recovery both feed the
// sequencer, and only the sequencer writes to the ring the book reads.

#include <atomic>
#include <cstdio>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "llte/args.hpp"
#include "llte/spsc_queue.hpp"

namespace {

constexpr std::size_t kQueueCapacity = 1024;

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
};

void verify(const Item& item, std::vector<std::uint64_t>& next_expected, Verdict& verdict) {
    verdict.received += 1;
    if (item.producer >= next_expected.size() ||
        item.checksum != checksum_for(item.producer, item.sequence)) {
        verdict.corrupted += 1;
        return;
    }
    if (item.sequence != next_expected[item.producer]) {
        verdict.out_of_order += 1;
    }
    next_expected[item.producer] = item.sequence + 1;
}

Verdict run_broken(int producer_count, std::uint64_t per_producer) {
    Queue queue;
    std::atomic<int> finished{0};
    Verdict verdict;
    verdict.sent = static_cast<std::uint64_t>(producer_count) * per_producer;

    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    for (int id = 0; id < producer_count; ++id) {
        producers.emplace_back([&, id] {
            for (std::uint64_t i = 0; i < per_producer; ++i) {
                const Item item{static_cast<std::uint64_t>(id), i,
                                checksum_for(static_cast<std::uint64_t>(id), i)};
                // Two threads calling try_push on one SpscQueue: they race on the
                // write index and can land on the same slot.
                while (!queue.try_push(item)) {
                }
            }
            finished.fetch_add(1, std::memory_order_release);
        });
    }

    std::vector<std::uint64_t> next_expected(producer_count, 0);
    std::thread consumer([&] {
        Item item{};
        for (;;) {
            if (queue.try_pop(item)) {
                verify(item, next_expected, verdict);
            } else if (finished.load(std::memory_order_acquire) == producer_count &&
                       queue.empty()) {
                return;
            }
        }
    });

    for (auto& producer : producers) {
        producer.join();
    }
    consumer.join();
    return verdict;
}

Verdict run_fixed(int producer_count, std::uint64_t per_producer) {
    // One queue per producer, so every queue still has exactly one writer.
    std::vector<std::unique_ptr<Queue>> inbound;
    inbound.reserve(producer_count);
    for (int i = 0; i < producer_count; ++i) {
        inbound.push_back(std::make_unique<Queue>());
    }
    Queue downstream;

    std::atomic<int> finished{0};
    std::atomic<bool> sequencer_done{false};
    Verdict verdict;
    verdict.sent = static_cast<std::uint64_t>(producer_count) * per_producer;

    std::vector<std::thread> producers;
    producers.reserve(producer_count);
    for (int id = 0; id < producer_count; ++id) {
        producers.emplace_back([&, id] {
            for (std::uint64_t i = 0; i < per_producer; ++i) {
                const Item item{static_cast<std::uint64_t>(id), i,
                                checksum_for(static_cast<std::uint64_t>(id), i)};
                while (!inbound[id]->try_push(item)) {
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
                    while (!downstream.try_push(item)) {
                    }
                    moved = true;
                }
            }
            if (!moved && finished.load(std::memory_order_acquire) == producer_count) {
                bool drained = true;
                for (auto& queue : inbound) {
                    drained = drained && queue->empty();
                }
                if (drained) {
                    sequencer_done.store(true, std::memory_order_release);
                    return;
                }
            }
        }
    });

    std::vector<std::uint64_t> next_expected(producer_count, 0);
    std::thread consumer([&] {
        Item item{};
        for (;;) {
            if (downstream.try_pop(item)) {
                verify(item, next_expected, verdict);
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
    return verdict;
}

}  // namespace

int main(int argc, char** argv) {
    const llte::Args args(argc, argv);
    if (args.has("help")) {
        std::printf("usage: spsc_race_demo [--mode broken|fixed] [--producers N] [--messages N]\n");
        return 0;
    }

    const std::string mode = args.str("mode", "fixed");
    const int producers = static_cast<int>(args.integer("producers", 2));
    const auto per_producer = static_cast<std::uint64_t>(args.integer("messages", 200000));

    std::printf("spsc_race_demo: mode=%s producers=%d messages/producer=%llu\n", mode.c_str(),
                producers, static_cast<unsigned long long>(per_producer));

    const Verdict verdict =
        mode == "broken" ? run_broken(producers, per_producer) : run_fixed(producers, per_producer);

    const std::uint64_t lost =
        verdict.sent > verdict.received ? verdict.sent - verdict.received : 0;
    std::printf("sent=%llu received=%llu lost=%llu corrupted=%llu out_of_order=%llu\n",
                static_cast<unsigned long long>(verdict.sent),
                static_cast<unsigned long long>(verdict.received),
                static_cast<unsigned long long>(lost),
                static_cast<unsigned long long>(verdict.corrupted),
                static_cast<unsigned long long>(verdict.out_of_order));

    const bool clean = lost == 0 && verdict.corrupted == 0 && verdict.out_of_order == 0;
    std::printf("result: %s\n", clean ? "stream intact" : "STREAM DAMAGED");
    // The broken mode is expected to fail; reporting it as success would defeat
    // the point of the demo.
    return clean ? 0 : 1;
}
