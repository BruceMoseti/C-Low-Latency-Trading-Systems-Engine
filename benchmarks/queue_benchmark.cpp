// Measures the producer->consumer handoff four ways, so the choice of a
// lock-free cache-aligned ring is backed by numbers instead of assumption.
//
//   1. mutex + deque          the obvious baseline
//   2. lock-free, shared line  indices adjacent, so the two cores fight over one
//                              cache line (false sharing)
//   3. lock-free, aligned      indices on separate cache lines
//   4. production SpscQueue    aligned indices plus producer/consumer-local
//                              cached copies of the opposite index

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <cstdint>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "llte/args.hpp"
#include "llte/clock.hpp"
#include "llte/cpu.hpp"
#include "llte/latency_stats.hpp"
#include "llte/spsc_queue.hpp"

namespace {

constexpr std::size_t kCapacity = 1024;

struct Item {
    std::uint64_t sequence;
    std::uint64_t sent_ns;
};

struct Outcome {
    double throughput_msg_per_sec = 0.0;
    llte::LatencySamples::Summary transit;
};

// Variant 1: the straightforward synchronized queue. Bounded to the same depth
// as the lock-free variants -- an unbounded queue would let the producer build a
// huge backlog and report queueing delay instead of handoff cost.
class MutexQueue {
public:
    void push(const Item& item) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            drained_.wait(lock, [&] { return items_.size() < kCapacity; });
            items_.push_back(item);
        }
        ready_.notify_one();
    }

    bool pop(Item& out) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            ready_.wait(lock, [&] { return !items_.empty() || done_; });
            if (items_.empty()) {
                return false;
            }
            out = items_.front();
            items_.pop_front();
        }
        drained_.notify_one();
        return true;
    }

    void finish() {
        {
            std::lock_guard<std::mutex> guard(mutex_);
            done_ = true;
        }
        ready_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable ready_;
    std::condition_variable drained_;
    std::deque<Item> items_;
    bool done_ = false;
};

// Variant 2: lock-free, but both indexes land on the same cache line.
class FalseSharedQueue {
public:
    bool try_push(const Item& value) {
        const std::size_t write = write_idx_.load(std::memory_order_relaxed);
        if (write - read_idx_.load(std::memory_order_acquire) >= kCapacity) {
            return false;
        }
        buffer_[write & (kCapacity - 1)] = value;
        write_idx_.store(write + 1, std::memory_order_release);
        return true;
    }

    bool try_pop(Item& out) {
        const std::size_t read = read_idx_.load(std::memory_order_relaxed);
        if (read == write_idx_.load(std::memory_order_acquire)) {
            return false;
        }
        out = buffer_[read & (kCapacity - 1)];
        read_idx_.store(read + 1, std::memory_order_release);
        return true;
    }

    bool empty() const {
        return read_idx_.load(std::memory_order_acquire) ==
               write_idx_.load(std::memory_order_acquire);
    }

private:
    // Deliberately adjacent: this is the anti-pattern being measured.
    std::atomic<std::size_t> write_idx_{0};
    std::atomic<std::size_t> read_idx_{0};
    Item buffer_[kCapacity]{};
};

// Variant 3: separate cache lines, but every operation still reads the other
// side's index.
class AlignedQueue {
public:
    bool try_push(const Item& value) {
        const std::size_t write = write_idx_.load(std::memory_order_relaxed);
        if (write - read_idx_.load(std::memory_order_acquire) >= kCapacity) {
            return false;
        }
        buffer_[write & (kCapacity - 1)] = value;
        write_idx_.store(write + 1, std::memory_order_release);
        return true;
    }

    bool try_pop(Item& out) {
        const std::size_t read = read_idx_.load(std::memory_order_relaxed);
        if (read == write_idx_.load(std::memory_order_acquire)) {
            return false;
        }
        out = buffer_[read & (kCapacity - 1)];
        read_idx_.store(read + 1, std::memory_order_release);
        return true;
    }

    bool empty() const {
        return read_idx_.load(std::memory_order_acquire) ==
               write_idx_.load(std::memory_order_acquire);
    }

private:
    alignas(llte::kCacheLineBytes) std::atomic<std::size_t> write_idx_{0};
    alignas(llte::kCacheLineBytes) std::atomic<std::size_t> read_idx_{0};
    alignas(llte::kCacheLineBytes) Item buffer_[kCapacity]{};
};

struct RunConfig {
    std::uint64_t messages;
    std::size_t sample_capacity;
    int producer_cpu;
    int consumer_cpu;
    // Messages per second, or 0 to run flat out. Pacing below every variant's
    // sustainable rate keeps the ring near-empty, so the recorded latency is the
    // cost of the handoff rather than the depth of the backlog.
    std::uint64_t pace_rate;
};

// Paces the producer without sleeping: at these intervals a nanosleep would
// overshoot by more than the interval itself.
void pace(const RunConfig& config, std::uint64_t start_ns, std::uint64_t index) {
    if (config.pace_rate == 0) {
        return;
    }
    const std::uint64_t interval_ns = 1'000'000'000ULL / config.pace_rate;
    const std::uint64_t deadline = start_ns + index * interval_ns;
    while (llte::now_ns() < deadline) {
    }
}

Outcome run_mutex(const RunConfig& config) {
    MutexQueue queue;
    llte::LatencySamples transit(config.sample_capacity);
    // Start only once the consumer is in its loop, so thread startup is not
    // charged to the first messages.
    std::atomic<bool> consumer_ready{false};
    std::atomic<std::uint64_t> measured_start{0};

    std::thread producer([&] {
        llte::pin_to_cpu(config.producer_cpu);
        while (!consumer_ready.load(std::memory_order_acquire)) {
        }
        const std::uint64_t start = llte::now_ns();
        measured_start.store(start, std::memory_order_release);
        for (std::uint64_t i = 0; i < config.messages; ++i) {
            pace(config, start, i);
            queue.push(Item{i, llte::now_ns()});
        }
        queue.finish();
    });

    std::thread consumer([&] {
        llte::pin_to_cpu(config.consumer_cpu);
        Item item{};
        consumer_ready.store(true, std::memory_order_release);
        for (std::uint64_t received = 0; received < config.messages; ++received) {
            if (!queue.pop(item)) {
                return;
            }
            transit.add(llte::now_ns() - item.sent_ns);
        }
    });

    producer.join();
    consumer.join();
    const double seconds =
        static_cast<double>(llte::now_ns() - measured_start.load(std::memory_order_acquire)) / 1e9;

    Outcome outcome;
    outcome.throughput_msg_per_sec = static_cast<double>(config.messages) / seconds;
    outcome.transit = transit.summarize();
    return outcome;
}

template <typename QueueType>
Outcome run_spinning(const RunConfig& config) {
    QueueType queue;
    std::atomic<bool> producer_done{false};
    llte::LatencySamples transit(config.sample_capacity);
    std::atomic<bool> consumer_ready{false};
    std::atomic<std::uint64_t> measured_start{0};

    std::thread producer([&] {
        llte::pin_to_cpu(config.producer_cpu);
        while (!consumer_ready.load(std::memory_order_acquire)) {
        }
        const std::uint64_t start = llte::now_ns();
        measured_start.store(start, std::memory_order_release);
        for (std::uint64_t i = 0; i < config.messages; ++i) {
            pace(config, start, i);
            const Item item{i, llte::now_ns()};
            while (!queue.try_push(item)) {
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    std::thread consumer([&] {
        llte::pin_to_cpu(config.consumer_cpu);
        Item item{};
        consumer_ready.store(true, std::memory_order_release);
        for (;;) {
            if (queue.try_pop(item)) {
                transit.add(llte::now_ns() - item.sent_ns);
            } else if (producer_done.load(std::memory_order_acquire) && queue.empty()) {
                return;
            }
        }
    });

    producer.join();
    consumer.join();
    const double seconds =
        static_cast<double>(llte::now_ns() - measured_start.load(std::memory_order_acquire)) / 1e9;

    Outcome outcome;
    outcome.throughput_msg_per_sec = static_cast<double>(config.messages) / seconds;
    outcome.transit = transit.summarize();
    return outcome;
}

struct Variant {
    std::string label;
    Outcome (*run)(const RunConfig&);
};

}  // namespace

int main(int argc, char** argv) {
    const llte::Args args(argc, argv);
    if (args.has("help")) {
        std::printf(
            "usage: queue_benchmark [--messages N] [--pace-rate PER_SEC]\n"
            "                       [--producer-cpu N] [--consumer-cpu N]\n");
        return 0;
    }

    RunConfig config{};
    config.messages = static_cast<std::uint64_t>(args.integer("messages", 2'000'000));
    config.sample_capacity = static_cast<std::size_t>(config.messages);
    config.producer_cpu = static_cast<int>(args.integer("producer-cpu", -1));
    config.consumer_cpu = static_cast<int>(args.integer("consumer-cpu", -1));
    const auto pace_rate = static_cast<std::uint64_t>(args.integer("pace-rate", 1'000'000));

    const std::vector<Variant> variants{
        {"1. mutex + deque", run_mutex},
        {"2. lock-free, shared line", run_spinning<FalseSharedQueue>},
        {"3. lock-free, cache-aligned", run_spinning<AlignedQueue>},
        {"4. + cached indices (shipped)", run_spinning<llte::SpscQueue<Item, kCapacity>>},
    };

    std::printf("queue_benchmark: %llu messages per variant",
                static_cast<unsigned long long>(config.messages));
    if (config.producer_cpu >= 0 || config.consumer_cpu >= 0) {
        std::printf(" (pinned: producer=%d consumer=%d)", config.producer_cpu,
                    config.consumer_cpu);
    }
    std::printf("\n");

    // Phase 1: producer runs flat out. This is the sustainable-rate measurement;
    // the queue sits full, so its latency only reflects backlog depth.
    config.pace_rate = 0;
    std::printf("\nsaturation (producer unthrottled) -- throughput\n");
    std::printf("%-30s %14s\n", "variant", "M msg/s");
    std::printf("%-30s %14s\n", "------------------------------", "--------------");
    std::vector<Outcome> saturated;
    for (const Variant& variant : variants) {
        saturated.push_back(variant.run(config));
        std::printf("%-30s %14.2f\n", variant.label.c_str(),
                    saturated.back().throughput_msg_per_sec / 1e6);
    }

    // Phase 2: hold every variant to the same offered load, well under the
    // slowest one's capacity, so latency is the handoff cost itself.
    config.pace_rate = pace_rate;
    std::printf("\npaced at %.2f M msg/s -- producer-to-consumer handoff latency\n",
                static_cast<double>(pace_rate) / 1e6);
    std::printf("%-30s %10s %10s %10s %10s\n", "variant", "p50 us", "p99 us", "p99.9 us",
                "max us");
    std::printf("%-30s %10s %10s %10s %10s\n", "------------------------------", "----------",
                "----------", "----------", "----------");
    auto us = [](std::uint64_t ns) { return static_cast<double>(ns) / 1000.0; };
    for (const Variant& variant : variants) {
        const Outcome outcome = variant.run(config);
        std::printf("%-30s %10.3f %10.3f %10.3f %10.3f\n", variant.label.c_str(),
                    us(outcome.transit.p50), us(outcome.transit.p99), us(outcome.transit.p999),
                    us(outcome.transit.max));
    }
    return 0;
}
