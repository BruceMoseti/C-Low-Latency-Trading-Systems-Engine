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

// Variant 1: the straightforward synchronized queue.
class MutexQueue {
public:
    void push(const Item& item) {
        {
            std::lock_guard<std::mutex> guard(mutex_);
            items_.push_back(item);
        }
        ready_.notify_one();
    }

    bool pop(Item& out) {
        std::unique_lock<std::mutex> lock(mutex_);
        ready_.wait(lock, [&] { return !items_.empty() || done_; });
        if (items_.empty()) {
            return false;
        }
        out = items_.front();
        items_.pop_front();
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
};

Outcome run_mutex(const RunConfig& config) {
    MutexQueue queue;
    llte::LatencySamples transit(config.sample_capacity);
    const std::uint64_t start = llte::now_ns();

    std::thread producer([&] {
        llte::pin_to_cpu(config.producer_cpu);
        for (std::uint64_t i = 0; i < config.messages; ++i) {
            queue.push(Item{i, llte::now_ns()});
        }
        queue.finish();
    });

    std::thread consumer([&] {
        llte::pin_to_cpu(config.consumer_cpu);
        Item item{};
        for (std::uint64_t received = 0; received < config.messages; ++received) {
            if (!queue.pop(item)) {
                return;
            }
            transit.add(llte::now_ns() - item.sent_ns);
        }
    });

    producer.join();
    consumer.join();
    const double seconds = static_cast<double>(llte::now_ns() - start) / 1e9;

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
    const std::uint64_t start = llte::now_ns();

    std::thread producer([&] {
        llte::pin_to_cpu(config.producer_cpu);
        for (std::uint64_t i = 0; i < config.messages; ++i) {
            const Item item{i, llte::now_ns()};
            while (!queue.try_push(item)) {
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    std::thread consumer([&] {
        llte::pin_to_cpu(config.consumer_cpu);
        Item item{};
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
    const double seconds = static_cast<double>(llte::now_ns() - start) / 1e9;

    Outcome outcome;
    outcome.throughput_msg_per_sec = static_cast<double>(config.messages) / seconds;
    outcome.transit = transit.summarize();
    return outcome;
}

void print_row(const std::string& label, const Outcome& outcome) {
    auto us = [](std::uint64_t ns) { return static_cast<double>(ns) / 1000.0; };
    std::printf("%-30s %12.2f %9.3f %9.3f %9.3f %9.3f\n", label.c_str(),
                outcome.throughput_msg_per_sec / 1e6, us(outcome.transit.p50),
                us(outcome.transit.p99), us(outcome.transit.p999), us(outcome.transit.max));
}

}  // namespace

int main(int argc, char** argv) {
    const llte::Args args(argc, argv);
    if (args.has("help")) {
        std::printf("usage: queue_benchmark [--messages N] [--producer-cpu N] [--consumer-cpu N]\n");
        return 0;
    }

    RunConfig config{};
    config.messages = static_cast<std::uint64_t>(args.integer("messages", 2'000'000));
    config.sample_capacity = static_cast<std::size_t>(config.messages);
    config.producer_cpu = static_cast<int>(args.integer("producer-cpu", -1));
    config.consumer_cpu = static_cast<int>(args.integer("consumer-cpu", -1));

    std::printf("queue_benchmark: %llu messages per variant",
                static_cast<unsigned long long>(config.messages));
    if (config.producer_cpu >= 0 || config.consumer_cpu >= 0) {
        std::printf(" (pinned: producer=%d consumer=%d)", config.producer_cpu,
                    config.consumer_cpu);
    }
    std::printf("\n\n");

    std::printf("%-30s %12s %9s %9s %9s %9s\n", "variant", "M msg/s", "p50 us", "p99 us",
                "p99.9 us", "max us");
    std::printf("%-30s %12s %9s %9s %9s %9s\n", "------------------------------", "------------",
                "---------", "---------", "---------", "---------");

    print_row("1. mutex + deque", run_mutex(config));
    print_row("2. lock-free, shared line", run_spinning<FalseSharedQueue>(config));
    print_row("3. lock-free, cache-aligned", run_spinning<AlignedQueue>(config));
    print_row("4. + cached indices (shipped)",
              run_spinning<llte::SpscQueue<Item, kCapacity>>(config));
    return 0;
}
