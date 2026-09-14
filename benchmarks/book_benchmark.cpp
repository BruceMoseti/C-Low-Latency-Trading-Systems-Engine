// Compares the preallocated book against a straightforward node-based one, and
// counts heap allocations to check the "no allocation on the hot path" claim
// rather than asserting it.

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <list>
#include <map>
#include <new>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "llte/args.hpp"
#include "llte/clock.hpp"
#include "llte/latency_stats.hpp"
#include "llte/order_book.hpp"

// GCC's allocation tracking does not model a *replaced* operator new. It sees
// container memory it believes came from the built-in operator new being released
// through the free() below and reports a mismatch. The pairing is correct -- every
// malloc here is matched by the corresponding free -- and the diagnostic only fires
// in sanitizer builds, where the extra inlining exposes the allocator internals.
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic ignored "-Wmismatched-new-delete"
#endif

namespace {
std::atomic<std::uint64_t> g_allocations{0};
}

void* operator new(std::size_t size) {
    g_allocations.fetch_add(1, std::memory_order_relaxed);
    void* memory = std::malloc(size);
    if (memory == nullptr) {
        throw std::bad_alloc();
    }
    return memory;
}

void operator delete(void* memory) noexcept { std::free(memory); }
void operator delete(void* memory, std::size_t) noexcept { std::free(memory); }

namespace {

using llte::OrderBook;
using llte::Price;
using llte::Side;

constexpr Price kMinPrice = 18250;
constexpr Price kMaxPrice = 19250;

enum class Action : std::uint8_t { Add, Cancel, Modify, Execute };

struct Operation {
    Action action;
    Side side;
    std::uint64_t order_id;
    Price price;
    std::uint32_t quantity;
};

// One shared workload so both implementations see identical order flow.
std::vector<Operation> build_workload(std::uint64_t count, std::uint64_t seed) {
    std::vector<Operation> operations;
    operations.reserve(count);

    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> offset(-40, 40);
    std::uniform_int_distribution<std::uint32_t> lot(1, 20);
    std::vector<std::uint64_t> live;
    live.reserve(count);
    std::uint64_t next_id = 1;

    for (std::uint64_t i = 0; i < count; ++i) {
        const int roll = static_cast<int>(rng() % 100);
        if (live.empty() || roll < 60) {
            const Side side = (rng() & 1) ? Side::Buy : Side::Sell;
            const std::uint64_t id = next_id++;
            operations.push_back({Action::Add, side, id,
                                  18750 + offset(rng) + (side == Side::Buy ? -2 : 2),
                                  lot(rng) * 10});
            live.push_back(id);
        } else {
            const std::size_t index = rng() % live.size();
            const std::uint64_t id = live[index];
            if (roll < 80) {
                operations.push_back({Action::Cancel, Side::Buy, id, 0, 0});
                live[index] = live.back();
                live.pop_back();
            } else if (roll < 92) {
                operations.push_back(
                    {Action::Modify, Side::Buy, id, 18750 + offset(rng), lot(rng) * 10});
            } else {
                operations.push_back({Action::Execute, Side::Buy, id, 0, lot(rng) * 5});
            }
        }
    }
    return operations;
}

// Baseline: the shape most people reach for first. Every level and every order
// is a separately allocated node.
//
// The index is reservable because leaving it unreserved conflates two different
// costs: node allocation, and one multi-millisecond rehash when the table grows.
// Comparing against the unreserved version would credit preallocation with a
// pause that a single reserve() call removes, so both are measured.
class NodeBook {
public:
    explicit NodeBook(std::size_t index_reserve) {
        if (index_reserve > 0) {
            index_.reserve(index_reserve);
        }
    }

    void add(std::uint64_t id, Side side, Price price, std::uint32_t quantity) {
        auto& levels = side == Side::Buy ? bids_ : asks_;
        auto& orders = levels[price];
        orders.push_back(Entry{id, quantity});
        index_[id] = Location{side, price, std::prev(orders.end())};
    }

    void cancel(std::uint64_t id) {
        const auto found = index_.find(id);
        if (found == index_.end()) {
            return;
        }
        auto& levels = found->second.side == Side::Buy ? bids_ : asks_;
        auto level = levels.find(found->second.price);
        if (level != levels.end()) {
            level->second.erase(found->second.position);
            if (level->second.empty()) {
                levels.erase(level);
            }
        }
        index_.erase(found);
    }

    void modify(std::uint64_t id, Price price, std::uint32_t quantity) {
        const auto found = index_.find(id);
        if (found == index_.end()) {
            return;
        }
        const Side side = found->second.side;
        cancel(id);
        add(id, side, price, quantity);
    }

    void execute(std::uint64_t id, std::uint32_t quantity) {
        const auto found = index_.find(id);
        if (found == index_.end()) {
            return;
        }
        if (found->second.position->quantity <= quantity) {
            cancel(id);
        } else {
            found->second.position->quantity -= quantity;
        }
    }

    Price best_bid() const { return bids_.empty() ? 0 : bids_.rbegin()->first; }
    Price best_ask() const { return asks_.empty() ? 0 : asks_.begin()->first; }

private:
    struct Entry {
        std::uint64_t id;
        std::uint32_t quantity;
    };
    struct Location {
        Side side;
        Price price;
        std::list<Entry>::iterator position;
    };

    std::map<Price, std::list<Entry>> bids_;
    std::map<Price, std::list<Entry>> asks_;
    std::unordered_map<std::uint64_t, Location> index_;
};

struct Result {
    double throughput_ops_per_sec = 0.0;
    std::uint64_t allocations = 0;
    llte::LatencySamples::Summary latency;
};

Result run_preallocated(const std::vector<Operation>& operations, std::size_t sample_capacity) {
    OrderBook book({kMinPrice, kMaxPrice, 1 << 20});
    llte::LatencySamples latency(sample_capacity);

    // Snapshot after construction: setup allocations are expected, steady-state
    // ones are what this benchmark is looking for.
    const std::uint64_t before = g_allocations.load(std::memory_order_relaxed);
    const std::uint64_t start = llte::now_ns();

    for (const Operation& operation : operations) {
        const std::uint64_t op_start = llte::now_ns();
        switch (operation.action) {
            case Action::Add:
                book.add(operation.order_id, operation.side, operation.price, operation.quantity);
                break;
            case Action::Cancel: book.cancel(operation.order_id); break;
            case Action::Modify:
                book.modify(operation.order_id, operation.price, operation.quantity);
                break;
            case Action::Execute: book.execute(operation.order_id, operation.quantity); break;
        }
        latency.add(llte::now_ns() - op_start);
    }

    const double seconds = static_cast<double>(llte::now_ns() - start) / 1e9;
    Result result;
    result.allocations = g_allocations.load(std::memory_order_relaxed) - before;
    result.throughput_ops_per_sec = static_cast<double>(operations.size()) / seconds;
    result.latency = latency.summarize();
    return result;
}

Result run_node_based(const std::vector<Operation>& operations, std::size_t sample_capacity,
                      std::size_t index_reserve) {
    NodeBook book(index_reserve);
    llte::LatencySamples latency(sample_capacity);

    const std::uint64_t before = g_allocations.load(std::memory_order_relaxed);
    const std::uint64_t start = llte::now_ns();

    for (const Operation& operation : operations) {
        const std::uint64_t op_start = llte::now_ns();
        switch (operation.action) {
            case Action::Add:
                book.add(operation.order_id, operation.side, operation.price, operation.quantity);
                break;
            case Action::Cancel: book.cancel(operation.order_id); break;
            case Action::Modify:
                book.modify(operation.order_id, operation.price, operation.quantity);
                break;
            case Action::Execute: book.execute(operation.order_id, operation.quantity); break;
        }
        latency.add(llte::now_ns() - op_start);
    }

    const double seconds = static_cast<double>(llte::now_ns() - start) / 1e9;
    Result result;
    result.allocations = g_allocations.load(std::memory_order_relaxed) - before;
    result.throughput_ops_per_sec = static_cast<double>(operations.size()) / seconds;
    result.latency = latency.summarize();
    return result;
}

// The allocator's worst case swings by orders of magnitude between runs, so a
// single `max` is not a measurement. Every row is repeated and summarised by its
// median.
Result median_of(std::vector<Result> results) {
    auto middle_by = [&results](auto projection) {
        std::vector<double> values;
        values.reserve(results.size());
        for (const Result& result : results) {
            values.push_back(static_cast<double>(projection(result)));
        }
        std::sort(values.begin(), values.end());
        return values[values.size() / 2];
    };

    Result summary;
    summary.throughput_ops_per_sec = middle_by([](const Result& r) {
        return r.throughput_ops_per_sec;
    });
    summary.allocations = static_cast<std::uint64_t>(
        middle_by([](const Result& r) { return r.allocations; }));
    summary.latency.p50 =
        static_cast<std::uint64_t>(middle_by([](const Result& r) { return r.latency.p50; }));
    summary.latency.p99 =
        static_cast<std::uint64_t>(middle_by([](const Result& r) { return r.latency.p99; }));
    summary.latency.p999 =
        static_cast<std::uint64_t>(middle_by([](const Result& r) { return r.latency.p999; }));
    summary.latency.max =
        static_cast<std::uint64_t>(middle_by([](const Result& r) { return r.latency.max; }));
    return summary;
}

void print_row(const std::string& label, const Result& result) {
    std::printf("%-34s %10.2f %12llu %8llu %8llu %9llu %11llu\n", label.c_str(),
                result.throughput_ops_per_sec / 1e6,
                static_cast<unsigned long long>(result.allocations),
                static_cast<unsigned long long>(result.latency.p50),
                static_cast<unsigned long long>(result.latency.p99),
                static_cast<unsigned long long>(result.latency.p999),
                static_cast<unsigned long long>(result.latency.max));
}

}  // namespace

int main(int argc, char** argv) {
    const llte::Args args(argc, argv);
    if (args.has("help")) {
        std::printf("usage: book_benchmark [--operations N] [--seed N] [--repeat N]\n");
        return 0;
    }

    const auto operation_count = static_cast<std::uint64_t>(args.integer("operations", 2'000'000));
    const auto seed = static_cast<std::uint64_t>(args.integer("seed", 11));

    const int repeat = std::max(1, static_cast<int>(args.integer("repeat", 5)));
    const std::vector<Operation> workload = build_workload(operation_count, seed);
    std::printf("book_benchmark: %zu operations, %d repeats, median reported\n\n",
                workload.size(), repeat);

    std::vector<Result> unreserved_runs, reserved_runs, preallocated_runs;
    for (int attempt = 0; attempt < repeat; ++attempt) {
        unreserved_runs.push_back(run_node_based(workload, workload.size(), 0));
        reserved_runs.push_back(run_node_based(workload, workload.size(), 1u << 20));
        preallocated_runs.push_back(run_preallocated(workload, workload.size()));
    }
    const Result unreserved = median_of(unreserved_runs);
    const Result reserved = median_of(reserved_runs);
    const Result preallocated = median_of(preallocated_runs);

    std::printf("%-34s %10s %12s %8s %8s %9s %11s\n", "implementation", "M ops/s",
                "heap allocs", "p50 ns", "p99 ns", "p99.9 ns", "max ns");
    std::printf("%-34s %10s %12s %8s %8s %9s %11s\n", "----------------------------------",
                "----------", "------------", "--------", "--------", "---------", "-----------");
    print_row("node-based, index unreserved", unreserved);
    print_row("node-based, index reserved", reserved);
    print_row("preallocated (shipped)", preallocated);

    std::printf(
        "\nThe unreserved row's worst case is one rehash of a growing unordered_map, not\n"
        "an allocation-per-operation cost: its allocation count is within a handful of\n"
        "the reserved row's. Compare the shipped book against the reserved baseline.\n");
    return 0;
}
