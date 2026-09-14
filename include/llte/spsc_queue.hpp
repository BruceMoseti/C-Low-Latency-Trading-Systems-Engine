#pragma once

#include <atomic>
#include <bit>
#include <cstddef>
#include <type_traits>

namespace llte {

inline constexpr std::size_t kCacheLineBytes = 64;

// Lock-free ring buffer for exactly one producer thread and one consumer thread.
//
// Ownership is the whole correctness argument: `write_idx_` is written only by the
// producer and `read_idx_` only by the consumer. Two producers sharing one instance
// is undefined behaviour, not a slow path -- see tools/spsc_race_demo.cpp.
//
// Layout is fixed and trivially constructible so the queue can be placement-new'd
// directly into a shared-memory mapping and used across process boundaries.
template <typename T, std::size_t Capacity>
class SpscQueue {
    static_assert(std::has_single_bit(Capacity), "Capacity must be a power of two");
    static_assert(std::is_trivially_copyable_v<T>, "shared-memory payloads must be POD");

public:
    SpscQueue() = default;
    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    bool try_push(const T& value) {
        const std::size_t write = write_idx_.load(std::memory_order_relaxed);
        // Re-reading the consumer's index costs a cross-core cache miss, so only do
        // it when the producer's cached view says the ring is full.
        if (write - cached_read_idx_ >= Capacity) {
            cached_read_idx_ = read_idx_.load(std::memory_order_acquire);
            if (write - cached_read_idx_ >= Capacity) {
                return false;
            }
        }
        buffer_[write & kMask] = value;
        // Release pairs with the consumer's acquire: the slot write above is visible
        // before the consumer can observe the published index.
        write_idx_.store(write + 1, std::memory_order_release);
        return true;
    }

    bool try_pop(T& out) {
        const std::size_t read = read_idx_.load(std::memory_order_relaxed);
        if (read == cached_write_idx_) {
            cached_write_idx_ = write_idx_.load(std::memory_order_acquire);
            if (read == cached_write_idx_) {
                return false;
            }
        }
        out = buffer_[read & kMask];
        read_idx_.store(read + 1, std::memory_order_release);
        return true;
    }

    // Both of these read the two indexes without a snapshot, so they are only
    // exact when called from a thread that knows the other side is not moving --
    // the consumer after the producer has finished, or either side in a test.
    // Concurrently they report a value that was true at some point in between.
    bool empty() const {
        return read_idx_.load(std::memory_order_acquire) ==
               write_idx_.load(std::memory_order_acquire);
    }

    std::size_t size() const {
        const std::size_t write = write_idx_.load(std::memory_order_acquire);
        const std::size_t read = read_idx_.load(std::memory_order_acquire);
        // The consumer can overtake the stale `write` snapshot between the two
        // loads, and the unsigned subtraction would then wrap to ~2^64.
        return write > read ? write - read : 0;
    }

    static constexpr std::size_t capacity() { return Capacity; }

private:
    static constexpr std::size_t kMask = Capacity - 1;

    // The producer's hot words share one line; the consumer's share another. Without
    // this split the two cores would invalidate each other's line on every operation
    // even though they never touch the same variable.
    alignas(kCacheLineBytes) std::atomic<std::size_t> write_idx_{0};
    std::size_t cached_read_idx_{0};

    alignas(kCacheLineBytes) std::atomic<std::size_t> read_idx_{0};
    std::size_t cached_write_idx_{0};

    alignas(kCacheLineBytes) T buffer_[Capacity]{};
};

}  // namespace llte
