#include "llte/spsc_queue.hpp"

#include <atomic>
#include <cstddef>
#include <thread>
#include <vector>

#include "test_support.hpp"

namespace {

struct Payload {
    std::uint64_t value;
    std::uint64_t checksum;
};

void test_fifo_and_capacity() {
    llte::SpscQueue<Payload, 8> queue;
    CHECK(queue.empty());
    CHECK_EQ(queue.size(), 0u);

    for (std::uint64_t i = 0; i < 8; ++i) {
        CHECK(queue.try_push(Payload{i, ~i}));
    }
    // The ring holds exactly Capacity entries, no more.
    CHECK(!queue.try_push(Payload{99, 0}));
    CHECK_EQ(queue.size(), 8u);

    Payload out{};
    for (std::uint64_t i = 0; i < 8; ++i) {
        CHECK(queue.try_pop(out));
        CHECK_EQ(out.value, i);
        CHECK_EQ(out.checksum, ~i);
    }
    CHECK(!queue.try_pop(out));
    CHECK(queue.empty());
}

void test_wraparound() {
    llte::SpscQueue<Payload, 4> queue;
    Payload out{};
    // Push/pop far past the capacity so the index wrap is exercised repeatedly.
    for (std::uint64_t round = 0; round < 1000; ++round) {
        CHECK(queue.try_push(Payload{round, round * 3}));
        CHECK(queue.try_pop(out));
        CHECK_EQ(out.value, round);
        CHECK_EQ(out.checksum, round * 3);
    }
    CHECK(queue.empty());
}

// The producer and consumer indexes must not share a cache line, otherwise the
// two cores invalidate each other's line on every single operation.
void test_indexes_are_on_separate_cache_lines() {
    llte::SpscQueue<Payload, 16> queue;
    const auto base = reinterpret_cast<std::uintptr_t>(&queue);
    CHECK_EQ(base % llte::kCacheLineBytes, 0u);
    CHECK(sizeof(queue) >= 2 * llte::kCacheLineBytes + 16 * sizeof(Payload));
    CHECK_EQ(alignof(llte::SpscQueue<Payload, 16>), llte::kCacheLineBytes);
}

// One producer, one consumer, checked payloads: anything torn or reordered shows
// up as a checksum mismatch or an out-of-order value.
void test_single_producer_single_consumer_stress() {
    constexpr std::uint64_t kMessages = 2'000'000;
    llte::SpscQueue<Payload, 1024> queue;
    std::atomic<bool> producer_done{false};
    std::uint64_t received = 0;
    std::uint64_t order_errors = 0;
    std::uint64_t checksum_errors = 0;

    std::thread producer([&] {
        for (std::uint64_t i = 0; i < kMessages; ++i) {
            const Payload item{i, i * 2654435761ULL};
            while (!queue.try_push(item)) {
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    std::thread consumer([&] {
        Payload item{};
        std::uint64_t expected = 0;
        for (;;) {
            if (queue.try_pop(item)) {
                if (item.value != expected) {
                    order_errors += 1;
                }
                if (item.checksum != item.value * 2654435761ULL) {
                    checksum_errors += 1;
                }
                expected = item.value + 1;
                received += 1;
            } else if (producer_done.load(std::memory_order_acquire) && queue.empty()) {
                return;
            }
        }
    });

    producer.join();
    consumer.join();

    CHECK_EQ(received, kMessages);
    CHECK_EQ(order_errors, 0u);
    CHECK_EQ(checksum_errors, 0u);
}

}  // namespace

int main() {
    test_fifo_and_capacity();
    test_wraparound();
    test_indexes_are_on_separate_cache_lines();
    test_single_producer_single_consumer_stress();
    return llte::test::report("test_queue");
}
