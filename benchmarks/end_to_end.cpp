// Runs the whole software path in one process -- encode, decode, sequence, ring
// handoff, book update -- with the kernel network stack taken out of the picture.
// The three-process run measures the real system; this isolates the part of the
// latency the code itself is responsible for.

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdint>
#include <cstring>
#include <memory>
#include <random>
#include <thread>
#include <vector>

#include "llte/args.hpp"
#include "llte/clock.hpp"
#include "llte/cpu.hpp"
#include "llte/latency_stats.hpp"
#include "llte/order_book.hpp"
#include "llte/pipeline.hpp"
#include "llte/protocol.hpp"
#include "llte/sequence_manager.hpp"
#include "llte/spsc_queue.hpp"

namespace {

constexpr llte::Price kMinPrice = 18250;
constexpr llte::Price kMaxPrice = 19250;
constexpr std::size_t kQueueCapacity = 1 << 16;

using Ring = llte::SpscQueue<llte::PipelineEvent, kQueueCapacity>;

std::vector<llte::MarketMessage> build_stream(std::uint64_t count, std::uint64_t seed) {
    std::vector<llte::MarketMessage> stream;
    stream.reserve(count);

    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int> offset(-40, 40);
    std::uniform_int_distribution<std::uint32_t> lot(1, 20);
    std::vector<std::uint64_t> live;
    live.reserve(count);
    std::uint64_t next_order_id = 1;
    std::uint64_t sequence = 1001;

    for (std::uint64_t i = 0; i < count; ++i) {
        llte::MarketMessage message{};
        message.sequence_number = sequence++;
        message.symbol_id = 1;

        const int roll = static_cast<int>(rng() % 100);
        if (live.empty() || roll < 60) {
            const llte::Side side = (rng() & 1) ? llte::Side::Buy : llte::Side::Sell;
            message.type = llte::MessageType::Add;
            message.order_id = next_order_id++;
            message.side = side;
            message.price = 18750 + offset(rng) + (side == llte::Side::Buy ? -2 : 2);
            message.quantity = lot(rng) * 10;
            live.push_back(message.order_id);
        } else {
            const std::size_t index = rng() % live.size();
            message.order_id = live[index];
            if (roll < 80) {
                message.type = llte::MessageType::Cancel;
                live[index] = live.back();
                live.pop_back();
            } else if (roll < 92) {
                message.type = llte::MessageType::Modify;
                message.price = 18750 + offset(rng);
                message.quantity = lot(rng) * 10;
            } else {
                message.type = llte::MessageType::Trade;
                message.quantity = lot(rng) * 5;
            }
        }
        stream.push_back(message);
    }
    return stream;
}

}  // namespace

int main(int argc, char** argv) {
    const llte::Args args(argc, argv);
    if (args.has("help")) {
        std::printf(
            "usage: end_to_end [--messages N] [--batch N] [--seed N]\n"
            "                  [--producer-cpu N] [--consumer-cpu N]\n");
        return 0;
    }

    const auto message_count = static_cast<std::uint64_t>(args.integer("messages", 2'000'000));
    auto batch = static_cast<std::uint16_t>(args.integer("batch", 1));
    const auto seed = static_cast<std::uint64_t>(args.integer("seed", 5));
    const int producer_cpu = static_cast<int>(args.integer("producer-cpu", -1));
    const int consumer_cpu = static_cast<int>(args.integer("consumer-cpu", -1));
    // Offered load. Running flat out fills the ring and turns every stage
    // measurement into queueing delay, so the default paces below saturation and
    // --rate 0 is reserved for measuring maximum throughput.
    const auto rate = static_cast<std::uint64_t>(args.integer("rate", 1'000'000));
    if (batch < 1) batch = 1;
    if (batch > llte::kMaxBatch) batch = llte::kMaxBatch;

    const std::vector<llte::MarketMessage> stream = build_stream(message_count, seed);
    std::printf("end_to_end: %zu messages, batch=%u, offered load=%s\n", stream.size(), batch,
                rate == 0 ? "unthrottled" : (std::to_string(rate) + " msg/s").c_str());

    // Heap, not stack: at 128 bytes per slot the ring is 8 MB, well past the
    // default stack limit. In the real pipeline it lives in a shared mapping.
    const auto ring_storage = std::make_unique<Ring>();
    Ring& ring = *ring_storage;
    std::atomic<bool> producer_done{false};
    // Both threads allocate sizeable state before their first message. Without a
    // barrier the consumer is still building its book while the producer is
    // already publishing, and that startup gap shows up as a multi-millisecond
    // tail that has nothing to do with steady-state behaviour.
    std::atomic<bool> consumer_ready{false};

    llte::LatencySamples decode_stage(message_count);
    llte::LatencySamples sequence_stage(message_count);
    llte::LatencySamples ring_stage(message_count);
    llte::LatencySamples book_stage(message_count);
    llte::LatencySamples total_stage(message_count);
    std::uint64_t consumed = 0;

    std::atomic<std::uint64_t> measured_start_ns{0};

    std::thread producer([&] {
        llte::require_pinned(producer_cpu, "producer");
        llte::SequenceManager sequencer(1 << 16, nullptr);
        while (!consumer_ready.load(std::memory_order_acquire)) {
        }
        const std::uint64_t start_ns = llte::now_ns();
        measured_start_ns.store(start_ns, std::memory_order_release);
        alignas(8) unsigned char packet[llte::kMaxPacketBytes];
        auto* header = reinterpret_cast<llte::FeedPacketHeader*>(packet);
        auto* payload =
            reinterpret_cast<llte::MarketMessage*>(packet + sizeof(llte::FeedPacketHeader));

        const std::uint64_t interval_ns = rate > 0 ? 1'000'000'000ULL / rate : 0;
        std::size_t position = 0;
        while (position < stream.size()) {
            if (interval_ns > 0) {
                // Spin rather than sleep: a nanosleep would overshoot these gaps.
                const std::uint64_t deadline = start_ns + position * interval_ns;
                while (llte::now_ns() < deadline) {
                }
            }
            const std::uint16_t count = static_cast<std::uint16_t>(
                std::min<std::size_t>(batch, stream.size() - position));
            header->magic = llte::kFeedMagic;
            header->count = count;
            header->reserved = 0;
            for (std::uint16_t i = 0; i < count; ++i) {
                payload[i] = stream[position + i];
                payload[i].timestamp_ns = llte::now_ns();
            }
            position += count;

            // Stands in for the instant recvfrom() would have returned.
            const std::uint64_t t0_recv = llte::now_ns();
            if (header->magic != llte::kFeedMagic || header->count == 0 ||
                header->count > llte::kMaxBatch) {
                continue;
            }
            const std::uint64_t t1_parsed = llte::now_ns();

            for (std::uint16_t i = 0; i < count; ++i) {
                sequencer.accept(llte::FeedMessage{payload[i], t0_recv, t1_parsed, false});
            }

            llte::FeedMessage deliverable;
            while (sequencer.next_deliverable(deliverable)) {
                llte::PipelineEvent event{};
                event.msg = deliverable.message;
                event.t0_recv = deliverable.t0_recv;
                event.t1_parsed = deliverable.t1_parsed;
                event.recovered = 0;
                event.t2_enqueue = llte::now_ns();
                while (!ring.try_push(event)) {
                    event.t2_enqueue = llte::now_ns();
                }
            }
        }
        producer_done.store(true, std::memory_order_release);
    });

    std::thread consumer([&] {
        llte::require_pinned(consumer_cpu, "consumer");
        llte::OrderBook book({kMinPrice, kMaxPrice, 1 << 20});
        consumer_ready.store(true, std::memory_order_release);
        llte::PipelineEvent event;
        for (;;) {
            if (!ring.try_pop(event)) {
                if (producer_done.load(std::memory_order_acquire) && ring.empty()) {
                    return;
                }
                continue;
            }
            const std::uint64_t t3_dequeue = llte::now_ns();
            const llte::MarketMessage& message = event.msg;
            switch (message.type) {
                case llte::MessageType::Add:
                    book.add(message.order_id, message.side, message.price, message.quantity);
                    break;
                case llte::MessageType::Cancel: book.cancel(message.order_id); break;
                case llte::MessageType::Modify:
                    book.modify(message.order_id, message.price, message.quantity);
                    break;
                case llte::MessageType::Trade:
                    book.execute(message.order_id, message.quantity);
                    break;
                case llte::MessageType::Heartbeat:
                    break;  // build_stream never emits these
            }
            const std::uint64_t t4_book = llte::now_ns();

            decode_stage.add(event.t1_parsed - event.t0_recv);
            sequence_stage.add(event.t2_enqueue - event.t1_parsed);
            ring_stage.add(t3_dequeue - event.t2_enqueue);
            book_stage.add(t4_book - t3_dequeue);
            total_stage.add(t4_book - event.t0_recv);
            consumed += 1;
        }
    });

    producer.join();
    consumer.join();

    const double seconds =
        static_cast<double>(llte::now_ns() - measured_start_ns.load(std::memory_order_acquire)) /
        1e9;
    std::printf("end_to_end: consumed %llu messages in %.3f s (%.2f M msg/s)\n\n",
                static_cast<unsigned long long>(consumed), seconds,
                static_cast<double>(consumed) / seconds / 1e6);

    llte::print_summary_header();
    llte::print_summary_row("decode", decode_stage.summarize());
    llte::print_summary_row("sequence -> enqueue", sequence_stage.summarize());
    llte::print_summary_row("ring transit", ring_stage.summarize());
    llte::print_summary_row("book update", book_stage.summarize());
    llte::print_summary_row("end to end", total_stage.summarize());
    return 0;
}
