// Order-book engine: the single consumer of the shared-memory ring. It applies
// each sequenced event to a preallocated central limit order book and records
// per-stage latency for the whole pipeline.

#include <csignal>
#include <cstdio>
#include <ctime>
#include <string>

#include "llte/args.hpp"
#include "llte/clock.hpp"
#include "llte/cpu.hpp"
#include "llte/latency_stats.hpp"
#include "llte/order_book.hpp"
#include "llte/shared_memory_queue.hpp"

namespace {

volatile std::sig_atomic_t g_stop = 0;
void handle_signal(int) { g_stop = 1; }

struct ApplyCounters {
    std::uint64_t applied = 0;
    std::uint64_t unknown_order = 0;
    std::uint64_t duplicate_order = 0;
    std::uint64_t out_of_band = 0;
    std::uint64_t pool_exhausted = 0;
    std::uint64_t invalid = 0;
    std::uint64_t over_filled = 0;
    std::uint64_t unexpected_heartbeat = 0;
};

void record(ApplyCounters& counters, llte::OrderBook::Result result) {
    switch (result) {
        case llte::OrderBook::Result::Ok: counters.applied += 1; break;
        case llte::OrderBook::Result::UnknownOrder: counters.unknown_order += 1; break;
        case llte::OrderBook::Result::DuplicateOrder: counters.duplicate_order += 1; break;
        case llte::OrderBook::Result::PriceOutOfBand: counters.out_of_band += 1; break;
        case llte::OrderBook::Result::PoolExhausted: counters.pool_exhausted += 1; break;
        case llte::OrderBook::Result::InvalidQuantity: counters.invalid += 1; break;
        // A fill larger than the resting order means an Add or Modify was missed.
        // It is the clearest desync signal the feed offers, so it is counted rather
        // than absorbed as a clean full fill.
        case llte::OrderBook::Result::OverFilled: counters.over_filled += 1; break;
    }
}

}  // namespace

int main(int argc, char** argv) {
    const llte::Args args(argc, argv);
    if (args.has("help")) {
        std::printf(
            "usage: order_book_engine [--shm NAME] [--min-price N] [--max-price N]\n"
            "                         [--max-orders N] [--samples N] [--csv PATH]\n"
            "                         [--cpu N] [--attach-timeout-ms N]\n"
            "                         [--stall-timeout-ms N] [--quiet]\n");
        return 0;
    }

    const std::string shm_name = args.str("shm", "/llte_feed");
    const auto min_price = static_cast<llte::Price>(args.integer("min-price", 18250));
    const auto max_price = static_cast<llte::Price>(args.integer("max-price", 19250));
    const auto max_orders = static_cast<std::size_t>(args.integer("max-orders", 1 << 20));
    const auto sample_capacity = static_cast<std::size_t>(args.integer("samples", 2'000'000));
    const std::string csv_path = args.str("csv", "");
    const int cpu = static_cast<int>(args.integer("cpu", -1));
    const int attach_timeout_ms = static_cast<int>(args.integer("attach-timeout-ms", 10000));
    const int stall_timeout_ms = static_cast<int>(args.integer("stall-timeout-ms", 30000));
    const bool quiet = args.has("quiet");

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    llte::pin_to_cpu_or_warn(cpu, "engine");

    std::string error;
    llte::SharedMemoryQueue shm;
    if (!shm.attach(shm_name, attach_timeout_ms, error)) {
        std::fprintf(stderr, "engine: %s\n", error.c_str());
        return 1;
    }
    llte::ShmChannel* channel = shm.channel();

    llte::OrderBook book({min_price, max_price, max_orders});
    ApplyCounters counters;

    llte::LatencySamples wire_to_book(sample_capacity);
    llte::LatencySamples parse_stage(sample_capacity);
    llte::LatencySamples enqueue_stage(sample_capacity);
    llte::LatencySamples queue_stage(sample_capacity);
    llte::LatencySamples book_stage(sample_capacity);
    llte::LatencySamples handler_to_book(sample_capacity);

    std::FILE* csv = nullptr;
    if (!csv_path.empty()) {
        csv = std::fopen(csv_path.c_str(), "w");
        if (csv == nullptr) {
            std::fprintf(stderr, "engine: cannot write %s\n", csv_path.c_str());
            return 1;
        }
        std::fprintf(csv, "sequence,recovered,parse_ns,enqueue_ns,queue_ns,book_ns,handler_ns,wire_ns\n");
    }

    if (!quiet) {
        std::printf("engine: attached to %s, draining events\n", shm_name.c_str());
        std::fflush(stdout);
    }

    std::uint64_t consumed = 0;
    std::uint64_t recovered_seen = 0;
    std::uint64_t first_ns = 0;
    std::uint64_t last_ns = 0;
    bool stalled = false;
    llte::PipelineEvent event;

    // A producer that dies never sets producer_done, and the consumer would
    // otherwise spin on an empty ring forever at 100% of a core.
    const std::uint64_t stall_limit_ns =
        static_cast<std::uint64_t>(stall_timeout_ms) * 1'000'000ULL;
    std::uint64_t last_progress_ns = llte::now_ns();

    while (g_stop == 0) {
        if (!channel->queue.try_pop(event)) {
            if (channel->producer_done.load(std::memory_order_acquire) == 1 &&
                channel->queue.empty()) {
                break;
            }
            if (stall_limit_ns > 0 && llte::now_ns() - last_progress_ns > stall_limit_ns) {
                std::fprintf(stderr,
                             "engine: no event for %d ms and the producer never finished; "
                             "giving up after %llu events\n",
                             stall_timeout_ms, static_cast<unsigned long long>(consumed));
                stalled = true;
                break;
            }
            continue;
        }
        last_progress_ns = llte::now_ns();

        const std::uint64_t t3_dequeue = llte::now_ns();
        const llte::MarketMessage& message = event.msg;

        switch (message.type) {
            case llte::MessageType::Add:
                record(counters, book.add(message.order_id, message.side, message.price,
                                          message.quantity));
                break;
            case llte::MessageType::Cancel:
                record(counters, book.cancel(message.order_id));
                break;
            case llte::MessageType::Modify:
                record(counters, book.modify(message.order_id, message.price, message.quantity));
                break;
            case llte::MessageType::Trade:
                record(counters, book.execute(message.order_id, message.quantity));
                break;
            case llte::MessageType::Heartbeat:
                // The handler consumes these; one reaching the book would mean the
                // sequencer had published a non-event, so count it rather than
                // silently applying nothing.
                counters.unexpected_heartbeat += 1;
                break;
        }
        const std::uint64_t t4_book = llte::now_ns();

        parse_stage.add(event.t1_parsed - event.t0_recv);
        enqueue_stage.add(event.t2_enqueue - event.t1_parsed);
        queue_stage.add(t3_dequeue - event.t2_enqueue);
        book_stage.add(t4_book - t3_dequeue);
        handler_to_book.add(t4_book - event.t0_recv);
        if (message.timestamp_ns != 0 && t4_book > message.timestamp_ns) {
            wire_to_book.add(t4_book - message.timestamp_ns);
        }

        if (csv != nullptr) {
            std::fprintf(csv, "%llu,%u,%llu,%llu,%llu,%llu,%llu,%llu\n",
                         static_cast<unsigned long long>(message.sequence_number),
                         static_cast<unsigned>(event.recovered),
                         static_cast<unsigned long long>(event.t1_parsed - event.t0_recv),
                         static_cast<unsigned long long>(event.t2_enqueue - event.t1_parsed),
                         static_cast<unsigned long long>(t3_dequeue - event.t2_enqueue),
                         static_cast<unsigned long long>(t4_book - t3_dequeue),
                         static_cast<unsigned long long>(t4_book - event.t0_recv),
                         static_cast<unsigned long long>(
                             t4_book > message.timestamp_ns ? t4_book - message.timestamp_ns : 0));
        }

        consumed += 1;
        recovered_seen += event.recovered;
        if (first_ns == 0) {
            first_ns = t3_dequeue;
        }
        last_ns = t4_book;
    }

    if (csv != nullptr) {
        std::fclose(csv);
    }

    if (!quiet) {
        const double seconds =
            last_ns > first_ns ? static_cast<double>(last_ns - first_ns) / 1e9 : 0.0;
        std::printf("\nengine: consumed %llu events (%llu arrived via TCP recovery)\n",
                    static_cast<unsigned long long>(consumed),
                    static_cast<unsigned long long>(recovered_seen));
        std::printf("engine: applied=%llu unknown_order=%llu duplicate=%llu out_of_band=%llu\n",
                    static_cast<unsigned long long>(counters.applied),
                    static_cast<unsigned long long>(counters.unknown_order),
                    static_cast<unsigned long long>(counters.duplicate_order),
                    static_cast<unsigned long long>(counters.out_of_band));
        std::printf("engine: best_bid=%lld best_ask=%lld spread=%lld live_orders=%zu\n",
                    static_cast<long long>(book.best_bid()),
                    static_cast<long long>(book.best_ask()),
                    static_cast<long long>(book.best_ask() - book.best_bid()),
                    book.live_order_count());
        std::printf("engine: book_digest=%016llx\n",
                    static_cast<unsigned long long>(book.structural_digest()));
        std::printf("engine: over_filled=%llu unexpected_heartbeat=%llu\n",
                    static_cast<unsigned long long>(counters.over_filled),
                    static_cast<unsigned long long>(counters.unexpected_heartbeat));
        if (seconds > 0.0) {
            std::printf("engine: consumer throughput %.0f msg/s\n",
                        static_cast<double>(consumed) / seconds);
        }

        const std::uint64_t samples_dropped =
            parse_stage.dropped() + enqueue_stage.dropped() + queue_stage.dropped() +
            book_stage.dropped() + handler_to_book.dropped() + wire_to_book.dropped();
        if (samples_dropped > 0) {
            std::printf(
                "engine: WARNING %llu latency samples dropped; percentiles cover only\n"
                "        the first --samples events, biased toward the start of the run\n",
                static_cast<unsigned long long>(samples_dropped));
        }

        std::printf("\nPer-stage latency (microseconds)\n");
        llte::print_summary_header();
        llte::print_summary_row("udp recv -> parsed", parse_stage.summarize());
        llte::print_summary_row("parsed -> enqueue", enqueue_stage.summarize());
        llte::print_summary_row("ring transit", queue_stage.summarize());
        llte::print_summary_row("book update", book_stage.summarize());
        llte::print_summary_row("recv -> book (handler)", handler_to_book.summarize());
        llte::print_summary_row("exchange -> book (wire)", wire_to_book.summarize());
        std::fflush(stdout);
    }
    return stalled ? 1 : 0;
}
