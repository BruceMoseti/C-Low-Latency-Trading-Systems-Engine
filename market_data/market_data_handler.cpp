// Market-data handler: joins the multicast feed, validates and sequences it,
// recovers gaps over TCP, and publishes an in-order stream into the shared-memory
// ring buffer consumed by the order-book process.
//
// This process is the single producer for that ring. The multicast path and the
// recovery path both feed the SequenceManager, and only the drain loop below
// writes to the queue.

#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>

#include "llte/args.hpp"
#include "llte/clock.hpp"
#include "llte/cpu.hpp"
#include "llte/multicast_receiver.hpp"
#include "llte/protocol.hpp"
#include "llte/recovery_client.hpp"
#include "llte/sequence_manager.hpp"
#include "llte/shared_memory_queue.hpp"

namespace {

volatile std::sig_atomic_t g_stop = 0;
void handle_signal(int) { g_stop = 1; }

}  // namespace

int main(int argc, char** argv) {
    const llte::Args args(argc, argv);
    if (args.has("help")) {
        std::printf(
            "usage: market_data_handler [--group IP] [--port N] [--interface IP]\n"
            "                           [--recovery-host IP] [--recovery-port N]\n"
            "                           [--shm NAME] [--expect N] [--idle-ms N]\n"
            "                           [--startup-timeout-ms N] [--reorder N] [--cpu N]\n"
            "                           [--no-recovery] [--quiet]\n");
        return 0;
    }

    const std::string group = args.str("group", llte::kDefaultMulticastGroup);
    const auto port = static_cast<std::uint16_t>(args.integer("port", llte::kDefaultMulticastPort));
    const std::string interface_ip = args.str("interface", llte::kDefaultInterface);
    const std::string recovery_host = args.str("recovery-host", "127.0.0.1");
    const auto recovery_port =
        static_cast<std::uint16_t>(args.integer("recovery-port", llte::kDefaultRecoveryPort));
    const std::string shm_name = args.str("shm", "/llte_feed");
    const auto expect = static_cast<std::uint64_t>(args.integer("expect", 0));
    const int idle_ms = static_cast<int>(args.integer("idle-ms", 1500));
    // Without this a handler pointed at a silent group waits forever, which looks
    // identical to a handler that is working but has nothing to do yet.
    const int startup_timeout_ms = static_cast<int>(args.integer("startup-timeout-ms", 15000));
    const auto reorder = static_cast<std::size_t>(args.integer("reorder", 1 << 16));
    const int cpu = static_cast<int>(args.integer("cpu", -1));
    const bool use_recovery = !args.has("no-recovery");
    const bool quiet = args.has("quiet");

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    llte::pin_to_cpu(cpu);

    std::string error;

    llte::SharedMemoryQueue shm;
    if (!shm.create(shm_name, error)) {
        std::fprintf(stderr, "handler: shared memory failed: %s\n", error.c_str());
        return 1;
    }
    llte::ShmChannel* channel = shm.channel();

    llte::RecoveryClient recovery;
    if (use_recovery && !recovery.connect(recovery_host, recovery_port, 5000, error)) {
        std::fprintf(stderr, "handler: recovery connect failed: %s\n", error.c_str());
        return 1;
    }
    llte::SequenceManager sequencer(reorder, use_recovery ? &recovery : nullptr);

    llte::MulticastReceiver receiver;
    if (!receiver.open(group, port, interface_ip, error)) {
        std::fprintf(stderr, "handler: receiver failed: %s\n", error.c_str());
        return 1;
    }
    // A short timeout keeps the loop responsive to shutdown and lets the idle
    // detector notice when the feed has gone quiet.
    if (!receiver.set_receive_timeout(50, error)) {
        std::fprintf(stderr, "handler: %s\n", error.c_str());
        return 1;
    }

    if (!quiet) {
        std::printf("handler: listening on %s:%u, publishing to shm %s\n", group.c_str(), port,
                    shm_name.c_str());
        std::fflush(stdout);
    }

    alignas(8) unsigned char packet[llte::kMaxPacketBytes];
    std::uint64_t published = 0;
    std::uint64_t queue_full_spins = 0;
    std::uint64_t malformed_packets = 0;
    std::uint64_t packets_received = 0;
    std::uint64_t first_message_ns = 0;
    std::uint64_t last_message_ns = 0;
    int idle_elapsed_ms = 0;
    bool startup_timed_out = false;

    while (g_stop == 0) {
        const long received = receiver.receive(packet, sizeof(packet));
        if (received < 0) {
            std::fprintf(stderr, "handler: recv error\n");
            break;
        }
        if (received == 0) {
            idle_elapsed_ms += 50;
            // Before the feed opens the countdown is a startup timeout; afterwards
            // it detects a feed that has gone quiet.
            if (first_message_ns == 0) {
                if (idle_elapsed_ms >= startup_timeout_ms) {
                    std::fprintf(stderr,
                                 "handler: no message on %s:%u within %d ms; giving up\n",
                                 group.c_str(), port, startup_timeout_ms);
                    startup_timed_out = true;
                    break;
                }
            } else if (idle_elapsed_ms >= idle_ms) {
                break;
            }
            continue;
        }
        const std::uint64_t t0_recv = llte::now_ns();
        idle_elapsed_ms = 0;
        packets_received += 1;

        if (static_cast<std::size_t>(received) < sizeof(llte::FeedPacketHeader)) {
            malformed_packets += 1;
            continue;
        }
        const auto* header = reinterpret_cast<const llte::FeedPacketHeader*>(packet);
        const std::size_t expected_bytes =
            sizeof(llte::FeedPacketHeader) + header->count * sizeof(llte::MarketMessage);
        if (header->magic != llte::kFeedMagic || header->count == 0 ||
            header->count > llte::kMaxBatch ||
            static_cast<std::size_t>(received) != expected_bytes) {
            malformed_packets += 1;
            continue;
        }

        const auto* payload = reinterpret_cast<const llte::MarketMessage*>(
            packet + sizeof(llte::FeedPacketHeader));
        const std::uint64_t t1_parsed = llte::now_ns();
        for (std::uint16_t i = 0; i < header->count; ++i) {
            sequencer.accept(llte::FeedMessage{payload[i], t0_recv, t1_parsed, false});
        }

        llte::FeedMessage deliverable;
        while (sequencer.next_deliverable(deliverable)) {
            llte::PipelineEvent event{};
            event.msg = deliverable.message;
            event.t0_recv = deliverable.t0_recv;
            event.t1_parsed = deliverable.t1_parsed;
            event.recovered = deliverable.recovered ? 1 : 0;
            event.t2_enqueue = llte::now_ns();

            while (!channel->queue.try_push(event) && g_stop == 0) {
                queue_full_spins += 1;
                event.t2_enqueue = llte::now_ns();
            }
            published += 1;
            if (first_message_ns == 0) {
                first_message_ns = event.t2_enqueue;
            }
            last_message_ns = event.t2_enqueue;
        }

        channel->produced_count.store(published, std::memory_order_relaxed);
        if (expect > 0 && published >= expect) {
            break;
        }
    }

    channel->produced_count.store(published, std::memory_order_release);
    channel->producer_done.store(1, std::memory_order_release);

    if (!quiet) {
        const auto& stats = sequencer.stats();
        const double seconds =
            last_message_ns > first_message_ns
                ? static_cast<double>(last_message_ns - first_message_ns) / 1e9
                : 0.0;
        std::printf("handler: packets=%llu published=%llu malformed=%llu queue_full_spins=%llu\n",
                    static_cast<unsigned long long>(packets_received),
                    static_cast<unsigned long long>(published),
                    static_cast<unsigned long long>(malformed_packets),
                    static_cast<unsigned long long>(queue_full_spins));
        std::printf(
            "handler: gaps=%llu missing=%llu recovery_requests=%llu recovered=%llu "
            "unrecoverable=%llu duplicates=%llu\n",
            static_cast<unsigned long long>(stats.gaps_detected),
            static_cast<unsigned long long>(stats.missing_messages),
            static_cast<unsigned long long>(stats.recovery_requests),
            static_cast<unsigned long long>(stats.messages_recovered),
            static_cast<unsigned long long>(stats.unrecoverable),
            static_cast<unsigned long long>(stats.duplicates));
        if (seconds > 0.0) {
            std::printf("handler: throughput %.0f msg/s\n",
                        static_cast<double>(published) / seconds);
        }
        std::fflush(stdout);
    }

    // The consumer maps the same segment; give it a moment to drain before the
    // mapping is torn down and unlinked.
    for (int waited = 0; waited < 5000 && !channel->queue.empty(); waited += 10) {
        timespec ts{0, 10'000'000L};
        ::nanosleep(&ts, nullptr);
    }
    return startup_timed_out ? 1 : 0;
}
