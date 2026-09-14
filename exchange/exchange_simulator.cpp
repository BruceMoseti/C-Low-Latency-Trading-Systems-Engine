// Exchange simulator: generates order flow, maintains the authoritative book,
// stamps every event with a sequence number, publishes over UDP multicast and
// answers retransmission requests over TCP.

#include <csignal>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <random>
#include <string>
#include <vector>

#include "llte/args.hpp"
#include "llte/clock.hpp"
#include "llte/cpu.hpp"
#include "llte/multicast_publisher.hpp"
#include "llte/order_book.hpp"
#include "llte/protocol.hpp"
#include "llte/recovery_server.hpp"

namespace {

volatile std::sig_atomic_t g_stop = 0;
void handle_signal(int) { g_stop = 1; }

constexpr llte::Price kMinPrice = 18250;  // $182.50
constexpr llte::Price kMaxPrice = 19250;  // $192.50
constexpr llte::Price kMidPrice = 18750;  // $187.50

struct LiveOrder {
    std::uint64_t id;
    llte::Side side;
    llte::Price price;
    std::uint32_t quantity;
};

void sleep_ms(int milliseconds) {
    timespec ts{milliseconds / 1000, static_cast<long>(milliseconds % 1000) * 1'000'000L};
    ::nanosleep(&ts, nullptr);
}

void busy_wait_until(std::uint64_t deadline_ns) {
    while (llte::now_ns() < deadline_ns) {
        // Spin: at these rates a nanosleep would overshoot by more than the gap.
    }
}

}  // namespace

int main(int argc, char** argv) {
    const llte::Args args(argc, argv);
    if (args.has("help")) {
        std::printf(
            "usage: exchange_simulator [--group IP] [--port N] [--recovery-port N]\n"
            "                          [--interface IP] [--messages N] [--rate PER_SEC]\n"
            "                          [--batch N] [--drop-rate F] [--seed N]\n"
            "                          [--history N] [--start-delay-ms N] [--linger-ms N]\n"
            "                          [--cpu N] [--quiet]\n");
        return 0;
    }

    const std::string group = args.str("group", llte::kDefaultMulticastGroup);
    const auto port = static_cast<std::uint16_t>(args.integer("port", llte::kDefaultMulticastPort));
    const auto recovery_port =
        static_cast<std::uint16_t>(args.integer("recovery-port", llte::kDefaultRecoveryPort));
    const std::string interface_ip = args.str("interface", llte::kDefaultInterface);
    const auto total_messages = static_cast<std::uint64_t>(args.integer("messages", 200000));
    const auto rate_per_second = static_cast<std::uint64_t>(args.integer("rate", 0));
    auto batch = static_cast<std::uint16_t>(args.integer("batch", 1));
    const double drop_rate = args.real("drop-rate", 0.0);
    const auto seed = static_cast<std::uint64_t>(args.integer("seed", 42));
    const auto history_capacity = static_cast<std::size_t>(args.integer("history", 1 << 20));
    const int start_delay_ms = static_cast<int>(args.integer("start-delay-ms", 0));
    const int linger_ms = static_cast<int>(args.integer("linger-ms", 2000));
    const int cpu = static_cast<int>(args.integer("cpu", -1));
    const bool quiet = args.has("quiet");

    if (batch < 1) batch = 1;
    if (batch > llte::kMaxBatch) batch = llte::kMaxBatch;

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);
    llte::pin_to_cpu(cpu);

    llte::HistoryStore history(history_capacity);
    llte::RecoveryServer recovery_server(history);
    std::string error;
    if (!recovery_server.start(recovery_port, error)) {
        std::fprintf(stderr, "exchange: recovery server failed: %s\n", error.c_str());
        return 1;
    }

    llte::MulticastPublisher publisher;
    if (!publisher.open(group, port, interface_ip, error)) {
        std::fprintf(stderr, "exchange: publisher failed: %s\n", error.c_str());
        return 1;
    }

    llte::OrderBook book({kMinPrice, kMaxPrice, 1 << 20});
    std::vector<LiveOrder> live;
    live.reserve(1 << 20);

    std::mt19937_64 rng(seed);
    std::uniform_real_distribution<double> unit(0.0, 1.0);
    std::uniform_int_distribution<int> depth(1, 40);
    std::uniform_int_distribution<std::uint32_t> lot(1, 20);

    // Bids rest below the mid and asks above it. A real venue never displays a
    // crossed book, because anything that would cross trades instead.
    auto price_for = [&](llte::Side side) {
        const int distance = depth(rng);
        return side == llte::Side::Buy ? kMidPrice - distance : kMidPrice + distance;
    };

    alignas(8) unsigned char packet[llte::kMaxPacketBytes];
    auto* header = reinterpret_cast<llte::FeedPacketHeader*>(packet);
    auto* payload = reinterpret_cast<llte::MarketMessage*>(packet + sizeof(llte::FeedPacketHeader));

    std::uint64_t sequence = 1001;  // exchanges start mid-stream; so do we
    std::uint64_t next_order_id = 1;
    std::uint64_t packets_sent = 0;
    std::uint64_t packets_dropped = 0;
    std::uint64_t messages_dropped = 0;
    std::uint16_t pending = 0;

    const std::uint64_t interval_ns = rate_per_second > 0 ? 1'000'000'000ULL / rate_per_second : 0;

    if (!quiet) {
        std::printf("exchange: publishing %llu messages to %s:%u (batch=%u, drop-rate=%.4f)\n",
                    static_cast<unsigned long long>(total_messages), group.c_str(), port, batch,
                    drop_rate);
        std::printf("exchange: recovery server listening on port %u\n", recovery_port);
        std::fflush(stdout);
    }

    // Give subscribers a chance to join the group before the feed opens, so a
    // demo run can account for every published message. Timing starts after the
    // delay so it measures publishing, not waiting.
    sleep_ms(start_delay_ms);
    const std::uint64_t start_ns = llte::now_ns();

    auto flush_packet = [&]() {
        if (pending == 0) {
            return true;
        }
        header->magic = llte::kFeedMagic;
        header->count = pending;
        header->reserved = 0;
        const std::size_t bytes = sizeof(llte::FeedPacketHeader) + pending * sizeof(llte::MarketMessage);

        // Simulated loss: the datagram is built and retained in history but never
        // put on the wire, which is exactly what the receiver must cope with.
        const bool drop = drop_rate > 0.0 && unit(rng) < drop_rate;
        if (drop) {
            packets_dropped += 1;
            messages_dropped += pending;
        } else {
            std::string send_error;
            if (!publisher.send(packet, bytes, send_error)) {
                std::fprintf(stderr, "exchange: send failed: %s\n", send_error.c_str());
                return false;
            }
            packets_sent += 1;
        }
        pending = 0;
        return true;
    };

    for (std::uint64_t published = 0; published < total_messages && g_stop == 0; ++published) {
        llte::MarketMessage message{};
        message.sequence_number = sequence++;
        message.symbol_id = 1;

        const double roll = unit(rng);
        const bool can_amend = !live.empty();

        if (!can_amend || roll < 0.60) {
            const llte::Side side = unit(rng) < 0.5 ? llte::Side::Buy : llte::Side::Sell;
            const llte::Price price = price_for(side);
            const std::uint32_t quantity = lot(rng) * 10;
            message.type = llte::MessageType::Add;
            message.order_id = next_order_id++;
            message.side = side;
            message.price = price;
            message.quantity = quantity;
            if (book.add(message.order_id, side, price, quantity) == llte::OrderBook::Result::Ok) {
                live.push_back({message.order_id, side, price, quantity});
            }
        } else {
            std::uniform_int_distribution<std::size_t> pick(0, live.size() - 1);
            const std::size_t index = pick(rng);
            LiveOrder& target = live[index];
            message.order_id = target.id;
            message.side = target.side;

            if (roll < 0.75) {
                message.type = llte::MessageType::Cancel;
                message.price = target.price;
                message.quantity = target.quantity;
                book.cancel(target.id);
                target = live.back();
                live.pop_back();
            } else if (roll < 0.90) {
                const llte::Price price = price_for(target.side);
                const std::uint32_t quantity = lot(rng) * 10;
                message.type = llte::MessageType::Modify;
                message.price = price;
                message.quantity = quantity;
                if (book.modify(target.id, price, quantity) == llte::OrderBook::Result::Ok) {
                    target.price = price;
                    target.quantity = quantity;
                }
            } else {
                const std::uint32_t fill =
                    target.quantity > 10 ? target.quantity / 2 : target.quantity;
                message.type = llte::MessageType::Trade;
                message.price = target.price;
                message.quantity = fill;
                book.execute(target.id, fill);
                if (fill >= target.quantity) {
                    target = live.back();
                    live.pop_back();
                } else {
                    target.quantity -= fill;
                }
            }
        }

        // Stamp before retaining: history must hold exactly what went on the
        // wire, so a retransmission still carries its original publish time and
        // the recovery round trip shows up in end-to-end latency.
        message.timestamp_ns = llte::now_ns();
        history.append(message);
        payload[pending++] = message;

        if (pending == batch) {
            if (!flush_packet()) {
                return 1;
            }
            if (interval_ns > 0) {
                busy_wait_until(start_ns + (published + 1) * interval_ns);
            }
        }
    }
    flush_packet();

    const std::uint64_t elapsed_ns = llte::now_ns() - start_ns;
    const std::uint64_t produced = sequence - 1001;

    if (!quiet) {
        const double seconds = static_cast<double>(elapsed_ns) / 1e9;
        std::printf("exchange: published %llu messages in %.3f s (%.0f msg/s)\n",
                    static_cast<unsigned long long>(produced), seconds,
                    seconds > 0 ? static_cast<double>(produced) / seconds : 0.0);
        std::printf("exchange: packets sent=%llu dropped=%llu (messages dropped=%llu)\n",
                    static_cast<unsigned long long>(packets_sent),
                    static_cast<unsigned long long>(packets_dropped),
                    static_cast<unsigned long long>(messages_dropped));
        std::printf("exchange: book best_bid=%lld best_ask=%lld live_orders=%zu\n",
                    static_cast<long long>(book.best_bid()),
                    static_cast<long long>(book.best_ask()), book.live_order_count());
        std::fflush(stdout);
    }

    // Stay up briefly so in-flight recovery requests still find a server.
    sleep_ms(linger_ms);
    recovery_server.stop();

    if (!quiet) {
        std::printf("exchange: recovery served %llu requests / %llu messages\n",
                    static_cast<unsigned long long>(recovery_server.served_requests()),
                    static_cast<unsigned long long>(recovery_server.served_messages()));
        std::fflush(stdout);
    }
    return 0;
}
