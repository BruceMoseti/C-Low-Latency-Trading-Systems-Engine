// Exercises the recovery path end to end over real TCP loopback: a live
// HistoryStore and RecoveryServer on one side, a RecoveryClient driven by the
// SequenceManager on the other.

#include <cstdint>
#include <string>
#include <vector>

#include "llte/market_message.hpp"
#include "llte/recovery_client.hpp"
#include "llte/recovery_server.hpp"
#include "llte/sequence_manager.hpp"
#include "test_support.hpp"

using llte::FeedMessage;
using llte::HistoryStore;
using llte::MarketMessage;
using llte::MessageType;
using llte::RecoveryClient;
using llte::RecoveryServer;
using llte::SequenceManager;
using llte::Side;

namespace {

constexpr std::uint64_t kFirstSequence = 1001;

MarketMessage make_message(std::uint64_t sequence) {
    MarketMessage message{};
    message.sequence_number = sequence;
    message.timestamp_ns = sequence * 1000;
    message.order_id = sequence;
    message.price = 18700 + static_cast<llte::Price>(sequence % 20);
    message.quantity = 10;
    message.symbol_id = 1;
    message.type = MessageType::Add;
    message.side = (sequence % 2) ? Side::Buy : Side::Sell;
    return message;
}

// The sandbox has no reserved port, so take the first one that binds.
std::uint16_t start_on_free_port(RecoveryServer& server) {
    for (std::uint16_t port = 39001; port < 39100; ++port) {
        std::string error;
        if (server.start(port, error)) {
            return port;
        }
    }
    return 0;
}

std::vector<std::uint64_t> drain(SequenceManager& sequencer, std::uint64_t& recovered_count) {
    std::vector<std::uint64_t> delivered;
    FeedMessage out;
    while (sequencer.next_deliverable(out)) {
        delivered.push_back(out.message.sequence_number);
        recovered_count += out.recovered ? 1 : 0;
    }
    return delivered;
}

void test_history_store_retains_a_bounded_window() {
    HistoryStore history(16);
    for (std::uint64_t sequence = 100; sequence < 140; ++sequence) {
        history.append(make_message(sequence));
    }

    MarketMessage buffer[64];
    // The oldest entries have been overwritten; only the last 16 survive.
    CHECK_EQ(history.fetch(100, 120, buffer, 64), 0u);
    CHECK_EQ(history.fetch(124, 139, buffer, 64), 16u);
    CHECK_EQ(buffer[0].sequence_number, 124u);
    CHECK_EQ(buffer[15].sequence_number, 139u);

    // A request that straddles the retention edge returns only what is left.
    CHECK_EQ(history.fetch(120, 130, buffer, 64), 7u);
    CHECK_EQ(buffer[0].sequence_number, 124u);
}

void test_recovers_dropped_messages() {
    HistoryStore history(4096);
    RecoveryServer server(history);
    const std::uint16_t port = start_on_free_port(server);
    CHECK(port != 0);
    if (port == 0) {
        return;
    }

    constexpr std::uint64_t kCount = 500;
    for (std::uint64_t i = 0; i < kCount; ++i) {
        history.append(make_message(kFirstSequence + i));
    }

    RecoveryClient client;
    std::string error;
    CHECK(client.connect("127.0.0.1", port, 3000, error));

    SequenceManager sequencer(1024, &client);
    std::vector<std::uint64_t> delivered;
    std::uint64_t recovered_count = 0;

    // Drop a single message, a short burst, and a longer burst.
    auto dropped = [](std::uint64_t sequence) {
        const std::uint64_t offset = sequence - kFirstSequence;
        return offset == 5 || (offset >= 100 && offset <= 103) ||
               (offset >= 300 && offset <= 330);
    };

    std::uint64_t drop_count = 0;
    for (std::uint64_t i = 0; i < kCount; ++i) {
        const std::uint64_t sequence = kFirstSequence + i;
        if (dropped(sequence)) {
            drop_count += 1;
            continue;
        }
        sequencer.accept(FeedMessage{make_message(sequence), 0, 0, false});
        const auto batch = drain(sequencer, recovered_count);
        delivered.insert(delivered.end(), batch.begin(), batch.end());
    }

    const auto& stats = sequencer.stats();
    CHECK_EQ(stats.gaps_detected, 3u);
    CHECK_EQ(stats.missing_messages, drop_count);
    CHECK_EQ(stats.messages_recovered, drop_count);
    CHECK_EQ(stats.unrecoverable, 0u);
    CHECK_EQ(recovered_count, drop_count);

    // Everything the exchange published reaches the book, in order and exactly once.
    CHECK_EQ(delivered.size(), kCount);
    bool contiguous = true;
    for (std::size_t i = 0; i < delivered.size(); ++i) {
        if (delivered[i] != kFirstSequence + i) {
            contiguous = false;
            break;
        }
    }
    CHECK(contiguous);

    CHECK(server.served_requests() >= 3u);
    CHECK_EQ(server.served_messages(), drop_count);
    server.stop();
}

void test_duplicates_and_reordering() {
    SequenceManager sequencer(64, nullptr);
    std::uint64_t recovered_count = 0;

    sequencer.accept(FeedMessage{make_message(1001), 0, 0, false});
    CHECK_EQ(drain(sequencer, recovered_count).size(), 1u);

    // A message that arrives early is held until its predecessor shows up.
    sequencer.accept(FeedMessage{make_message(1003), 0, 0, false});
    CHECK_EQ(sequencer.stats().gaps_detected, 1u);

    sequencer.accept(FeedMessage{make_message(1002), 0, 0, false});
    const auto delivered = drain(sequencer, recovered_count);
    CHECK_EQ(delivered.size(), 2u);
    if (delivered.size() == 2) {
        CHECK_EQ(delivered[0], 1002u);
        CHECK_EQ(delivered[1], 1003u);
    }

    // Replays of already-delivered sequences are dropped, not re-emitted.
    sequencer.accept(FeedMessage{make_message(1002), 0, 0, false});
    sequencer.accept(FeedMessage{make_message(1003), 0, 0, false});
    CHECK_EQ(drain(sequencer, recovered_count).size(), 0u);
    CHECK_EQ(sequencer.stats().duplicates, 2u);
}

// Without this, a hole that recovery cannot fill would stall the book forever.
void test_unrecoverable_gap_does_not_stall() {
    SequenceManager sequencer(64, nullptr);
    std::uint64_t recovered_count = 0;

    sequencer.accept(FeedMessage{make_message(1001), 0, 0, false});
    drain(sequencer, recovered_count);

    sequencer.accept(FeedMessage{make_message(1005), 0, 0, false});
    const auto delivered = drain(sequencer, recovered_count);

    CHECK_EQ(sequencer.stats().gaps_detected, 1u);
    CHECK_EQ(sequencer.stats().unrecoverable, 3u);
    CHECK_EQ(delivered.size(), 1u);
    if (!delivered.empty()) {
        CHECK_EQ(delivered[0], 1005u);
    }

    // The stream keeps flowing past the abandoned range.
    sequencer.accept(FeedMessage{make_message(1006), 0, 0, false});
    CHECK_EQ(drain(sequencer, recovered_count).size(), 1u);
}

void test_reorder_window_overflow_resynchronizes() {
    SequenceManager sequencer(16, nullptr);
    std::uint64_t recovered_count = 0;

    sequencer.accept(FeedMessage{make_message(1001), 0, 0, false});
    drain(sequencer, recovered_count);

    // A jump wider than the reorder window cannot be buffered, so the sequencer
    // resynchronizes instead of pretending it can catch up.
    sequencer.accept(FeedMessage{make_message(2000), 0, 0, false});
    const auto delivered = drain(sequencer, recovered_count);

    CHECK_EQ(sequencer.stats().reorder_overflows, 1u);
    CHECK_EQ(delivered.size(), 1u);
    if (!delivered.empty()) {
        CHECK_EQ(delivered[0], 2000u);
    }
    CHECK_EQ(sequencer.expected_sequence(), 2001u);
}

}  // namespace

int main() {
    test_history_store_retains_a_bounded_window();
    test_recovers_dropped_messages();
    test_duplicates_and_reordering();
    test_unrecoverable_gap_does_not_stall();
    test_reorder_window_overflow_resynchronizes();
    return llte::test::report("test_gap_recovery");
}
