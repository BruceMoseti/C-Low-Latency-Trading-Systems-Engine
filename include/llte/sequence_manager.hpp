#pragma once

#include <cstdint>
#include <vector>

#include "llte/market_message.hpp"
#include "llte/recovery_client.hpp"

namespace llte {

// A message plus the timestamps taken when this process obtained it. The stamps
// travel with the payload because the sequencer may hold a message back, and a
// retransmitted message is obtained at a completely different instant from the
// packet that exposed the gap.
struct FeedMessage {
    MarketMessage message;
    std::uint64_t t0_recv = 0;
    std::uint64_t t1_parsed = 0;
    bool recovered = false;
};

// Turns the unreliable multicast feed into a gap-free, in-order stream.
//
// Both inputs -- live multicast and TCP retransmissions -- are fed in here, and
// everything downstream reads only from next_deliverable(). That is what keeps a
// single writer on the ring buffer: the two sources converge *before* the queue,
// never at it.
class SequenceManager {
public:
    struct Stats {
        std::uint64_t accepted = 0;
        std::uint64_t delivered = 0;
        std::uint64_t duplicates = 0;
        std::uint64_t gaps_detected = 0;
        std::uint64_t missing_messages = 0;
        std::uint64_t recovery_requests = 0;
        std::uint64_t messages_recovered = 0;
        std::uint64_t unrecoverable = 0;
        std::uint64_t reorder_overflows = 0;
        std::uint64_t heartbeats = 0;
    };

    SequenceManager(std::size_t reorder_capacity, RecoveryClient* recovery);

    // Takes one message from the feed. A sequence number beyond the expected one
    // triggers synchronous recovery for the intervening range.
    void accept(const FeedMessage& incoming);

    // Takes a heartbeat, which asserts that every sequence below `next_sequence`
    // has been published. This is what makes a loss at the tail of the stream
    // detectable, and it immediately repairs anything now known to be missing.
    void observe_heartbeat(std::uint64_t next_sequence);

    // Recovers any hole below the highest sequence number known to exist.
    //
    // accept() can only notice a gap when a later message arrives to expose it, so
    // callers should also invoke this when the feed goes quiet, in case the
    // heartbeat that would have revealed the hole was itself lost.
    void flush_gaps();

    // Pops the next in-order message, if it is available.
    bool next_deliverable(FeedMessage& out);

    std::uint64_t highest_sequence_seen() const { return highest_seen_; }

    std::uint64_t expected_sequence() const { return expected_; }
    const Stats& stats() const { return stats_; }

private:
    enum class SlotState : std::uint8_t { Empty, Filled, Skipped };

    struct Slot {
        FeedMessage payload;
        SlotState state = SlotState::Empty;
    };

    void recover_range(std::uint64_t from, std::uint64_t to);
    void store(const FeedMessage& incoming);

    std::vector<Slot> buffer_;
    std::vector<MarketMessage> recovery_scratch_;
    RecoveryClient* recovery_;
    std::uint64_t expected_ = 0;
    std::uint64_t highest_seen_ = 0;
    bool started_ = false;
    Stats stats_;
};

}  // namespace llte
