#include "llte/sequence_manager.hpp"

#include <algorithm>
#include <string>

#include "llte/clock.hpp"
#include "llte/protocol.hpp"

namespace llte {

SequenceManager::SequenceManager(std::size_t reorder_capacity, RecoveryClient* recovery)
    : buffer_(reorder_capacity), recovery_(recovery) {
    recovery_scratch_.reserve(kMaxRecoveryBatch);
}

void SequenceManager::store(const FeedMessage& incoming) {
    Slot& slot = buffer_[incoming.message.sequence_number % buffer_.size()];
    if (slot.state == SlotState::Filled) {
        stats_.duplicates += 1;
        return;
    }
    slot.payload = incoming;
    slot.state = SlotState::Filled;
}

void SequenceManager::accept(const FeedMessage& incoming) {
    stats_.accepted += 1;
    const std::uint64_t sequence = incoming.message.sequence_number;

    if (!started_) {
        expected_ = sequence;
        started_ = true;
    }

    if (sequence < expected_) {
        stats_.duplicates += 1;
        return;
    }

    // Beyond the reorder window there is nothing useful left to do: the hole is
    // wider than anything we could hold, so resynchronize and account for it.
    if (sequence - expected_ >= buffer_.size()) {
        stats_.reorder_overflows += 1;
        stats_.unrecoverable += sequence - expected_;
        for (auto& slot : buffer_) {
            slot.state = SlotState::Empty;
        }
        expected_ = sequence;
        store(incoming);
        return;
    }

    const bool gap =
        sequence > expected_ && buffer_[expected_ % buffer_.size()].state == SlotState::Empty;
    store(incoming);

    if (gap) {
        recover_range(expected_, sequence - 1);
    }
}

void SequenceManager::recover_range(std::uint64_t from, std::uint64_t to) {
    stats_.gaps_detected += 1;
    stats_.missing_messages += to - from + 1;

    if (recovery_ != nullptr) {
        for (std::uint64_t start = from; start <= to; start += kMaxRecoveryBatch) {
            const std::uint64_t end = std::min(to, start + kMaxRecoveryBatch - 1);
            std::string error;
            stats_.recovery_requests += 1;
            if (!recovery_->request(start, end, recovery_scratch_, error)) {
                break;
            }
            const std::uint64_t arrived = now_ns();
            for (const MarketMessage& message : recovery_scratch_) {
                if (message.sequence_number < expected_ ||
                    message.sequence_number - expected_ >= buffer_.size()) {
                    continue;
                }
                Slot& slot = buffer_[message.sequence_number % buffer_.size()];
                if (slot.state == SlotState::Filled) {
                    continue;
                }
                slot.payload = FeedMessage{message, arrived, now_ns(), true};
                slot.state = SlotState::Filled;
                stats_.messages_recovered += 1;
            }
        }
    }

    // Whatever recovery could not supply is gone for good. Mark it skipped so the
    // book is not held up behind a hole that will never be filled.
    for (std::uint64_t sequence = from; sequence <= to; ++sequence) {
        Slot& slot = buffer_[sequence % buffer_.size()];
        if (slot.state == SlotState::Empty) {
            slot.state = SlotState::Skipped;
            stats_.unrecoverable += 1;
        }
    }
}

bool SequenceManager::next_deliverable(FeedMessage& out) {
    if (!started_) {
        return false;
    }
    for (;;) {
        Slot& slot = buffer_[expected_ % buffer_.size()];
        if (slot.state == SlotState::Empty) {
            return false;
        }
        if (slot.state == SlotState::Skipped) {
            slot.state = SlotState::Empty;
            expected_ += 1;
            continue;
        }
        out = slot.payload;
        slot.state = SlotState::Empty;
        expected_ += 1;
        stats_.delivered += 1;
        return true;
    }
}

}  // namespace llte
