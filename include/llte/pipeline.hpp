#pragma once

#include <cstddef>
#include <cstdint>

#include "llte/market_message.hpp"
#include "llte/spsc_queue.hpp"

namespace llte {

// One market message plus the timestamps collected as it crosses the pipeline.
// The tail of the chain (t3/t4) is filled in by the order-book process, so the
// stage timings travel with the payload through shared memory.
//
// Padded to two cache lines on purpose. Aligning only the start of the ring's
// storage still leaves adjacent slots sharing a line, and in the near-empty
// steady state the producer is writing slot N while the consumer reads slot N-1 --
// exactly the pair that would then contend.
struct alignas(2 * kCacheLineBytes) PipelineEvent {
    MarketMessage msg;
    std::uint64_t t0_recv;             // returned from recvfrom()
    std::uint64_t t1_parsed;           // header validated, message decoded
    std::uint64_t t2_enqueue;          // first attempt to hand it to the ring
    std::uint64_t enqueue_blocked_ns;  // time spent waiting for ring space
    std::uint8_t recovered;            // arrived over TCP recovery, not multicast
    std::uint8_t reserved[15];
};

static_assert(sizeof(PipelineEvent) == 128);
static_assert(sizeof(PipelineEvent) % kCacheLineBytes == 0,
              "a slot must not share a cache line with its neighbour");
static_assert(offsetof(PipelineEvent, msg) == 0);
static_assert(offsetof(PipelineEvent, t0_recv) == 48);
static_assert(offsetof(PipelineEvent, enqueue_blocked_ns) == 72);

}  // namespace llte
