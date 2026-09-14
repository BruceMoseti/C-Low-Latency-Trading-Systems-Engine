#pragma once

#include <cstdint>

#include "llte/market_message.hpp"

namespace llte {

// One market message plus the timestamps collected as it crosses the pipeline.
// The tail of the chain (t3/t4) is filled in by the order-book process, so the
// stage timings travel with the payload through shared memory.
struct PipelineEvent {
    MarketMessage msg;
    std::uint64_t t0_recv;     // returned from recvfrom()
    std::uint64_t t1_parsed;   // header validated, message decoded
    std::uint64_t t2_enqueue;  // handed to the shared-memory ring
    std::uint8_t recovered;    // arrived over the TCP recovery path, not multicast
    std::uint8_t reserved[7];
};

static_assert(sizeof(PipelineEvent) == 80);

}  // namespace llte
