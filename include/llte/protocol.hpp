#pragma once

#include <cstddef>
#include <cstdint>

#include "llte/market_message.hpp"

namespace llte {

// Defaults for a single-host demo; every binary accepts overrides on the command line.
inline constexpr const char* kDefaultMulticastGroup = "239.1.1.1";
inline constexpr const char* kDefaultInterface = "127.0.0.1";
inline constexpr std::uint16_t kDefaultMulticastPort = 30001;
inline constexpr std::uint16_t kDefaultRecoveryPort = 30002;

inline constexpr std::uint32_t kFeedMagic = 0x4C4C5445;      // 'LLTE'
inline constexpr std::uint32_t kRecoveryMagic = 0x4C4C5452;  // 'LLTR'

// Largest number of messages carried by one multicast datagram. Batching amortizes
// the per-packet syscall cost; the receiver handles any count up to this bound.
inline constexpr std::uint16_t kMaxBatch = 32;

struct FeedPacketHeader {
    std::uint32_t magic;
    std::uint16_t count;
    std::uint16_t reserved;
};

static_assert(sizeof(FeedPacketHeader) == 8);

inline constexpr std::size_t kMaxPacketBytes =
    sizeof(FeedPacketHeader) + kMaxBatch * sizeof(MarketMessage);

// Recovery runs over TCP so a retransmission never stalls the multicast fast path.
struct RecoveryRequest {
    std::uint32_t magic;
    std::uint32_t reserved;
    std::uint64_t from_sequence;
    std::uint64_t to_sequence;
};

struct RecoveryResponseHeader {
    std::uint32_t magic;
    std::uint32_t count;
};

static_assert(sizeof(RecoveryRequest) == 24);
static_assert(sizeof(RecoveryResponseHeader) == 8);

// Largest range the recovery server will serve in one response.
inline constexpr std::uint32_t kMaxRecoveryBatch = 1024;

}  // namespace llte
