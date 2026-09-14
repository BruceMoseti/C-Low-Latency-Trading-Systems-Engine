#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace llte {

enum class MessageType : std::uint8_t {
    Add = 1,
    Cancel = 2,
    Modify = 3,
    Trade = 4,
    // Carries the next sequence number the venue will assign, so a receiver can
    // tell "the feed is quiet" from "I missed the end of it". Without this a loss
    // at the tail of the stream is undetectable: nothing arrives behind it to
    // expose the hole. A heartbeat consumes no sequence number of its own.
    Heartbeat = 5,
};

enum class Side : std::uint8_t {
    Buy = 1,
    Sell = 2,
};

// Prices are integer ticks (cents), never floating point: financial values need
// exact representation and integer compares keep the book's hot path branchy-free.
using Price = std::int64_t;

// Fixed-layout wire struct. Producer and consumer are built from the same source
// on the same host, so the struct is memcpy'd onto the wire without conversion.
struct MarketMessage {
    std::uint64_t sequence_number;
    std::uint64_t timestamp_ns;
    std::uint64_t order_id;
    Price price;
    std::uint32_t quantity;
    std::uint32_t symbol_id;
    MessageType type;
    Side side;
    std::uint16_t reserved;
};

// The struct goes onto the wire as raw bytes, so the layout is part of the
// protocol. Size alone would not catch two same-width fields being swapped, so
// every offset is pinned individually.
static_assert(std::is_trivially_copyable_v<MarketMessage>);
static_assert(sizeof(MarketMessage) == 48, "wire layout must stay fixed");
static_assert(offsetof(MarketMessage, sequence_number) == 0);
static_assert(offsetof(MarketMessage, timestamp_ns) == 8);
static_assert(offsetof(MarketMessage, order_id) == 16);
static_assert(offsetof(MarketMessage, price) == 24);
static_assert(offsetof(MarketMessage, quantity) == 32);
static_assert(offsetof(MarketMessage, symbol_id) == 36);
static_assert(offsetof(MarketMessage, type) == 40);
static_assert(offsetof(MarketMessage, side) == 41);
static_assert(offsetof(MarketMessage, reserved) == 42);

constexpr const char* to_string(MessageType type) {
    switch (type) {
        case MessageType::Add: return "ADD";
        case MessageType::Cancel: return "CANCEL";
        case MessageType::Modify: return "MODIFY";
        case MessageType::Trade: return "TRADE";
        case MessageType::Heartbeat: return "HEARTBEAT";
    }
    return "?";
}

constexpr const char* to_string(Side side) {
    return side == Side::Buy ? "BUY" : "SELL";
}

}  // namespace llte
