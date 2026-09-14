#pragma once

#include <cstdint>
#include <type_traits>

namespace llte {

enum class MessageType : std::uint8_t {
    Add = 1,
    Cancel = 2,
    Modify = 3,
    Trade = 4,
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

static_assert(std::is_trivially_copyable_v<MarketMessage>);
static_assert(sizeof(MarketMessage) == 48, "wire layout must stay fixed");

constexpr const char* to_string(MessageType type) {
    switch (type) {
        case MessageType::Add: return "ADD";
        case MessageType::Cancel: return "CANCEL";
        case MessageType::Modify: return "MODIFY";
        case MessageType::Trade: return "TRADE";
    }
    return "?";
}

constexpr const char* to_string(Side side) {
    return side == Side::Buy ? "BUY" : "SELL";
}

}  // namespace llte
