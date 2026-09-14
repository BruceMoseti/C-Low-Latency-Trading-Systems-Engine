#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "llte/market_message.hpp"

namespace llte {

// Central limit order book with price-time priority.
//
// "Preallocated" means every container is sized in the constructor and the
// steady-state paths (add/cancel/modify/execute) never touch the allocator:
//   * price levels live in a flat array indexed by (price - min_price)
//   * orders come from a fixed pool with an intrusive free list
//   * order_id -> pool slot uses an open-addressed table with backward-shift
//     deletion, so it needs neither tombstones nor rehashing
class OrderBook {
public:
    static constexpr Price kNoPrice = std::numeric_limits<Price>::min();

    enum class Result {
        Ok,
        UnknownOrder,
        DuplicateOrder,
        PriceOutOfBand,
        PoolExhausted,
        InvalidQuantity,
    };

    struct Config {
        Price min_price = 0;
        Price max_price = 0;
        std::size_t max_orders = 0;
    };

    explicit OrderBook(const Config& config);

    Result add(std::uint64_t order_id, Side side, Price price, std::uint32_t quantity);
    Result cancel(std::uint64_t order_id);
    Result modify(std::uint64_t order_id, Price new_price, std::uint32_t new_quantity);
    // Applies a fill against a resting order, removing it once fully executed.
    Result execute(std::uint64_t order_id, std::uint32_t quantity);

    Price best_bid() const;
    Price best_ask() const;
    std::uint64_t quantity_at(Side side, Price price) const;
    std::uint32_t order_count_at(Side side, Price price) const;
    bool contains(std::uint64_t order_id) const;
    std::size_t live_order_count() const { return live_orders_; }

    void clear();

private:
    static constexpr std::uint32_t kNullOrder = std::numeric_limits<std::uint32_t>::max();
    static constexpr std::size_t kNoLevel = std::numeric_limits<std::size_t>::max();
    static constexpr std::uint64_t kEmptyKey = 0;

    struct Order {
        std::uint64_t id;
        Price price;
        std::uint32_t quantity;
        std::uint32_t next;
        std::uint32_t prev;
        Side side;
    };

    struct Level {
        std::uint64_t total_quantity;
        std::uint32_t order_count;
        std::uint32_t head;
        std::uint32_t tail;
    };

    struct IndexSlot {
        std::uint64_t key;
        std::uint32_t value;
    };

    std::vector<Level>& levels_for(Side side) {
        return side == Side::Buy ? bid_levels_ : ask_levels_;
    }
    const std::vector<Level>& levels_for(Side side) const {
        return side == Side::Buy ? bid_levels_ : ask_levels_;
    }

    bool in_band(Price price) const { return price >= min_price_ && price <= max_price_; }
    std::size_t level_index(Price price) const {
        return static_cast<std::size_t>(price - min_price_);
    }
    Price level_price(std::size_t index) const {
        return min_price_ + static_cast<Price>(index);
    }

    void link_back(std::size_t level_idx, Side side, std::uint32_t slot);
    void unlink(std::size_t level_idx, Side side, std::uint32_t slot);
    void on_level_emptied(Side side, std::size_t level_idx);
    void note_level_filled(Side side, std::size_t level_idx);

    std::uint32_t acquire_slot();
    void release_slot(std::uint32_t slot);

    static std::uint64_t mix(std::uint64_t key);
    std::size_t find_index(std::uint64_t order_id) const;
    void index_insert(std::uint64_t order_id, std::uint32_t slot);
    void index_erase(std::size_t position);

    Price min_price_;
    Price max_price_;

    std::vector<Level> bid_levels_;
    std::vector<Level> ask_levels_;
    std::vector<Order> pool_;
    std::vector<IndexSlot> index_;

    std::size_t index_mask_;
    std::uint32_t free_head_;
    std::size_t live_orders_;
    std::size_t best_bid_level_;
    std::size_t best_ask_level_;
};

}  // namespace llte
