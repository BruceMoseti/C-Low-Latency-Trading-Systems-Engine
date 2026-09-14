#include "llte/order_book.hpp"

#include <bit>
#include <cassert>

namespace llte {
namespace {

std::size_t round_up_pow2(std::size_t value) {
    if (value < 2) {
        return 2;
    }
    return std::bit_ceil(value);
}

}  // namespace

OrderBook::OrderBook(const Config& config)
    : min_price_(config.min_price),
      max_price_(config.max_price),
      index_mask_(0),
      free_head_(kNullOrder),
      live_orders_(0),
      best_bid_level_(kNoLevel),
      best_ask_level_(kNoLevel) {
    assert(config.max_price >= config.min_price);
    assert(config.max_orders > 0);

    const std::size_t level_count =
        static_cast<std::size_t>(config.max_price - config.min_price) + 1;
    bid_levels_.assign(level_count, Level{0, 0, kNullOrder, kNullOrder});
    ask_levels_.assign(level_count, Level{0, 0, kNullOrder, kNullOrder});

    pool_.resize(config.max_orders);
    // Keeping the load factor at or below 0.5 bounds linear-probe cluster length.
    const std::size_t index_size = round_up_pow2(config.max_orders * 2);
    index_.assign(index_size, IndexSlot{kEmptyKey, 0});
    index_mask_ = index_size - 1;

    clear();
}

void OrderBook::clear() {
    for (auto& level : bid_levels_) {
        level = Level{0, 0, kNullOrder, kNullOrder};
    }
    for (auto& level : ask_levels_) {
        level = Level{0, 0, kNullOrder, kNullOrder};
    }
    for (auto& slot : index_) {
        slot = IndexSlot{kEmptyKey, 0};
    }

    const auto slots = static_cast<std::uint32_t>(pool_.size());
    for (std::uint32_t i = 0; i < slots; ++i) {
        pool_[i].next = (i + 1 < slots) ? i + 1 : kNullOrder;
    }
    free_head_ = slots > 0 ? 0 : kNullOrder;
    live_orders_ = 0;
    best_bid_level_ = kNoLevel;
    best_ask_level_ = kNoLevel;
}

std::uint32_t OrderBook::acquire_slot() {
    if (free_head_ == kNullOrder) {
        return kNullOrder;
    }
    const std::uint32_t slot = free_head_;
    free_head_ = pool_[slot].next;
    return slot;
}

void OrderBook::release_slot(std::uint32_t slot) {
    pool_[slot].next = free_head_;
    free_head_ = slot;
}

void OrderBook::link_back(std::size_t level_idx, Side side, std::uint32_t slot) {
    Level& level = levels_for(side)[level_idx];
    Order& order = pool_[slot];
    order.next = kNullOrder;
    order.prev = level.tail;
    if (level.tail != kNullOrder) {
        pool_[level.tail].next = slot;
    } else {
        level.head = slot;
    }
    level.tail = slot;
    level.order_count += 1;
    level.total_quantity += order.quantity;
}

void OrderBook::unlink(std::size_t level_idx, Side side, std::uint32_t slot) {
    Level& level = levels_for(side)[level_idx];
    Order& order = pool_[slot];
    if (order.prev != kNullOrder) {
        pool_[order.prev].next = order.next;
    } else {
        level.head = order.next;
    }
    if (order.next != kNullOrder) {
        pool_[order.next].prev = order.prev;
    } else {
        level.tail = order.prev;
    }
    level.order_count -= 1;
    level.total_quantity -= order.quantity;
    if (level.order_count == 0) {
        on_level_emptied(side, level_idx);
    }
}

void OrderBook::note_level_filled(Side side, std::size_t level_idx) {
    if (side == Side::Buy) {
        if (best_bid_level_ == kNoLevel || level_idx > best_bid_level_) {
            best_bid_level_ = level_idx;
        }
    } else {
        if (best_ask_level_ == kNoLevel || level_idx < best_ask_level_) {
            best_ask_level_ = level_idx;
        }
    }
}

void OrderBook::on_level_emptied(Side side, std::size_t level_idx) {
    // Walk outward from the vacated level. Book updates are overwhelmingly near
    // the touch, so this is a short scan in practice.
    if (side == Side::Buy) {
        if (level_idx != best_bid_level_) {
            return;
        }
        std::size_t scan = level_idx;
        while (scan-- > 0) {
            if (bid_levels_[scan].order_count > 0) {
                best_bid_level_ = scan;
                return;
            }
        }
        best_bid_level_ = kNoLevel;
    } else {
        if (level_idx != best_ask_level_) {
            return;
        }
        for (std::size_t scan = level_idx + 1; scan < ask_levels_.size(); ++scan) {
            if (ask_levels_[scan].order_count > 0) {
                best_ask_level_ = scan;
                return;
            }
        }
        best_ask_level_ = kNoLevel;
    }
}

OrderBook::Result OrderBook::add(std::uint64_t order_id, Side side, Price price,
                                 std::uint32_t quantity) {
    if (order_id == kEmptyKey) {
        return Result::UnknownOrder;
    }
    if (quantity == 0) {
        return Result::InvalidQuantity;
    }
    if (!in_band(price)) {
        return Result::PriceOutOfBand;
    }
    if (find_index(order_id) != kNoLevel) {
        return Result::DuplicateOrder;
    }

    const std::uint32_t slot = acquire_slot();
    if (slot == kNullOrder) {
        return Result::PoolExhausted;
    }

    Order& order = pool_[slot];
    order.id = order_id;
    order.price = price;
    order.quantity = quantity;
    order.side = side;

    const std::size_t level_idx = level_index(price);
    link_back(level_idx, side, slot);
    note_level_filled(side, level_idx);
    index_insert(order_id, slot);
    live_orders_ += 1;
    return Result::Ok;
}

OrderBook::Result OrderBook::cancel(std::uint64_t order_id) {
    const std::size_t position = find_index(order_id);
    if (position == kNoLevel) {
        return Result::UnknownOrder;
    }
    const std::uint32_t slot = index_[position].value;
    const Order& order = pool_[slot];
    unlink(level_index(order.price), order.side, slot);
    index_erase(position);
    release_slot(slot);
    live_orders_ -= 1;
    return Result::Ok;
}

OrderBook::Result OrderBook::modify(std::uint64_t order_id, Price new_price,
                                    std::uint32_t new_quantity) {
    if (new_quantity == 0) {
        return Result::InvalidQuantity;
    }
    if (!in_band(new_price)) {
        return Result::PriceOutOfBand;
    }
    const std::size_t position = find_index(order_id);
    if (position == kNoLevel) {
        return Result::UnknownOrder;
    }

    const std::uint32_t slot = index_[position].value;
    Order& order = pool_[slot];
    const std::size_t old_level = level_index(order.price);

    // Exchange semantics: shrinking in place keeps time priority, while a price
    // change or a size increase sends the order to the back of the queue.
    if (new_price == order.price && new_quantity < order.quantity) {
        Level& level = levels_for(order.side)[old_level];
        level.total_quantity -= (order.quantity - new_quantity);
        order.quantity = new_quantity;
        return Result::Ok;
    }

    const Side side = order.side;
    unlink(old_level, side, slot);
    order.price = new_price;
    order.quantity = new_quantity;
    const std::size_t new_level = level_index(new_price);
    link_back(new_level, side, slot);
    note_level_filled(side, new_level);
    return Result::Ok;
}

OrderBook::Result OrderBook::execute(std::uint64_t order_id, std::uint32_t quantity) {
    if (quantity == 0) {
        return Result::InvalidQuantity;
    }
    const std::size_t position = find_index(order_id);
    if (position == kNoLevel) {
        return Result::UnknownOrder;
    }

    const std::uint32_t slot = index_[position].value;
    Order& order = pool_[slot];
    const std::size_t level_idx = level_index(order.price);

    if (quantity >= order.quantity) {
        unlink(level_idx, order.side, slot);
        index_erase(position);
        release_slot(slot);
        live_orders_ -= 1;
        return Result::Ok;
    }

    levels_for(order.side)[level_idx].total_quantity -= quantity;
    order.quantity -= quantity;
    return Result::Ok;
}

Price OrderBook::best_bid() const {
    return best_bid_level_ == kNoLevel ? kNoPrice : level_price(best_bid_level_);
}

Price OrderBook::best_ask() const {
    return best_ask_level_ == kNoLevel ? kNoPrice : level_price(best_ask_level_);
}

std::uint64_t OrderBook::quantity_at(Side side, Price price) const {
    if (!in_band(price)) {
        return 0;
    }
    return levels_for(side)[level_index(price)].total_quantity;
}

std::uint32_t OrderBook::order_count_at(Side side, Price price) const {
    if (!in_band(price)) {
        return 0;
    }
    return levels_for(side)[level_index(price)].order_count;
}

bool OrderBook::contains(std::uint64_t order_id) const {
    return find_index(order_id) != kNoLevel;
}

std::uint64_t OrderBook::mix(std::uint64_t key) {
    // splitmix64 finalizer: sequential order ids would otherwise cluster badly.
    key += 0x9e3779b97f4a7c15ULL;
    key = (key ^ (key >> 30)) * 0xbf58476d1ce4e5b9ULL;
    key = (key ^ (key >> 27)) * 0x94d049bb133111ebULL;
    return key ^ (key >> 31);
}

std::size_t OrderBook::find_index(std::uint64_t order_id) const {
    std::size_t position = mix(order_id) & index_mask_;
    while (index_[position].key != kEmptyKey) {
        if (index_[position].key == order_id) {
            return position;
        }
        position = (position + 1) & index_mask_;
    }
    return kNoLevel;
}

void OrderBook::index_insert(std::uint64_t order_id, std::uint32_t slot) {
    std::size_t position = mix(order_id) & index_mask_;
    while (index_[position].key != kEmptyKey) {
        position = (position + 1) & index_mask_;
    }
    index_[position] = IndexSlot{order_id, slot};
}

void OrderBook::index_erase(std::size_t position) {
    // Backward-shift deletion keeps every probe sequence contiguous, which avoids
    // tombstones and the rehash pause they would eventually force.
    std::size_t hole = position;
    for (;;) {
        index_[hole].key = kEmptyKey;
        std::size_t probe = hole;
        for (;;) {
            probe = (probe + 1) & index_mask_;
            if (index_[probe].key == kEmptyKey) {
                return;
            }
            const std::size_t ideal = mix(index_[probe].key) & index_mask_;
            const bool must_stay = (hole < probe) ? (ideal > hole && ideal <= probe)
                                                  : (ideal > hole || ideal <= probe);
            if (!must_stay) {
                index_[hole] = index_[probe];
                hole = probe;
                break;
            }
        }
    }
}

}  // namespace llte
