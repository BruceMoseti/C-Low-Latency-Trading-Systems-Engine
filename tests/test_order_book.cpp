#include "llte/order_book.hpp"

#include <random>
#include <vector>
#include <unordered_map>
#include <vector>

#include "test_support.hpp"

using llte::OrderBook;
using llte::Price;
using llte::Side;
using Result = llte::OrderBook::Result;

namespace {

OrderBook make_book(std::size_t max_orders = 1024) {
    return OrderBook({18000, 19000, max_orders});
}

void test_add_and_touch() {
    OrderBook book = make_book();
    CHECK_EQ(book.best_bid(), OrderBook::kNoPrice);
    CHECK_EQ(book.best_ask(), OrderBook::kNoPrice);

    CHECK(book.add(1, Side::Buy, 18750, 100) == Result::Ok);
    CHECK(book.add(2, Side::Buy, 18751, 200) == Result::Ok);
    CHECK(book.add(3, Side::Sell, 18755, 300) == Result::Ok);
    CHECK(book.add(4, Side::Sell, 18754, 50) == Result::Ok);

    CHECK_EQ(book.best_bid(), 18751);
    CHECK_EQ(book.best_ask(), 18754);
    CHECK_EQ(book.quantity_at(Side::Buy, 18750), 100u);
    CHECK_EQ(book.quantity_at(Side::Sell, 18755), 300u);
    CHECK_EQ(book.live_order_count(), 4u);
}

void test_rejects_bad_input() {
    OrderBook book = make_book();
    CHECK(book.add(1, Side::Buy, 17000, 100) == Result::PriceOutOfBand);
    CHECK(book.add(2, Side::Buy, 18500, 0) == Result::InvalidQuantity);
    CHECK(book.add(3, Side::Buy, 18500, 100) == Result::Ok);
    CHECK(book.add(3, Side::Buy, 18501, 100) == Result::DuplicateOrder);
    CHECK(book.cancel(999) == Result::UnknownOrder);
    CHECK(book.modify(999, 18500, 10) == Result::UnknownOrder);
    CHECK(book.execute(999, 10) == Result::UnknownOrder);
}

// Reads the resting queue at a price, so assertions can be about position and not
// just about totals. Level quantity and order count are identical under FIFO and
// LIFO, so a test that only checks those cannot see time priority at all.
std::vector<std::uint64_t> queue_at(const OrderBook& book, Side side, Price price) {
    std::uint64_t ids[16] = {};
    const std::size_t count = book.order_ids_at(side, price, ids, 16);
    return std::vector<std::uint64_t>(ids, ids + count);
}

void test_price_time_priority() {
    OrderBook book = make_book();
    book.add(1, Side::Buy, 18700, 100);
    book.add(2, Side::Buy, 18700, 200);
    book.add(3, Side::Buy, 18700, 300);
    CHECK_EQ(book.order_count_at(Side::Buy, 18700), 3u);
    CHECK_EQ(book.quantity_at(Side::Buy, 18700), 600u);

    // Arrival order is queue order.
    CHECK(queue_at(book, Side::Buy, 18700) == (std::vector<std::uint64_t>{1, 2, 3}));

    // Shrinking at the same price keeps position.
    CHECK(book.modify(1, 18700, 50) == Result::Ok);
    CHECK_EQ(book.quantity_at(Side::Buy, 18700), 550u);
    CHECK(queue_at(book, Side::Buy, 18700) == (std::vector<std::uint64_t>{1, 2, 3}));

    // A modify that changes nothing must not cost position either.
    CHECK(book.modify(1, 18700, 50) == Result::Ok);
    CHECK(queue_at(book, Side::Buy, 18700) == (std::vector<std::uint64_t>{1, 2, 3}));

    // A size increase goes to the back of the queue.
    CHECK(book.modify(1, 18700, 400) == Result::Ok);
    CHECK_EQ(book.quantity_at(Side::Buy, 18700), 900u);
    CHECK_EQ(book.order_count_at(Side::Buy, 18700), 3u);
    CHECK(queue_at(book, Side::Buy, 18700) == (std::vector<std::uint64_t>{2, 3, 1}));

    // A price change joins the back of its new level.
    book.add(4, Side::Buy, 18690, 10);
    CHECK(book.modify(2, 18690, 20) == Result::Ok);
    CHECK(queue_at(book, Side::Buy, 18690) == (std::vector<std::uint64_t>{4, 2}));
    CHECK(queue_at(book, Side::Buy, 18700) == (std::vector<std::uint64_t>{3, 1}));

    // Cancelling from the middle preserves the order of the rest.
    book.add(5, Side::Buy, 18700, 10);
    CHECK(queue_at(book, Side::Buy, 18700) == (std::vector<std::uint64_t>{3, 1, 5}));
    CHECK(book.cancel(1) == Result::Ok);
    CHECK(queue_at(book, Side::Buy, 18700) == (std::vector<std::uint64_t>{3, 5}));

    // A partial fill leaves the order where it was.
    CHECK(book.execute(3, 1) == Result::Ok);
    CHECK(queue_at(book, Side::Buy, 18700) == (std::vector<std::uint64_t>{3, 5}));
}

// The digest is what the end-to-end check compares across processes, so it has to
// notice a difference that the aggregate accessors cannot.
void test_digest_detects_queue_order() {
    OrderBook first = make_book();
    first.add(1, Side::Buy, 18700, 100);
    first.add(2, Side::Buy, 18700, 100);

    OrderBook second = make_book();
    second.add(2, Side::Buy, 18700, 100);
    second.add(1, Side::Buy, 18700, 100);

    // Same orders, same level totals, opposite queue order.
    CHECK_EQ(second.quantity_at(Side::Buy, 18700), first.quantity_at(Side::Buy, 18700));
    CHECK_EQ(second.order_count_at(Side::Buy, 18700), first.order_count_at(Side::Buy, 18700));
    CHECK_EQ(second.best_bid(), first.best_bid());
    CHECK(first.structural_digest() != second.structural_digest());

    // And it is stable for books built the same way.
    OrderBook third = make_book();
    third.add(1, Side::Buy, 18700, 100);
    third.add(2, Side::Buy, 18700, 100);
    CHECK_EQ(third.structural_digest(), first.structural_digest());

    // A quantity difference at one level must change it too.
    CHECK(third.modify(2, 18700, 99) == Result::Ok);
    CHECK(third.structural_digest() != first.structural_digest());
}

void test_modify_moves_levels() {
    OrderBook book = make_book();
    book.add(1, Side::Buy, 18700, 100);
    book.add(2, Side::Buy, 18690, 100);
    CHECK_EQ(book.best_bid(), 18700);

    CHECK(book.modify(1, 18680, 100) == Result::Ok);
    CHECK_EQ(book.best_bid(), 18690);
    CHECK_EQ(book.quantity_at(Side::Buy, 18700), 0u);
    CHECK_EQ(book.quantity_at(Side::Buy, 18680), 100u);

    CHECK(book.modify(1, 18710, 100) == Result::Ok);
    CHECK_EQ(book.best_bid(), 18710);
}

void test_execute_and_cancel() {
    OrderBook book = make_book();
    book.add(1, Side::Sell, 18800, 100);
    book.add(2, Side::Sell, 18800, 100);
    book.add(3, Side::Sell, 18810, 500);
    CHECK_EQ(book.best_ask(), 18800);

    CHECK(book.execute(1, 40) == Result::Ok);
    CHECK_EQ(book.quantity_at(Side::Sell, 18800), 160u);
    CHECK(book.contains(1));

    // Filling the remainder removes the order entirely.
    CHECK(book.execute(1, 60) == Result::Ok);
    CHECK(!book.contains(1));
    CHECK_EQ(book.quantity_at(Side::Sell, 18800), 100u);

    // An oversized fill still removes the order, but it must be reported: it can
    // only happen if an Add or Modify for that order never arrived.
    CHECK(book.execute(2, 999) == Result::OverFilled);
    CHECK_EQ(book.quantity_at(Side::Sell, 18800), 0u);
    CHECK_EQ(book.best_ask(), 18810);

    CHECK(book.cancel(3) == Result::Ok);
    CHECK_EQ(book.best_ask(), OrderBook::kNoPrice);
    CHECK_EQ(book.live_order_count(), 0u);
}

void test_pool_exhaustion() {
    OrderBook book = make_book(4);
    for (std::uint64_t id = 1; id <= 4; ++id) {
        CHECK(book.add(id, Side::Buy, 18500, 10) == Result::Ok);
    }
    CHECK(book.add(5, Side::Buy, 18500, 10) == Result::PoolExhausted);

    // Freeing a slot must make the pool usable again.
    CHECK(book.cancel(2) == Result::Ok);
    CHECK(book.add(5, Side::Buy, 18500, 10) == Result::Ok);
}

// Churns the order index hard so backward-shift deletion is exercised against a
// reference model rather than only on tidy sequences.
void test_index_churn_against_model() {
    OrderBook book = make_book(2048);
    std::unordered_map<std::uint64_t, std::pair<Price, std::uint32_t>> model;
    std::mt19937_64 rng(7);
    std::uniform_int_distribution<int> price_pick(18400, 18600);
    std::uniform_int_distribution<std::uint32_t> quantity_pick(1, 500);
    std::vector<std::uint64_t> live;
    std::uint64_t next_id = 1;

    for (int step = 0; step < 60000; ++step) {
        const bool can_remove = !live.empty();
        const int roll = static_cast<int>(rng() % 100);

        if (!can_remove || roll < 55) {
            if (live.size() >= 2000) {
                continue;
            }
            const std::uint64_t id = next_id++;
            const Price price = price_pick(rng);
            const std::uint32_t quantity = quantity_pick(rng);
            if (book.add(id, Side::Buy, price, quantity) == Result::Ok) {
                model[id] = {price, quantity};
                live.push_back(id);
            }
        } else {
            const std::size_t index = rng() % live.size();
            const std::uint64_t id = live[index];
            if (roll < 85) {
                CHECK(book.cancel(id) == Result::Ok);
                model.erase(id);
                live[index] = live.back();
                live.pop_back();
            } else {
                const Price price = price_pick(rng);
                const std::uint32_t quantity = quantity_pick(rng);
                CHECK(book.modify(id, price, quantity) == Result::Ok);
                model[id] = {price, quantity};
            }
        }
    }

    CHECK_EQ(book.live_order_count(), model.size());

    std::unordered_map<Price, std::uint64_t> expected_depth;
    for (const auto& [id, state] : model) {
        CHECK(book.contains(id));
        expected_depth[state.first] += state.second;
    }
    for (const auto& [price, quantity] : expected_depth) {
        CHECK_EQ(book.quantity_at(Side::Buy, price), quantity);
    }

    Price expected_best = OrderBook::kNoPrice;
    for (const auto& [price, quantity] : expected_depth) {
        if (quantity > 0 && price > expected_best) {
            expected_best = price;
        }
    }
    CHECK_EQ(book.best_bid(), expected_best);
}

}  // namespace

int main() {
    test_add_and_touch();
    test_rejects_bad_input();
    test_price_time_priority();
    test_digest_detects_queue_order();
    test_modify_moves_levels();
    test_execute_and_cancel();
    test_pool_exhaustion();
    test_index_churn_against_model();
    return llte::test::report("test_order_book");
}
