#include "llte/order_book.hpp"

#include <random>
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

void test_price_time_priority() {
    OrderBook book = make_book();
    book.add(1, Side::Buy, 18700, 100);
    book.add(2, Side::Buy, 18700, 200);
    book.add(3, Side::Buy, 18700, 300);
    CHECK_EQ(book.order_count_at(Side::Buy, 18700), 3u);
    CHECK_EQ(book.quantity_at(Side::Buy, 18700), 600u);

    // Shrinking in place must keep the order at the front of the queue.
    CHECK(book.modify(1, 18700, 50) == Result::Ok);
    CHECK_EQ(book.quantity_at(Side::Buy, 18700), 550u);
    CHECK_EQ(book.order_count_at(Side::Buy, 18700), 3u);

    // A size increase gives up time priority but keeps the level consistent.
    CHECK(book.modify(1, 18700, 400) == Result::Ok);
    CHECK_EQ(book.quantity_at(Side::Buy, 18700), 900u);
    CHECK_EQ(book.order_count_at(Side::Buy, 18700), 3u);
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

    // An oversized fill also removes it rather than underflowing the level.
    CHECK(book.execute(2, 999) == Result::Ok);
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
    test_modify_moves_levels();
    test_execute_and_cancel();
    test_pool_exhaustion();
    test_index_churn_against_model();
    return llte::test::report("test_order_book");
}
