#pragma once

#include <ctime>
#include <cstdint>

namespace llte {

// CLOCK_MONOTONIC is system-wide on Linux, so timestamps taken in the exchange,
// the market-data handler and the book engine are directly comparable.
inline std::uint64_t now_ns() {
    timespec ts{};
    ::clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<std::uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
           static_cast<std::uint64_t>(ts.tv_nsec);
}

}  // namespace llte
