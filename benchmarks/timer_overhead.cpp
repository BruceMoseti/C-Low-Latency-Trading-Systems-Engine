// Measures the instrumentation's own cost, which is the floor under every stage
// latency this project reports.
//
// The pipeline timestamps each hop with clock_gettime(CLOCK_MONOTONIC). Two
// back-to-back reads therefore give the smallest interval the instrumentation can
// resolve: any stage whose measured time is close to this number is reporting the
// cost of being measured rather than the cost of the work.

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <vector>

#include "llte/args.hpp"
#include "llte/clock.hpp"
#include "llte/cpu.hpp"
#include "llte/latency_stats.hpp"

int main(int argc, char** argv) {
    const llte::Args args(argc, argv);
    if (args.has("help")) {
        std::printf("usage: timer_overhead [--samples N] [--cpu N]\n");
        return 0;
    }

    const auto samples = static_cast<std::size_t>(args.integer("samples", 2'000'000));
    llte::pin_to_cpu(static_cast<int>(args.integer("cpu", -1)));

    llte::LatencySamples pair_cost(samples);
    for (std::size_t i = 0; i < samples; ++i) {
        const std::uint64_t before = llte::now_ns();
        const std::uint64_t after = llte::now_ns();
        pair_cost.add(after - before);
    }

    const auto summary = pair_cost.summarize();
    std::printf("timer_overhead: %zu back-to-back clock_gettime(CLOCK_MONOTONIC) pairs\n\n",
                samples);
    std::printf("%-34s %8s %8s %8s %8s\n", "measurement", "min", "p50", "p90", "p99");
    std::printf("%-34s %8s %8s %8s %8s\n", "----------------------------------", "--------",
                "--------", "--------", "--------");
    std::printf("%-34s %6llu ns %6llu ns %6llu ns %6llu ns\n", "cost of reading the clock twice",
                static_cast<unsigned long long>(summary.min),
                static_cast<unsigned long long>(summary.p50),
                static_cast<unsigned long long>(summary.p90),
                static_cast<unsigned long long>(summary.p99));
    std::printf(
        "\nAny stage reported at or near %llu ns is at the measurement floor: the number\n"
        "describes the instrumentation, not the work.\n",
        static_cast<unsigned long long>(summary.p50));
    return 0;
}
