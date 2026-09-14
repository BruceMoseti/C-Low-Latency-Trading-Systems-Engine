#pragma once

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

namespace llte {

// Fixed-capacity sample buffer. Capacity is reserved up front so recording a
// sample never allocates; once full, further samples are counted but dropped so
// the measurement itself cannot perturb the run.
class LatencySamples {
public:
    struct Summary {
        std::uint64_t count = 0;
        std::uint64_t min = 0;
        std::uint64_t p50 = 0;
        std::uint64_t p90 = 0;
        std::uint64_t p95 = 0;
        std::uint64_t p99 = 0;
        std::uint64_t p999 = 0;
        std::uint64_t max = 0;
        double mean = 0.0;
    };

    LatencySamples() = default;
    explicit LatencySamples(std::size_t capacity) { reserve(capacity); }

    void reserve(std::size_t capacity) { samples_.reserve(capacity); }

    void add(std::uint64_t nanoseconds) {
        if (samples_.size() < samples_.capacity()) {
            samples_.push_back(nanoseconds);
        } else {
            dropped_ += 1;
        }
    }

    std::size_t size() const { return samples_.size(); }
    std::uint64_t dropped() const { return dropped_; }
    const std::vector<std::uint64_t>& raw() const { return samples_; }

    Summary summarize() {
        Summary summary;
        if (samples_.empty()) {
            return summary;
        }
        std::sort(samples_.begin(), samples_.end());
        summary.count = samples_.size();
        summary.min = samples_.front();
        summary.max = samples_.back();
        summary.p50 = percentile(0.50);
        summary.p90 = percentile(0.90);
        summary.p95 = percentile(0.95);
        summary.p99 = percentile(0.99);
        summary.p999 = percentile(0.999);

        long double total = 0.0L;
        for (std::uint64_t value : samples_) {
            total += static_cast<long double>(value);
        }
        summary.mean = static_cast<double>(total / static_cast<long double>(samples_.size()));
        return summary;
    }

private:
    // Call only after summarize() has sorted the buffer.
    std::uint64_t percentile(double fraction) const {
        const auto last = static_cast<double>(samples_.size() - 1);
        auto index = static_cast<std::size_t>(fraction * last + 0.5);
        return samples_[std::min(index, samples_.size() - 1)];
    }

    std::vector<std::uint64_t> samples_;
    std::uint64_t dropped_ = 0;
};

inline void print_summary_header() {
    std::printf("%-26s %10s %9s %9s %9s %9s %9s %9s\n", "stage", "count", "p50", "p90", "p95",
                "p99", "p99.9", "max");
    std::printf("%-26s %10s %9s %9s %9s %9s %9s %9s\n", "--------------------------",
                "----------", "---------", "---------", "---------", "---------", "---------",
                "---------");
}

inline void print_summary_row(const std::string& label, const LatencySamples::Summary& s) {
    auto us = [](std::uint64_t ns) { return static_cast<double>(ns) / 1000.0; };
    std::printf("%-26s %10llu %8.3fu %8.3fu %8.3fu %8.3fu %8.3fu %8.3fu\n", label.c_str(),
                static_cast<unsigned long long>(s.count), us(s.p50), us(s.p90), us(s.p95),
                us(s.p99), us(s.p999), us(s.max));
}

}  // namespace llte
