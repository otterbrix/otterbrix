#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include <integration/cpp/wrapper_dispatcher.hpp>

#include "benchmark_configuration.hpp"

namespace otterbrix::benchmark {

using components::session::session_id_t;

struct benchmark_state_t {
    wrapper_dispatcher_t* dispatcher = nullptr;
    session_id_t session;
    bool failed = false;
    benchmark_io_options_t io;
};

struct benchmark_result_t {
    std::string name;
    std::string group;
    uint64_t nruns = 0;
    std::vector<double> timings_ms;
    bool verified = true;
    std::string error;

    double min_ms() const {
        if (timings_ms.empty()) return 0.0;
        return *std::min_element(timings_ms.begin(), timings_ms.end());
    }

    double max_ms() const {
        if (timings_ms.empty()) return 0.0;
        return *std::max_element(timings_ms.begin(), timings_ms.end());
    }

    double avg_ms() const {
        if (timings_ms.empty()) return 0.0;
        return std::accumulate(timings_ms.begin(), timings_ms.end(), 0.0) /
               static_cast<double>(timings_ms.size());
    }

    double median_ms() const {
        if (timings_ms.empty()) return 0.0;
        auto sorted = timings_ms;
        std::sort(sorted.begin(), sorted.end());
        auto n = sorted.size();
        if (n % 2 == 0) {
            return (sorted[n / 2 - 1] + sorted[n / 2]) / 2.0;
        }
        return sorted[n / 2];
    }

    double quantile_ms(double percentile) const {
        if (timings_ms.empty()) return 0.0;
        if (percentile <= 0.0) return min_ms();
        if (percentile >= 100.0) return max_ms();
        auto sorted = timings_ms;
        std::sort(sorted.begin(), sorted.end());
        const double pos = (percentile / 100.0) * static_cast<double>(sorted.size() - 1);
        const auto lo = static_cast<size_t>(pos);
        const auto hi = std::min(lo + 1, sorted.size() - 1);
        const double frac = pos - static_cast<double>(lo);
        return sorted[lo] * (1.0 - frac) + sorted[hi] * frac;
    }
};

class benchmark_t {
public:
    virtual ~benchmark_t() = default;

    virtual std::string name() const = 0;
    virtual std::string group() const = 0;
    virtual std::string description() const = 0;
    virtual std::string query() const = 0;

    virtual void load(benchmark_state_t& state) = 0;
    virtual void run(benchmark_state_t& state) = 0;
    virtual void cleanup(benchmark_state_t& /*state*/) {}
    virtual std::string verify(benchmark_state_t& /*state*/) { return ""; }

    virtual uint64_t nruns() const { return 5; }
    virtual uint64_t timeout_seconds() const { return 30; }
};

} // namespace otterbrix::benchmark
