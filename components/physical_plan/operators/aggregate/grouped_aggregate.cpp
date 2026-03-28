#include "grouped_aggregate.hpp"

#include <algorithm>

namespace components::operators::aggregate {

    builtin_agg classify(const std::string& func_name) {
        if (func_name == "sum")
            return builtin_agg::SUM;
        if (func_name == "min")
            return builtin_agg::MIN;
        if (func_name == "max")
            return builtin_agg::MAX;
        if (func_name == "count")
            return builtin_agg::COUNT;
        if (func_name == "avg")
            return builtin_agg::AVG;
        return builtin_agg::UNKNOWN;
    }

    void raw_agg_state_t::update_sum(int64_t v) {
        if (!initialized) {
            i64 = v;
            initialized = true;
        } else {
            i64 += v;
        }
        count++;
    }

    void raw_agg_state_t::update_sum(uint64_t v) {
        if (!initialized) {
            u64 = v;
            initialized = true;
        } else {
            u64 += v;
        }
        count++;
    }

    void raw_agg_state_t::update_sum(double v) {
        if (!initialized) {
            f64 = v;
            initialized = true;
        } else {
            f64 += v;
        }
        count++;
    }

    void raw_agg_state_t::update_min(int64_t v) {
        if (!initialized) {
            i64 = v;
            initialized = true;
        } else {
            i64 = std::min(i64, v);
        }
        count++;
    }

    void raw_agg_state_t::update_min(uint64_t v) {
        if (!initialized) {
            u64 = v;
            initialized = true;
        } else {
            u64 = std::min(u64, v);
        }
        count++;
    }

    void raw_agg_state_t::update_min(double v) {
        if (!initialized) {
            f64 = v;
            initialized = true;
        } else {
            f64 = std::min(f64, v);
        }
        count++;
    }

    void raw_agg_state_t::update_max(int64_t v) {
        if (!initialized) {
            i64 = v;
            initialized = true;
        } else {
            i64 = std::max(i64, v);
        }
        count++;
    }

    void raw_agg_state_t::update_max(uint64_t v) {
        if (!initialized) {
            u64 = v;
            initialized = true;
        } else {
            u64 = std::max(u64, v);
        }
        count++;
    }

    void raw_agg_state_t::update_max(double v) {
        if (!initialized) {
            f64 = v;
            initialized = true;
        } else {
            f64 = std::max(f64, v);
        }
        count++;
    }

    void raw_agg_state_t::update_count() {
        if (!initialized) {
            u64 = 1;
            initialized = true;
        } else {
            u64++;
        }
        count++;
    }

    void raw_agg_state_t::update_avg(int64_t v) {
        if (!initialized) {
            f64 = static_cast<double>(v);
            initialized = true;
        } else {
            f64 += static_cast<double>(v);
        }
        count++;
    }

    void raw_agg_state_t::update_avg(uint64_t v) {
        if (!initialized) {
            f64 = static_cast<double>(v);
            initialized = true;
        } else {
            f64 += static_cast<double>(v);
        }
        count++;
    }

    void raw_agg_state_t::update_avg(double v) {
        if (!initialized) {
            f64 = v;
            initialized = true;
        } else {
            f64 += v;
        }
        count++;
    }

} // namespace components::operators::aggregate
