#include "grouped_aggregate.hpp"

#include <algorithm>

namespace components::operators::aggregate {

    namespace {
        using value_kind_t = raw_agg_state_t::value_kind_t;

        void ensure_value_kind(raw_agg_state_t& state, value_kind_t kind) {
            if (!state.initialized) {
                state.value_kind = kind;
            }
        }
    } // namespace

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
        ensure_value_kind(*this, value_kind_t::signed_integer);
        if (!initialized) {
            i64 = v;
            initialized = true;
        } else {
            i64 += v;
        }
        count++;
    }

    void raw_agg_state_t::update_sum(uint64_t v) {
        ensure_value_kind(*this, value_kind_t::unsigned_integer);
        if (!initialized) {
            u64 = v;
            initialized = true;
        } else {
            u64 += v;
        }
        count++;
    }

    void raw_agg_state_t::update_sum(double v) {
        ensure_value_kind(*this, value_kind_t::floating_point);
        if (!initialized) {
            f64 = v;
            initialized = true;
        } else {
            f64 += v;
        }
        count++;
    }

    void raw_agg_state_t::update_min(int64_t v) {
        ensure_value_kind(*this, value_kind_t::signed_integer);
        if (!initialized) {
            i64 = v;
            initialized = true;
        } else {
            i64 = std::min(i64, v);
        }
        count++;
    }

    void raw_agg_state_t::update_min(uint64_t v) {
        ensure_value_kind(*this, value_kind_t::unsigned_integer);
        if (!initialized) {
            u64 = v;
            initialized = true;
        } else {
            u64 = std::min(u64, v);
        }
        count++;
    }

    void raw_agg_state_t::update_min(double v) {
        ensure_value_kind(*this, value_kind_t::floating_point);
        if (!initialized) {
            f64 = v;
            initialized = true;
        } else {
            f64 = std::min(f64, v);
        }
        count++;
    }

    void raw_agg_state_t::update_max(int64_t v) {
        ensure_value_kind(*this, value_kind_t::signed_integer);
        if (!initialized) {
            i64 = v;
            initialized = true;
        } else {
            i64 = std::max(i64, v);
        }
        count++;
    }

    void raw_agg_state_t::update_max(uint64_t v) {
        ensure_value_kind(*this, value_kind_t::unsigned_integer);
        if (!initialized) {
            u64 = v;
            initialized = true;
        } else {
            u64 = std::max(u64, v);
        }
        count++;
    }

    void raw_agg_state_t::update_max(double v) {
        ensure_value_kind(*this, value_kind_t::floating_point);
        if (!initialized) {
            f64 = v;
            initialized = true;
        } else {
            f64 = std::max(f64, v);
        }
        count++;
    }

    void raw_agg_state_t::update_count() {
        ensure_value_kind(*this, value_kind_t::unsigned_integer);
        if (!initialized) {
            u64 = 1;
            initialized = true;
        } else {
            u64++;
        }
        count++;
    }

    void raw_agg_state_t::update_avg(int64_t v) {
        ensure_value_kind(*this, value_kind_t::floating_point);
        if (!initialized) {
            f64 = static_cast<double>(v);
            initialized = true;
        } else {
            f64 += static_cast<double>(v);
        }
        count++;
    }

    void raw_agg_state_t::update_avg(uint64_t v) {
        ensure_value_kind(*this, value_kind_t::floating_point);
        if (!initialized) {
            f64 = static_cast<double>(v);
            initialized = true;
        } else {
            f64 += static_cast<double>(v);
        }
        count++;
    }

    void raw_agg_state_t::update_avg(double v) {
        ensure_value_kind(*this, value_kind_t::floating_point);
        if (!initialized) {
            f64 = v;
            initialized = true;
        } else {
            f64 += v;
        }
        count++;
    }

    void raw_agg_state_t::merge_sum(const raw_agg_state_t& source) {
        switch (value_kind) {
            case value_kind_t::signed_integer:
                i64 += source.i64;
                break;
            case value_kind_t::unsigned_integer:
                u64 += source.u64;
                break;
            case value_kind_t::floating_point:
                f64 += source.f64;
                break;
            case value_kind_t::none:
                break;
        }
        count += source.count;
    }

    void raw_agg_state_t::merge_min(const raw_agg_state_t& source) {
        switch (value_kind) {
            case value_kind_t::signed_integer:
                i64 = std::min(i64, source.i64);
                break;
            case value_kind_t::unsigned_integer:
                u64 = std::min(u64, source.u64);
                break;
            case value_kind_t::floating_point:
                f64 = std::min(f64, source.f64);
                break;
            case value_kind_t::none:
                break;
        }
        count += source.count;
    }

    void raw_agg_state_t::merge_max(const raw_agg_state_t& source) {
        switch (value_kind) {
            case value_kind_t::signed_integer:
                i64 = std::max(i64, source.i64);
                break;
            case value_kind_t::unsigned_integer:
                u64 = std::max(u64, source.u64);
                break;
            case value_kind_t::floating_point:
                f64 = std::max(f64, source.f64);
                break;
            case value_kind_t::none:
                break;
        }
        count += source.count;
    }

    void raw_agg_state_t::merge_count(const raw_agg_state_t& source) {
        u64 += source.u64;
        count += source.count;
    }

    void raw_agg_state_t::merge_avg(const raw_agg_state_t& source) {
        f64 += source.f64;
        count += source.count;
    }

    void merge_state(builtin_agg agg, raw_agg_state_t& target, const raw_agg_state_t& source) {
        if (!source.initialized) {
            return;
        }

        if (!target.initialized) {
            target = source;
            return;
        }

        switch (agg) {
            case builtin_agg::COUNT:
                target.merge_count(source);
                return;
            case builtin_agg::AVG:
                target.merge_avg(source);
                return;
            case builtin_agg::SUM:
                target.merge_sum(source);
                return;
            case builtin_agg::MIN:
                target.merge_min(source);
                return;
            case builtin_agg::MAX:
                target.merge_max(source);
                return;
            case builtin_agg::UNKNOWN:
                return;
        }
    }

} // namespace components::operators::aggregate
