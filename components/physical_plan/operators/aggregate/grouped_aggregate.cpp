#include "grouped_aggregate.hpp"

#include <algorithm>
#include <cmath>
#include <limits>

namespace components::operators::aggregate {

    builtin_agg classify(const std::string& func_name) {
        if (func_name == "sum") return builtin_agg::SUM;
        if (func_name == "min") return builtin_agg::MIN;
        if (func_name == "max") return builtin_agg::MAX;
        if (func_name == "count") return builtin_agg::COUNT;
        if (func_name == "avg") return builtin_agg::AVG;
        return builtin_agg::UNKNOWN;
    }

    // --- raw_agg_state_t update methods ---

    void raw_agg_state_t::update_sum(int64_t v) {
        if (!initialized) { i64 = v; initialized = true; } else { i64 += v; }
        count++;
    }
    void raw_agg_state_t::update_sum(uint64_t v) {
        if (!initialized) { u64 = v; initialized = true; } else { u64 += v; }
        count++;
    }
    void raw_agg_state_t::update_sum(double v) {
        if (!initialized) { f64 = v; initialized = true; } else { f64 += v; }
        count++;
    }

    void raw_agg_state_t::update_min(int64_t v) {
        if (!initialized) { i64 = v; initialized = true; } else { i64 = std::min(i64, v); }
        count++;
    }
    void raw_agg_state_t::update_min(uint64_t v) {
        if (!initialized) { u64 = v; initialized = true; } else { u64 = std::min(u64, v); }
        count++;
    }
    void raw_agg_state_t::update_min(double v) {
        if (!initialized) { f64 = v; initialized = true; } else { f64 = std::min(f64, v); }
        count++;
    }

    void raw_agg_state_t::update_max(int64_t v) {
        if (!initialized) { i64 = v; initialized = true; } else { i64 = std::max(i64, v); }
        count++;
    }
    void raw_agg_state_t::update_max(uint64_t v) {
        if (!initialized) { u64 = v; initialized = true; } else { u64 = std::max(u64, v); }
        count++;
    }
    void raw_agg_state_t::update_max(double v) {
        if (!initialized) { f64 = v; initialized = true; } else { f64 = std::max(f64, v); }
        count++;
    }

    void raw_agg_state_t::update_count() {
        if (!initialized) { u64 = 1; initialized = true; } else { u64++; }
        count++;
    }

    void raw_agg_state_t::update_avg(int64_t v) {
        // Accumulate sum as double for avg
        if (!initialized) { f64 = static_cast<double>(v); initialized = true; } else { f64 += static_cast<double>(v); }
        count++;
    }
    void raw_agg_state_t::update_avg(uint64_t v) {
        if (!initialized) { f64 = static_cast<double>(v); initialized = true; } else { f64 += static_cast<double>(v); }
        count++;
    }
    void raw_agg_state_t::update_avg(double v) {
        if (!initialized) { f64 = v; initialized = true; } else { f64 += v; }
        count++;
    }

    // --- Type-dispatched update loop ---

    namespace {

        // Promote small types to int64_t/uint64_t/double for state operations
        template<typename T>
        auto promote(T v) {
            if constexpr (std::is_floating_point_v<T>) {
                return static_cast<double>(v);
            } else if constexpr (std::is_signed_v<T>) {
                return static_cast<int64_t>(v);
            } else {
                return static_cast<uint64_t>(v);
            }
        }

        template<typename T>
        void update_loop(builtin_agg agg,
                         const T* data,
                         const vector::vector_t& vec,
                         const uint32_t* group_ids,
                         uint64_t count,
                         std::pmr::vector<raw_agg_state_t>& states) {
            for (uint64_t i = 0; i < count; i++) {
                if (vec.is_null(i)) continue;
                auto& st = states[group_ids[i]];
                auto v = promote(data[i]);
                switch (agg) {
                    case builtin_agg::SUM: st.update_sum(v); break;
                    case builtin_agg::MIN: st.update_min(v); break;
                    case builtin_agg::MAX: st.update_max(v); break;
                    case builtin_agg::AVG: st.update_avg(v); break;
                    default: break;
                }
            }
        }

    } // namespace

    void update_all(builtin_agg agg,
                    const vector::vector_t& vec,
                    const uint32_t* group_ids,
                    uint64_t count,
                    std::pmr::vector<raw_agg_state_t>& states) {
        if (agg == builtin_agg::COUNT) {
            // COUNT doesn't need column data, just count non-null rows
            for (uint64_t i = 0; i < count; i++) {
                if (!vec.is_null(i)) {
                    states[group_ids[i]].update_count();
                }
            }
            return;
        }

        auto type = vec.type().type();
        switch (type) {
            case types::logical_type::TINYINT:
                update_loop<int8_t>(agg, vec.data<int8_t>(), vec, group_ids, count, states);
                break;
            case types::logical_type::SMALLINT:
                update_loop<int16_t>(agg, vec.data<int16_t>(), vec, group_ids, count, states);
                break;
            case types::logical_type::INTEGER:
                update_loop<int32_t>(agg, vec.data<int32_t>(), vec, group_ids, count, states);
                break;
            case types::logical_type::BIGINT:
                update_loop<int64_t>(agg, vec.data<int64_t>(), vec, group_ids, count, states);
                break;
            case types::logical_type::UTINYINT:
                update_loop<uint8_t>(agg, vec.data<uint8_t>(), vec, group_ids, count, states);
                break;
            case types::logical_type::USMALLINT:
                update_loop<uint16_t>(agg, vec.data<uint16_t>(), vec, group_ids, count, states);
                break;
            case types::logical_type::UINTEGER:
                update_loop<uint32_t>(agg, vec.data<uint32_t>(), vec, group_ids, count, states);
                break;
            case types::logical_type::UBIGINT:
                update_loop<uint64_t>(agg, vec.data<uint64_t>(), vec, group_ids, count, states);
                break;
            case types::logical_type::FLOAT:
                update_loop<float>(agg, vec.data<float>(), vec, group_ids, count, states);
                break;
            case types::logical_type::DOUBLE:
                update_loop<double>(agg, vec.data<double>(), vec, group_ids, count, states);
                break;
            default:
                break; // unsupported type — caller should fall back
        }
    }

    types::logical_value_t finalize_state(std::pmr::memory_resource* resource,
                                          builtin_agg agg,
                                          const raw_agg_state_t& state,
                                          types::logical_type col_type) {
        if (!state.initialized) {
            return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
        }

        if (agg == builtin_agg::COUNT) {
            return types::logical_value_t(resource, state.u64);
        }

        if (agg == builtin_agg::AVG) {
            double avg = state.count > 0 ? state.f64 / static_cast<double>(state.count) : 0.0;
            // Return in the original column type (matches existing kernel behavior)
            switch (col_type) {
                case types::logical_type::TINYINT:
                    return types::logical_value_t(resource, static_cast<int8_t>(avg));
                case types::logical_type::SMALLINT:
                    return types::logical_value_t(resource, static_cast<int16_t>(avg));
                case types::logical_type::INTEGER:
                    return types::logical_value_t(resource, static_cast<int32_t>(avg));
                case types::logical_type::BIGINT:
                    return types::logical_value_t(resource, static_cast<int64_t>(avg));
                case types::logical_type::UTINYINT:
                    return types::logical_value_t(resource, static_cast<uint8_t>(avg));
                case types::logical_type::USMALLINT:
                    return types::logical_value_t(resource, static_cast<uint16_t>(avg));
                case types::logical_type::UINTEGER:
                    return types::logical_value_t(resource, static_cast<uint32_t>(avg));
                case types::logical_type::UBIGINT:
                    return types::logical_value_t(resource, static_cast<uint64_t>(avg));
                case types::logical_type::FLOAT:
                    return types::logical_value_t(resource, static_cast<float>(avg));
                case types::logical_type::DOUBLE:
                    return types::logical_value_t(resource, avg);
                default:
                    return types::logical_value_t(resource, avg);
            }
        }

        // SUM, MIN, MAX — return in the original column type
        switch (col_type) {
            case types::logical_type::TINYINT:
                return types::logical_value_t(resource, static_cast<int8_t>(state.i64));
            case types::logical_type::SMALLINT:
                return types::logical_value_t(resource, static_cast<int16_t>(state.i64));
            case types::logical_type::INTEGER:
                return types::logical_value_t(resource, static_cast<int32_t>(state.i64));
            case types::logical_type::BIGINT:
                return types::logical_value_t(resource, state.i64);
            case types::logical_type::UTINYINT:
                return types::logical_value_t(resource, static_cast<uint8_t>(state.u64));
            case types::logical_type::USMALLINT:
                return types::logical_value_t(resource, static_cast<uint16_t>(state.u64));
            case types::logical_type::UINTEGER:
                return types::logical_value_t(resource, static_cast<uint32_t>(state.u64));
            case types::logical_type::UBIGINT:
                return types::logical_value_t(resource, state.u64);
            case types::logical_type::FLOAT:
                return types::logical_value_t(resource, static_cast<float>(state.f64));
            case types::logical_type::DOUBLE:
                return types::logical_value_t(resource, state.f64);
            default:
                return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
        }
    }

} // namespace components::operators::aggregate
