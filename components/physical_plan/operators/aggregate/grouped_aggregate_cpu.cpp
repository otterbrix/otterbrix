#include "grouped_aggregate.hpp"

#include <algorithm>
#include <type_traits>

namespace components::operators::aggregate {
    namespace {

        template<typename T>
        auto promote(T value) {
            if constexpr (std::is_floating_point_v<T>) {
                return static_cast<double>(value);
            } else if constexpr (std::is_signed_v<T>) {
                return static_cast<int64_t>(value);
            } else {
                return static_cast<uint64_t>(value);
            }
        }

        template<typename T>
        void update_loop(builtin_agg agg,
                         const T* data,
                         const vector::vector_t& vec,
                         const uint32_t* group_ids,
                         uint64_t count,
                         std::pmr::vector<raw_agg_state_t>& states) {
            for (uint64_t row = 0; row < count; row++) {
                if (group_ids[row] == UINT32_MAX || vec.is_null(row)) {
                    continue;
                }

                auto& state = states[group_ids[row]];
                const auto value = promote(data[row]);

                switch (agg) {
                    case builtin_agg::SUM:
                        state.update_sum(value);
                        break;
                    case builtin_agg::MIN:
                        state.update_min(value);
                        break;
                    case builtin_agg::MAX:
                        state.update_max(value);
                        break;
                    case builtin_agg::AVG:
                        state.update_avg(value);
                        break;
                    default:
                        break;
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
            for (uint64_t row = 0; row < count; row++) {
                if (group_ids[row] != UINT32_MAX && !vec.is_null(row)) {
                    states[group_ids[row]].update_count();
                }
            }
            return;
        }

        switch (vec.type().type()) {
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
                break;
        }
    }

    void update_count_star(const uint32_t* group_ids, uint64_t count, std::pmr::vector<raw_agg_state_t>& states) {
        for (uint64_t row = 0; row < count; row++) {
            if (group_ids[row] != UINT32_MAX) {
                states[group_ids[row]].update_count();
            }
        }
    }

    namespace {
        template<typename T>
        void write_value(vector::vector_t& target, uint64_t row, T value) {
            target.set_null(row, false);
            target.data<T>()[row] = value;
        }
    } // namespace

    types::complex_logical_type result_type(builtin_agg agg, types::logical_type col_type) {
        if (agg == builtin_agg::COUNT) {
            return types::complex_logical_type{types::logical_type::UBIGINT};
        }
        return types::complex_logical_type{col_type};
    }

    void write_finalized_state(vector::vector_t& target,
                               uint64_t row,
                               builtin_agg agg,
                               const raw_agg_state_t& state,
                               types::logical_type col_type) {
        if (!state.initialized) {
            target.set_null(row, true);
            return;
        }

        if (agg == builtin_agg::COUNT) {
            write_value(target, row, state.u64);
            return;
        }

        if (agg == builtin_agg::AVG) {
            const double avg = state.count > 0 ? state.f64 / static_cast<double>(state.count) : 0.0;
            switch (col_type) {
                case types::logical_type::TINYINT:
                    write_value(target, row, static_cast<int8_t>(avg));
                    return;
                case types::logical_type::SMALLINT:
                    write_value(target, row, static_cast<int16_t>(avg));
                    return;
                case types::logical_type::INTEGER:
                    write_value(target, row, static_cast<int32_t>(avg));
                    return;
                case types::logical_type::BIGINT:
                    write_value(target, row, static_cast<int64_t>(avg));
                    return;
                case types::logical_type::UTINYINT:
                    write_value(target, row, static_cast<uint8_t>(avg));
                    return;
                case types::logical_type::USMALLINT:
                    write_value(target, row, static_cast<uint16_t>(avg));
                    return;
                case types::logical_type::UINTEGER:
                    write_value(target, row, static_cast<uint32_t>(avg));
                    return;
                case types::logical_type::UBIGINT:
                    write_value(target, row, static_cast<uint64_t>(avg));
                    return;
                case types::logical_type::FLOAT:
                    write_value(target, row, static_cast<float>(avg));
                    return;
                case types::logical_type::DOUBLE:
                    write_value(target, row, avg);
                    return;
                default:
                    target.set_null(row, true);
                    return;
            }
        }

        switch (col_type) {
            case types::logical_type::TINYINT:
                write_value(target, row, static_cast<int8_t>(state.i64));
                return;
            case types::logical_type::SMALLINT:
                write_value(target, row, static_cast<int16_t>(state.i64));
                return;
            case types::logical_type::INTEGER:
                write_value(target, row, static_cast<int32_t>(state.i64));
                return;
            case types::logical_type::BIGINT:
                write_value(target, row, state.i64);
                return;
            case types::logical_type::UTINYINT:
                write_value(target, row, static_cast<uint8_t>(state.u64));
                return;
            case types::logical_type::USMALLINT:
                write_value(target, row, static_cast<uint16_t>(state.u64));
                return;
            case types::logical_type::UINTEGER:
                write_value(target, row, static_cast<uint32_t>(state.u64));
                return;
            case types::logical_type::UBIGINT:
                write_value(target, row, state.u64);
                return;
            case types::logical_type::FLOAT:
                write_value(target, row, static_cast<float>(state.f64));
                return;
            case types::logical_type::DOUBLE:
                write_value(target, row, state.f64);
                return;
            default:
                target.set_null(row, true);
                return;
        }
    }

} // namespace components::operators::aggregate
