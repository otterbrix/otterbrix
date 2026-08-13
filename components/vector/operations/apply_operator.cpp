#include "apply_operator.hpp"

#include <components/types/operations_helper.hpp>
#include <core/date/date_types.hpp>

#include <cassert>
#include <cmath>
#include <type_traits>

namespace components::vector::operations {

    using operators::operator_code;

    namespace {

        template<typename T>
        inline constexpr bool is_number = (std::is_arithmetic_v<T> && !std::is_same_v<T, bool>) ||
                                          std::is_same_v<T, types::int128_t> || std::is_same_v<T, types::uint128_t>;

        template<typename T>
        inline constexpr bool
            is_integral_value = (std::is_integral_v<T> && !std::is_same_v<T, bool>) ||
                                std::is_same_v<T, types::int128_t> || std::is_same_v<T, types::uint128_t>;

        template<typename T>
        inline constexpr bool is_decimal_storage = (std::is_integral_v<T> && std::is_signed_v<T> &&
                                                    !std::is_same_v<T, bool>) ||
                                                   std::is_same_v<T, types::int128_t>;

        core::error_t shift_out_of_range(std::pmr::memory_resource* resource) {
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string{"shift count is out of range", resource});
        }

        template<typename T>
        bool is_zero(T value) {
            if constexpr (std::is_floating_point_v<T>) {
                return std::fpclassify(value) == FP_ZERO;
            } else {
                return value == T{};
            }
        }

        // A CASE arm runs only over the rows its WHEN selected. `active` is one bool per row, or
        // null when unconstrained; a skipped row is left exactly as it was, so cardinality never
        // changes. This is what keeps a guarded `10 / x` from dividing on the rows the guard
        // excluded, while the rows that DO select the arm still raise on a zero divisor.
        inline bool skip_row(const bool* active, uint64_t row) noexcept { return active != nullptr && !active[row]; }

        // SQL equality on floats is exact comparison; so we explicitly suppress warning here
        template<typename T>
        bool equals(const T& lhs, const T& rhs) {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfloat-equal"
            return lhs == rhs;
#pragma GCC diagnostic pop
        }

        template<typename T>
        void write(vector_t* output, uint64_t row, T value) {
            output->set_null(row, false);
            output->set_value(row, std::move(value));
        }

        template<typename T>
        core::error_t arithmetic_rows(operator_code code,
                                      const vector_t& left,
                                      const vector_t& right,
                                      vector_t* output,
                                      uint64_t count,
                                      const bool* active) {
            for (uint64_t row = 0; row < count; row++) {
                if (skip_row(active, row)) {
                    continue;
                }
                if (left.is_null(row) || right.is_null(row)) {
                    output->set_null(row, true);
                    continue;
                }
                const T lhs = left.get_value<T>(row);
                const T rhs = right.get_value<T>(row);
                switch (code) {
                    case operator_code::add:
                        write<T>(output, row, static_cast<T>(lhs + rhs));
                        break;
                    case operator_code::subtract:
                        write<T>(output, row, static_cast<T>(lhs - rhs));
                        break;
                    case operator_code::multiply:
                        write<T>(output, row, static_cast<T>(lhs * rhs));
                        break;
                    case operator_code::divide:
                        if (is_zero(rhs)) {
                            return core::error_t(core::error_code_t::invalid_parameter,
                                                 std::pmr::string{"division by zero", output->resource()});
                        }
                        write<T>(output, row, static_cast<T>(lhs / rhs));
                        break;
                    case operator_code::mod:
                        if constexpr (std::is_floating_point_v<T>) {
                            // Not data-dependent: no float value of the operand makes % meaningful.
                            return core::error_t(
                                core::error_code_t::invalid_parameter,
                                std::pmr::string{"operator does not accept modulus on floating point types",
                                                 output->resource()});
                        } else {
                            if (is_zero(rhs)) {
                                return core::error_t(core::error_code_t::invalid_parameter,
                                                     std::pmr::string{"division by zero", output->resource()});
                            }
                            write<T>(output, row, static_cast<T>(lhs % rhs));
                        }
                        break;
                    default:
                        return core::error_t(
                            core::error_code_t::invalid_parameter,
                            std::pmr::string{"operator encountered unsupported operator code", output->resource()});
                }
            }
            return core::error_t::no_error();
        }

        template<typename T>
        core::error_t compare_rows(operator_code code,
                                   const vector_t& left,
                                   const vector_t& right,
                                   vector_t* output,
                                   uint64_t count,
                                   const bool* active) {
            for (uint64_t row = 0; row < count; row++) {
                if (skip_row(active, row)) {
                    continue;
                }
                // Comparing against a null is UNKNOWN (encoded as null value)
                if (left.is_null(row) || right.is_null(row)) {
                    output->set_null(row, true);
                    continue;
                }
                const T lhs = left.get_value<T>(row);
                const T rhs = right.get_value<T>(row);
                switch (code) {
                    case operator_code::equal:
                        write<bool>(output, row, equals(lhs, rhs));
                        break;
                    case operator_code::not_equal:
                        write<bool>(output, row, !equals(lhs, rhs));
                        break;
                    case operator_code::less:
                        write<bool>(output, row, lhs < rhs);
                        break;
                    case operator_code::less_equal:
                        write<bool>(output, row, lhs <= rhs);
                        break;
                    case operator_code::greater:
                        write<bool>(output, row, lhs > rhs);
                        break;
                    case operator_code::greater_equal:
                        write<bool>(output, row, lhs >= rhs);
                        break;
                    default:
                        return core::error_t(
                            core::error_code_t::invalid_parameter,
                            std::pmr::string{"compare_rows encountered unsupported operator code", output->resource()});
                }
            }
            return core::error_t::no_error();
        }

        core::error_t logical_rows(operator_code code,
                                   const vector_t& left,
                                   const vector_t& right,
                                   vector_t* output,
                                   uint64_t count,
                                   const bool* active) {
            if (left.type().to_physical_type() != types::physical_type::BOOL ||
                right.type().to_physical_type() != types::physical_type::BOOL) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"compare_rows encountered unsupported type", output->resource()});
            }
            for (uint64_t row = 0; row < count; row++) {
                if (skip_row(active, row)) {
                    continue;
                }
                const bool left_unknown = left.is_null(row);
                const bool right_unknown = right.is_null(row);
                const bool lhs = !left_unknown && left.get_value<bool>(row);
                const bool rhs = !right_unknown && right.get_value<bool>(row);

                if (code == operator_code::logical_and) {
                    if ((!left_unknown && !lhs) || (!right_unknown && !rhs)) {
                        write<bool>(output, row, false);
                    } else if (left_unknown || right_unknown) {
                        output->set_null(row, true);
                    } else {
                        write<bool>(output, row, true);
                    }
                } else {
                    if ((!left_unknown && lhs) || (!right_unknown && rhs)) {
                        write<bool>(output, row, true);
                    } else if (left_unknown || right_unknown) {
                        output->set_null(row, true);
                    } else {
                        write<bool>(output, row, false);
                    }
                }
            }
            return core::error_t::no_error();
        }

        template<typename Fn>
        core::error_t each_row(const vector_t& left,
                               const vector_t& right,
                               vector_t* output,
                               uint64_t count,
                               const bool* active,
                               Fn&& compute) {
            for (uint64_t row = 0; row < count; row++) {
                if (skip_row(active, row)) {
                    continue;
                }
                if (left.is_null(row) || right.is_null(row)) {
                    output->set_null(row, true);
                    continue;
                }
                output->set_null(row, false);
                compute(row);
            }
            return core::error_t::no_error();
        }

        template<typename T>
        core::error_t bitwise_rows(operator_code code,
                                   const vector_t& left,
                                   const vector_t& right,
                                   vector_t* output,
                                   uint64_t count,
                                   const bool* active) {
            if constexpr (!is_integral_value<T>) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"bitwise_rows encountered unsupported type", output->resource()});
            } else {
                core::error_t failure = core::error_t::no_error();
                auto error = each_row(left, right, output, count, active, [&](uint64_t row) {
                    if (failure.contains_error()) {
                        return;
                    }
                    const T lhs = left.get_value<T>(row);
                    const T rhs = right.get_value<T>(row);
                    switch (code) {
                        case operator_code::bit_and:
                            write<T>(output, row, static_cast<T>(lhs & rhs));
                            break;
                        case operator_code::bit_or:
                            write<T>(output, row, static_cast<T>(lhs | rhs));
                            break;
                        case operator_code::bit_xor:
                            write<T>(output, row, static_cast<T>(lhs ^ rhs));
                            break;
                        case operator_code::shift_left:
                        case operator_code::shift_right: {
                            // TODO: setup should be done outside the loop
                            // absl::int128 takes its shift amount as an int
                            const auto amount = static_cast<int64_t>(rhs);
                            if (amount < 0 || amount >= static_cast<int64_t>(sizeof(T) * 8)) {
                                failure = shift_out_of_range(output->resource());
                                return;
                            }
                            const int places = static_cast<int>(amount);
                            write<T>(output,
                                     row,
                                     code == operator_code::shift_left ? static_cast<T>(lhs << places)
                                                                       : static_cast<T>(lhs >> places));
                            break;
                        }
                        default:
                            failure = core::error_t(
                                core::error_code_t::invalid_parameter,
                                std::pmr::string{"compare_rows encountered unsupported operation", output->resource()});
                            return;
                    }
                });
                return failure.contains_error() ? failure : error;
            }
        }

        constexpr bool is_bitwise(operator_code code) noexcept {
            switch (code) {
                case operator_code::bit_and:
                case operator_code::bit_or:
                case operator_code::bit_xor:
                case operator_code::shift_left:
                case operator_code::shift_right:
                    return true;
                default:
                    return false;
            }
        }

        bool is_decimal(const vector_t& vector) noexcept {
            return vector.type().type() == types::logical_type::DECIMAL;
        }

        uint8_t decimal_scale(const vector_t& vector) noexcept {
            return vector.type().extension_as<types::decimal_logical_type_extension>()->scale();
        }

        uint8_t decimal_width(const vector_t& vector) noexcept {
            return vector.type().extension_as<types::decimal_logical_type_extension>()->width();
        }

        bool multiplication_overflows(types::int128_t lhs, types::int128_t rhs) noexcept {
            if (lhs == 0 || rhs == 0) {
                return false;
            }
            const types::int128_t product = lhs * rhs;
            return product / rhs != lhs;
        }

        template<typename T>
        core::error_t decimal_scaled_rows(operator_code code,
                                          const vector_t& left,
                                          const vector_t& right,
                                          vector_t* output,
                                          uint8_t width,
                                          uint8_t scale,
                                          uint64_t count,
                                          const bool* active) {
            if constexpr (!is_decimal_storage<T>) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"decimal_scaled_rows received non-decimal type", output->resource()});
            } else {
                types::int128_t factor = types::POWERS_OF_TEN[scale];
                types::int128_t limit = types::POWERS_OF_TEN[width];
                core::error_t failure = core::error_t::no_error();
                auto error = each_row(left, right, output, count, active, [&](uint64_t row) {
                    if (failure.contains_error()) {
                        return;
                    }
                    const auto lhs = static_cast<types::int128_t>(left.get_value<T>(row));
                    const auto rhs = static_cast<types::int128_t>(right.get_value<T>(row));
                    types::int128_t scaled = 0;
                    if (code == operator_code::multiply) {
                        if (multiplication_overflows(lhs, rhs)) {
                            failure = core::error_t(core::error_code_t::invalid_parameter,
                                                    std::pmr::string{"decimal overflow", output->resource()});
                            return;
                        }
                        scaled = lhs * rhs / factor;
                    } else {
                        if (rhs == 0) {
                            failure = core::error_t(core::error_code_t::invalid_parameter,
                                                    std::pmr::string{"division by zero", output->resource()});
                            return;
                        }
                        if (multiplication_overflows(lhs, factor)) {
                            failure = core::error_t(core::error_code_t::invalid_parameter,
                                                    std::pmr::string{"decimal overflow", output->resource()});
                            return;
                        }
                        scaled = lhs * factor / rhs;
                    }
                    if (scaled >= limit || scaled <= -limit) {
                        failure = core::error_t(core::error_code_t::invalid_parameter,
                                                std::pmr::string{"decimal overflow", output->resource()});
                        return;
                    }
                    write<T>(output, row, static_cast<T>(scaled));
                });
                return failure.contains_error() ? failure : error;
            }
        }

        template<typename...>
        struct decimal_dispatch {
            template<typename T>
            core::error_t operator()(operator_code code,
                                     const vector_t& left,
                                     const vector_t& right,
                                     vector_t* output,
                                     uint8_t width,
                                     uint8_t scale,
                                     uint64_t count,
                                     const bool* active) {
                return decimal_scaled_rows<T>(code, left, right, output, width, scale, count, active);
            }
        };

        constexpr core::date::microseconds ONE_DAY_US =
            std::chrono::duration_cast<core::date::microseconds>(core::date::days{1});

        bool is_temporal(types::logical_type type) noexcept {
            return type == types::logical_type::DATE || type == types::logical_type::TIME ||
                   type == types::logical_type::TIME_TZ || type == types::logical_type::TIMESTAMP ||
                   type == types::logical_type::TIMESTAMP_TZ;
        }

        core::date::days
        add_interval_to_date(core::date::days date, core::date::interval_t interval, int sign) noexcept {
            auto day = core::date::pg_epoch + std::chrono::days{date.count()};
            if (interval.month.count()) {
                day = core::date::apply_months(day, (sign * interval.month).count());
            }
            day += std::chrono::days{(sign * interval.day).count()};
            return core::date::days{static_cast<core::date::days::rep>((day - core::date::pg_epoch).count())};
        }

        core::date::microseconds add_interval_to_timestamp(core::date::microseconds timestamp,
                                                           core::date::interval_t interval,
                                                           int sign) noexcept {
            auto [day_part, time_part] = core::date::split_timestamp(timestamp);
            auto day = core::date::pg_epoch + std::chrono::days{day_part.count()};
            if (interval.month.count()) {
                day = core::date::apply_months(day, (sign * interval.month).count());
            }
            day += std::chrono::days{(sign * interval.day).count()};
            return core::date::from_sys_days_us(day, time_part + sign * interval.time);
        }

        core::date::microseconds wrap_time_of_day(core::date::microseconds time) noexcept {
            auto wrapped = time % ONE_DAY_US;
            if (wrapped.count() < 0) {
                wrapped += ONE_DAY_US;
            }
            return wrapped;
        }

        core::date::interval_t scale_interval(core::date::interval_t interval, double factor) noexcept {
            return {core::date::microseconds{std::llround(static_cast<double>(interval.time.count()) * factor)},
                    core::date::days{
                        static_cast<int32_t>(std::llround(static_cast<double>(interval.day.count()) * factor))},
                    core::date::months{
                        static_cast<int32_t>(std::llround(static_cast<double>(interval.month.count()) * factor))}};
        }

        // Scaling an interval applies one factor to all three components
        double numeric_at(const vector_t& vector, uint64_t row) noexcept {
            switch (vector.type().to_physical_type()) {
                case types::physical_type::INT8:
                    return static_cast<double>(vector.get_value<int8_t>(row));
                case types::physical_type::UINT8:
                    return static_cast<double>(vector.get_value<uint8_t>(row));
                case types::physical_type::INT16:
                    return static_cast<double>(vector.get_value<int16_t>(row));
                case types::physical_type::UINT16:
                    return static_cast<double>(vector.get_value<uint16_t>(row));
                case types::physical_type::INT32:
                    return static_cast<double>(vector.get_value<int32_t>(row));
                case types::physical_type::UINT32:
                    return static_cast<double>(vector.get_value<uint32_t>(row));
                case types::physical_type::INT64:
                    return static_cast<double>(vector.get_value<int64_t>(row));
                case types::physical_type::UINT64:
                    return static_cast<double>(vector.get_value<uint64_t>(row));
                case types::physical_type::FLOAT:
                    return static_cast<double>(vector.get_value<float>(row));
                case types::physical_type::DOUBLE:
                    return vector.get_value<double>(row);
                default:
                    assert(false && "interval scaling requires a numeric factor");
                    return 0.0;
            }
        }

        // Arithmetic where at least one side is an INTERVAL, or where two temporals subtract.
        bool is_temporal_arithmetic(const vector_t& left, const vector_t& right) noexcept {
            const auto lhs = left.type().type();
            const auto rhs = right.type().type();
            return lhs == types::logical_type::INTERVAL || rhs == types::logical_type::INTERVAL ||
                   (is_temporal(lhs) && is_temporal(rhs));
        }

        // Arithmetics on temporal type requires specific combinations
        core::error_t temporal_arithmetic(operator_code code,
                                          const vector_t& left,
                                          const vector_t& right,
                                          vector_t* output,
                                          uint64_t count,
                                          const bool* active) {
            using lt = types::logical_type;
            namespace date = core::date;
            const auto lhs = left.type().type();
            const auto rhs = right.type().type();
            const bool is_add = code == operator_code::add;
            const int sign = is_add ? 1 : -1;

            if (code == operator_code::multiply || code == operator_code::divide) {
                const bool interval_left = lhs == lt::INTERVAL;
                assert((interval_left || rhs == lt::INTERVAL) && "scaling needs an interval operand");
                if (code == operator_code::divide && !interval_left) {
                    return core::error_t(
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"temporal_arithmetic encountered unsupported type", output->resource()});
                }
                const vector_t& interval = interval_left ? left : right;
                const vector_t& factor = interval_left ? right : left;
                const bool divide = code == operator_code::divide;
                return each_row(left, right, output, count, active, [&](uint64_t row) {
                    const double scale = numeric_at(factor, row);
                    output->set_value(
                        row,
                        scale_interval(interval.get_value<date::interval_t>(row), divide ? 1.0 / scale : scale));
                });
            }

            if (!is_add && code != operator_code::subtract) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"temporal_arithmetic encountered unsupported type", output->resource()});
            }

            if ((lhs == lt::DATE && rhs == lt::INTERVAL) || (is_add && lhs == lt::INTERVAL && rhs == lt::DATE)) {
                const vector_t& dates = lhs == lt::DATE ? left : right;
                const vector_t& intervals = lhs == lt::DATE ? right : left;
                const int applied = lhs == lt::DATE ? sign : 1;
                return each_row(left, right, output, count, active, [&](uint64_t row) {
                    output->set_value(row,
                                      date::date_t{add_interval_to_date(dates.get_value<date::date_t>(row).value,
                                                                        intervals.get_value<date::interval_t>(row),
                                                                        applied)});
                });
            }

            const bool timestamp_left = lhs == lt::TIMESTAMP || lhs == lt::TIMESTAMP_TZ;
            const bool timestamp_right = rhs == lt::TIMESTAMP || rhs == lt::TIMESTAMP_TZ;
            if ((timestamp_left && rhs == lt::INTERVAL) || (is_add && lhs == lt::INTERVAL && timestamp_right)) {
                const vector_t& stamps = timestamp_left ? left : right;
                const vector_t& intervals = timestamp_left ? right : left;
                const int applied = timestamp_left ? sign : 1;
                return each_row(left, right, output, count, active, [&](uint64_t row) {
                    output->set_value(
                        row,
                        date::timestamp_t{add_interval_to_timestamp(stamps.get_value<date::timestamp_t>(row).value,
                                                                    intervals.get_value<date::interval_t>(row),
                                                                    applied)});
                });
            }

            if ((lhs == lt::TIME && rhs == lt::INTERVAL) || (is_add && lhs == lt::INTERVAL && rhs == lt::TIME)) {
                const vector_t& times = lhs == lt::TIME ? left : right;
                const vector_t& intervals = lhs == lt::TIME ? right : left;
                const int applied = lhs == lt::TIME ? sign : 1;
                return each_row(left, right, output, count, active, [&](uint64_t row) {
                    const auto shifted = times.get_value<date::time_t>(row).value +
                                         applied * intervals.get_value<date::interval_t>(row).time;
                    output->set_value(row, date::time_t{wrap_time_of_day(shifted)});
                });
            }

            if ((lhs == lt::TIME_TZ && rhs == lt::INTERVAL) || (is_add && lhs == lt::INTERVAL && rhs == lt::TIME_TZ)) {
                const vector_t& times = lhs == lt::TIME_TZ ? left : right;
                const vector_t& intervals = lhs == lt::TIME_TZ ? right : left;
                const int applied = lhs == lt::TIME_TZ ? sign : 1;
                return each_row(left, right, output, count, active, [&](uint64_t row) {
                    const auto zoned = times.get_value<date::timetz_t>(row);
                    const auto shifted = zoned.time + applied * intervals.get_value<date::interval_t>(row).time;
                    output->set_value(row, date::timetz_t{wrap_time_of_day(shifted), zoned.zone});
                });
            }

            if (lhs == lt::INTERVAL && rhs == lt::INTERVAL) {
                return each_row(left, right, output, count, active, [&](uint64_t row) {
                    const auto first = left.get_value<date::interval_t>(row);
                    const auto second = right.get_value<date::interval_t>(row);
                    output->set_value(row,
                                      date::interval_t{first.time + sign * second.time,
                                                       first.day + sign * second.day,
                                                       first.month + sign * second.month});
                });
            }

            if (is_add) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"temporal_arithmetic encountered unsupported type", output->resource()});
            }

            if (lhs == lt::DATE && rhs == lt::DATE) {
                return each_row(left, right, output, count, active, [&](uint64_t row) {
                    output->set_value(row,
                                      date::interval_t{date::microseconds{0},
                                                       left.get_value<date::date_t>(row).value -
                                                           right.get_value<date::date_t>(row).value,
                                                       date::months{0}});
                });
            }
            if (timestamp_left && timestamp_right) {
                return each_row(left, right, output, count, active, [&](uint64_t row) {
                    output->set_value(row,
                                      date::interval_t{left.get_value<date::timestamp_t>(row).value -
                                                           right.get_value<date::timestamp_t>(row).value,
                                                       date::days{0},
                                                       date::months{0}});
                });
            }
            if (lhs == lt::TIME && rhs == lt::TIME) {
                return each_row(left, right, output, count, active, [&](uint64_t row) {
                    output->set_value(row,
                                      date::interval_t{left.get_value<date::time_t>(row).value -
                                                           right.get_value<date::time_t>(row).value,
                                                       date::days{0},
                                                       date::months{0}});
                });
            }
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{"temporal_arithmetic encountered unsupported type", output->resource()});
        }

        core::error_t struct_compare(operator_code code,
                                     const vector_t& left,
                                     const vector_t& right,
                                     vector_t* output,
                                     uint64_t count,
                                     const bool* active) {
            namespace date = core::date;
            const bool interval = left.type().type() == types::logical_type::INTERVAL;
            assert(left.type().type() == right.type().type() && "comparison operands must share a type");
            assert((interval || left.type().type() == types::logical_type::TIME_TZ) &&
                   "only INTERVAL and TIME_TZ compare as structs");
            return each_row(left, right, output, count, active, [&](uint64_t row) {
                std::strong_ordering ordering = std::strong_ordering::equal;
                if (interval) {
                    const auto lhs = left.get_value<date::interval_t>(row);
                    const auto rhs = right.get_value<date::interval_t>(row);
                    ordering =
                        lhs.month.count() != rhs.month.count()
                            ? (lhs.month < rhs.month ? std::strong_ordering::less : std::strong_ordering::greater)
                        : lhs.day.count() != rhs.day.count()
                            ? (lhs.day < rhs.day ? std::strong_ordering::less : std::strong_ordering::greater)
                        : lhs.time.count() != rhs.time.count()
                            ? (lhs.time < rhs.time ? std::strong_ordering::less : std::strong_ordering::greater)
                            : std::strong_ordering::equal;
                } else {
                    const auto lhs = left.get_value<date::timetz_t>(row);
                    const auto rhs = right.get_value<date::timetz_t>(row);
                    const auto lhs_utc = lhs.time - std::chrono::duration_cast<date::microseconds>(lhs.zone);
                    const auto rhs_utc = rhs.time - std::chrono::duration_cast<date::microseconds>(rhs.zone);
                    ordering = lhs_utc < rhs_utc   ? std::strong_ordering::less
                               : rhs_utc < lhs_utc ? std::strong_ordering::greater
                                                   : std::strong_ordering::equal;
                }
                switch (code) {
                    case operator_code::equal:
                        write<bool>(output, row, ordering == std::strong_ordering::equal);
                        break;
                    case operator_code::not_equal:
                        write<bool>(output, row, ordering != std::strong_ordering::equal);
                        break;
                    case operator_code::less:
                        write<bool>(output, row, ordering == std::strong_ordering::less);
                        break;
                    case operator_code::less_equal:
                        write<bool>(output, row, ordering != std::strong_ordering::greater);
                        break;
                    case operator_code::greater:
                        write<bool>(output, row, ordering == std::strong_ordering::greater);
                        break;
                    default:
                        write<bool>(output, row, ordering != std::strong_ordering::less);
                        break;
                }
            });
        }

        std::pair<const vector_t*, uint64_t> resolve_row(const vector_t& vector, uint64_t row) {
            const vector_t* current = &vector;
            uint64_t index = row;
            while (true) {
                switch (current->get_vector_type()) {
                    case vector_type::CONSTANT:
                        return {current, 0};
                    case vector_type::DICTIONARY:
                        index = current->indexing().get_index(index);
                        current = &current->child();
                        break;
                    default:
                        return {current, index};
                }
            }
        }

        std::pair<uint64_t, uint64_t> element_range(const vector_t& vector, uint64_t row) {
            if (vector.type().type() == types::logical_type::LIST) {
                const auto entry = vector.data<types::list_entry_t>()[row];
                return {entry.offset, entry.offset + entry.length};
            }
            const auto stride =
                static_cast<const types::array_logical_type_extension*>(vector.type().extension())->size();
            return {row * stride, row * stride + stride};
        }

        std::partial_ordering
        compare_element_at(const vector_t& left, uint64_t left_index, const vector_t& right, uint64_t right_index);

        template<typename...>
        struct element_ordering {
            template<typename T>
            std::partial_ordering
            operator()(const vector_t& left, uint64_t left_index, const vector_t& right, uint64_t right_index) {
                const T lhs = left.get_value<T>(left_index);
                const T rhs = right.get_value<T>(right_index);
                if (equals(lhs, rhs)) {
                    return std::partial_ordering::equivalent;
                }
                return lhs < rhs ? std::partial_ordering::less : std::partial_ordering::greater;
            }
        };

        // Lexicographic over the shared prefix, then by length.
        std::partial_ordering compare_container_rows(const vector_t& left_vector,
                                                     uint64_t left_row,
                                                     const vector_t& right_vector,
                                                     uint64_t right_row) {
            const auto [left, left_index] = resolve_row(left_vector, left_row);
            const auto [right, right_index] = resolve_row(right_vector, right_row);
            const auto [left_begin, left_end] = element_range(*left, left_index);
            const auto [right_begin, right_end] = element_range(*right, right_index);
            const uint64_t left_length = left_end - left_begin;
            const uint64_t right_length = right_end - right_begin;
            const uint64_t shared = std::min(left_length, right_length);
            const vector_t& left_child = left->entry();
            const vector_t& right_child = right->entry();
            for (uint64_t offset = 0; offset < shared; offset++) {
                const std::partial_ordering ordering =
                    compare_element_at(left_child, left_begin + offset, right_child, right_begin + offset);
                if (ordering != std::partial_ordering::equivalent) {
                    return ordering;
                }
            }
            if (left_length == right_length) {
                return std::partial_ordering::equivalent;
            }
            return left_length < right_length ? std::partial_ordering::less : std::partial_ordering::greater;
        }

        std::partial_ordering
        compare_element_at(const vector_t& left, uint64_t left_index, const vector_t& right, uint64_t right_index) {
            if (left.is_null(left_index) || right.is_null(right_index)) {
                return std::partial_ordering::unordered;
            }
            const auto physical = left.type().to_physical_type();
            if (physical == types::physical_type::ARRAY || physical == types::physical_type::LIST) {
                return compare_container_rows(left, left_index, right, right_index); // nested containers
            }
            return types::simple_physical_type_switch<element_ordering>(physical, left, left_index, right, right_index);
        }

        core::error_t container_compare(operator_code code,
                                        const vector_t& left,
                                        const vector_t& right,
                                        vector_t* output,
                                        uint64_t count,
                                        const bool* active) {
            for (uint64_t row = 0; row < count; row++) {
                if (skip_row(active, row)) {
                    continue;
                }
                if (left.is_null(row) || right.is_null(row)) {
                    output->set_null(row, true);
                    continue;
                }
                const std::partial_ordering ordering = compare_container_rows(left, row, right, row);
                if (ordering == std::partial_ordering::unordered) {
                    output->set_null(row, true);
                    continue;
                }
                output->set_null(row, false);
                switch (code) {
                    case operator_code::equal:
                        write<bool>(output, row, ordering == std::partial_ordering::equivalent);
                        break;
                    case operator_code::not_equal:
                        write<bool>(output, row, ordering != std::partial_ordering::equivalent);
                        break;
                    case operator_code::less:
                        write<bool>(output, row, ordering == std::partial_ordering::less);
                        break;
                    case operator_code::less_equal:
                        write<bool>(output, row, ordering != std::partial_ordering::greater);
                        break;
                    case operator_code::greater:
                        write<bool>(output, row, ordering == std::partial_ordering::greater);
                        break;
                    case operator_code::greater_equal:
                        write<bool>(output, row, ordering != std::partial_ordering::less);
                        break;
                    default:
                        return core::error_t(core::error_code_t::invalid_parameter,
                                             std::pmr::string{"container_compare encountered unsupported operator code",
                                                              output->resource()});
                }
            }
            return core::error_t::no_error();
        }

        template<typename...>
        struct binary_dispatch {
            template<typename T>
            core::error_t operator()(operator_code code,
                                     const vector_t& left,
                                     const vector_t& right,
                                     vector_t* output,
                                     uint64_t count,
                                     const bool* active) {
                if (is_comparison(code)) {
                    return compare_rows<T>(code, left, right, output, count, active);
                }
                if (is_bitwise(code)) {
                    return bitwise_rows<T>(code, left, right, output, count, active);
                }
                if constexpr (is_number<T>) {
                    return arithmetic_rows<T>(code, left, right, output, count, active);
                } else {
                    return core::error_t(
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"binary operator encountered unsupported operation", output->resource()});
                }
            }
        };

        template<typename...>
        struct negate_dispatch {
            template<typename T>
            core::error_t operator()(const vector_t& operand, vector_t* output, uint64_t count) {
                if constexpr (is_number<T> && !std::is_unsigned_v<T>) {
                    for (uint64_t row = 0; row < count; row++) {
                        if (operand.is_null(row)) {
                            output->set_null(row, true);
                            continue;
                        }
                        write<T>(output, row, static_cast<T>(-operand.get_value<T>(row)));
                    }
                    return core::error_t::no_error();
                } else {
                    return core::error_t(
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"unary operator encountered unsupported operation", output->resource()});
                }
            }
        };

        template<typename...>
        struct bit_not_dispatch {
            template<typename T>
            core::error_t operator()(const vector_t& operand, vector_t* output, uint64_t count) {
                if constexpr (is_integral_value<T>) {
                    for (uint64_t row = 0; row < count; row++) {
                        if (operand.is_null(row)) {
                            output->set_null(row, true);
                            continue;
                        }
                        write<T>(output, row, static_cast<T>(~operand.get_value<T>(row)));
                    }
                    return core::error_t::no_error();
                } else {
                    return core::error_t(
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"unary operator encountered unsupported operation", output->resource()});
                }
            }
        };

        core::error_t negate_interval(const vector_t& operand, vector_t* output, uint64_t count) {
            for (uint64_t row = 0; row < count; row++) {
                if (operand.is_null(row)) {
                    output->set_null(row, true);
                    continue;
                }
                output->set_null(row, false);
                const auto interval = operand.get_value<core::date::interval_t>(row);
                output->set_value(row, core::date::interval_t{-interval.time, -interval.day, -interval.month});
            }
            return core::error_t::no_error();
        }

    } // namespace

    bool is_comparison(operator_code code) noexcept {
        switch (code) {
            case operator_code::equal:
            case operator_code::not_equal:
            case operator_code::less:
            case operator_code::less_equal:
            case operator_code::greater:
            case operator_code::greater_equal:
                return true;
            default:
                return false;
        }
    }

    core::error_t apply_binary(operator_code code,
                               const vector_t& left,
                               const vector_t& right,
                               vector_t* output,
                               const graph_execution_context& context,
                               uint64_t count,
                               const bool* active_rows) {
        if (code == operator_code::logical_and || code == operator_code::logical_or) {
            return logical_rows(code, left, right, output, count, active_rows);
        }
        if (code == operator_code::strict_equal) {
            if (auto error = apply_binary(operator_code::equal, left, right, output, context, count, active_rows);
                error.contains_error()) {
                return error;
            }
            for (uint64_t row = 0; row < count; row++) {
                const bool left_null = left.is_null(row);
                const bool right_null = right.is_null(row);
                if (left_null || right_null) {
                    output->set_null(row, false);
                    write<bool>(output, row, left_null && right_null);
                }
            }
            return core::error_t::no_error();
        }
        // Special cases
        if (is_comparison(code)) {
            if (left.type().to_physical_type() == types::physical_type::STRUCT) {
                return struct_compare(code, left, right, output, count, active_rows);
            }
            const auto physical = left.type().to_physical_type();
            if (physical == types::physical_type::ARRAY || physical == types::physical_type::LIST) {
                return container_compare(code, left, right, output, count, active_rows);
            }
        } else if (is_temporal_arithmetic(left, right)) {
            return temporal_arithmetic(code, left, right, output, count, active_rows);
        }
        // If casts were applied correctly, left and right should be the same
        assert(left.type().to_physical_type() == right.type().to_physical_type() &&
               "operator operands must be pre-cast to a common type");
        if (is_decimal(left) && (code == operator_code::multiply || code == operator_code::divide)) {
            assert(decimal_scale(left) == decimal_scale(right) && decimal_scale(left) == decimal_scale(*output) &&
                   "decimal operands and result must share a scale");
            return types::simple_physical_type_switch<decimal_dispatch>(left.type().to_physical_type(),
                                                                        code,
                                                                        left,
                                                                        right,
                                                                        output,
                                                                        decimal_width(*output),
                                                                        decimal_scale(left),
                                                                        count,
                                                                        active_rows);
        }
        return types::simple_physical_type_switch<binary_dispatch>(left.type().to_physical_type(),
                                                                   code,
                                                                   left,
                                                                   right,
                                                                   output,
                                                                   count,
                                                                   active_rows);
    }

    core::error_t apply_unary(operator_code code,
                              const vector_t& operand,
                              vector_t* output,
                              const graph_execution_context&,
                              uint64_t count) {
        if (operand.type().type() == types::logical_type::NA && code != operator_code::is_null &&
            code != operator_code::is_not_null) {
            return core::error_t::no_error();
        }
        switch (code) {
            case operator_code::is_null:
                for (uint64_t row = 0; row < count; row++) {
                    write<bool>(output, row, operand.is_null(row));
                }
                return core::error_t::no_error();
            case operator_code::is_not_null:
                for (uint64_t row = 0; row < count; row++) {
                    write<bool>(output, row, !operand.is_null(row));
                }
                return core::error_t::no_error();
            case operator_code::logical_not:
                if (operand.type().to_physical_type() != types::physical_type::BOOL) {
                    return core::error_t(
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"unary operator encountered unsupported type", output->resource()});
                }
                for (uint64_t row = 0; row < count; row++) {
                    if (operand.is_null(row)) {
                        output->set_null(row, true);
                        continue;
                    }
                    write<bool>(output, row, !operand.get_value<bool>(row));
                }
                return core::error_t::no_error();
            case operator_code::negate:
                if (operand.type().type() == types::logical_type::INTERVAL) {
                    return negate_interval(operand, output, count);
                }
                return types::simple_physical_type_switch<negate_dispatch>(operand.type().to_physical_type(),
                                                                           operand,
                                                                           output,
                                                                           count);
            case operator_code::bit_not:
                return types::simple_physical_type_switch<bit_not_dispatch>(operand.type().to_physical_type(),
                                                                            operand,
                                                                            output,
                                                                            count);
            default:
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"unary operator encountered unsupported operation", output->resource()});
        }
    }

} // namespace components::vector::operations