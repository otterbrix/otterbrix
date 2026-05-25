#pragma once

#include "append_data.hpp"
#include "scalar_data.hpp"

#include <components/vector/vector.hpp>

#include <cassert>

namespace components::vector::arrow::appender {

    static constexpr int32_t EPOCH_DAYS_OFFSET = 10957;           // days from 1970-01-01 to 2000-01-01
    static constexpr int64_t EPOCH_US_OFFSET = 946684800000000LL; // µs from 1970-01-01 to 2000-01-01

    struct arrow_date_op_t {
        template<class TGT, class SRC>
        static TGT operation(SRC input) {
            return input + EPOCH_DAYS_OFFSET;
        }
        static bool skip_nulls() { return false; }
        template<class TGT>
        static void set_null(TGT&) {}
    };

    struct arrow_timestamp_op_t {
        template<class TGT, class SRC>
        static TGT operation(SRC input) {
            return input + EPOCH_US_OFFSET;
        }
        static bool skip_nulls() { return false; }
        template<class TGT>
        static void set_null(TGT&) {}
    };

    // DATE: int32 days since pg_epoch → int32 days since Unix epoch
    using arrow_date_appender_t = arrow_scala_data<int32_t, int32_t, arrow_date_op_t>;

    // TIME: int64 µs since midnight → int64 µs since midnight (direct copy)
    using arrow_time_appender_t = arrow_scala_data<int64_t>;

    // TIMESTAMP / TIMESTAMP_TZ: int64 µs since pg_epoch → int64 µs since Unix epoch
    using arrow_timestamp_appender_t = arrow_scala_data<int64_t, int64_t, arrow_timestamp_op_t>;

    // INTERVAL: child vectors {int64 µs, int32 days, int32 months} → Arrow "tin" {int32 months, int32 days, int64 nanos}
    struct arrow_interval_appender_t {
        struct arrow_interval_buf_t {
            int32_t months;
            int32_t days;
            int64_t nanoseconds;
        };
        static_assert(sizeof(arrow_interval_buf_t) == 16);

        static void initialize(arrow_append_data_t& result, const types::complex_logical_type&, uint64_t capacity) {
            result.main_buffer().reserve(capacity * sizeof(arrow_interval_buf_t));
        }

        static void
        append(arrow_append_data_t& append_data, vector_t& input, uint64_t from, uint64_t to, uint64_t input_size) {
            uint64_t size = to - from;
            unified_vector_format format(input.resource(), input_size);
            input.to_unified_format(input_size, format);
            append_data.add_validity(format, from, to);

            auto& child_entries = input.entries();
            assert(child_entries.size() == 3);

            unified_vector_format us_fmt(input.resource(), input_size);
            child_entries[0]->to_unified_format(input_size, us_fmt);
            unified_vector_format day_fmt(input.resource(), input_size);
            child_entries[1]->to_unified_format(input_size, day_fmt);
            unified_vector_format month_fmt(input.resource(), input_size);
            child_entries[2]->to_unified_format(input_size, month_fmt);

            auto& main_buf = append_data.main_buffer();
            main_buf.resize(main_buf.size() + sizeof(arrow_interval_buf_t) * size);
            auto result_data = main_buf.data<arrow_interval_buf_t>();

            const auto* us_data = us_fmt.get_data<int64_t>();
            const auto* day_data = day_fmt.get_data<int32_t>();
            const auto* month_data = month_fmt.get_data<int32_t>();

            for (uint64_t i = from; i < to; i++) {
                auto result_idx = append_data.row_count + i - from;
                auto us_idx = us_fmt.referenced_indexing->get_index(i);
                auto day_idx = day_fmt.referenced_indexing->get_index(i);
                auto month_idx = month_fmt.referenced_indexing->get_index(i);

                result_data[result_idx].months = month_data[month_idx];
                result_data[result_idx].days = day_data[day_idx];
                result_data[result_idx].nanoseconds = us_data[us_idx] * 1000LL;
            }
            append_data.row_count += size;
        }

        static void finalize(arrow_append_data_t& append_data, const types::complex_logical_type&, ArrowArray* result) {
            result->n_buffers = 2;
            result->buffers[1] = append_data.main_buffer().data();
        }
    };

} // namespace components::vector::arrow::appender