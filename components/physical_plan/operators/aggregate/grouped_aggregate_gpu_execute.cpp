#include "grouped_aggregate_gpu_internal.hpp"

#include <bit>
#include <limits>
#include <vector>

#include <fmt/format.h>

namespace components::operators::aggregate::gpu {

#if defined(__linux__)
    namespace {

        struct device_column_t {
            uint32_t input_id{0};
            cl_int has_mask{0};
            device_buffer_t values{};
            device_buffer_t mask{};
        };

        struct device_query_t {
            device_buffer_t group_ids{};
            std::vector<device_column_t> columns{};

            const device_column_t* find_column(uint32_t input_id) const {
                for (const auto& column : columns) {
                    if (column.input_id == input_id) {
                        return &column;
                    }
                }
                return nullptr;
            }
        };

        std::vector<cl_uint> to_opencl_group_ids(const uint32_t* group_ids, uint64_t count) {
            std::vector<cl_uint> result(static_cast<size_t>(count));
            for (uint64_t row = 0; row < count; row++) {
                result[static_cast<size_t>(row)] = static_cast<cl_uint>(group_ids[row]);
            }
            return result;
        }

        std::vector<cl_ulong> validity_words(const vector::vector_t& vec, uint64_t count, cl_int& has_mask) {
            if (auto* raw_mask = vec.validity().data(); raw_mask != nullptr) {
                has_mask = 1;
                const auto word_count = static_cast<size_t>((count + 63U) / 64U);
                std::vector<cl_ulong> result(word_count);
                for (size_t index = 0; index < word_count; index++) {
                    result[index] = static_cast<cl_ulong>(raw_mask[index]);
                }
                return result;
            }

            has_mask = 0;
            return std::vector<cl_ulong>(1, std::numeric_limits<cl_ulong>::max());
        }

        int agg_code_for_opencl(builtin_agg agg) {
            switch (agg) {
                case builtin_agg::SUM:
                    return 0;
                case builtin_agg::MIN:
                    return 1;
                case builtin_agg::MAX:
                    return 2;
                default:
                    return -1;
            }
        }

        template<typename T>
        std::vector<T> make_zero_vector(size_t count) {
            return std::vector<T>(count, T{});
        }

        template<typename T>
        std::vector<T> make_filled_vector(size_t count, T value) {
            return std::vector<T>(count, value);
        }

        std::vector<cl_ulong> make_float_identity_vector(size_t count, double value) {
            return std::vector<cl_ulong>(count, std::bit_cast<cl_ulong>(value));
        }

        void fill_count_states(std::pmr::vector<raw_agg_state_t>& states, const std::vector<cl_ulong>& values) {
            for (size_t group = 0; group < states.size(); group++) {
                states[group].u64 = static_cast<uint64_t>(values[group]);
                states[group].count = static_cast<uint64_t>(values[group]);
                states[group].initialized = values[group] > 0;
                states[group].value_kind = raw_agg_state_t::value_kind_t::unsigned_integer;
            }
        }

        void fill_signed_states(std::pmr::vector<raw_agg_state_t>& states,
                                const std::vector<cl_long>& values,
                                const std::vector<cl_ulong>& counts) {
            for (size_t group = 0; group < states.size(); group++) {
                states[group].i64 = static_cast<int64_t>(values[group]);
                states[group].count = static_cast<uint64_t>(counts[group]);
                states[group].initialized = counts[group] > 0;
                states[group].value_kind = raw_agg_state_t::value_kind_t::signed_integer;
            }
        }

        void fill_unsigned_states(std::pmr::vector<raw_agg_state_t>& states,
                                  const std::vector<cl_ulong>& values,
                                  const std::vector<cl_ulong>& counts) {
            for (size_t group = 0; group < states.size(); group++) {
                states[group].u64 = static_cast<uint64_t>(values[group]);
                states[group].count = static_cast<uint64_t>(counts[group]);
                states[group].initialized = counts[group] > 0;
                states[group].value_kind = raw_agg_state_t::value_kind_t::unsigned_integer;
            }
        }

        void fill_float_states(std::pmr::vector<raw_agg_state_t>& states,
                               const std::vector<cl_ulong>& value_bits,
                               const std::vector<cl_ulong>& counts) {
            for (size_t group = 0; group < states.size(); group++) {
                states[group].f64 = std::bit_cast<double>(value_bits[group]);
                states[group].count = static_cast<uint64_t>(counts[group]);
                states[group].initialized = counts[group] > 0;
                states[group].value_kind = raw_agg_state_t::value_kind_t::floating_point;
            }
        }

        template<typename T>
        bool set_scalar_arg_typed(opencl_runtime_t& runtime, cl_kernel kernel, cl_uint index, const T& value) {
            return set_scalar_arg(runtime, kernel, index, &value, sizeof(T));
        }

        template<typename InputValue>
        device_buffer_t create_input_buffer(opencl_runtime_t& runtime,
                                            const InputValue* input_values,
                                            uint64_t count,
                                            std::string_view label) {
            return create_device_buffer(runtime,
                                        cl_mem_read_only | cl_mem_copy_host_ptr,
                                        input_values,
                                        sizeof(InputValue) * static_cast<size_t>(count),
                                        label);
        }

        device_buffer_t create_input_buffer(opencl_runtime_t& runtime,
                                            const vector::vector_t& vec,
                                            uint64_t count,
                                            std::string_view label) {
            switch (vec.type().type()) {
                case types::logical_type::TINYINT:
                    return create_input_buffer(runtime, vec.data<int8_t>(), count, label);
                case types::logical_type::SMALLINT:
                    return create_input_buffer(runtime, vec.data<int16_t>(), count, label);
                case types::logical_type::INTEGER:
                    return create_input_buffer(runtime, vec.data<int32_t>(), count, label);
                case types::logical_type::BIGINT:
                    return create_input_buffer(runtime, vec.data<int64_t>(), count, label);
                case types::logical_type::UTINYINT:
                    return create_input_buffer(runtime, vec.data<uint8_t>(), count, label);
                case types::logical_type::USMALLINT:
                    return create_input_buffer(runtime, vec.data<uint16_t>(), count, label);
                case types::logical_type::UINTEGER:
                    return create_input_buffer(runtime, vec.data<uint32_t>(), count, label);
                case types::logical_type::UBIGINT:
                    return create_input_buffer(runtime, vec.data<uint64_t>(), count, label);
                case types::logical_type::FLOAT:
                    return create_input_buffer(runtime, vec.data<float>(), count, label);
                case types::logical_type::DOUBLE:
                    return create_input_buffer(runtime, vec.data<double>(), count, label);
                default:
                    runtime.set_last_error(
                        fmt::format("unsupported GPU aggregate type {}", static_cast<int>(vec.type().type())));
                    return {};
            }
        }

        bool prepare_batch_query(opencl_runtime_t& runtime,
                                 const gpu_update_request_t* requests,
                                 size_t request_count,
                                 const uint32_t* group_ids,
                                 uint64_t count,
                                 device_query_t& query) {
            auto gids = to_opencl_group_ids(group_ids, count);
            query.group_ids = create_device_buffer(runtime,
                                                   cl_mem_read_only | cl_mem_copy_host_ptr,
                                                   gids.data(),
                                                   sizeof(cl_uint) * gids.size(),
                                                   "group_ids");
            if (!query.group_ids) {
                return false;
            }

            query.columns.reserve(request_count);
            for (size_t index = 0; index < request_count; index++) {
                const auto& request = requests[index];
                if (request.is_count_star || request.vec == nullptr || query.find_column(request.input_id) != nullptr) {
                    continue;
                }

                cl_int has_mask = 0;
                auto mask = validity_words(*request.vec, count, has_mask);

                auto values = create_input_buffer(runtime, *request.vec, count, "input_values");
                auto d_mask = create_device_buffer(runtime,
                                                   cl_mem_read_only | cl_mem_copy_host_ptr,
                                                   mask.data(),
                                                   sizeof(cl_ulong) * mask.size(),
                                                   "validity_mask");
                if (!values || !d_mask) {
                    return false;
                }

                device_column_t column;
                column.input_id = request.input_id;
                column.has_mask = has_mask;
                column.values = std::move(values);
                column.mask = std::move(d_mask);
                query.columns.push_back(std::move(column));
            }

            return true;
        }

        bool run_count_kernel(std::string_view kernel_name,
                              opencl_runtime_t& runtime,
                              const device_query_t& query,
                              uint64_t count,
                              const device_column_t* column,
                              std::pmr::vector<raw_agg_state_t>& states) {
            auto out_values = make_zero_vector<cl_ulong>(states.size());
            auto out_counts = make_zero_vector<cl_ulong>(states.size());

            auto d_values = create_device_buffer(runtime,
                                                 cl_mem_read_write | cl_mem_copy_host_ptr,
                                                 out_values.data(),
                                                 sizeof(cl_ulong) * out_values.size(),
                                                 "count_values");
            auto d_counts = create_device_buffer(runtime,
                                                 cl_mem_read_write | cl_mem_copy_host_ptr,
                                                 out_counts.data(),
                                                 sizeof(cl_ulong) * out_counts.size(),
                                                 "count_counts");
            if (!d_values || !d_counts) {
                return false;
            }

            const cl_kernel kernel = runtime.kernel(kernel_name);
            if (!kernel) {
                return false;
            }

            cl_uint arg_index = 0;
            if (!set_buffer_arg(runtime, kernel, arg_index++, query.group_ids)) {
                return false;
            }

            if (column != nullptr) {
                if (!set_buffer_arg(runtime, kernel, arg_index++, column->mask) ||
                    !set_scalar_arg_typed(runtime, kernel, arg_index++, column->has_mask)) {
                    return false;
                }
            }

            const auto rows = static_cast<cl_ulong>(count);
            if (!set_scalar_arg_typed(runtime, kernel, arg_index++, rows) ||
                !set_buffer_arg(runtime, kernel, arg_index++, d_values) ||
                !set_buffer_arg(runtime, kernel, arg_index++, d_counts) || !run_1d_kernel(runtime, kernel, count) ||
                !read_device_buffer(runtime,
                                    d_values,
                                    out_values.data(),
                                    sizeof(cl_ulong) * out_values.size(),
                                    "count_values") ||
                !read_device_buffer(runtime,
                                    d_counts,
                                    out_counts.data(),
                                    sizeof(cl_ulong) * out_counts.size(),
                                    "count_counts")) {
                return false;
            }

            fill_count_states(states, out_values);
            return true;
        }

        template<typename OutputValue>
        bool run_regular_kernel(std::string_view kernel_name,
                                builtin_agg agg,
                                opencl_runtime_t& runtime,
                                const device_query_t& query,
                                const device_column_t& column,
                                uint64_t count,
                                std::vector<OutputValue>& out_values,
                                std::vector<cl_ulong>& out_counts,
                                std::string_view output_label) {
            auto d_out_values = create_device_buffer(runtime,
                                                     cl_mem_read_write | cl_mem_copy_host_ptr,
                                                     out_values.data(),
                                                     sizeof(OutputValue) * out_values.size(),
                                                     output_label);
            auto d_out_counts = create_device_buffer(runtime,
                                                     cl_mem_read_write | cl_mem_copy_host_ptr,
                                                     out_counts.data(),
                                                     sizeof(cl_ulong) * out_counts.size(),
                                                     "output_counts");
            if (!d_out_values || !d_out_counts) {
                return false;
            }

            const cl_kernel kernel = runtime.kernel(kernel_name);
            const cl_int agg_code = agg_code_for_opencl(agg);
            if (!kernel || agg_code < 0) {
                if (agg_code < 0) {
                    runtime.set_last_error("unsupported aggregate kind for OpenCL regular kernel");
                }
                return false;
            }

            const auto rows = static_cast<cl_ulong>(count);
            if (!set_buffer_arg(runtime, kernel, 0, column.values) || !set_buffer_arg(runtime, kernel, 1, query.group_ids) ||
                !set_buffer_arg(runtime, kernel, 2, column.mask) ||
                !set_scalar_arg_typed(runtime, kernel, 3, column.has_mask) ||
                !set_scalar_arg_typed(runtime, kernel, 4, rows) || !set_scalar_arg_typed(runtime, kernel, 5, agg_code) ||
                !set_buffer_arg(runtime, kernel, 6, d_out_values) ||
                !set_buffer_arg(runtime, kernel, 7, d_out_counts) || !run_1d_kernel(runtime, kernel, count) ||
                !read_device_buffer(runtime,
                                    d_out_values,
                                    out_values.data(),
                                    sizeof(OutputValue) * out_values.size(),
                                    output_label) ||
                !read_device_buffer(runtime,
                                    d_out_counts,
                                    out_counts.data(),
                                    sizeof(cl_ulong) * out_counts.size(),
                                    "output_counts")) {
                return false;
            }

            return true;
        }

        bool run_avg_kernel(std::string_view kernel_name,
                            opencl_runtime_t& runtime,
                            const device_query_t& query,
                            const device_column_t& column,
                            uint64_t count,
                            std::vector<cl_ulong>& out_sum_bits,
                            std::vector<cl_ulong>& out_counts) {
            auto d_out_sum = create_device_buffer(runtime,
                                                  cl_mem_read_write | cl_mem_copy_host_ptr,
                                                  out_sum_bits.data(),
                                                  sizeof(cl_ulong) * out_sum_bits.size(),
                                                  "output_sum_bits");
            auto d_out_counts = create_device_buffer(runtime,
                                                     cl_mem_read_write | cl_mem_copy_host_ptr,
                                                     out_counts.data(),
                                                     sizeof(cl_ulong) * out_counts.size(),
                                                     "output_counts");
            if (!d_out_sum || !d_out_counts) {
                return false;
            }

            const cl_kernel kernel = runtime.kernel(kernel_name);
            if (!kernel) {
                return false;
            }

            const auto rows = static_cast<cl_ulong>(count);
            if (!set_buffer_arg(runtime, kernel, 0, column.values) || !set_buffer_arg(runtime, kernel, 1, query.group_ids) ||
                !set_buffer_arg(runtime, kernel, 2, column.mask) ||
                !set_scalar_arg_typed(runtime, kernel, 3, column.has_mask) ||
                !set_scalar_arg_typed(runtime, kernel, 4, rows) || !set_buffer_arg(runtime, kernel, 5, d_out_sum) ||
                !set_buffer_arg(runtime, kernel, 6, d_out_counts) || !run_1d_kernel(runtime, kernel, count) ||
                !read_device_buffer(runtime,
                                    d_out_sum,
                                    out_sum_bits.data(),
                                    sizeof(cl_ulong) * out_sum_bits.size(),
                                    "output_sum_bits") ||
                !read_device_buffer(runtime,
                                    d_out_counts,
                                    out_counts.data(),
                                    sizeof(cl_ulong) * out_counts.size(),
                                    "output_counts")) {
                return false;
            }

            return true;
        }

        bool try_run_signed_aggregate(std::string_view regular_kernel,
                                      std::string_view avg_kernel,
                                      builtin_agg agg,
                                      opencl_runtime_t& runtime,
                                      const device_query_t& query,
                                      const device_column_t& column,
                                      uint64_t count,
                                      std::pmr::vector<raw_agg_state_t>& states) {
            auto out_counts = make_zero_vector<cl_ulong>(states.size());

            if (agg == builtin_agg::AVG) {
                auto out_sum_bits = make_float_identity_vector(states.size(), 0.0);
                if (!run_avg_kernel(avg_kernel, runtime, query, column, count, out_sum_bits, out_counts)) {
                    return false;
                }
                fill_float_states(states, out_sum_bits, out_counts);
                return true;
            }

            auto out_values = agg == builtin_agg::MIN
                                  ? make_filled_vector<cl_long>(states.size(), std::numeric_limits<cl_long>::max())
                              : agg == builtin_agg::MAX
                                  ? make_filled_vector<cl_long>(states.size(), std::numeric_limits<cl_long>::min())
                                  : make_zero_vector<cl_long>(states.size());

            if (!run_regular_kernel(
                    regular_kernel, agg, runtime, query, column, count, out_values, out_counts, "output_values")) {
                return false;
            }

            fill_signed_states(states, out_values, out_counts);
            return true;
        }

        bool try_run_unsigned_aggregate(std::string_view regular_kernel,
                                        std::string_view avg_kernel,
                                        builtin_agg agg,
                                        opencl_runtime_t& runtime,
                                        const device_query_t& query,
                                        const device_column_t& column,
                                        uint64_t count,
                                        std::pmr::vector<raw_agg_state_t>& states) {
            auto out_counts = make_zero_vector<cl_ulong>(states.size());

            if (agg == builtin_agg::AVG) {
                auto out_sum_bits = make_float_identity_vector(states.size(), 0.0);
                if (!run_avg_kernel(avg_kernel, runtime, query, column, count, out_sum_bits, out_counts)) {
                    return false;
                }
                fill_float_states(states, out_sum_bits, out_counts);
                return true;
            }

            auto out_values = agg == builtin_agg::MIN
                                  ? make_filled_vector<cl_ulong>(states.size(), std::numeric_limits<cl_ulong>::max())
                                  : make_zero_vector<cl_ulong>(states.size());

            if (!run_regular_kernel(
                    regular_kernel, agg, runtime, query, column, count, out_values, out_counts, "output_values")) {
                return false;
            }

            fill_unsigned_states(states, out_values, out_counts);
            return true;
        }

        bool try_run_floating_aggregate(std::string_view regular_kernel,
                                        std::string_view avg_kernel,
                                        builtin_agg agg,
                                        opencl_runtime_t& runtime,
                                        const device_query_t& query,
                                        const device_column_t& column,
                                        uint64_t count,
                                        std::pmr::vector<raw_agg_state_t>& states) {
            auto out_counts = make_zero_vector<cl_ulong>(states.size());

            if (agg == builtin_agg::AVG) {
                auto out_sum_bits = make_float_identity_vector(states.size(), 0.0);
                if (!run_avg_kernel(avg_kernel, runtime, query, column, count, out_sum_bits, out_counts)) {
                    return false;
                }
                fill_float_states(states, out_sum_bits, out_counts);
                return true;
            }

            auto out_value_bits = agg == builtin_agg::MIN
                                      ? make_float_identity_vector(states.size(), std::numeric_limits<double>::infinity())
                                  : agg == builtin_agg::MAX
                                      ? make_float_identity_vector(states.size(), -std::numeric_limits<double>::infinity())
                                      : make_float_identity_vector(states.size(), 0.0);

            if (!run_regular_kernel(regular_kernel,
                                    agg,
                                    runtime,
                                    query,
                                    column,
                                    count,
                                    out_value_bits,
                                    out_counts,
                                    "output_value_bits")) {
                return false;
            }

            fill_float_states(states, out_value_bits, out_counts);
            return true;
        }

        bool try_run_request_gpu(opencl_runtime_t& runtime,
                                 const device_query_t& query,
                                 uint64_t count,
                                 const gpu_update_request_t& request) {
            if (request.states == nullptr || request.states->empty()) {
                return true;
            }

            if (request.is_count_star) {
                return run_count_kernel("agg_count_star_rows", runtime, query, count, nullptr, *request.states);
            }

            const auto* column = query.find_column(request.input_id);
            if (column == nullptr || request.vec == nullptr) {
                runtime.set_last_error("GPU batch request is missing uploaded input column");
                return false;
            }

            if (request.kind == builtin_agg::COUNT) {
                return run_count_kernel("agg_count_non_null_rows", runtime, query, count, column, *request.states);
            }

            switch (request.vec->type().type()) {
                case types::logical_type::TINYINT:
                    return try_run_signed_aggregate(
                        "agg_i8_rows", "agg_i8_avg_rows", request.kind, runtime, query, *column, count, *request.states);
                case types::logical_type::SMALLINT:
                    return try_run_signed_aggregate(
                        "agg_i16_rows", "agg_i16_avg_rows", request.kind, runtime, query, *column, count, *request.states);
                case types::logical_type::INTEGER:
                    return try_run_signed_aggregate(
                        "agg_i32_rows", "agg_i32_avg_rows", request.kind, runtime, query, *column, count, *request.states);
                case types::logical_type::BIGINT:
                    return try_run_signed_aggregate(
                        "agg_i64_rows", "agg_i64_avg_rows", request.kind, runtime, query, *column, count, *request.states);
                case types::logical_type::UTINYINT:
                    return try_run_unsigned_aggregate(
                        "agg_u8_rows", "agg_u8_avg_rows", request.kind, runtime, query, *column, count, *request.states);
                case types::logical_type::USMALLINT:
                    return try_run_unsigned_aggregate(
                        "agg_u16_rows", "agg_u16_avg_rows", request.kind, runtime, query, *column, count, *request.states);
                case types::logical_type::UINTEGER:
                    return try_run_unsigned_aggregate(
                        "agg_u32_rows", "agg_u32_avg_rows", request.kind, runtime, query, *column, count, *request.states);
                case types::logical_type::UBIGINT:
                    return try_run_unsigned_aggregate(
                        "agg_u64_rows", "agg_u64_avg_rows", request.kind, runtime, query, *column, count, *request.states);
                case types::logical_type::FLOAT:
                    return try_run_floating_aggregate(
                        "agg_f32_rows", "agg_f32_avg_rows", request.kind, runtime, query, *column, count, *request.states);
                case types::logical_type::DOUBLE:
                    return try_run_floating_aggregate(
                        "agg_f64_rows", "agg_f64_avg_rows", request.kind, runtime, query, *column, count, *request.states);
                default:
                    runtime.set_last_error(
                        fmt::format("unsupported GPU aggregate type {}", static_cast<int>(request.vec->type().type())));
                    return false;
            }
        }

    } // namespace

    bool try_run_batch_gpu(const gpu_update_request_t* requests,
                           size_t request_count,
                           const uint32_t* group_ids,
                           uint64_t count) {
        if (request_count == 0) {
            return true;
        }

        auto& runtime = opencl_runtime();
        if (!runtime.ready()) {
            return false;
        }

        std::lock_guard<std::mutex> guard(runtime.mutex());
        runtime.clear_last_error();

        device_query_t query;
        if (!prepare_batch_query(runtime, requests, request_count, group_ids, count, query)) {
            return false;
        }

        for (size_t index = 0; index < request_count; index++) {
            if (!try_run_request_gpu(runtime, query, count, requests[index])) {
                return false;
            }
        }

        return true;
    }

    bool try_run_count_star_gpu(const uint32_t* group_ids, uint64_t count, std::pmr::vector<raw_agg_state_t>& states) {
        if (states.empty()) {
            return true;
        }

        gpu_update_request_t request{};
        request.kind = builtin_agg::COUNT;
        request.states = &states;
        request.is_count_star = true;
        return try_run_batch_gpu(&request, 1, group_ids, count);
    }

    bool try_run_update_all_gpu(builtin_agg agg,
                                const vector::vector_t& vec,
                                const uint32_t* group_ids,
                                uint64_t count,
                                std::pmr::vector<raw_agg_state_t>& states) {
        if (states.empty()) {
            return true;
        }

        gpu_update_request_t request{};
        request.kind = agg;
        request.vec = &vec;
        request.states = &states;
        request.input_id = 0;
        return try_run_batch_gpu(&request, 1, group_ids, count);
    }
#else
    bool try_run_batch_gpu(const gpu_update_request_t*, size_t, const uint32_t*, uint64_t) { return false; }

    bool try_run_count_star_gpu(const uint32_t*, uint64_t, std::pmr::vector<raw_agg_state_t>&) { return false; }

    bool try_run_update_all_gpu(builtin_agg,
                                const vector::vector_t&,
                                const uint32_t*,
                                uint64_t,
                                std::pmr::vector<raw_agg_state_t>&) {
        return false;
    }
#endif

} // namespace components::operators::aggregate::gpu
