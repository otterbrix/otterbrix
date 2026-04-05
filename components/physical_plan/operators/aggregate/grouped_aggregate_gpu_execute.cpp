#include "grouped_aggregate_gpu_internal.hpp"

#include <bit>
#include <limits>
#include <vector>

#include <fmt/format.h>

namespace components::operators::aggregate::gpu {

#if defined(__linux__)
    namespace {

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

        bool run_count_kernel(std::string_view kernel_name,
                              const uint32_t* group_ids,
                              uint64_t count,
                              const vector::vector_t* vec,
                              std::pmr::vector<raw_agg_state_t>& states) {
            auto& runtime = opencl_runtime();
            if (!runtime.ready()) {
                return false;
            }

            auto gids = to_opencl_group_ids(group_ids, count);
            auto out_values = make_zero_vector<cl_ulong>(states.size());
            auto out_counts = make_zero_vector<cl_ulong>(states.size());

            cl_int has_mask = 0;
            auto mask = vec ? validity_words(*vec, count, has_mask) : std::vector<cl_ulong>(1, 0);

            std::lock_guard<std::mutex> guard(runtime.mutex());
            runtime.clear_last_error();

            const auto d_gids = create_device_buffer(runtime,
                                                     cl_mem_read_only | cl_mem_copy_host_ptr,
                                                     gids.data(),
                                                     sizeof(cl_uint) * gids.size(),
                                                     "group_ids");
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
            if (!d_gids || !d_values || !d_counts) {
                return false;
            }

            const cl_kernel kernel = runtime.kernel(kernel_name);
            if (!kernel) {
                return false;
            }

            cl_uint arg_index = 0;
            if (!set_buffer_arg(runtime, kernel, arg_index++, d_gids)) {
                return false;
            }

            device_buffer_t d_mask;
            if (vec != nullptr) {
                d_mask = create_device_buffer(runtime,
                                              cl_mem_read_only | cl_mem_copy_host_ptr,
                                              mask.data(),
                                              sizeof(cl_ulong) * mask.size(),
                                              "validity_mask");
                if (!d_mask) {
                    return false;
                }
                if (!set_buffer_arg(runtime, kernel, arg_index++, d_mask) ||
                    !set_scalar_arg_typed(runtime, kernel, arg_index++, has_mask)) {
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

        template<typename InputValue, typename OutputValue>
        bool run_regular_kernel(std::string_view kernel_name,
                                const InputValue* input_values,
                                builtin_agg agg,
                                const uint32_t* group_ids,
                                uint64_t count,
                                const vector::vector_t& vec,
                                std::vector<OutputValue>& out_values,
                                std::vector<cl_ulong>& out_counts,
                                std::string_view input_label,
                                std::string_view output_label) {
            auto& runtime = opencl_runtime();
            if (!runtime.ready()) {
                return false;
            }

            auto gids = to_opencl_group_ids(group_ids, count);
            cl_int has_mask = 0;
            auto mask = validity_words(vec, count, has_mask);

            std::lock_guard<std::mutex> guard(runtime.mutex());
            runtime.clear_last_error();

            const auto d_values = create_input_buffer(runtime, input_values, count, input_label);
            const auto d_gids = create_device_buffer(runtime,
                                                     cl_mem_read_only | cl_mem_copy_host_ptr,
                                                     gids.data(),
                                                     sizeof(cl_uint) * gids.size(),
                                                     "group_ids");
            const auto d_mask = create_device_buffer(runtime,
                                                     cl_mem_read_only | cl_mem_copy_host_ptr,
                                                     mask.data(),
                                                     sizeof(cl_ulong) * mask.size(),
                                                     "validity_mask");
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
            if (!d_values || !d_gids || !d_mask || !d_out_values || !d_out_counts) {
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
            if (!set_buffer_arg(runtime, kernel, 0, d_values) || !set_buffer_arg(runtime, kernel, 1, d_gids) ||
                !set_buffer_arg(runtime, kernel, 2, d_mask) || !set_scalar_arg_typed(runtime, kernel, 3, has_mask) ||
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

        template<typename InputValue>
        bool run_avg_kernel(std::string_view kernel_name,
                            const InputValue* input_values,
                            const uint32_t* group_ids,
                            uint64_t count,
                            const vector::vector_t& vec,
                            std::vector<cl_ulong>& out_sum_bits,
                            std::vector<cl_ulong>& out_counts,
                            std::string_view input_label) {
            auto& runtime = opencl_runtime();
            if (!runtime.ready()) {
                return false;
            }

            auto gids = to_opencl_group_ids(group_ids, count);
            cl_int has_mask = 0;
            auto mask = validity_words(vec, count, has_mask);

            std::lock_guard<std::mutex> guard(runtime.mutex());
            runtime.clear_last_error();

            const auto d_values = create_input_buffer(runtime, input_values, count, input_label);
            const auto d_gids = create_device_buffer(runtime,
                                                     cl_mem_read_only | cl_mem_copy_host_ptr,
                                                     gids.data(),
                                                     sizeof(cl_uint) * gids.size(),
                                                     "group_ids");
            const auto d_mask = create_device_buffer(runtime,
                                                     cl_mem_read_only | cl_mem_copy_host_ptr,
                                                     mask.data(),
                                                     sizeof(cl_ulong) * mask.size(),
                                                     "validity_mask");
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
            if (!d_values || !d_gids || !d_mask || !d_out_sum || !d_out_counts) {
                return false;
            }

            const cl_kernel kernel = runtime.kernel(kernel_name);
            if (!kernel) {
                return false;
            }

            const auto rows = static_cast<cl_ulong>(count);
            if (!set_buffer_arg(runtime, kernel, 0, d_values) || !set_buffer_arg(runtime, kernel, 1, d_gids) ||
                !set_buffer_arg(runtime, kernel, 2, d_mask) || !set_scalar_arg_typed(runtime, kernel, 3, has_mask) ||
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

        template<typename InputValue>
        bool try_run_signed_aggregate(std::string_view regular_kernel,
                                      std::string_view avg_kernel,
                                      builtin_agg agg,
                                      const InputValue* input_values,
                                      const uint32_t* group_ids,
                                      uint64_t count,
                                      const vector::vector_t& vec,
                                      std::pmr::vector<raw_agg_state_t>& states) {
            auto out_counts = make_zero_vector<cl_ulong>(states.size());

            if (agg == builtin_agg::AVG) {
                auto out_sum_bits = make_float_identity_vector(states.size(), 0.0);
                if (!run_avg_kernel(avg_kernel,
                                    input_values,
                                    group_ids,
                                    count,
                                    vec,
                                    out_sum_bits,
                                    out_counts,
                                    "input_values")) {
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

            if (!run_regular_kernel(regular_kernel,
                                    input_values,
                                    agg,
                                    group_ids,
                                    count,
                                    vec,
                                    out_values,
                                    out_counts,
                                    "input_values",
                                    "output_values")) {
                return false;
            }

            fill_signed_states(states, out_values, out_counts);
            return true;
        }

        template<typename InputValue>
        bool try_run_unsigned_aggregate(std::string_view regular_kernel,
                                        std::string_view avg_kernel,
                                        builtin_agg agg,
                                        const InputValue* input_values,
                                        const uint32_t* group_ids,
                                        uint64_t count,
                                        const vector::vector_t& vec,
                                        std::pmr::vector<raw_agg_state_t>& states) {
            auto out_counts = make_zero_vector<cl_ulong>(states.size());

            if (agg == builtin_agg::AVG) {
                auto out_sum_bits = make_float_identity_vector(states.size(), 0.0);
                if (!run_avg_kernel(avg_kernel,
                                    input_values,
                                    group_ids,
                                    count,
                                    vec,
                                    out_sum_bits,
                                    out_counts,
                                    "input_values")) {
                    return false;
                }
                fill_float_states(states, out_sum_bits, out_counts);
                return true;
            }

            auto out_values = agg == builtin_agg::MIN
                                  ? make_filled_vector<cl_ulong>(states.size(), std::numeric_limits<cl_ulong>::max())
                                  : make_zero_vector<cl_ulong>(states.size());

            if (!run_regular_kernel(regular_kernel,
                                    input_values,
                                    agg,
                                    group_ids,
                                    count,
                                    vec,
                                    out_values,
                                    out_counts,
                                    "input_values",
                                    "output_values")) {
                return false;
            }

            fill_unsigned_states(states, out_values, out_counts);
            return true;
        }

        template<typename InputValue>
        bool try_run_floating_aggregate(std::string_view regular_kernel,
                                        std::string_view avg_kernel,
                                        builtin_agg agg,
                                        const InputValue* input_values,
                                        const uint32_t* group_ids,
                                        uint64_t count,
                                        const vector::vector_t& vec,
                                        std::pmr::vector<raw_agg_state_t>& states) {
            auto out_counts = make_zero_vector<cl_ulong>(states.size());

            if (agg == builtin_agg::AVG) {
                auto out_sum_bits = make_float_identity_vector(states.size(), 0.0);
                if (!run_avg_kernel(avg_kernel,
                                    input_values,
                                    group_ids,
                                    count,
                                    vec,
                                    out_sum_bits,
                                    out_counts,
                                    "input_values")) {
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
                                    input_values,
                                    agg,
                                    group_ids,
                                    count,
                                    vec,
                                    out_value_bits,
                                    out_counts,
                                    "input_values",
                                    "output_value_bits")) {
                return false;
            }

            fill_float_states(states, out_value_bits, out_counts);
            return true;
        }

    } // namespace

    bool try_run_count_star_gpu(const uint32_t* group_ids, uint64_t count, std::pmr::vector<raw_agg_state_t>& states) {
        if (states.empty()) {
            return true;
        }
        return run_count_kernel("agg_count_star_rows", group_ids, count, nullptr, states);
    }

    bool try_run_update_all_gpu(builtin_agg agg,
                                const vector::vector_t& vec,
                                const uint32_t* group_ids,
                                uint64_t count,
                                std::pmr::vector<raw_agg_state_t>& states) {
        if (states.empty()) {
            return true;
        }

        if (agg == builtin_agg::COUNT) {
            return run_count_kernel("agg_count_non_null_rows", group_ids, count, &vec, states);
        }

        switch (vec.type().type()) {
            case types::logical_type::TINYINT:
                return try_run_signed_aggregate("agg_i8_rows",
                                                "agg_i8_avg_rows",
                                                agg,
                                                vec.data<int8_t>(),
                                                group_ids,
                                                count,
                                                vec,
                                                states);
            case types::logical_type::SMALLINT:
                return try_run_signed_aggregate("agg_i16_rows",
                                                "agg_i16_avg_rows",
                                                agg,
                                                vec.data<int16_t>(),
                                                group_ids,
                                                count,
                                                vec,
                                                states);
            case types::logical_type::INTEGER:
                return try_run_signed_aggregate("agg_i32_rows",
                                                "agg_i32_avg_rows",
                                                agg,
                                                vec.data<int32_t>(),
                                                group_ids,
                                                count,
                                                vec,
                                                states);
            case types::logical_type::BIGINT:
                return try_run_signed_aggregate("agg_i64_rows",
                                                "agg_i64_avg_rows",
                                                agg,
                                                vec.data<int64_t>(),
                                                group_ids,
                                                count,
                                                vec,
                                                states);
            case types::logical_type::UTINYINT:
                return try_run_unsigned_aggregate("agg_u8_rows",
                                                  "agg_u8_avg_rows",
                                                  agg,
                                                  vec.data<uint8_t>(),
                                                  group_ids,
                                                  count,
                                                  vec,
                                                  states);
            case types::logical_type::USMALLINT:
                return try_run_unsigned_aggregate("agg_u16_rows",
                                                  "agg_u16_avg_rows",
                                                  agg,
                                                  vec.data<uint16_t>(),
                                                  group_ids,
                                                  count,
                                                  vec,
                                                  states);
            case types::logical_type::UINTEGER:
                return try_run_unsigned_aggregate("agg_u32_rows",
                                                  "agg_u32_avg_rows",
                                                  agg,
                                                  vec.data<uint32_t>(),
                                                  group_ids,
                                                  count,
                                                  vec,
                                                  states);
            case types::logical_type::UBIGINT:
                return try_run_unsigned_aggregate("agg_u64_rows",
                                                  "agg_u64_avg_rows",
                                                  agg,
                                                  vec.data<uint64_t>(),
                                                  group_ids,
                                                  count,
                                                  vec,
                                                  states);
            case types::logical_type::FLOAT:
                return try_run_floating_aggregate("agg_f32_rows",
                                                  "agg_f32_avg_rows",
                                                  agg,
                                                  vec.data<float>(),
                                                  group_ids,
                                                  count,
                                                  vec,
                                                  states);
            case types::logical_type::DOUBLE:
                return try_run_floating_aggregate("agg_f64_rows",
                                                  "agg_f64_avg_rows",
                                                  agg,
                                                  vec.data<double>(),
                                                  group_ids,
                                                  count,
                                                  vec,
                                                  states);
            default:
                opencl_runtime().set_last_error(
                    fmt::format("unsupported GPU aggregate type {}", static_cast<int>(vec.type().type())));
                return false;
        }
    }

#endif

} // namespace components::operators::aggregate::gpu
