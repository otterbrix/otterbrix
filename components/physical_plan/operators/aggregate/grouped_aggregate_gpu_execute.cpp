#include "grouped_aggregate_gpu_internal.hpp"

#include <bit>
#include <limits>
#include <vector>

#include <fmt/format.h>

namespace components::operators::aggregate::gpu {

#if defined(__linux__)
    namespace {

        template<typename Src, typename Dst>
        std::vector<Dst> materialize_input(const Src* data, uint64_t count) {
            std::vector<Dst> result(static_cast<size_t>(count));
            for (uint64_t row = 0; row < count; row++) {
                result[static_cast<size_t>(row)] = static_cast<Dst>(data[row]);
            }
            return result;
        }

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

        enum class gpu_domain_t
        {
            signed_integer,
            unsigned_integer,
            floating_point,
            unsupported,
        };

        gpu_domain_t detect_gpu_domain(types::logical_type type) {
            switch (type) {
                case types::logical_type::TINYINT:
                case types::logical_type::SMALLINT:
                case types::logical_type::INTEGER:
                case types::logical_type::BIGINT:
                    return gpu_domain_t::signed_integer;
                case types::logical_type::UTINYINT:
                case types::logical_type::USMALLINT:
                case types::logical_type::UINTEGER:
                case types::logical_type::UBIGINT:
                    return gpu_domain_t::unsigned_integer;
                case types::logical_type::FLOAT:
                case types::logical_type::DOUBLE:
                    return gpu_domain_t::floating_point;
                default:
                    return gpu_domain_t::unsupported;
            }
        }

        std::vector<cl_long> materialize_signed_input(const vector::vector_t& vec, uint64_t count, bool& ok) {
            ok = true;
            switch (vec.type().type()) {
                case types::logical_type::TINYINT:
                    return materialize_input<int8_t, cl_long>(vec.data<int8_t>(), count);
                case types::logical_type::SMALLINT:
                    return materialize_input<int16_t, cl_long>(vec.data<int16_t>(), count);
                case types::logical_type::INTEGER:
                    return materialize_input<int32_t, cl_long>(vec.data<int32_t>(), count);
                case types::logical_type::BIGINT:
                    return materialize_input<int64_t, cl_long>(vec.data<int64_t>(), count);
                default:
                    ok = false;
                    return {};
            }
        }

        std::vector<cl_ulong> materialize_unsigned_input(const vector::vector_t& vec, uint64_t count, bool& ok) {
            ok = true;
            switch (vec.type().type()) {
                case types::logical_type::UTINYINT:
                    return materialize_input<uint8_t, cl_ulong>(vec.data<uint8_t>(), count);
                case types::logical_type::USMALLINT:
                    return materialize_input<uint16_t, cl_ulong>(vec.data<uint16_t>(), count);
                case types::logical_type::UINTEGER:
                    return materialize_input<uint32_t, cl_ulong>(vec.data<uint32_t>(), count);
                case types::logical_type::UBIGINT:
                    return materialize_input<uint64_t, cl_ulong>(vec.data<uint64_t>(), count);
                default:
                    ok = false;
                    return {};
            }
        }

        std::vector<cl_double> materialize_floating_input(const vector::vector_t& vec, uint64_t count, bool& ok) {
            ok = true;
            switch (vec.type().type()) {
                case types::logical_type::FLOAT:
                    return materialize_input<float, cl_double>(vec.data<float>(), count);
                case types::logical_type::DOUBLE:
                    return materialize_input<double, cl_double>(vec.data<double>(), count);
                default:
                    ok = false;
                    return {};
            }
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
            }
        }

        void fill_signed_states(std::pmr::vector<raw_agg_state_t>& states,
                                const std::vector<cl_long>& values,
                                const std::vector<cl_ulong>& counts) {
            for (size_t group = 0; group < states.size(); group++) {
                states[group].i64 = static_cast<int64_t>(values[group]);
                states[group].count = static_cast<uint64_t>(counts[group]);
                states[group].initialized = counts[group] > 0;
            }
        }

        void fill_unsigned_states(std::pmr::vector<raw_agg_state_t>& states,
                                  const std::vector<cl_ulong>& values,
                                  const std::vector<cl_ulong>& counts) {
            for (size_t group = 0; group < states.size(); group++) {
                states[group].u64 = static_cast<uint64_t>(values[group]);
                states[group].count = static_cast<uint64_t>(counts[group]);
                states[group].initialized = counts[group] > 0;
            }
        }

        void fill_float_states(std::pmr::vector<raw_agg_state_t>& states,
                               const std::vector<cl_ulong>& value_bits,
                               const std::vector<cl_ulong>& counts) {
            for (size_t group = 0; group < states.size(); group++) {
                states[group].f64 = std::bit_cast<double>(value_bits[group]);
                states[group].count = static_cast<uint64_t>(counts[group]);
                states[group].initialized = counts[group] > 0;
            }
        }

        template<typename T>
        bool set_scalar_arg_typed(opencl_runtime_t& runtime, cl_kernel kernel, cl_uint index, const T& value) {
            return set_scalar_arg(runtime, kernel, index, &value, sizeof(T));
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

        template<typename HostValue>
        bool run_regular_integer_kernel(std::string_view kernel_name,
                                        std::vector<HostValue>& input_values,
                                        builtin_agg agg,
                                        const uint32_t* group_ids,
                                        uint64_t count,
                                        const vector::vector_t& vec,
                                        std::vector<HostValue>& out_values,
                                        std::vector<cl_ulong>& out_counts) {
            auto& runtime = opencl_runtime();
            if (!runtime.ready()) {
                return false;
            }

            auto gids = to_opencl_group_ids(group_ids, count);
            cl_int has_mask = 0;
            auto mask = validity_words(vec, count, has_mask);

            std::lock_guard<std::mutex> guard(runtime.mutex());
            runtime.clear_last_error();

            const auto d_values = create_device_buffer(runtime,
                                                       cl_mem_read_only | cl_mem_copy_host_ptr,
                                                       input_values.data(),
                                                       sizeof(HostValue) * input_values.size(),
                                                       "input_values");
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
                                                     sizeof(HostValue) * out_values.size(),
                                                     "output_values");
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
                    runtime.set_last_error("unsupported aggregate kind for OpenCL integer kernel");
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
                                    sizeof(HostValue) * out_values.size(),
                                    "output_values") ||
                !read_device_buffer(runtime,
                                    d_out_counts,
                                    out_counts.data(),
                                    sizeof(cl_ulong) * out_counts.size(),
                                    "output_counts")) {
                return false;
            }

            return true;
        }

        bool run_float_kernel(std::vector<cl_double>& input_values,
                              builtin_agg agg,
                              const uint32_t* group_ids,
                              uint64_t count,
                              const vector::vector_t& vec,
                              std::vector<cl_ulong>& out_value_bits,
                              std::vector<cl_ulong>& out_counts) {
            auto& runtime = opencl_runtime();
            if (!runtime.ready()) {
                return false;
            }

            auto gids = to_opencl_group_ids(group_ids, count);
            cl_int has_mask = 0;
            auto mask = validity_words(vec, count, has_mask);

            std::lock_guard<std::mutex> guard(runtime.mutex());
            runtime.clear_last_error();

            const auto d_values = create_device_buffer(runtime,
                                                       cl_mem_read_only | cl_mem_copy_host_ptr,
                                                       input_values.data(),
                                                       sizeof(cl_double) * input_values.size(),
                                                       "input_values");
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
                                                     out_value_bits.data(),
                                                     sizeof(cl_ulong) * out_value_bits.size(),
                                                     "output_value_bits");
            auto d_out_counts = create_device_buffer(runtime,
                                                     cl_mem_read_write | cl_mem_copy_host_ptr,
                                                     out_counts.data(),
                                                     sizeof(cl_ulong) * out_counts.size(),
                                                     "output_counts");
            if (!d_values || !d_gids || !d_mask || !d_out_values || !d_out_counts) {
                return false;
            }

            const cl_kernel kernel = runtime.kernel("agg_float_rows");
            const cl_int agg_code = agg_code_for_opencl(agg);
            if (!kernel || agg_code < 0) {
                if (agg_code < 0) {
                    runtime.set_last_error("unsupported aggregate kind for OpenCL floating kernel");
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
                                    out_value_bits.data(),
                                    sizeof(cl_ulong) * out_value_bits.size(),
                                    "output_value_bits") ||
                !read_device_buffer(runtime,
                                    d_out_counts,
                                    out_counts.data(),
                                    sizeof(cl_ulong) * out_counts.size(),
                                    "output_counts")) {
                return false;
            }

            return true;
        }

        template<typename HostValue>
        bool run_avg_kernel(std::string_view kernel_name,
                            std::vector<HostValue>& input_values,
                            const uint32_t* group_ids,
                            uint64_t count,
                            const vector::vector_t& vec,
                            std::vector<cl_ulong>& out_sum_bits,
                            std::vector<cl_ulong>& out_counts) {
            auto& runtime = opencl_runtime();
            if (!runtime.ready()) {
                return false;
            }

            auto gids = to_opencl_group_ids(group_ids, count);
            cl_int has_mask = 0;
            auto mask = validity_words(vec, count, has_mask);

            std::lock_guard<std::mutex> guard(runtime.mutex());
            runtime.clear_last_error();

            const auto d_values = create_device_buffer(runtime,
                                                       cl_mem_read_only | cl_mem_copy_host_ptr,
                                                       input_values.data(),
                                                       sizeof(HostValue) * input_values.size(),
                                                       "input_values");
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

        const auto domain = detect_gpu_domain(vec.type().type());
        if (domain == gpu_domain_t::unsupported) {
            opencl_runtime().set_last_error(
                fmt::format("unsupported GPU aggregate type {}", static_cast<int>(vec.type().type())));
            return false;
        }

        auto out_counts = make_zero_vector<cl_ulong>(states.size());

        if (agg == builtin_agg::AVG) {
            auto out_sum_bits = make_float_identity_vector(states.size(), 0.0);

            if (domain == gpu_domain_t::signed_integer) {
                bool ok = false;
                auto input_values = materialize_signed_input(vec, count, ok);
                if (!ok) {
                    return false;
                }
                if (!run_avg_kernel("agg_signed_avg_rows",
                                    input_values,
                                    group_ids,
                                    count,
                                    vec,
                                    out_sum_bits,
                                    out_counts)) {
                    return false;
                }
            } else if (domain == gpu_domain_t::unsigned_integer) {
                bool ok = false;
                auto input_values = materialize_unsigned_input(vec, count, ok);
                if (!ok) {
                    return false;
                }
                if (!run_avg_kernel("agg_unsigned_avg_rows",
                                    input_values,
                                    group_ids,
                                    count,
                                    vec,
                                    out_sum_bits,
                                    out_counts)) {
                    return false;
                }
            } else {
                bool ok = false;
                auto input_values = materialize_floating_input(vec, count, ok);
                if (!ok) {
                    return false;
                }
                if (!run_avg_kernel("agg_float_avg_rows",
                                    input_values,
                                    group_ids,
                                    count,
                                    vec,
                                    out_sum_bits,
                                    out_counts)) {
                    return false;
                }
            }

            fill_float_states(states, out_sum_bits, out_counts);
            return true;
        }

        if (domain == gpu_domain_t::signed_integer) {
            bool ok = false;
            auto input_values = materialize_signed_input(vec, count, ok);
            if (!ok) {
                return false;
            }

            auto out_values = agg == builtin_agg::MIN
                                  ? make_filled_vector<cl_long>(states.size(), std::numeric_limits<cl_long>::max())
                              : agg == builtin_agg::MAX
                                  ? make_filled_vector<cl_long>(states.size(), std::numeric_limits<cl_long>::min())
                                  : make_zero_vector<cl_long>(states.size());

            if (!run_regular_integer_kernel("agg_signed_rows",
                                            input_values,
                                            agg,
                                            group_ids,
                                            count,
                                            vec,
                                            out_values,
                                            out_counts)) {
                return false;
            }

            fill_signed_states(states, out_values, out_counts);
            return true;
        }

        if (domain == gpu_domain_t::unsigned_integer) {
            bool ok = false;
            auto input_values = materialize_unsigned_input(vec, count, ok);
            if (!ok) {
                return false;
            }

            auto out_values = agg == builtin_agg::MIN
                                  ? make_filled_vector<cl_ulong>(states.size(), std::numeric_limits<cl_ulong>::max())
                                  : make_zero_vector<cl_ulong>(states.size());

            if (!run_regular_integer_kernel("agg_unsigned_rows",
                                            input_values,
                                            agg,
                                            group_ids,
                                            count,
                                            vec,
                                            out_values,
                                            out_counts)) {
                return false;
            }

            fill_unsigned_states(states, out_values, out_counts);
            return true;
        }

        bool ok = false;
        auto input_values = materialize_floating_input(vec, count, ok);
        if (!ok) {
            return false;
        }

        auto out_value_bits = agg == builtin_agg::MIN
                                  ? make_float_identity_vector(states.size(), std::numeric_limits<double>::infinity())
                              : agg == builtin_agg::MAX
                                  ? make_float_identity_vector(states.size(), -std::numeric_limits<double>::infinity())
                                  : make_float_identity_vector(states.size(), 0.0);

        if (!run_float_kernel(input_values, agg, group_ids, count, vec, out_value_bits, out_counts)) {
            return false;
        }

        fill_float_states(states, out_value_bits, out_counts);
        return true;
    }

#endif

} // namespace components::operators::aggregate::gpu
