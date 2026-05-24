#include "grouped_aggregate_gpu_internal.hpp"

#include <bit>
#include <limits>
#include <type_traits>
#include <vector>

#include <fmt/format.h>

#include <components/compute/gpu_aggregate.hpp>

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

        // Holds all the state for a single GPU aggregation request that must outlive
        // its kernel and read enqueues until flush_queue() returns:
        //   * the device output buffers (RAII-managed),
        //   * the host staging vectors that the non-blocking reads write into,
        //   * the shape that selects the post-flush state filler.
        struct pending_request_t {
            const gpu_update_request_t* request{nullptr};

            device_buffer_t d_values{};
            device_buffer_t d_counts{};

            std::vector<cl_long> i64_values;
            std::vector<cl_ulong> u64_values;
            std::vector<cl_ulong> bit_values;
            std::vector<cl_ulong> counts;

            enum class shape_t : uint8_t
            {
                none,
                count_only,
                signed_int,
                unsigned_int,
                floating
            };
            shape_t shape{shape_t::none};
        };

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
        requires std::is_trivially_copyable_v<T>
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
            // cl_uint is required to match uint32_t so that we can upload the
            // operator-supplied group_ids buffer directly without a redundant copy.
            static_assert(std::is_same_v<cl_uint, uint32_t>, "cl_uint must be uint32_t");

            query.group_ids = create_device_buffer(runtime,
                                                   cl_mem_read_only | cl_mem_copy_host_ptr,
                                                   group_ids,
                                                   sizeof(cl_uint) * static_cast<size_t>(count),
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

        // Shared kernel arg setup for the typed regular kernels (sum/min/max).
        bool set_regular_kernel_args(opencl_runtime_t& runtime,
                                     cl_kernel kernel,
                                     builtin_agg agg,
                                     const device_query_t& query,
                                     const device_column_t& column,
                                     uint64_t count,
                                     const device_buffer_t& d_out_values,
                                     const device_buffer_t& d_out_counts) {
            const auto rows = static_cast<cl_ulong>(count);
            const cl_int agg_code = agg_code_for_opencl(agg);
            if (agg_code < 0) {
                runtime.set_last_error("unsupported aggregate kind for OpenCL regular kernel");
                return false;
            }
            return set_buffer_arg(runtime, kernel, 0, column.values) &&
                   set_buffer_arg(runtime, kernel, 1, query.group_ids) &&
                   set_buffer_arg(runtime, kernel, 2, column.mask) &&
                   set_scalar_arg_typed(runtime, kernel, 3, column.has_mask) &&
                   set_scalar_arg_typed(runtime, kernel, 4, rows) &&
                   set_scalar_arg_typed(runtime, kernel, 5, agg_code) &&
                   set_buffer_arg(runtime, kernel, 6, d_out_values) &&
                   set_buffer_arg(runtime, kernel, 7, d_out_counts);
        }

        // Shared kernel arg setup for the typed avg kernels.
        bool set_avg_kernel_args(opencl_runtime_t& runtime,
                                 cl_kernel kernel,
                                 const device_query_t& query,
                                 const device_column_t& column,
                                 uint64_t count,
                                 const device_buffer_t& d_out_sum_bits,
                                 const device_buffer_t& d_out_counts) {
            const auto rows = static_cast<cl_ulong>(count);
            return set_buffer_arg(runtime, kernel, 0, column.values) &&
                   set_buffer_arg(runtime, kernel, 1, query.group_ids) &&
                   set_buffer_arg(runtime, kernel, 2, column.mask) &&
                   set_scalar_arg_typed(runtime, kernel, 3, column.has_mask) &&
                   set_scalar_arg_typed(runtime, kernel, 4, rows) &&
                   set_buffer_arg(runtime, kernel, 5, d_out_sum_bits) &&
                   set_buffer_arg(runtime, kernel, 6, d_out_counts);
        }

        bool enqueue_count_kernel(std::string_view kernel_name,
                                  opencl_runtime_t& runtime,
                                  const device_query_t& query,
                                  uint64_t count,
                                  const device_column_t* column,
                                  size_t state_count,
                                  pending_request_t& out) {
            out.shape = pending_request_t::shape_t::count_only;
            out.u64_values.assign(state_count, 0);
            // The kernel writes both buffers, but the host only consumes values.
            // We still allocate the counts buffer because it's a required kernel argument.
            out.counts.assign(state_count, 0);

            out.d_values = create_device_buffer(runtime,
                                                cl_mem_read_write | cl_mem_copy_host_ptr,
                                                out.u64_values.data(),
                                                sizeof(cl_ulong) * state_count,
                                                "count_values");
            out.d_counts = create_device_buffer(runtime,
                                                cl_mem_read_write | cl_mem_copy_host_ptr,
                                                out.counts.data(),
                                                sizeof(cl_ulong) * state_count,
                                                "count_counts");
            if (!out.d_values || !out.d_counts) {
                return false;
            }

            const cl_kernel kernel = runtime.kernel(kernel_name);
            if (!kernel) {
                return false;
            }

            cl_uint arg = 0;
            if (!set_buffer_arg(runtime, kernel, arg++, query.group_ids)) {
                return false;
            }
            if (column != nullptr) {
                if (!set_buffer_arg(runtime, kernel, arg++, column->mask) ||
                    !set_scalar_arg_typed(runtime, kernel, arg++, column->has_mask)) {
                    return false;
                }
            }
            const auto rows = static_cast<cl_ulong>(count);
            if (!set_scalar_arg_typed(runtime, kernel, arg++, rows) ||
                !set_buffer_arg(runtime, kernel, arg++, out.d_values) ||
                !set_buffer_arg(runtime, kernel, arg++, out.d_counts)) {
                return false;
            }

            // Counts buffer is intentionally not read back: fill_count_states
            // derives both raw_agg_state_t::u64 and ::count from values alone.
            return enqueue_1d_kernel(runtime, kernel, count) &&
                   enqueue_read_buffer_async(runtime,
                                             out.d_values,
                                             out.u64_values.data(),
                                             sizeof(cl_ulong) * state_count,
                                             "count_values");
        }

        bool enqueue_signed_aggregate(std::string_view regular_kernel_name,
                                      std::string_view avg_kernel_name,
                                      builtin_agg agg,
                                      opencl_runtime_t& runtime,
                                      const device_query_t& query,
                                      const device_column_t& column,
                                      uint64_t count,
                                      size_t state_count,
                                      pending_request_t& out) {
            out.counts.assign(state_count, 0);
            out.d_counts = create_device_buffer(runtime,
                                                cl_mem_read_write | cl_mem_copy_host_ptr,
                                                out.counts.data(),
                                                sizeof(cl_ulong) * state_count,
                                                "output_counts");
            if (!out.d_counts) {
                return false;
            }

            if (agg == builtin_agg::AVG) {
                out.shape = pending_request_t::shape_t::floating;
                out.bit_values.assign(state_count, std::bit_cast<cl_ulong>(0.0));
                out.d_values = create_device_buffer(runtime,
                                                    cl_mem_read_write | cl_mem_copy_host_ptr,
                                                    out.bit_values.data(),
                                                    sizeof(cl_ulong) * state_count,
                                                    "output_sum_bits");
                if (!out.d_values) {
                    return false;
                }
                const cl_kernel kernel = runtime.kernel(avg_kernel_name);
                if (!kernel) {
                    return false;
                }
                return set_avg_kernel_args(runtime, kernel, query, column, count, out.d_values, out.d_counts) &&
                       enqueue_1d_kernel(runtime, kernel, count) &&
                       enqueue_read_buffer_async(runtime,
                                                 out.d_values,
                                                 out.bit_values.data(),
                                                 sizeof(cl_ulong) * state_count,
                                                 "output_sum_bits") &&
                       enqueue_read_buffer_async(runtime,
                                                 out.d_counts,
                                                 out.counts.data(),
                                                 sizeof(cl_ulong) * state_count,
                                                 "output_counts");
            }

            out.shape = pending_request_t::shape_t::signed_int;
            const cl_long initial = (agg == builtin_agg::MIN) ? std::numeric_limits<cl_long>::max()
                                  : (agg == builtin_agg::MAX) ? std::numeric_limits<cl_long>::min()
                                                              : cl_long{0};
            out.i64_values.assign(state_count, initial);
            out.d_values = create_device_buffer(runtime,
                                                cl_mem_read_write | cl_mem_copy_host_ptr,
                                                out.i64_values.data(),
                                                sizeof(cl_long) * state_count,
                                                "output_values");
            if (!out.d_values) {
                return false;
            }
            const cl_kernel kernel = runtime.kernel(regular_kernel_name);
            if (!kernel) {
                return false;
            }
            return set_regular_kernel_args(runtime, kernel, agg, query, column, count, out.d_values, out.d_counts) &&
                   enqueue_1d_kernel(runtime, kernel, count) &&
                   enqueue_read_buffer_async(runtime,
                                             out.d_values,
                                             out.i64_values.data(),
                                             sizeof(cl_long) * state_count,
                                             "output_values") &&
                   enqueue_read_buffer_async(runtime,
                                             out.d_counts,
                                             out.counts.data(),
                                             sizeof(cl_ulong) * state_count,
                                             "output_counts");
        }

        bool enqueue_unsigned_aggregate(std::string_view regular_kernel_name,
                                        std::string_view avg_kernel_name,
                                        builtin_agg agg,
                                        opencl_runtime_t& runtime,
                                        const device_query_t& query,
                                        const device_column_t& column,
                                        uint64_t count,
                                        size_t state_count,
                                        pending_request_t& out) {
            out.counts.assign(state_count, 0);
            out.d_counts = create_device_buffer(runtime,
                                                cl_mem_read_write | cl_mem_copy_host_ptr,
                                                out.counts.data(),
                                                sizeof(cl_ulong) * state_count,
                                                "output_counts");
            if (!out.d_counts) {
                return false;
            }

            if (agg == builtin_agg::AVG) {
                out.shape = pending_request_t::shape_t::floating;
                out.bit_values.assign(state_count, std::bit_cast<cl_ulong>(0.0));
                out.d_values = create_device_buffer(runtime,
                                                    cl_mem_read_write | cl_mem_copy_host_ptr,
                                                    out.bit_values.data(),
                                                    sizeof(cl_ulong) * state_count,
                                                    "output_sum_bits");
                if (!out.d_values) {
                    return false;
                }
                const cl_kernel kernel = runtime.kernel(avg_kernel_name);
                if (!kernel) {
                    return false;
                }
                return set_avg_kernel_args(runtime, kernel, query, column, count, out.d_values, out.d_counts) &&
                       enqueue_1d_kernel(runtime, kernel, count) &&
                       enqueue_read_buffer_async(runtime,
                                                 out.d_values,
                                                 out.bit_values.data(),
                                                 sizeof(cl_ulong) * state_count,
                                                 "output_sum_bits") &&
                       enqueue_read_buffer_async(runtime,
                                                 out.d_counts,
                                                 out.counts.data(),
                                                 sizeof(cl_ulong) * state_count,
                                                 "output_counts");
            }

            out.shape = pending_request_t::shape_t::unsigned_int;
            const cl_ulong initial = (agg == builtin_agg::MIN) ? std::numeric_limits<cl_ulong>::max() : cl_ulong{0};
            out.u64_values.assign(state_count, initial);
            out.d_values = create_device_buffer(runtime,
                                                cl_mem_read_write | cl_mem_copy_host_ptr,
                                                out.u64_values.data(),
                                                sizeof(cl_ulong) * state_count,
                                                "output_values");
            if (!out.d_values) {
                return false;
            }
            const cl_kernel kernel = runtime.kernel(regular_kernel_name);
            if (!kernel) {
                return false;
            }
            return set_regular_kernel_args(runtime, kernel, agg, query, column, count, out.d_values, out.d_counts) &&
                   enqueue_1d_kernel(runtime, kernel, count) &&
                   enqueue_read_buffer_async(runtime,
                                             out.d_values,
                                             out.u64_values.data(),
                                             sizeof(cl_ulong) * state_count,
                                             "output_values") &&
                   enqueue_read_buffer_async(runtime,
                                             out.d_counts,
                                             out.counts.data(),
                                             sizeof(cl_ulong) * state_count,
                                             "output_counts");
        }

        bool enqueue_floating_aggregate(std::string_view regular_kernel_name,
                                        std::string_view avg_kernel_name,
                                        builtin_agg agg,
                                        opencl_runtime_t& runtime,
                                        const device_query_t& query,
                                        const device_column_t& column,
                                        uint64_t count,
                                        size_t state_count,
                                        pending_request_t& out) {
            out.shape = pending_request_t::shape_t::floating;
            out.counts.assign(state_count, 0);
            out.d_counts = create_device_buffer(runtime,
                                                cl_mem_read_write | cl_mem_copy_host_ptr,
                                                out.counts.data(),
                                                sizeof(cl_ulong) * state_count,
                                                "output_counts");
            if (!out.d_counts) {
                return false;
            }

            const double initial =
                (agg == builtin_agg::AVG)   ? 0.0
                : (agg == builtin_agg::MIN) ? std::numeric_limits<double>::infinity()
                : (agg == builtin_agg::MAX) ? -std::numeric_limits<double>::infinity()
                                            : 0.0;
            out.bit_values.assign(state_count, std::bit_cast<cl_ulong>(initial));
            const std::string_view value_label = (agg == builtin_agg::AVG) ? "output_sum_bits" : "output_value_bits";
            out.d_values = create_device_buffer(runtime,
                                                cl_mem_read_write | cl_mem_copy_host_ptr,
                                                out.bit_values.data(),
                                                sizeof(cl_ulong) * state_count,
                                                value_label);
            if (!out.d_values) {
                return false;
            }

            const cl_kernel kernel =
                runtime.kernel(agg == builtin_agg::AVG ? avg_kernel_name : regular_kernel_name);
            if (!kernel) {
                return false;
            }

            const bool args_ok =
                (agg == builtin_agg::AVG)
                    ? set_avg_kernel_args(runtime, kernel, query, column, count, out.d_values, out.d_counts)
                    : set_regular_kernel_args(
                          runtime, kernel, agg, query, column, count, out.d_values, out.d_counts);

            return args_ok && enqueue_1d_kernel(runtime, kernel, count) &&
                   enqueue_read_buffer_async(runtime,
                                             out.d_values,
                                             out.bit_values.data(),
                                             sizeof(cl_ulong) * state_count,
                                             value_label) &&
                   enqueue_read_buffer_async(runtime,
                                             out.d_counts,
                                             out.counts.data(),
                                             sizeof(cl_ulong) * state_count,
                                             "output_counts");
        }

        bool enqueue_request(opencl_runtime_t& runtime,
                             const device_query_t& query,
                             uint64_t count,
                             const gpu_update_request_t& request,
                             pending_request_t& out) {
            if (request.states == nullptr || request.states->empty()) {
                return true;
            }
            out.request = &request;
            const size_t state_count = request.states->size();

            if (request.descriptor != nullptr && request.descriptor->is_custom()) {
                out.shape = pending_request_t::shape_t::none;
                return true;
            }

            if (request.is_count_star) {
                return enqueue_count_kernel(
                    "agg_count_star_rows", runtime, query, count, nullptr, state_count, out);
            }

            const auto* column = query.find_column(request.input_id);
            if (column == nullptr || request.vec == nullptr) {
                runtime.set_last_error("GPU batch request is missing uploaded input column");
                return false;
            }

            if (request.kind == builtin_agg::COUNT) {
                return enqueue_count_kernel(
                    "agg_count_non_null_rows", runtime, query, count, column, state_count, out);
            }

            switch (request.vec->type().type()) {
                case types::logical_type::TINYINT:
                    return enqueue_signed_aggregate("agg_i8_rows",
                                                    "agg_i8_avg_rows",
                                                    request.kind,
                                                    runtime,
                                                    query,
                                                    *column,
                                                    count,
                                                    state_count,
                                                    out);
                case types::logical_type::SMALLINT:
                    return enqueue_signed_aggregate("agg_i16_rows",
                                                    "agg_i16_avg_rows",
                                                    request.kind,
                                                    runtime,
                                                    query,
                                                    *column,
                                                    count,
                                                    state_count,
                                                    out);
                case types::logical_type::INTEGER:
                    return enqueue_signed_aggregate("agg_i32_rows",
                                                    "agg_i32_avg_rows",
                                                    request.kind,
                                                    runtime,
                                                    query,
                                                    *column,
                                                    count,
                                                    state_count,
                                                    out);
                case types::logical_type::BIGINT:
                    return enqueue_signed_aggregate("agg_i64_rows",
                                                    "agg_i64_avg_rows",
                                                    request.kind,
                                                    runtime,
                                                    query,
                                                    *column,
                                                    count,
                                                    state_count,
                                                    out);
                case types::logical_type::UTINYINT:
                    return enqueue_unsigned_aggregate("agg_u8_rows",
                                                      "agg_u8_avg_rows",
                                                      request.kind,
                                                      runtime,
                                                      query,
                                                      *column,
                                                      count,
                                                      state_count,
                                                      out);
                case types::logical_type::USMALLINT:
                    return enqueue_unsigned_aggregate("agg_u16_rows",
                                                      "agg_u16_avg_rows",
                                                      request.kind,
                                                      runtime,
                                                      query,
                                                      *column,
                                                      count,
                                                      state_count,
                                                      out);
                case types::logical_type::UINTEGER:
                    return enqueue_unsigned_aggregate("agg_u32_rows",
                                                      "agg_u32_avg_rows",
                                                      request.kind,
                                                      runtime,
                                                      query,
                                                      *column,
                                                      count,
                                                      state_count,
                                                      out);
                case types::logical_type::UBIGINT:
                    return enqueue_unsigned_aggregate("agg_u64_rows",
                                                      "agg_u64_avg_rows",
                                                      request.kind,
                                                      runtime,
                                                      query,
                                                      *column,
                                                      count,
                                                      state_count,
                                                      out);
                case types::logical_type::FLOAT:
                    return enqueue_floating_aggregate("agg_f32_rows",
                                                      "agg_f32_avg_rows",
                                                      request.kind,
                                                      runtime,
                                                      query,
                                                      *column,
                                                      count,
                                                      state_count,
                                                      out);
                case types::logical_type::DOUBLE:
                    return enqueue_floating_aggregate("agg_f64_rows",
                                                      "agg_f64_avg_rows",
                                                      request.kind,
                                                      runtime,
                                                      query,
                                                      *column,
                                                      count,
                                                      state_count,
                                                      out);
                default:
                    runtime.set_last_error(
                        fmt::format("unsupported GPU aggregate type {}", static_cast<int>(request.vec->type().type())));
                    return false;
            }
        }

        void finalize_pending(pending_request_t& pending) {
            if (pending.request == nullptr || pending.request->states == nullptr) {
                return;
            }
            auto& states = *pending.request->states;
            switch (pending.shape) {
                case pending_request_t::shape_t::count_only:
                    fill_count_states(states, pending.u64_values);
                    return;
                case pending_request_t::shape_t::signed_int:
                    fill_signed_states(states, pending.i64_values, pending.counts);
                    return;
                case pending_request_t::shape_t::unsigned_int:
                    fill_unsigned_states(states, pending.u64_values, pending.counts);
                    return;
                case pending_request_t::shape_t::floating:
                    fill_float_states(states, pending.bit_values, pending.counts);
                    return;
                case pending_request_t::shape_t::none:
                    return;
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

        for (size_t index = 0; index < request_count; index++) {
            if (requests[index].descriptor != nullptr && requests[index].descriptor->is_custom()) {
                opencl_runtime().set_last_error(
                    "custom GPU aggregate descriptors are not yet wired into the operator pipeline");
                return false;
            }
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

        std::vector<pending_request_t> pending(request_count);
        for (size_t index = 0; index < request_count; index++) {
            if (!enqueue_request(runtime, query, count, requests[index], pending[index])) {
                return false;
            }
        }

        if (!flush_queue(runtime)) {
            return false;
        }

        for (auto& p : pending) {
            finalize_pending(p);
        }

        return true;
    }
#else
    bool try_run_batch_gpu(const gpu_update_request_t*, size_t, const uint32_t*, uint64_t) { return false; }
#endif

} // namespace components::operators::aggregate::gpu
