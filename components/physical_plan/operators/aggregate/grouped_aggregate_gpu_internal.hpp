#pragma once

#include "grouped_aggregate.hpp"

#include <cstdint>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace components::operators::aggregate::gpu {

    const char* agg_name(builtin_agg agg);
    bool read_bool_env(std::string_view name);
    bool gpu_group_aggregate_strict_enabled();
    void log_info_if_available(const std::string& message);
    void log_warn_if_available(const std::string& message);

#if defined(__linux__)
    struct _cl_platform_id;
    struct _cl_device_id;
    struct _cl_context;
    struct _cl_command_queue;
    struct _cl_program;
    struct _cl_kernel;
    struct _cl_mem;

    using cl_platform_id = _cl_platform_id*;
    using cl_device_id = _cl_device_id*;
    using cl_context = _cl_context*;
    using cl_command_queue = _cl_command_queue*;
    using cl_program = _cl_program*;
    using cl_kernel = _cl_kernel*;
    using cl_mem = _cl_mem*;

    using cl_int = int32_t;
    using cl_uint = uint32_t;
    using cl_ulong = uint64_t;
    using cl_long = int64_t;
    using cl_double = double;
    using cl_bool = cl_uint;
    using cl_bitfield = cl_ulong;
    using cl_device_type = cl_bitfield;
    using cl_mem_flags = cl_bitfield;
    using cl_command_queue_properties = cl_bitfield;
    using cl_context_properties = intptr_t;

    constexpr cl_int cl_success = 0;
    constexpr cl_int cl_device_not_found = -1;
    constexpr cl_bool cl_true = 1;
    constexpr cl_device_type cl_device_type_gpu = (cl_device_type(1) << 2);
    constexpr cl_mem_flags cl_mem_read_write = (cl_mem_flags(1) << 0);
    constexpr cl_mem_flags cl_mem_read_only = (cl_mem_flags(1) << 2);
    constexpr cl_mem_flags cl_mem_copy_host_ptr = (cl_mem_flags(1) << 5);
    constexpr cl_uint cl_program_build_log = 0x1183U;
    constexpr cl_context_properties cl_context_platform = 0x1084;
    constexpr size_t default_local_size = 256;

    using clGetPlatformIDs_fn = cl_int (*)(cl_uint, cl_platform_id*, cl_uint*);
    using clGetDeviceIDs_fn = cl_int (*)(cl_platform_id, cl_device_type, cl_uint, cl_device_id*, cl_uint*);
    using clCreateContext_fn = cl_context (*)(const cl_context_properties*,
                                              cl_uint,
                                              const cl_device_id*,
                                              void (*)(const char*, const void*, size_t, void*),
                                              void*,
                                              cl_int*);
    using clCreateCommandQueue_fn = cl_command_queue (*)(cl_context,
                                                         cl_device_id,
                                                         cl_command_queue_properties,
                                                         cl_int*);
    using clCreateProgramWithSource_fn =
        cl_program (*)(cl_context, cl_uint, const char**, const size_t*, cl_int*);
    using clBuildProgram_fn =
        cl_int (*)(cl_program, cl_uint, const cl_device_id*, const char*, void (*)(cl_program, void*), void*);
    using clGetProgramBuildInfo_fn =
        cl_int (*)(cl_program, cl_device_id, cl_uint, size_t, void*, size_t*);
    using clCreateKernel_fn = cl_kernel (*)(cl_program, const char*, cl_int*);
    using clCreateBuffer_fn = cl_mem (*)(cl_context, cl_mem_flags, size_t, void*, cl_int*);
    using clSetKernelArg_fn = cl_int (*)(cl_kernel, cl_uint, size_t, const void*);
    using clEnqueueNDRangeKernel_fn = cl_int (*)(cl_command_queue,
                                                 cl_kernel,
                                                 cl_uint,
                                                 const size_t*,
                                                 const size_t*,
                                                 const size_t*,
                                                 cl_uint,
                                                 const void*,
                                                 void*);
    using clEnqueueReadBuffer_fn =
        cl_int (*)(cl_command_queue, cl_mem, cl_bool, size_t, size_t, void*, cl_uint, const void*, void*);
    using clFinish_fn = cl_int (*)(cl_command_queue);
    using clReleaseMemObject_fn = cl_int (*)(cl_mem);
    using clReleaseKernel_fn = cl_int (*)(cl_kernel);
    using clReleaseProgram_fn = cl_int (*)(cl_program);
    using clReleaseCommandQueue_fn = cl_int (*)(cl_command_queue);
    using clReleaseContext_fn = cl_int (*)(cl_context);

    struct opencl_api_t {
        void* lib{nullptr};
        clGetPlatformIDs_fn clGetPlatformIDs{nullptr};
        clGetDeviceIDs_fn clGetDeviceIDs{nullptr};
        clCreateContext_fn clCreateContext{nullptr};
        clCreateCommandQueue_fn clCreateCommandQueue{nullptr};
        clCreateProgramWithSource_fn clCreateProgramWithSource{nullptr};
        clBuildProgram_fn clBuildProgram{nullptr};
        clGetProgramBuildInfo_fn clGetProgramBuildInfo{nullptr};
        clCreateKernel_fn clCreateKernel{nullptr};
        clCreateBuffer_fn clCreateBuffer{nullptr};
        clSetKernelArg_fn clSetKernelArg{nullptr};
        clEnqueueNDRangeKernel_fn clEnqueueNDRangeKernel{nullptr};
        clEnqueueReadBuffer_fn clEnqueueReadBuffer{nullptr};
        clFinish_fn clFinish{nullptr};
        clReleaseMemObject_fn clReleaseMemObject{nullptr};
        clReleaseKernel_fn clReleaseKernel{nullptr};
        clReleaseProgram_fn clReleaseProgram{nullptr};
        clReleaseCommandQueue_fn clReleaseCommandQueue{nullptr};
        clReleaseContext_fn clReleaseContext{nullptr};
    };

    class opencl_runtime_t {
    public:
        opencl_runtime_t();
        ~opencl_runtime_t();

        opencl_runtime_t(const opencl_runtime_t&) = delete;
        opencl_runtime_t& operator=(const opencl_runtime_t&) = delete;

        bool ready() const;
        const std::string& init_error() const;
        const std::string& last_error() const;
        std::string failure_reason() const;
        void clear_last_error();
        void set_last_error(std::string error);

        const opencl_api_t& api() const;
        cl_context context() const;
        cl_command_queue queue() const;
        std::mutex& mutex();
        cl_kernel kernel(std::string_view name);

    private:
        void init();
        std::string build_log() const;
        std::string load_kernel_source() const;

        opencl_api_t api_{};
        cl_device_id device_{nullptr};
        cl_context context_{nullptr};
        cl_command_queue queue_{nullptr};
        cl_program program_{nullptr};
        std::unordered_map<std::string, cl_kernel> kernels_{};
        bool ready_{false};
        std::string init_error_{"not initialized"};
        std::string last_error_{};
        std::mutex mutex_{};
    };

    class device_buffer_t {
    public:
        device_buffer_t();
        device_buffer_t(const opencl_api_t* api, cl_mem mem);
        device_buffer_t(device_buffer_t&& other) noexcept;
        device_buffer_t& operator=(device_buffer_t&& other) noexcept;
        ~device_buffer_t();

        device_buffer_t(const device_buffer_t&) = delete;
        device_buffer_t& operator=(const device_buffer_t&) = delete;

        cl_mem get() const;
        explicit operator bool() const;

    private:
        void reset();

        const opencl_api_t* api_{nullptr};
        cl_mem mem_{nullptr};
    };

    opencl_runtime_t& opencl_runtime();

    device_buffer_t create_device_buffer(opencl_runtime_t& runtime,
                                         cl_mem_flags flags,
                                         const void* host_ptr,
                                         size_t byte_size,
                                         std::string_view label);
    bool read_device_buffer(opencl_runtime_t& runtime,
                            const device_buffer_t& buffer,
                            void* target,
                            size_t byte_size,
                            std::string_view label);
    bool set_scalar_arg(opencl_runtime_t& runtime, cl_kernel kernel, cl_uint index, const void* value, size_t size);
    bool set_buffer_arg(opencl_runtime_t& runtime, cl_kernel kernel, cl_uint index, const device_buffer_t& buffer);
    bool run_1d_kernel(opencl_runtime_t& runtime, cl_kernel kernel, uint64_t row_count);

    bool try_run_count_star_gpu(const uint32_t* group_ids, uint64_t count, std::pmr::vector<raw_agg_state_t>& states);
    bool try_run_update_all_gpu(builtin_agg agg,
                                const vector::vector_t& vec,
                                const uint32_t* group_ids,
                                uint64_t count,
                                std::pmr::vector<raw_agg_state_t>& states);
#else
    bool try_run_count_star_gpu(const uint32_t*, uint64_t, std::pmr::vector<raw_agg_state_t>&);
    bool try_run_update_all_gpu(builtin_agg,
                                const vector::vector_t&,
                                const uint32_t*,
                                uint64_t,
                                std::pmr::vector<raw_agg_state_t>&);
#endif

} // namespace components::operators::aggregate::gpu
