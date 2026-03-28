#include "grouped_aggregate_gpu_internal.hpp"

#include <array>
#include <cstring>
#include <dlfcn.h>
#include <fstream>

#include <fmt/format.h>
#include <spdlog/spdlog.h>

namespace components::operators::aggregate::gpu {

    const char* agg_name(builtin_agg agg) {
        switch (agg) {
            case builtin_agg::SUM:
                return "sum";
            case builtin_agg::MIN:
                return "min";
            case builtin_agg::MAX:
                return "max";
            case builtin_agg::COUNT:
                return "count";
            case builtin_agg::AVG:
                return "avg";
            default:
                return "unknown";
        }
    }

    bool read_bool_env(std::string_view name) {
        const char* raw = std::getenv(name.data());
        if (!raw) {
            return false;
        }

        const std::string_view value(raw);
        return value == "1" || value == "true" || value == "TRUE" || value == "on" || value == "ON" ||
               value == "yes" || value == "YES";
    }

    bool gpu_group_aggregate_strict_enabled() { return read_bool_env("OTTERBRIX_GROUP_AGG_GPU_STRICT"); }

    void log_info_if_available(const std::string& message) {
        if (auto* logger = spdlog::default_logger_raw(); logger != nullptr) {
            logger->info(message);
        }
    }

    void log_warn_if_available(const std::string& message) {
        if (auto* logger = spdlog::default_logger_raw(); logger != nullptr) {
            logger->warn(message);
        }
    }

#if defined(__linux__)
    namespace {

        template<typename Fn>
        Fn load_opencl_symbol(void* lib, const char* name) {
            void* raw = dlsym(lib, name);
            Fn fn{};
            std::memcpy(&fn, &raw, std::min(sizeof(fn), sizeof(raw)));
            return fn;
        }

        bool load_opencl_api(opencl_api_t& api) {
            api.lib = dlopen("libOpenCL.so.1", RTLD_NOW | RTLD_LOCAL);
            if (!api.lib) {
                api.lib = dlopen("libOpenCL.so", RTLD_NOW | RTLD_LOCAL);
            }
            if (!api.lib) {
                return false;
            }

            api.clGetPlatformIDs = load_opencl_symbol<clGetPlatformIDs_fn>(api.lib, "clGetPlatformIDs");
            api.clGetDeviceIDs = load_opencl_symbol<clGetDeviceIDs_fn>(api.lib, "clGetDeviceIDs");
            api.clCreateContext = load_opencl_symbol<clCreateContext_fn>(api.lib, "clCreateContext");
            api.clCreateCommandQueue = load_opencl_symbol<clCreateCommandQueue_fn>(api.lib, "clCreateCommandQueue");
            api.clCreateProgramWithSource =
                load_opencl_symbol<clCreateProgramWithSource_fn>(api.lib, "clCreateProgramWithSource");
            api.clBuildProgram = load_opencl_symbol<clBuildProgram_fn>(api.lib, "clBuildProgram");
            api.clGetProgramBuildInfo = load_opencl_symbol<clGetProgramBuildInfo_fn>(api.lib, "clGetProgramBuildInfo");
            api.clCreateKernel = load_opencl_symbol<clCreateKernel_fn>(api.lib, "clCreateKernel");
            api.clCreateBuffer = load_opencl_symbol<clCreateBuffer_fn>(api.lib, "clCreateBuffer");
            api.clSetKernelArg = load_opencl_symbol<clSetKernelArg_fn>(api.lib, "clSetKernelArg");
            api.clEnqueueNDRangeKernel =
                load_opencl_symbol<clEnqueueNDRangeKernel_fn>(api.lib, "clEnqueueNDRangeKernel");
            api.clEnqueueReadBuffer = load_opencl_symbol<clEnqueueReadBuffer_fn>(api.lib, "clEnqueueReadBuffer");
            api.clFinish = load_opencl_symbol<clFinish_fn>(api.lib, "clFinish");
            api.clReleaseMemObject = load_opencl_symbol<clReleaseMemObject_fn>(api.lib, "clReleaseMemObject");
            api.clReleaseKernel = load_opencl_symbol<clReleaseKernel_fn>(api.lib, "clReleaseKernel");
            api.clReleaseProgram = load_opencl_symbol<clReleaseProgram_fn>(api.lib, "clReleaseProgram");
            api.clReleaseCommandQueue =
                load_opencl_symbol<clReleaseCommandQueue_fn>(api.lib, "clReleaseCommandQueue");
            api.clReleaseContext = load_opencl_symbol<clReleaseContext_fn>(api.lib, "clReleaseContext");

            const bool ok = api.clGetPlatformIDs && api.clGetDeviceIDs && api.clCreateContext &&
                            api.clCreateCommandQueue && api.clCreateProgramWithSource && api.clBuildProgram &&
                            api.clGetProgramBuildInfo && api.clCreateKernel && api.clCreateBuffer &&
                            api.clSetKernelArg && api.clEnqueueNDRangeKernel && api.clEnqueueReadBuffer &&
                            api.clFinish && api.clReleaseMemObject && api.clReleaseKernel && api.clReleaseProgram &&
                            api.clReleaseCommandQueue && api.clReleaseContext;

            if (!ok) {
                dlclose(api.lib);
                api = {};
            }
            return ok;
        }

    } // namespace

    opencl_runtime_t::opencl_runtime_t() { init(); }

    opencl_runtime_t::~opencl_runtime_t() {
        for (auto& [_, kernel] : kernels_) {
            if (kernel && api_.clReleaseKernel) {
                api_.clReleaseKernel(kernel);
            }
        }
        if (program_ && api_.clReleaseProgram) {
            api_.clReleaseProgram(program_);
        }
        if (queue_ && api_.clReleaseCommandQueue) {
            api_.clReleaseCommandQueue(queue_);
        }
        if (context_ && api_.clReleaseContext) {
            api_.clReleaseContext(context_);
        }
        if (api_.lib) {
            dlclose(api_.lib);
        }
    }

    bool opencl_runtime_t::ready() const { return ready_; }

    const std::string& opencl_runtime_t::init_error() const { return init_error_; }

    const std::string& opencl_runtime_t::last_error() const { return last_error_; }

    std::string opencl_runtime_t::failure_reason() const { return last_error_.empty() ? init_error_ : last_error_; }

    void opencl_runtime_t::clear_last_error() { last_error_.clear(); }

    void opencl_runtime_t::set_last_error(std::string error) { last_error_ = std::move(error); }

    const opencl_api_t& opencl_runtime_t::api() const { return api_; }

    cl_context opencl_runtime_t::context() const { return context_; }

    cl_command_queue opencl_runtime_t::queue() const { return queue_; }

    std::mutex& opencl_runtime_t::mutex() { return mutex_; }

    cl_kernel opencl_runtime_t::kernel(std::string_view name) {
        if (const auto it = kernels_.find(std::string(name)); it != kernels_.end()) {
            return it->second;
        }

        cl_int status = cl_success;
        cl_kernel result = api_.clCreateKernel(program_, std::string(name).c_str(), &status);
        if (status != cl_success || !result) {
            set_last_error(fmt::format("failed to create OpenCL kernel '{}'", name));
            return nullptr;
        }

        kernels_.emplace(std::string(name), result);
        return result;
    }

    void opencl_runtime_t::init() {
        if (!load_opencl_api(api_)) {
            init_error_ = "libOpenCL is not available";
            return;
        }

        cl_uint platform_count = 0;
        if (api_.clGetPlatformIDs(0, nullptr, &platform_count) != cl_success || platform_count == 0) {
            init_error_ = "no OpenCL platforms found";
            return;
        }

        std::vector<cl_platform_id> platforms(platform_count);
        if (api_.clGetPlatformIDs(platform_count, platforms.data(), nullptr) != cl_success) {
            init_error_ = "failed to enumerate OpenCL platforms";
            return;
        }

        cl_platform_id selected_platform = nullptr;
        for (auto platform : platforms) {
            cl_uint device_count = 0;
            const auto status = api_.clGetDeviceIDs(platform, cl_device_type_gpu, 0, nullptr, &device_count);
            if ((status == cl_success || status == cl_device_not_found) && device_count > 0) {
                std::vector<cl_device_id> devices(device_count);
                if (api_.clGetDeviceIDs(platform, cl_device_type_gpu, device_count, devices.data(), nullptr) ==
                    cl_success) {
                    selected_platform = platform;
                    device_ = devices.front();
                    break;
                }
            }
        }

        if (!selected_platform || !device_) {
            init_error_ = "no OpenCL GPU device found";
            return;
        }

        const std::array<cl_context_properties, 3> context_properties{
            cl_context_platform,
            static_cast<cl_context_properties>(reinterpret_cast<intptr_t>(selected_platform)),
            0};

        cl_int status = cl_success;
        context_ = api_.clCreateContext(context_properties.data(), 1, &device_, nullptr, nullptr, &status);
        if (status != cl_success || !context_) {
            init_error_ = "failed to create OpenCL context";
            return;
        }

        queue_ = api_.clCreateCommandQueue(context_, device_, 0, &status);
        if (status != cl_success || !queue_) {
            init_error_ = "failed to create OpenCL command queue";
            return;
        }

        const auto source = load_kernel_source();
        const char* raw_source = source.c_str();
        const size_t source_size = source.size();

        program_ = api_.clCreateProgramWithSource(context_, 1, &raw_source, &source_size, &status);
        if (status != cl_success || !program_) {
            init_error_ = "failed to create OpenCL program";
            return;
        }

        status = api_.clBuildProgram(program_, 1, &device_, "-cl-std=CL1.2", nullptr, nullptr);
        if (status != cl_success) {
            init_error_ = build_log();
            return;
        }

        ready_ = true;
        init_error_.clear();
        log_info_if_available("grouped_aggregate GPU runtime initialized via OpenCL");
    }

    std::string opencl_runtime_t::build_log() const {
        size_t log_size = 0;
        if (!api_.clGetProgramBuildInfo ||
            api_.clGetProgramBuildInfo(program_, device_, cl_program_build_log, 0, nullptr, &log_size) !=
                cl_success ||
            log_size == 0) {
            return "failed to build OpenCL grouped aggregate kernels";
        }

        std::string log(log_size, '\0');
        if (api_.clGetProgramBuildInfo(program_, device_, cl_program_build_log, log.size(), log.data(), nullptr) !=
            cl_success) {
            return "failed to build OpenCL grouped aggregate kernels";
        }

        while (!log.empty() && (log.back() == '\0' || log.back() == '\n' || log.back() == '\r')) {
            log.pop_back();
        }

        if (log.empty()) {
            return "failed to build OpenCL grouped aggregate kernels";
        }
        return fmt::format("failed to build OpenCL grouped aggregate kernels: {}", log);
    }

    std::string opencl_runtime_t::load_kernel_source() const {
        std::ifstream file(OTTERBRIX_GROUPED_AGGREGATE_GPU_KERNEL_FILE);
        if (!file.is_open()) {
            return {};
        }
        return std::string((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    }

    device_buffer_t::device_buffer_t() = default;

    device_buffer_t::device_buffer_t(const opencl_api_t* api, cl_mem mem)
        : api_(api)
        , mem_(mem) {}

    device_buffer_t::device_buffer_t(device_buffer_t&& other) noexcept
        : api_(other.api_)
        , mem_(other.mem_) {
        other.api_ = nullptr;
        other.mem_ = nullptr;
    }

    device_buffer_t& device_buffer_t::operator=(device_buffer_t&& other) noexcept {
        if (this != &other) {
            reset();
            api_ = other.api_;
            mem_ = other.mem_;
            other.api_ = nullptr;
            other.mem_ = nullptr;
        }
        return *this;
    }

    device_buffer_t::~device_buffer_t() { reset(); }

    cl_mem device_buffer_t::get() const { return mem_; }

    device_buffer_t::operator bool() const { return mem_ != nullptr; }

    void device_buffer_t::reset() {
        if (mem_ && api_ && api_->clReleaseMemObject) {
            api_->clReleaseMemObject(mem_);
        }
        mem_ = nullptr;
        api_ = nullptr;
    }

    opencl_runtime_t& opencl_runtime() {
        static opencl_runtime_t runtime;
        return runtime;
    }

    device_buffer_t create_device_buffer(opencl_runtime_t& runtime,
                                         cl_mem_flags flags,
                                         void* host_ptr,
                                         size_t byte_size,
                                         std::string_view label) {
        cl_int status = cl_success;
        cl_mem mem = runtime.api().clCreateBuffer(runtime.context(), flags, byte_size, host_ptr, &status);
        if (status != cl_success || !mem) {
            runtime.set_last_error(fmt::format("failed to allocate OpenCL buffer '{}'", label));
            return {};
        }
        return device_buffer_t(&runtime.api(), mem);
    }

    bool read_device_buffer(opencl_runtime_t& runtime,
                            const device_buffer_t& buffer,
                            void* target,
                            size_t byte_size,
                            std::string_view label) {
        if (runtime.api().clEnqueueReadBuffer(runtime.queue(),
                                              buffer.get(),
                                              cl_true,
                                              0,
                                              byte_size,
                                              target,
                                              0,
                                              nullptr,
                                              nullptr) != cl_success) {
            runtime.set_last_error(fmt::format("failed to read OpenCL buffer '{}'", label));
            return false;
        }
        return true;
    }

    bool set_scalar_arg(opencl_runtime_t& runtime, cl_kernel kernel, cl_uint index, const void* value, size_t size) {
        if (runtime.api().clSetKernelArg(kernel, index, size, value) != cl_success) {
            runtime.set_last_error(fmt::format("failed to set OpenCL scalar argument {}", index));
            return false;
        }
        return true;
    }

    bool set_buffer_arg(opencl_runtime_t& runtime,
                        cl_kernel kernel,
                        cl_uint index,
                        const device_buffer_t& buffer) {
        const cl_mem mem = buffer.get();
        if (runtime.api().clSetKernelArg(kernel, index, sizeof(cl_mem), &mem) != cl_success) {
            runtime.set_last_error(fmt::format("failed to set OpenCL buffer argument {}", index));
            return false;
        }
        return true;
    }

    bool run_1d_kernel(opencl_runtime_t& runtime, cl_kernel kernel, uint64_t row_count) {
        if (row_count == 0) {
            return true;
        }

        const size_t local_size = default_local_size;
        const size_t global_size = ((static_cast<size_t>(row_count) + local_size - 1) / local_size) * local_size;

        if (runtime.api().clEnqueueNDRangeKernel(runtime.queue(),
                                                 kernel,
                                                 1,
                                                 nullptr,
                                                 &global_size,
                                                 &local_size,
                                                 0,
                                                 nullptr,
                                                 nullptr) != cl_success) {
            runtime.set_last_error("failed to launch OpenCL grouped aggregate kernel");
            return false;
        }

        if (runtime.api().clFinish(runtime.queue()) != cl_success) {
            runtime.set_last_error("failed to synchronize OpenCL grouped aggregate kernel");
            return false;
        }

        return true;
    }
#else
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
