#include "log.hpp"

#include <chrono>
#include <spdlog/sinks/base_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <mutex>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

log_t::log_t(std::shared_ptr<spdlog::async_logger> logger)
    : logger_(std::move(logger)) {}

log_t::log_t(std::shared_ptr<spdlog::logger> logger)
    : logger_(std::move(logger)) {}

auto log_t::clone() const noexcept -> log_t { return logger_; }

auto log_t::set_level(level l) -> void { logger_->set_level(static_cast<spdlog::level::level_enum>(l)); }
auto log_t::get_level() const -> log_t::level {
    auto lvl = logger_->level();
    return static_cast<log_t::level>(lvl);
}

auto log_t::context(std::shared_ptr<spdlog::async_logger> logger) noexcept -> void { logger_ = std::move(logger); }

auto log_t::is_valid() noexcept -> bool { return logger_ != nullptr; }

namespace {

    // A file sink that opens nothing itself: make_log hands it a FILE* it opened with errno-based calls,
    // so a refusal is an error value, never an exception out of a sink constructor.
    class file_sink_t final : public spdlog::sinks::base_sink<std::mutex> {
    public:
        explicit file_sink_t(std::FILE* file)
            : file_(file) {}
        file_sink_t(const file_sink_t&) = delete;
        file_sink_t& operator=(const file_sink_t&) = delete;
        ~file_sink_t() override { std::fclose(file_); }

    private:
        void sink_it_(const spdlog::details::log_msg& msg) override {
            spdlog::memory_buf_t formatted;
            formatter_->format(msg, formatted);
            std::fwrite(formatted.data(), 1, formatted.size(), file_);
        }
        void flush_() override { std::fflush(file_); }

        std::FILE* file_;
    };

    core::error_t log_refused(std::pmr::memory_resource* resource, const std::string& what) {
        return core::error_t(core::error_code_t::io_error, std::pmr::string{what.data(), what.size(), resource});
    }

} // namespace

auto make_log(std::string_view name, const std::filesystem::path& directory, std::pmr::memory_resource* resource)
    -> core::result_wrapper_t<log_t> {
    std::error_code ec;
    std::filesystem::create_directories(directory, ec);
    if (ec) {
        return log_refused(resource,
                           "the log directory " + directory.string() + " could not be created: " + ec.message());
    }

    using namespace std::chrono;
    const auto seconds_since_epoch = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    const auto file_name = directory / fmt::format("{}-{}.txt", name, seconds_since_epoch);
    std::FILE* file = std::fopen(file_name.c_str(), "wb");
    if (file == nullptr) {
        return log_refused(resource,
                           "the log file " + file_name.string() + " could not be opened: " + std::strerror(errno));
    }

    auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto file_sink = std::make_shared<file_sink_t>(file);
    std::vector<spdlog::sink_ptr> sinks{stdout_sink, file_sink};
    auto logger = std::make_shared<spdlog::logger>(std::string(name), sinks.begin(), sinks.end());
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] [pid %P tid %t] %v");
    logger->flush_on(spdlog::level::debug);
    return log_t{std::move(logger)};
}
