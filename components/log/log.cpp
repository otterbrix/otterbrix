#include "log.hpp"

#include <chrono>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <filesystem>
#include <memory_resource>
#include <string>
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

// spdlog reports a file it cannot open by throwing spdlog_ex; this is the one place the throw is
// caught and turned into an error value.
auto make_log(std::string_view name, const std::filesystem::path& directory) -> core::result_wrapper_t<log_t> {
    using namespace std::chrono;
    const auto seconds_since_epoch = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    const auto file_name = directory / fmt::format("{}-{}.txt", name, seconds_since_epoch);
    try {
        // basic_file_sink_mt creates the parent directory itself.
        auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(file_name.string(), true);
        auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        std::vector<spdlog::sink_ptr> sinks{stdout_sink, file_sink};
        auto logger = std::make_shared<spdlog::logger>(std::string(name), sinks.begin(), sinks.end());
        logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] [pid %P tid %t] %v");
        logger->flush_on(spdlog::level::debug);
        return log_t{std::move(logger)};
    } catch (const spdlog::spdlog_ex& refusal) {
        const std::string what = "the log file " + file_name.string() + " could not be opened: " + refusal.what();
        return core::error_t(core::error_code_t::io_error,
                             std::pmr::string{what.data(), what.size(), std::pmr::new_delete_resource()});
    }
}
