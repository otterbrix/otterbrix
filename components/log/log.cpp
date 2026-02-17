#include "log.hpp"

#include <chrono>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <boost/process/v1/environment.hpp>
#include <filesystem>
#include <spdlog/async.h>
#include <utility>

constexpr static const char* __default__ = "__default__";

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

auto initialization_logger(std::string_view name, std::string prefix) -> log_t {
    std::filesystem::create_directories(prefix);
    if (prefix.back() != '/') {
        prefix += '/';
    }

    using namespace std::chrono;
    system_clock::time_point tp = system_clock::now();
    system_clock::duration dtn = tp.time_since_epoch();

    auto file_name = fmt::format("{}{}-{}.txt", prefix, name, duration_cast<seconds>(dtn).count());
    ///spdlog::init_thread_pool(8192, 1);
    auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(file_name, true);
    std::vector<spdlog::sink_ptr> sinks{stdout_sink, file_sink};
    auto logger = std::make_shared<spdlog::logger>( ///async_logger
        __default__,
        sinks.begin(),
        sinks.end()
        /*,spdlog::thread_pool(),*/
        /*spdlog::async_overflow_policy::block*/);

    spdlog::flush_every(std::chrono::seconds(1)); //todo: hack
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] [pid %P tid %t] %v");
    logger->flush_on(spdlog::level::debug);
    spdlog::set_default_logger(logger); ///spdlog::register_logger(logger);
    return logger;
}

auto get_logger(const std::string& name) -> log_t { return spdlog::get(name); }

auto get_logger() -> log_t { return spdlog::get(__default__); }

auto initialization_logger(std::shared_ptr<spdlog::logger> logger) -> void {
    spdlog::register_logger(std::move(logger));
}

void drop_logger(const std::string& name) { spdlog::drop(name); }

void drop_all_loggers() { spdlog::drop_all(); }
