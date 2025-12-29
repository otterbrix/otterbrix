#pragma once

#include <spdlog/async_logger.h>
#include <string>

class log_t final {
public:
    enum class level : char
    {
        trace = spdlog::level::trace,
        debug = spdlog::level::debug,
        info = spdlog::level::info,
        warn = spdlog::level::warn,
        err = spdlog::level::err,
        critical = spdlog::level::critical,
        off = spdlog::level::off,
        level_nums
    };

    log_t() = default;
    log_t(std::shared_ptr<spdlog::async_logger>);
    log_t(std::shared_ptr<spdlog::logger>);
    ~log_t() = default;
    auto clone() noexcept -> log_t;
    auto set_level(level l) -> void;
    auto get_level() const -> log_t::level;
    auto context(std::shared_ptr<spdlog::async_logger> logger) noexcept -> void;
    auto is_valid() noexcept -> bool;

    inline spdlog::logger* operator->() const noexcept { return logger_.get(); }

private:
    std::shared_ptr<spdlog::logger> logger_;
};

template<typename S, typename... Args>
auto info(log_t& log, const S& format_str, Args&&... args) -> void {
    log->info(fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
}

template<typename S, typename... Args>
auto debug(log_t& log, const S& format_str, Args&&... args) -> void {
    log->debug(fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
}

template<typename S, typename... Args>
auto warn(log_t& log, const S& format_str, Args&&... args) -> void {
    log->warn(fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
}

template<typename S, typename... Args>
auto error(log_t& log, const S& format_str, Args&&... args) -> void {
    log->error(fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
}

template<typename S, typename... Args>
auto critical(log_t& log, const S& format_str, Args&&... args) -> void {
    log->critical(fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
}

template<typename S, typename... Args>
auto trace(log_t& log, const S& format_str, Args&&... args) -> void {
    log->trace(fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
}

template<typename S>
auto info(log_t& log, const S& format_str) -> void {
    log->info(format_str);
}

template<typename S>
auto debug(log_t& log, const S& format_str) -> void {
    log->debug(format_str);
}

template<typename S>
auto warn(log_t& log, const S& format_str) -> void {
    log->warn(format_str);
}

template<typename S>
auto error(log_t& log, const S& format_str) -> void {
    log->error(format_str);
}

template<typename S>
auto critical(log_t& log, const S& format_str) -> void {
    log->critical(format_str);
}

template<typename S>
auto trace(log_t& log, const S& format_str) -> void {
    log->trace(format_str);
}

auto get_logger(const std::string&) -> log_t;
auto get_logger() -> log_t;
auto initialization_logger(std::shared_ptr<spdlog::logger>) -> void;
auto initialization_logger(std::string_view name, std::string prefix) -> log_t;
auto drop_logger(const std::string&) -> void;
auto drop_all_loggers() -> void;
