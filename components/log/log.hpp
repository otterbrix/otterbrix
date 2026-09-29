#pragma once

#include <core/result_wrapper.hpp>
#include <spdlog/async_logger.h>

#include <filesystem>
#include <string>
#include <string_view>

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
    auto clone() const noexcept -> log_t;
    auto set_level(level l) -> void;
    auto get_level() const -> log_t::level;
    auto context(std::shared_ptr<spdlog::async_logger> logger) noexcept -> void;
    auto is_valid() noexcept -> bool;

    inline spdlog::logger* operator->() const noexcept { return logger_.get(); }

    // True when a message at this level would actually be written. Call sites use it to skip
    // building an argument that exists only for the log line: arguments are evaluated BEFORE
    // the logging function is entered, so a guard inside those functions cannot save that work.
    inline bool should_log(level l) const noexcept {
        return logger_ != nullptr && logger_->should_log(static_cast<spdlog::level::level_enum>(l));
    }

private:
    std::shared_ptr<spdlog::logger> logger_;
};

// The guards below skip formatting only, so they change no observable behaviour; an expensive
// argument must still be guarded at the call site.
template<typename S, typename... Args>
auto info(log_t& log, const S& format_str, Args&&... args) -> void {
    if (!log.should_log(log_t::level::info)) {
        return;
    }
    log->info(fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
}

template<typename S, typename... Args>
auto debug(log_t& log, const S& format_str, Args&&... args) -> void {
    if (!log.should_log(log_t::level::debug)) {
        return;
    }
    log->debug(fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
}

template<typename S, typename... Args>
auto warn(log_t& log, const S& format_str, Args&&... args) -> void {
    if (!log.should_log(log_t::level::warn)) {
        return;
    }
    log->warn(fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
}

template<typename S, typename... Args>
auto error(log_t& log, const S& format_str, Args&&... args) -> void {
    if (!log.should_log(log_t::level::err)) {
        return;
    }
    log->error(fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
}

template<typename S, typename... Args>
auto critical(log_t& log, const S& format_str, Args&&... args) -> void {
    if (!log.should_log(log_t::level::critical)) {
        return;
    }
    log->critical(fmt::format(fmt::runtime(format_str), std::forward<Args>(args)...));
}

template<typename S, typename... Args>
auto trace(log_t& log, const S& format_str, Args&&... args) -> void {
    if (!log.should_log(log_t::level::trace)) {
        return;
    }
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

// A logger owned by its caller: nothing is registered with spdlog, so two engines in one process
// never share one. Writes to stdout and to <directory>/<name>-<seconds>.txt. A directory that cannot
// be created or a file that cannot be opened is answered as an error on `resource`, never thrown.
[[nodiscard]] auto make_log(std::string_view name,
                            const std::filesystem::path& directory,
                            std::pmr::memory_resource* resource) -> core::result_wrapper_t<log_t>;
