#include "log.hpp"

#include <spdlog/sinks/base_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <ctime>
#include <fcntl.h>
#include <filesystem>
#include <memory_resource>
#include <mutex>
#include <string>
#include <system_error>
#include <unistd.h>
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

    // spdlog's basic_file_sink_mt is not used: its file_helper throws spdlog_ex when it cannot open or write.
    // A failed write is reported the way spdlog's logger reports a sink that threw: one stderr line at most
    // once a second, the line itself lost to the file.
    class file_sink_t final : public spdlog::sinks::base_sink<std::mutex> {
    public:
        file_sink_t(std::string logger_name, std::string path, std::FILE* file) noexcept
            : logger_name_(std::move(logger_name))
            , path_(std::move(path))
            , file_(file) {}

        ~file_sink_t() override { std::fclose(file_); }

    private:
        void sink_it_(const spdlog::details::log_msg& msg) override {
            spdlog::memory_buf_t formatted;
            formatter_->format(msg, formatted);
            if (std::fwrite(formatted.data(), 1, formatted.size(), file_) != formatted.size()) {
                report("Failed writing to file", errno);
            }
        }

        void flush_() override {
            if (std::fflush(file_) != 0) {
                report("Failed flush to file", errno);
            }
        }

        void report(const char* failure, int error_number) {
            ++failures_;
            const auto now = std::chrono::steady_clock::now();
            if (reported_ && now - last_report_ < std::chrono::seconds(1)) {
                return;
            }
            reported_ = true;
            last_report_ = now;
            const std::time_t seconds = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
            std::tm local{};
            localtime_r(&seconds, &local);
            char date[64];
            std::strftime(date, sizeof(date), "%Y-%m-%d %H:%M:%S", &local);
            const auto reason = std::generic_category().message(error_number);
            std::fprintf(stderr,
                         "[*** LOG ERROR #%04zu ***] [%s] [%s] %s %s: %s\n",
                         failures_,
                         date,
                         logger_name_.c_str(),
                         failure,
                         path_.c_str(),
                         reason.c_str());
        }

        std::string logger_name_;
        std::string path_;
        std::FILE* file_;
        std::size_t failures_{0};
        bool reported_{false};
        std::chrono::steady_clock::time_point last_report_{};
    };

    core::error_t refusal(core::error_code_t code, const std::string& what) {
        return core::error_t(code, std::pmr::string{what.data(), what.size(), std::pmr::new_delete_resource()});
    }

} // namespace

auto make_log(std::string_view name, const std::filesystem::path& directory) -> core::result_wrapper_t<log_t> {
    if (directory.empty()) {
        return refusal(core::error_code_t::invalid_parameter,
                       "the log directory is empty: there is no directory to write the log file into");
    }
    using namespace std::chrono;
    const auto seconds_since_epoch = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    const auto file_name = directory / fmt::format("{}-{}.txt", name, seconds_since_epoch);
    std::error_code ec;
    std::filesystem::create_directories(directory, ec);
    if (ec) {
        return refusal(core::error_code_t::io_error,
                       "the log directory " + directory.string() + " could not be created: " + ec.message());
    }
    const int descriptor = ::open(file_name.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_APPEND, 0666);
    if (descriptor < 0) {
        const int error_number = errno;
        return refusal(core::error_code_t::io_error,
                       "the log file " + file_name.string() +
                           " could not be opened: " + std::generic_category().message(error_number));
    }
    std::FILE* file = ::fdopen(descriptor, "a");
    if (file == nullptr) {
        const int error_number = errno;
        ::close(descriptor);
        return refusal(core::error_code_t::io_error,
                       "the log file " + file_name.string() +
                           " could not be opened: " + std::generic_category().message(error_number));
    }
    auto file_sink = std::make_shared<file_sink_t>(std::string(name), file_name.string(), file);
    auto stdout_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    std::vector<spdlog::sink_ptr> sinks{stdout_sink, file_sink};
    auto logger = std::make_shared<spdlog::logger>(std::string(name), sinks.begin(), sinks.end());
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%l] [pid %P tid %t] %v");
    logger->flush_on(spdlog::level::debug);
    return log_t{std::move(logger)};
}
