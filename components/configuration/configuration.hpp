#pragma once

#include <components/log/log.hpp>
#include <filesystem>

namespace configuration {

    struct config_log final {
        std::filesystem::path path{std::filesystem::current_path() / "log"};
        log_t::level level{log_t::level::trace};

        explicit config_log(const std::filesystem::path& path = std::filesystem::current_path())
            : path(path / "log") {}
    };

    struct config_wal final {
        std::filesystem::path path{std::filesystem::current_path() / "wal"};
        bool on{true};
        bool sync_to_disk{true};
        int agent = 1;

        explicit config_wal(const std::filesystem::path& path = std::filesystem::current_path())
            : path(path / "wal") {}
    };

    struct config_disk final {
        std::filesystem::path path{std::filesystem::current_path() / "disk"};
        bool on{true};
        int agent = 1;

        explicit config_disk(const std::filesystem::path& path = std::filesystem::current_path())
            : path(path / "wal") {}
    };

    struct config final {
        config_log log;
        config_wal wal;
        config_disk disk;
        std::filesystem::path main_path; // mainly used for checking, because log, wal and disk could be missing

        config(const std::filesystem::path& path = std::filesystem::current_path());

        static config default_config() { return config(); }
        static config create_config(const std::filesystem::path& path) { return config(path); }
    };

    inline config::config(const std::filesystem::path& path)
        : log(path)
        , wal(path)
        , disk(path)
        , main_path(path) {}
} // namespace configuration