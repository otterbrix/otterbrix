#pragma once

#include <components/log/log.hpp>
#include <cstdint>
#include <filesystem>

namespace configuration {

    inline constexpr uint16_t default_pax_rows_per_page = 256;

    enum class disk_layout_policy : uint8_t
    {
        auto_select = 0,
        columnar_only = 1,
        pax_only = 2
    };

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
        uint32_t page_size{4096};
        std::size_t max_segment_size{4 * 1024 * 1024}; // 4 MB per segment
        // WAL_AUTO_CHECKPOINT_THRESHOLD_BYTES: trigger checkpoint_all when cumulative WAL
        // bytes since the last checkpoint exceed this value. Default 16 MB (4 segments).
        std::uintmax_t auto_checkpoint_threshold_bytes{16 * 1024 * 1024};

        explicit config_wal(const std::filesystem::path& path = std::filesystem::current_path())
            : path(path / "wal") {}
    };

    struct config_disk final {
        std::filesystem::path path{std::filesystem::current_path() / "disk"};
        bool on{true};
        disk_layout_policy layout_policy{disk_layout_policy::auto_select};
        int agent = 2;
        uint64_t bitcask_flush_threshold{1000};
        uint64_t bitcask_segment_record_limit{100};
        uint64_t btree_flush_threshold{1000};
        uint16_t pax_rows_per_page{default_pax_rows_per_page};

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
