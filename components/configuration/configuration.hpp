#pragma once

#include <components/log/log.hpp>
#include <cstdint>
#include <filesystem>

namespace configuration {

    struct config_log final {
        std::filesystem::path path;
        log_t::level level{log_t::level::trace};

        explicit config_log(const std::filesystem::path& path = std::filesystem::current_path())
            : path(path / "log") {}
    };

    struct config_wal final {
        std::filesystem::path path;
        // The WAL can genuinely be switched off — manager_wal_replicate_t reads this into
        // `enabled_` and base_spaces hands the dispatcher an empty address when it is false.
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
        // No default member initializer: the constructor is the only way to build this (not an
        // aggregate), so a second initializer here would just silently disagree with it.
        std::filesystem::path path;
        int agent = 2;
        uint64_t bitcask_flush_threshold{1000};
        uint64_t bitcask_segment_record_limit{100};
        uint64_t btree_flush_threshold{1000};

        // `<base>/wal`, not `<base>/disk`: the table tree shares the WAL directory on purpose --
        // the shipped layout since 96d5ffaa. Renaming would not move existing databases, only
        // strand them (a reopen finds an empty directory and bootstraps fresh). Pinned by
        // config_disk_path_layout in services/disk/tests/test_config_layout.cpp.
        explicit config_disk(const std::filesystem::path& path = std::filesystem::current_path())
            : path(path / "wal") {}
    };

    struct config_pandas final {
        uint64_t analyze_sample_size{1000};
    };

    struct config_execution final {
        // Mid-pump flush threshold (rows) for streaming DML sinks. 0 = DISABLED:
        // single post-pump flush, unbounded accumulator.
        // A rollout gate — the executor guards `threshold != 0`.
        uint64_t dml_flush_row_threshold{0};
    };

    struct config final {
        config_log log;
        config_wal wal;
        config_disk disk;
        config_pandas pandas;
        config_execution execution;
        std::filesystem::path main_path; // mainly used for checking, because log, wal and disk could be missing

        config(const std::filesystem::path& path = std::filesystem::current_path());

        static config default_config() { return config(); }
        static config create_config(const std::filesystem::path& path) { return config(path); }
    };

    inline config::config(const std::filesystem::path& path)
        : log(path)
        , wal(path)
        , disk(path)
        , pandas()
        , execution()
        , main_path(path) {}
} // namespace configuration
