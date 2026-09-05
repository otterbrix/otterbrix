#pragma once

#include <components/log/log.hpp>
#include <cstdint>
#include <filesystem>

namespace configuration {

    // The three path structs below each carry exactly ONE initializer for `path` — the
    // constructor. None of them is an aggregate and none has a second constructor, so a
    // default member initializer on `path` would be unreachable; see config_disk for what
    // happened the last time one sat there disagreeing with the constructor.

    struct config_log final {
        std::filesystem::path path;
        log_t::level level{log_t::level::trace};

        explicit config_log(const std::filesystem::path& path = std::filesystem::current_path())
            : path(path / "log") {}
    };

    struct config_wal final {
        std::filesystem::path path;
        // Stays. The WAL can genuinely be switched off — manager_wal_replicate_t reads this
        // into `enabled_` and base_spaces hands the dispatcher an empty address when it is
        // false. Unlike config_disk::on, which after B4 selected nothing.
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
        // No default member initializer: the constructor below is the only way to build a
        // config_disk (the struct is not an aggregate, and the constructor's default argument
        // covers default-construction), so a second initializer here could only ever disagree
        // with it. One did, from 96d5ffaa (2024-08-27) until it was removed: it said
        // `<cwd>/disk` while the constructor said `<base>/wal`, and being unreachable it
        // misled readers for two years — three call sites hand-assigned `<cwd>/disk` back.
        std::filesystem::path path;
        int agent = 2;
        uint64_t bitcask_flush_threshold{1000};
        uint64_t bitcask_segment_record_limit{100};
        uint64_t btree_flush_threshold{1000};

        // `<base>/wal`, not `<base>/disk` — the table tree shares the WAL's directory. It
        // reads like a copy-paste of config_wal above, and that is where it came from, but it
        // is the shipped layout now: every database written since 96d5ffaa is under it,
        // including every one the Python package made, since `Client(path)` goes straight to
        // `config::create_config`. Renaming the directory would not move those files, it would
        // strand them — a reopen would find an empty directory, bootstrap a fresh pg_catalog
        // and report success. The two trees interleave without colliding (WAL scans below
        // `<base>/wal/<db_oid>` take regular files only, disk scans take numeric directories
        // only), so there is nothing to gain by splitting them and a database to lose.
        // Pinned by config_disk_path_layout in services/disk/tests/test_config_layout.cpp.
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
