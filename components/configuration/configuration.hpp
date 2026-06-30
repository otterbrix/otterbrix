#pragma once

#include <components/log/log.hpp>
#include <cstdint>
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
        int agent = 2;
        uint64_t bitcask_flush_threshold{1000};
        uint64_t bitcask_segment_record_limit{100};
        uint64_t btree_flush_threshold{1000};

        // Spill configuration for grace operators.
        // Default OFF: the optimizer's spill_strategy rule stamps every
        // sort/group/join node with exec_strategy=spill when this is true, which
        // lowers them to the grace/external operators unconditionally (no
        // threshold check, R3/R6). Those operators are validated on large inputs
        // (see test_spill_red "[spill]") but lose rows / abort on small or
        // mixed-type data, so spilling must be an opt-in plan choice, not the
        // default plan for every query. Tests that exercise the spill path set
        // this to true explicitly.
        bool spill_enabled{false};
        std::filesystem::path spill_path{std::filesystem::current_path() / "spill"}; // Temp file directory
        uint32_t partition_count{16};                       // Number of partitions for grace hash join/aggregate

        explicit config_disk(const std::filesystem::path& path = std::filesystem::current_path())
            : path(path / "wal")
            , spill_path(path / "spill") {}
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
