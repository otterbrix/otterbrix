#pragma once

#include "bitcask_task_executor.hpp"
#include "disk_hash_table.hpp"
#include "index_disk.hpp"

#include <components/types/logical_value.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <core/result_wrapper.hpp>

#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <memory_resource>
#include <set>
#include <shared_mutex>
#include <vector>

namespace services::index {

    class bitcask_index_disk_t final : public index_disk_t {
    public:
        static constexpr uint64_t default_flush_threshold_{1000};
        static constexpr uint64_t default_segment_record_limit_{10000};

        // committed_txn_ids: WAL-replay set of committed transaction ids. The
        // txn-log recover gate (M1.1) applies a frame only when its txn_id is in
        // this set; uncommitted-txn frames are skipped (their WAL commit marker
        // never landed). A fresh, runtime-created instance passes an EMPTY set —
        // a fresh dir has no txn-log to gate.
        bitcask_index_disk_t(const path_t& path,
                             std::pmr::memory_resource* resource,
                             uint64_t flush_threshold,
                             uint64_t segment_record_limit,
                             std::pmr::set<std::uint64_t> committed_txn_ids,
                             disk_hash_table_ptr shared_hash_index = nullptr);
        ~bitcask_index_disk_t() override;

        // Factory returning the instance, or a core::error_t when on-disk
        // recovery fails (e.g. segment CRC mismatch). Production code MUST use
        // this: the direct ctor below loads from disk and aborts on corruption.
        // committed_txn_ids carries the same recover-gate meaning as the ctor.
        [[nodiscard]] static core::result_wrapper_t<std::unique_ptr<bitcask_index_disk_t>>
        create(const path_t& path,
               std::pmr::memory_resource* resource,
               uint64_t flush_threshold,
               uint64_t segment_record_limit,
               std::pmr::set<std::uint64_t> committed_txn_ids);

        using entry_t = std::pair<value_t, size_t>;
        using entries_t = std::pmr::vector<entry_t>;

        void insert(const value_t& key, size_t value) override;
        void remove(value_t key) override;
        void remove(const value_t& key, size_t row_id) override;
        void find(const value_t& value, result& res) const override;
        result find(const value_t& value) const override;
        void lower_bound(const value_t& value, result& res) const override;
        result lower_bound(const value_t& value) const override;
        void upper_bound(const value_t& value, result& res) const override;
        result upper_bound(const value_t& value) const override;

        void drop() override;
        void clear() override;
        void force_flush() override;
        void load_entries(entries_t& entries) const;
        void enqueue_task(std::function<void()> task);
        void set_bulk_mode(bool enabled);
        // M3.5 error channel: the txn-log write path can fail on a file open /
        // write / sync, and surfaces a core::error_t so the manager's commit
        // handler can return an index-side abort instead of taking the whole
        // process down. A clean success returns core::error_t::no_error(). True
        // logic invariants (corrupt magic, bad op_kind) stay asserts in the
        // recovery path.
        [[nodiscard]] core::error_t apply_txn_inserts(uint64_t txn_id,
                                                      const std::vector<std::pair<value_t, size_t>>& values);
        [[nodiscard]] core::error_t apply_txn_deletes(uint64_t txn_id,
                                                      const std::vector<std::pair<value_t, size_t>>& values);
        void insert_bulk_unchecked(const value_t& key, size_t value);

        bool load_hash_key_at(uint32_t segment_id, uint64_t value_offset, std::string& out_key) const;
        bool load_hash_key_at_unlocked(uint32_t segment_id, uint64_t value_offset, std::string& out_key) const;

    private:
        enum class record_kind_t : uint8_t
        {
            value = 1,
            tombstone = 2
        };

        struct skip_load_tag {};

        // Skip-load ctor used by create() — performs no disk I/O so the
        // factory can stage load_from_disk() and check crc_failure_ before
        // running the rest of the recovery pipeline. committed_txn_ids is stored
        // here so the recover gate is armed when create() later runs recovery.
        bitcask_index_disk_t(const path_t& path,
                             std::pmr::memory_resource* resource,
                             uint64_t flush_threshold,
                             uint64_t segment_record_limit,
                             std::pmr::set<std::uint64_t> committed_txn_ids,
                             skip_load_tag);

        struct segment_info_t {
            uint64_t id{0};
            std::filesystem::path path;
            uint64_t record_count{0};
        };

        using row_ids_t = std::pmr::vector<size_t>;

        void initialize_storage();
        void load_from_disk();
        std::vector<segment_info_t> collect_segments() const;
        void open_active_segment();
        void rotate_active_segment();
        void rotate_active_segment_if_needed();
        uint64_t allocate_next_segment_id();
        void merge_immutable_segments();
        row_ids_t current_rows(const value_t& key) const;
        bool
        read_rows_at(uint32_t segment_id, uint64_t value_offset, row_ids_t& rows, value_t* out_key = nullptr) const;
        std::string key_bytes_for_hash(const value_t& key) const;
        void erase_all_refs_for_key(std::string_view key_bytes);
        void append_snapshot(const value_t& key, const row_ids_t& rows);
        void append_tombstone(const value_t& key);
        // M3.5: returns no_error() on a clean append, an index_create_fail
        // error if the txn-log file cannot be opened (the only recoverable IO
        // failure on this path; write/sync surface through the file handle).
        [[nodiscard]] core::error_t append_txn_record_unlocked(uint64_t txn_id,
                                                               uint8_t op_kind,
                                                               const std::vector<std::pair<value_t, size_t>>& values);
        void recover_txn_log_unlocked();
        std::filesystem::path txn_log_file_path() const;
        std::filesystem::path txn_applied_file_path() const;
        uint64_t read_applied_log_offset() const;
        // M3.5: returns no_error() once the applied-offset sidecar is durably
        // rewritten, an index_create_fail error if the temp file cannot be
        // opened or flushed. The ctor-time recovery path treats a failure here
        // as terminal (no error channel mid-construction); apply_txn_* surface
        // it as the index-side abort.
        [[nodiscard]] core::error_t write_applied_log_offset(uint64_t offset) const;
        void flush_if_needed();
        void force_flush_unlocked();
        void install_hash_key_loader();

        std::filesystem::path path_;
        std::filesystem::path hash_index_file_path_;
        std::filesystem::path active_data_file_path_;
        std::pmr::memory_resource* resource_;
        mutable core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::filesystem::file_handle_t> file_;
        std::unique_ptr<core::filesystem::file_handle_t> txn_log_file_;
        disk_hash_table_ptr hash_index_;
        uint64_t next_timestamp_{0};
        std::atomic<uint64_t> next_segment_id_{1};
        uint64_t active_segment_id_{0};
        uint64_t active_segment_records_{0};
        uint64_t segment_record_limit_{default_segment_record_limit_};
        bool bulk_mode_{false};
        bool bulk_rehash_guard_active_{false};
        bool bulk_prev_rehash_suppressed_{false};
        mutable std::shared_mutex mutex_;
        std::unique_ptr<bitcask_task_executor_t> task_executor_;
        // WAL-replay committed transaction ids — the recover gate (M1.1) applies
        // a txn-log frame only when committed_txn_ids_.count(header.txn_id) > 0.
        // Allocated on resource_ (the resource the class stores). Empty for a
        // fresh, runtime-created instance (no txn-log to gate).
        std::pmr::set<std::uint64_t> committed_txn_ids_;
        // Set by load_from_disk when a segment's CRC check fails. The
        // factory checks this flag to convert the failure into a
        // core::error_t; the direct ctor asserts.
        bool crc_failure_{false};
    };

} // namespace services::index
