#pragma once

#include "disk_hash_table.hpp"

#include <components/types/logical_value.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <core/result_wrapper.hpp>

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <memory_resource>
#include <set>
#include <string_view>
#include <vector>

namespace services::index {

#ifdef DEV_MODE
    [[nodiscard]] uint64_t bitcask_rotated_segment_opens() noexcept;
    void reset_bitcask_rotated_segment_opens() noexcept;

    struct bitcask_file_interposer_t {
        virtual ~bitcask_file_interposer_t() = default;
        virtual std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) = 0;
    };

    void dev_set_bitcask_file_interposer(bitcask_file_interposer_t* interposer); // nullptr = off
    bitcask_file_interposer_t* dev_bitcask_file_interposer();
#endif

    // No base class: backend questions resolve by concrete type, not virtual dispatch.
    class bitcask_index_disk_t final {
    public:
        using value_t = components::types::logical_value_t;
        using path_t = std::filesystem::path;
        using result = std::pmr::vector<size_t>;

        static constexpr uint64_t default_flush_threshold_{1000};
        static constexpr uint64_t default_segment_record_limit_{10000};
        // Regular segments start at 2; ids 0–1 are reserved for merged output, replayed first.
        static constexpr uint64_t regular_segment_id_start_{2};

        // Store is a byvalue member with a deleted copy ctor, so construction and open() are
        // separate steps.
        struct deferred_open_t {};

        bitcask_index_disk_t(const path_t& path,
                             std::pmr::memory_resource* resource,
                             uint64_t flush_threshold,
                             uint64_t segment_record_limit,
                             std::pmr::set<std::uint64_t> committed_commit_ids,
                             deferred_open_t);

        bitcask_index_disk_t(const path_t& path,
                             std::pmr::memory_resource* resource,
                             uint64_t flush_threshold,
                             uint64_t segment_record_limit,
                             std::pmr::set<std::uint64_t> committed_commit_ids);
        ~bitcask_index_disk_t();

        bitcask_index_disk_t(const bitcask_index_disk_t&) = delete;
        bitcask_index_disk_t& operator=(const bitcask_index_disk_t&) = delete;

        [[nodiscard]] std::pmr::memory_resource* resource() const noexcept { return resource_; }

        [[nodiscard]] core::error_t open();

        using entry_t = std::pair<value_t, size_t>;
        using entries_t = std::pmr::vector<entry_t>;

        void insert(const value_t& key, size_t value);
        void remove(value_t key);
        void remove(const value_t& key, size_t row_id);

        // Equality only — no scan_range; a range predicate is refused one level up in
        // bitcask_index_agent_t::read_rows. Refuses outright on a partial keydir walk, never a truncated set.
        [[nodiscard]] core::error_t find(const value_t& value, result& res) const;

        [[nodiscard]] core::result_wrapper_t<result> find(const value_t& value) const {
            result res(resource_);
            if (auto read_error = find(value, res); read_error.contains_error()) {
                return read_error;
            }
            return res;
        }

        void drop();
        [[nodiscard]] core::error_t clear();
        // io_error on refusal must fail the statement, or the table and its index silently disagree.
        [[nodiscard]] core::error_t force_flush();
        [[nodiscard]] core::error_t load_entries(entries_t& entries) const;

        // Runs on the caller's thread, once, at the end of the write handler that incurred the debt.
        [[nodiscard]] core::error_t merge_pending_segments();
        void set_bulk_mode(bool enabled);
        [[nodiscard]] core::error_t
        apply_txn_inserts(uint64_t txn_id, uint64_t commit_id, const std::vector<std::pair<value_t, size_t>>& values);
        [[nodiscard]] core::error_t
        apply_txn_deletes(uint64_t txn_id, uint64_t commit_id, const std::vector<std::pair<value_t, size_t>>& values);
        void insert_bulk_unchecked(const value_t& key, size_t value);
        void remove_bulk_unchecked(const value_t& key, size_t row_id);

        [[nodiscard]] const disk_hash_table_t& hash_storage() const noexcept { return *hash_index_; }
        [[nodiscard]] disk_hash_table_t& hash_storage() noexcept { return *hash_index_; }

    private:
        enum class record_kind_t : uint8_t
        {
            value = 1,
            tombstone = 2
        };

        [[nodiscard]] core::result_wrapper_t<std::pmr::string> load_hash_key_at(uint32_t segment_id,
                                                                                uint64_t value_offset) const;

        [[nodiscard]] auto key_loader() const noexcept {
            return [this](uint32_t log_file_id, uint64_t log_offset) -> core::result_wrapper_t<std::pmr::string> {
                return load_hash_key_at(log_file_id, log_offset);
            };
        }

        struct segment_info_t {
            uint64_t id{0};
            std::filesystem::path path;
            uint64_t record_count{0};
            uint64_t scan_end{0};
        };

        using row_ids_t = std::pmr::vector<size_t>;

        [[nodiscard]] core::error_t initialize_storage();
        [[nodiscard]] core::error_t load_from_disk();
        // An unfinished merge left unrepaired at open replays a source it already rewrote,
        // resurrecting keys the merge dropped.
        [[nodiscard]] core::error_t apply_merge_recovery_cleanup();
        // A refused directory listing must not collapse to empty, or load_from_disk rebuilds the
        // keydir as if the (missing) directory were correctly empty.
        [[nodiscard]] core::result_wrapper_t<std::pmr::vector<segment_info_t>> collect_segments() const;
        [[nodiscard]] core::error_t open_active_segment();
        [[nodiscard]] core::error_t rotate_active_segment();
        [[nodiscard]] core::error_t rotate_active_segment_if_needed();
        uint64_t allocate_next_segment_id();
        [[nodiscard]] core::error_t merge_immutable_segments();
        [[nodiscard]] core::result_wrapper_t<row_ids_t> current_rows(const value_t& key) const;
        // true=value (rows filled), false=tombstone (legitimately no rows), error=unreadable — a
        // bare bool would misread a corrupt record as "no rows" and let append_snapshot erase the row list.
        [[nodiscard]] core::result_wrapper_t<bool>
        read_rows_at(uint32_t segment_id, uint64_t value_offset, row_ids_t& rows, value_t* out_key = nullptr) const;
        std::string key_bytes_for_hash(const value_t& key, bool* ok = nullptr) const;
        [[nodiscard]] core::error_t erase_all_refs_for_key(std::string_view key_bytes);
        // Reports a hash-index write failure rather than dropping it — a lost entry goes unfindable silently.
        [[nodiscard]] core::error_t append_snapshot(const value_t& key, const row_ids_t& rows);
        [[nodiscard]] core::error_t append_tombstone(const value_t& key);
        [[nodiscard]] core::error_t append_txn_record(uint64_t txn_id,
                                                      uint64_t commit_id,
                                                      uint8_t op_kind,
                                                      const std::vector<std::pair<value_t, size_t>>& values);
        [[nodiscard]] core::error_t recover_txn_log();
        std::filesystem::path txn_log_file_path() const;
        std::filesystem::path txn_applied_file_path() const;
        [[nodiscard]] core::result_wrapper_t<uint64_t> read_applied_log_offset() const;
        [[nodiscard]] core::error_t write_applied_log_offset(uint64_t offset) const;
        void flush_if_needed();
        // On refusal the store stays dirty, so force_flush() can stop a checkpoint trimming the WAL early.
        [[nodiscard]] core::error_t sync_if_dirty();
        void note_write_error(core::error_t err);
        // Seals writes after an unrepairable stump, so a later append can't land behind it and
        // pass as an interior frame; clear() is the only door back open.
        [[nodiscard]] core::error_t seal_writes(std::string_view reason);
        [[nodiscard]] core::error_t refuse_if_sealed() const;
        [[nodiscard]] core::error_t io_failure(std::string_view message) const;
        [[nodiscard]] core::error_t open_hash_index();

        [[nodiscard]] bool should_flush() const noexcept { return ops_since_flush_ >= flush_threshold_; }
        void mark_operation_dirty() noexcept {
            dirty_ = true;
            ++ops_since_flush_;
        }
        [[nodiscard]] bool is_dirty() const noexcept { return dirty_; }
        void reset_flush_state() noexcept {
            dirty_ = false;
            ops_since_flush_ = 0;
        }

        std::pmr::memory_resource* resource_;
        uint64_t flush_threshold_;
        bool dirty_{false};
        uint64_t ops_since_flush_{0};
        std::filesystem::path path_;
        std::filesystem::path hash_index_file_path_;
        std::filesystem::path active_data_file_path_;
        mutable core::filesystem::local_file_system_t fs_;
        std::unique_ptr<core::filesystem::file_handle_t> file_;
        std::unique_ptr<core::filesystem::file_handle_t> txn_log_file_;
        std::unique_ptr<disk_hash_table_t> hash_index_;
        core::error_t pending_write_error_{core::error_t::no_error()};
        // LRU (capacity 8) of held descriptors for ROTATED-segment reads; stale only when the file is replaced.
        struct rotated_segment_lease_t {
            uint64_t segment_id{0};
            std::unique_ptr<core::filesystem::file_handle_t> handle;
            uint64_t last_used{0};
        };
        static constexpr size_t rotated_read_cache_capacity_ = 8;
        mutable std::vector<rotated_segment_lease_t> rotated_read_cache_;
        mutable uint64_t rotated_read_tick_{0};
        void invalidate_rotated_read_cache_() const noexcept { rotated_read_cache_.clear(); }
        void drop_cached_rotated_segment_(uint64_t segment_id) const noexcept;
        bool writes_sealed_{false};
        uint64_t next_timestamp_{0};
        uint64_t next_segment_id_{regular_segment_id_start_};
        uint64_t active_segment_id_{0};
        uint64_t active_segment_records_{0};
        // Where replay's walk of the active segment stopped; open_active_segment trims to here so
        // a post-crash insert can't land behind the stump and read back as garbage.
        static constexpr uint64_t no_tail_to_trim{UINT64_MAX};
        uint64_t active_segment_clean_end_{no_tail_to_trim};
        // Same mechanism, for the txn log.
        uint64_t txn_log_clean_end_{no_tail_to_trim};
        uint64_t segment_record_limit_{default_segment_record_limit_};
        bool bulk_mode_{false};
        bool bulk_rehash_guard_active_{false};
        bool bulk_prev_rehash_suppressed_{false};
        bool merge_pending_{false};
        // WAL-replay committed COMMIT ids, not txn ids: txn ids restart at TRANSACTION_ID_START
        // each process, so an older incarnation's marker could vouch for a newer, uncommitted frame.
        std::pmr::set<std::uint64_t> committed_commit_ids_;
        // Set only for a ROTATED segment's CRC failure (damage, since rotated segments never
        // change) — never the ACTIVE segment's, which is a torn write repaired in place.
        bool crc_failure_{false};
    };

} // namespace services::index
