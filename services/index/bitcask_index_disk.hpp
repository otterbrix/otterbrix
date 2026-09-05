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
    // Fault-injection seam for this store's own files: a refused write/fsync on an fd it opened
    // itself can't be staged via chmod (unlike single_file_block_manager_t's interposer). Shaped
    // like services::wal's dev_set_wal_file_interposer; wrap() returning nullptr = file won't open.

    // Test-observable count of descriptor opens for reading a ROTATED segment (the active
    // segment uses the store's own held descriptor); lets a test pin the budget instead of
    // timing syscalls.
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

    // No base class: one agent per family holds its store by value and concrete type instead, so
    // backend questions (txn log? bulk window? ordered probe?) resolve by type, not virtual
    // dispatch. The shared bits (resource, flush accounting) are duplicated in btree_index_disk_t.
    class bitcask_index_disk_t final {
    public:
        using value_t = components::types::logical_value_t;
        using path_t = std::filesystem::path;
        using result = std::pmr::vector<size_t>;

        static constexpr uint64_t default_flush_threshold_{1000};
        static constexpr uint64_t default_segment_record_limit_{10000};
        // Regular (non-merged) segments start at 2; id 0–1 are reserved for merged
        // output so merged data is always replayed before rotated segments.
        static constexpr uint64_t regular_segment_id_start_{2};

        // Store is a byvalue member of bitcask_index_agent_t (deleted copy ctor blocks the
        // implicit move), so the agent constructs it here and opens it as a separate step;
        // this ctor does no I/O, so open()'s failures are values, not aborts.
        //
        // committed_commit_ids: WAL-replay COMMIT ids, not txn ids -- txn ids restart at
        // TRANSACTION_ID_START every process (an earlier incarnation's marker could vouch for a
        // later one's uncommitted frame), while a commit id is issued once ever
        // (restore_commit_clock advances the clock past the durable frontier on reopen).
        struct deferred_open_t {};

        bitcask_index_disk_t(const path_t& path,
                             std::pmr::memory_resource* resource,
                             uint64_t flush_threshold,
                             uint64_t segment_record_limit,
                             std::pmr::set<std::uint64_t> committed_commit_ids,
                             deferred_open_t);

        // Construct-and-open in one step, aborting on failure. Test-only: production
        // uses the deferred ctor + open() so an environmental failure costs the index its
        // registration, not the engine its start (see test_index_bootstrap_failure).
        bitcask_index_disk_t(const path_t& path,
                             std::pmr::memory_resource* resource,
                             uint64_t flush_threshold,
                             uint64_t segment_record_limit,
                             std::pmr::set<std::uint64_t> committed_commit_ids);
        ~bitcask_index_disk_t();

        bitcask_index_disk_t(const bitcask_index_disk_t&) = delete;
        bitcask_index_disk_t& operator=(const bitcask_index_disk_t&) = delete;

        // The resource every answer this store produces is built on (not the process default).
        [[nodiscard]] std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // Opens the keydir (hash_index.bin) and reports why it could not, as a value; called
        // exactly once, right after the deferred ctor, by bitcask_index_agent_t::create(), which
        // destroys the half-built agent on failure instead of publishing one with a closed store.
        [[nodiscard]] core::error_t open();

        using entry_t = std::pair<value_t, size_t>;
        using entries_t = std::pmr::vector<entry_t>;

        void insert(const value_t& key, size_t value);
        void remove(value_t key);
        void remove(const value_t& key, size_t row_id);

        // The only read this family has: equality (no ordering, so no scan_range -- a range
        // predicate is refused one level up, in bitcask_index_agent_t::read_rows). Reads the
        // snapshot record and unrolls the whole row list rather than just its `rows.back()`
        // payload pointer. Refuses outright on a partial keydir walk rather than returning a
        // silently truncated row set.
        [[nodiscard]] core::error_t find(const value_t& value, result& res) const;

        // Built on resource_, never a default-constructed std::pmr::vector -- that would put
        // the process default resource on the write path via insert()/remove() reaching it.
        [[nodiscard]] core::result_wrapper_t<result> find(const value_t& value) const {
            result res(resource_);
            if (auto read_error = find(value, res); read_error.contains_error()) {
                return read_error;
            }
            return res;
        }

        void drop();
        // Wipes index data in place (not the terminal drop -- files survive, re-initialized
        // empty). Reports failure by value rather than via pending_write_error_, which find()
        // never reads and would otherwise mask a failed rebuild as "no rows" over intact segments.
        [[nodiscard]] core::error_t clear();
        // Returns io_error when the data did not reach the disk. The caller must fail the
        // statement: a discarded failure here means the table and its index disagree, and
        // nothing downstream would ever notice.
        [[nodiscard]] core::error_t force_flush();
        // Refuses when the keydir walk could not finish: a rebuild fed from PART of an
        // index is a rebuild that drops rows without saying so.
        [[nodiscard]] core::error_t load_entries(entries_t& entries) const;

        // Compacts rotated segments on the caller's thread (rotate_active_segment only records
        // the debt; a worker thread here would need a second lock around the keydir). Called once,
        // at the end of the write handler that incurred it, never mid-append. A refusal is
        // returned directly rather than parked in pending_write_error_, which would surface it on
        // the wrong statement (the next force_flush).
        [[nodiscard]] core::error_t merge_pending_segments();
        // bitcask-internal rehash-suppression window (pre-existing optimization, opened
        // around the bulk run in bitcask_index_agent_t::commit_inserts).
        void set_bulk_mode(bool enabled);
        // Surfaces txn-log write failures (open/write/sync) as core::error_t rather than
        // asserting, so the commit handler can abort just this index. commit_id is part of the
        // journalled fact (matched by the recover gate against the WAL's committed set), not
        // decoration -- it's the one identifier in the frame that never repeats across restarts.
        [[nodiscard]] core::error_t apply_txn_inserts(uint64_t txn_id,
                                                      uint64_t commit_id,
                                                      const std::vector<std::pair<value_t, size_t>>& values);
        [[nodiscard]] core::error_t apply_txn_deletes(uint64_t txn_id,
                                                      uint64_t commit_id,
                                                      const std::vector<std::pair<value_t, size_t>>& values);
        // Bulk-load fast path: skips per-op dedup find() and flush (force_flush() persists once
        // at the end). Caller guarantees each (key, row_id) PAIR is fed at most once -- not
        // unique keys, which are the ordinary non-unique-index case.
        void insert_bulk_unchecked(const value_t& key, size_t value);
        // Already O(1) (hash lookup) and honours bulk mode via flush_if_needed, so this is just
        // the normal remove path -- no find()-scan to avoid (that's a btree concern).
        void remove_bulk_unchecked(const value_t& key, size_t row_id);

        // The keydir (key -> record location). Exposed for tests pinning its own state
        // (clear() wipes it in place rather than replacing it); production uses find/insert/remove.
        [[nodiscard]] const disk_hash_table_t& hash_storage() const noexcept { return *hash_index_; }
        [[nodiscard]] disk_hash_table_t& hash_storage() noexcept { return *hash_index_; }

    private:
        enum class record_kind_t : uint8_t
        {
            value = 1,
            tombstone = 2
        };

        // The whole encoded key at (segment_id, value_offset) -- what a truncated keydir entry
        // cannot answer for itself, or the reason it could not be read.
        [[nodiscard]] core::result_wrapper_t<std::pmr::string> load_hash_key_at(uint32_t segment_id,
                                                                               uint64_t value_offset) const;

        // Deduced callable, not a virtual interface or std::function: one
        // implementation, one caller, both known at compile time. Handed to each call rather
        // than installed on the table, so there's no unhook to forget and no null-loader state.
        [[nodiscard]] auto key_loader() const noexcept {
            return [this](uint32_t log_file_id, uint64_t log_offset) -> core::result_wrapper_t<std::pmr::string> {
                return load_hash_key_at(log_file_id, log_offset);
            };
        }

        struct segment_info_t {
            uint64_t id{0};
            std::filesystem::path path;
            uint64_t record_count{0};
            // Where the replay's walk stopped, not always the segment's size (a crash inside
            // write_record leaves an unreadable tail). Read for the ACTIVE segment only.
            uint64_t scan_end{0};
        };

        using row_ids_t = std::pmr::vector<size_t>;

        // Not the throwing std::filesystem overload -- open()'s failures must be values.
        [[nodiscard]] core::error_t initialize_storage();
        [[nodiscard]] core::error_t load_from_disk();
        // An unfinished merge that can't be finished must not fail silently at open: replaying
        // a source the merge already rewrote brings the keys the merge dropped back as live rows.
        [[nodiscard]] core::error_t apply_merge_recovery_cleanup();
        // A refused directory listing must not collapse to an empty list: load_from_disk
        // resets the keydir before rebuilding it from this list, so an unlistable directory would
        // silently report an empty index as success. A MISSING directory is still empty-and-ok.
        [[nodiscard]] core::result_wrapper_t<std::pmr::vector<segment_info_t>> collect_segments() const;
        // Can refuse on every start and every rotation; an unopenable path or full disk must
        // cost the index its registration, not the engine its process.
        [[nodiscard]] core::error_t open_active_segment();
        [[nodiscard]] core::error_t rotate_active_segment();
        [[nodiscard]] core::error_t rotate_active_segment_if_needed();
        uint64_t allocate_next_segment_id();
        [[nodiscard]] core::error_t merge_immutable_segments();
        [[nodiscard]] core::result_wrapper_t<row_ids_t> current_rows(const value_t& key) const;
        // true = value record, `rows` filled. false = tombstone (legitimately no rows). error =
        // unreadable record (bad open, bad header/payload, CRC mismatch) -- a bare bool would
        // read that as "no rows" and let append_snapshot replace the row list with nothing.
        // Same three-way shape as disk_hash_table_t::erase.
        [[nodiscard]] core::result_wrapper_t<bool>
        read_rows_at(uint32_t segment_id, uint64_t value_offset, row_ids_t& rows, value_t* out_key = nullptr) const;
        std::string key_bytes_for_hash(const value_t& key, bool* ok = nullptr) const;
        [[nodiscard]] core::error_t erase_all_refs_for_key(std::string_view key_bytes);
        // Reports a hash-index write failure rather than dropping it: the segment record is
        // already durable, so a lost index entry would leave the key unfindable silently.
        [[nodiscard]] core::error_t append_snapshot(const value_t& key, const row_ids_t& rows);
        [[nodiscard]] core::error_t append_tombstone(const value_t& key);
        // Returns no_error() on a clean append, an index_create_fail
        // error if the txn-log file cannot be opened (the only recoverable IO
        // failure on this path; write/sync surface through the file handle).
        [[nodiscard]] core::error_t append_txn_record(uint64_t txn_id,
                                                      uint64_t commit_id,
                                                      uint8_t op_kind,
                                                      const std::vector<std::pair<value_t, size_t>>& values);
        // An unreadable txn log is not an empty one: recovery refuses rather than silently
        // dropping every committed frame since the last window; open() hands the reason up.
        [[nodiscard]] core::error_t recover_txn_log();
        std::filesystem::path txn_log_file_path() const;
        std::filesystem::path txn_applied_file_path() const;
        // A result, not a number: "no sidecar yet" is zero, "sidecar present but unreadable" is
        // a refusal -- collapsing both would replay an already-applied txn log from its start.
        [[nodiscard]] core::result_wrapper_t<uint64_t> read_applied_log_offset() const;
        // Returns no_error() once the applied-offset sidecar is durably rewritten, an
        // index_create_fail error if the temp file cannot be opened or flushed. The ctor-time
        // recovery path treats a failure here as terminal (no error channel mid-construction);
        // apply_txn_* surface it as the index-side abort.
        [[nodiscard]] core::error_t write_applied_log_offset(uint64_t offset) const;
        void flush_if_needed();
        // Fsyncs the active segment and the keydir. On refusal the store STAYS DIRTY (clearing
        // it would tell the next flush there's nothing left to write), and force_flush() passes
        // the reason up so a checkpoint can't trim the WAL behind an index that never landed.
        [[nodiscard]] core::error_t sync_if_dirty();
        void note_write_error(core::error_t err);
        // Closes this store for writing when a stump's repair itself fails (truncate/fsync
        // refused): carrying on would let the next append land behind the stump, turning it into
        // an interior frame that load_from_disk can't tell from a truncated tail -- silently
        // dropping every record after it. Reads stay open; clear() is the repair door and the
        // only place the seal lifts.
        [[nodiscard]] core::error_t seal_writes(std::string_view reason);
        [[nodiscard]] core::error_t refuse_if_sealed() const;
        // One door for this store's I/O refusals (same idea as disk_hash_table_t::io_failure
        // one layer down); index_create_fail is the code the index error channel already carries.
        [[nodiscard]] core::error_t io_failure(std::string_view message) const;
        // Opens the keydir file and reports why it could not, as a value; open() forwards it
        // and the construct-and-open ctor aborts on it.
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
        // Sole owner: nothing outside this store holds the keydir.
        std::unique_ptr<disk_hash_table_t> hash_index_;
        // Set when a hash-index write fails on a void-returning path (direct/bulk insert,
        // rebuild, merge); force_flush() is the first point on those paths that can report it.
        core::error_t pending_write_error_{core::error_t::no_error()};
        // LRU (capacity 8) of held descriptors for ROTATED-segment reads, avoiding an open/close
        // pair per find(). Rotated segments never change, so a handle only goes stale when the
        // FILE is replaced -- every path that unlinks/re-derives segments drops the whole cache.
        // Mutable but safe: the agent's mailbox serializes every reader.
        struct rotated_segment_lease_t {
            uint64_t segment_id{0};
            std::unique_ptr<core::filesystem::file_handle_t> handle;
            uint64_t last_used{0};
        };
        static constexpr size_t rotated_read_cache_capacity_ = 8;
        mutable std::vector<rotated_segment_lease_t> rotated_read_cache_;
        mutable uint64_t rotated_read_tick_{0};
        void invalidate_rotated_read_cache_() const noexcept { rotated_read_cache_.clear(); }
        // A read that failed through a held handle drops that handle, so the next attempt
        // re-opens instead of retrying a descriptor that may be the problem.
        void drop_cached_rotated_segment_(uint64_t segment_id) const noexcept;
        // Set by seal_writes, read by refuse_if_sealed, cleared by clear(). See seal_writes.
        bool writes_sealed_{false};
        uint64_t next_timestamp_{0};
        uint64_t next_segment_id_{regular_segment_id_start_};
        uint64_t active_segment_id_{0};
        uint64_t active_segment_records_{0};
        // Where the replay's walk of the active segment stopped, handed to open_active_segment
        // to cut the file back to before anything new is appended. Without this, an insert after
        // a power-cut would write behind the stump, turning it into an interior frame that the
        // next start reads as garbage and stops at -- silently dropping every later record.
        // no_tail_to_trim = replay had nothing to say; reset after each use so a stale value
        // can't apply to a different file.
        static constexpr uint64_t no_tail_to_trim{UINT64_MAX};
        uint64_t active_segment_clean_end_{no_tail_to_trim};
        // Same measurement, for the txn log: without it, a stump turned interior by a later
        // append gets read as a corrupt header or bad CRC and recovery stops there, losing every
        // committed frame behind it. Same lifecycle as active_segment_clean_end_.
        uint64_t txn_log_clean_end_{no_tail_to_trim};
        uint64_t segment_record_limit_{default_segment_record_limit_};
        bool bulk_mode_{false};
        bool bulk_rehash_guard_active_{false};
        bool bulk_prev_rehash_suppressed_{false};
        // A rotation happened and the segments below active are owed a merge. Set by
        // rotate_active_segment, paid by merge_pending_segments; clear()/drop() drop it on the
        // floor since they wipe the segments it names.
        bool merge_pending_{false};
        // WAL-replay committed COMMIT ids; the recover gate applies a frame only when its commit
        // id is a member. See deferred_open_t above for why commit ids, not txn ids.
        std::pmr::set<std::uint64_t> committed_commit_ids_;
        // Set only by load_from_disk on a ROTATED segment's CRC failure (a rotated segment never
        // changes, so a bad CRC means a damaged file); open() turns it into a refusal so the index
        // doesn't register rather than silently dropping rows. Must NOT be set for the ACTIVE
        // segment's CRC failure -- that's a torn write-tail repaired in place, and this flag is
        // checked before the repair can run.
        bool crc_failure_{false};
    };

} // namespace services::index
