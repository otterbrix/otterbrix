#pragma once

#include "bitcask_task_executor.hpp"
#include "disk_hash_table.hpp"

#include <components/types/logical_value.hpp>
#include <core/file/file_handle.hpp>
#include <core/file/local_file_system.hpp>
#include <core/result_wrapper.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <memory_resource>
#include <set>
#include <shared_mutex>
#include <vector>

namespace services::index {

    // THE HASHED STORE. It has no base class and there deliberately is not going to be
    // one: the erased index_disk_t it used to derive from existed so a single index agent
    // could hold either family behind one pointer and ASK it at runtime which it was --
    // does it own a txn log, has it a bulk window, can it answer an ordered probe. There
    // is one agent class per family now, each holding its store BY VALUE and by this
    // concrete type, so every one of those questions is answered by the type.
    //
    // What the base really owned -- the resource, the flush accounting, the by-value read
    // shorthands -- is duplicated here and in btree_index_disk_t. That duplication is the
    // price of not having the coupling, and it is the cheaper half.
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

        // CONSTRUCTED WHERE IT LIVES. This store is a MEMBER of bitcask_index_agent_t, BY
        // VALUE: the agent is its sole owner, the type is known statically, and there is
        // no erased base left to hold it by, so the unique_ptr that used to stand between
        // the two was indirection and nothing else. It cannot be MOVED into place either
        // -- the shared_mutex below is immovable and the deleted copy ctor suppresses the
        // implicit move -- so the agent builds it in its MEMBER INITIALIZER LIST, from
        // parameters, and the open runs as a separate step afterwards.
        //
        // deferred_open_t is that split, spelled in the type: this ctor performs NO I/O,
        // which is what lets the failures open() meets (an unopenable keydir, a segment
        // CRC mismatch) be VALUES a caller acts on instead of aborts inside a constructor
        // that rule 2 forbids from refusing.
        //
        // committed_txn_ids: WAL-replay set of committed transaction ids. The
        // txn-log recover gate (M1.1) applies a frame only when its txn_id is in
        // this set; uncommitted-txn frames are skipped (their WAL commit marker
        // never landed). A fresh, runtime-created instance passes an EMPTY set —
        // a fresh dir has no txn-log to gate. It is stored HERE, by this ctor, so the gate
        // is armed before open() runs recovery.
        struct deferred_open_t {};

        bitcask_index_disk_t(const path_t& path,
                             std::pmr::memory_resource* resource,
                             uint64_t flush_threshold,
                             uint64_t segment_record_limit,
                             std::pmr::set<std::uint64_t> committed_txn_ids,
                             deferred_open_t);

        // CONSTRUCT AND OPEN IN ONE STEP, aborting on a failure it has no way to report
        // (rule 2: a constructor cannot return a value). Production never takes this road
        // -- it goes through the deferred ctor above plus open() below, because these
        // failures are environmental and must cost the INDEX its registration, never the
        // ENGINE its start (integration test test_index_bootstrap_failure). What this is
        // for is the backend tests, which want a store that is simply open.
        bitcask_index_disk_t(const path_t& path,
                             std::pmr::memory_resource* resource,
                             uint64_t flush_threshold,
                             uint64_t segment_record_limit,
                             std::pmr::set<std::uint64_t> committed_txn_ids);
        ~bitcask_index_disk_t();

        bitcask_index_disk_t(const bitcask_index_disk_t&) = delete;
        bitcask_index_disk_t& operator=(const bitcask_index_disk_t&) = delete;

        // The resource every answer this store produces is built on. A result built
        // anywhere else is a result built on the process default resource.
        [[nodiscard]] std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // BRING THE BACKING UP, and say why it could not AS A VALUE: a keydir file
        // (hash_index.bin) that will not open -- an unopenable path, an unreadable or
        // incompatible header -- or a segment CRC mismatch during recovery. The keydir is
        // opened HERE, by the store that owns it; there is no handle to hand in (C2c,
        // rule 10).
        //
        // Called EXACTLY ONCE, immediately after the deferred ctor, by
        // bitcask_index_agent_t::create() -- which destroys the half-built agent and hands
        // this error back in its place, so an agent only ever exists over a store that
        // opened. Nothing is recorded on the object: there is no state-then-ask
        // convention for an owner to forget.
        [[nodiscard]] core::error_t open();

        using entry_t = std::pair<value_t, size_t>;
        using entries_t = std::pmr::vector<entry_t>;

        void insert(const value_t& key, size_t value);
        void remove(value_t key);
        void remove(const value_t& key, size_t row_id);

        // THE read, and the ONLY one this family has: equality. A hashed store has no
        // ordering, so there is no scan_range here to refuse a range predicate loudly --
        // the refusal moved UP, to bitcask_index_agent_t::read_rows, which is the only
        // caller that could ever ask and is the one place a caller can be told. What the
        // erased base forced on this class was a scan_range override that existed solely
        // to abort; the member is gone with the base.
        //
        // find() reads the SNAPSHOT RECORD and unrolls the whole row list: the keydir
        // keeps one entry per key whose payload field is `rows.back()`, so a reader that
        // consulted it would silently drop every duplicate.
        void find(const value_t& value, result& res) const;

        // By-value shorthand, built on resource_. Never on a default-constructed
        // std::pmr::vector, which is std::pmr::get_default_resource() by consequence --
        // and insert()/remove() reach it internally, so that would put the process default
        // resource on the WRITE path.
        [[nodiscard]] result find(const value_t& value) const {
            result res(resource_);
            find(value, res);
            return res;
        }

        void drop();
        // Wipe all stored index data IN PLACE, keeping the backing live and writable:
        // subsequent insert/remove repopulate cleanly. NOT the terminal drop -- the
        // files/directory survive (re-initialized empty), the instance stays usable.
        void clear();
        // Returns io_error when the data did not reach the disk. The caller must fail the
        // statement: a discarded failure here means the table and its index disagree, and
        // nothing downstream would ever notice.
        [[nodiscard]] core::error_t force_flush();
        void load_entries(entries_t& entries) const;
        void enqueue_task(std::function<void()> task);
        // bitcask-internal rehash-suppression window (pre-existing optimization, opened
        // around the bulk run in bitcask_index_agent_t::commit_inserts).
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
        // Bulk-load fast path: skips the per-operation dedup find() and the per-operation
        // flush; force_flush() persists once at the end.
        //
        // WHAT THE CALLER GUARANTEES, precisely: each (key, row_id) PAIR is fed at most
        // once and, for the remove side, is present. It does NOT guarantee unique KEYS --
        // a non-unique index is the ordinary case, and every rebuild feed replays a whole
        // table, repeated keys included.
        void insert_bulk_unchecked(const value_t& key, size_t value);
        // bitcask remove is already O(1) (hash lookup) and honours bulk mode
        // (flush_if_needed skips while bulk), so the bulk remove is the normal
        // remove path — no per-key find()-scan to avoid (that is a btree concern).
        void remove_bulk_unchecked(const value_t& key, size_t row_id);

        // The keydir this store keeps its (key -> record location) entries in. Reachable
        // for tests that pin the keydir's OWN state -- that clear() wipes it in place
        // rather than replacing it. Production reaches it only through find/insert/remove
        // above.
        [[nodiscard]] const disk_hash_table_t& hash_storage() const noexcept { return *hash_index_; }
        [[nodiscard]] disk_hash_table_t& hash_storage() noexcept { return *hash_index_; }

    private:
        enum class record_kind_t : uint8_t
        {
            value = 1,
            tombstone = 2
        };

        // The whole encoded key of the record at (segment_id, value_offset) — the answer
        // to the one question a truncated keydir entry cannot answer for itself. Reads
        // the segment WITHOUT taking this store's lock, because every caller of it below
        // already holds that lock.
        bool load_hash_key_at_unlocked(uint32_t segment_id, uint64_t value_offset, std::string& out_key) const;

        // This store's answer to disk_hash_table_t's truncated-key question, as a
        // DEDUCED callable rather than a virtual interface and rather than a
        // std::function (rule 14). The customization point is not virtual and has ONE
        // implementation and ONE caller, both known at compile time, so the erasure buys
        // nothing — the same rule C0b recorded when for_each stopped taking a
        // std::function.
        //
        // It is HANDED TO THE CALL rather than installed on the table: nothing outlives
        // anything, there is no unhook to forget in the destructor, and the table has no
        // null-loader state to answer a long key false from. It crosses no actor
        // boundary either (rule 10) — the table it is handed to is owned by this store.
        [[nodiscard]] auto key_loader() const noexcept {
            return [this](uint32_t log_file_id, uint64_t log_offset, std::string& out_key) -> bool {
                return load_hash_key_at_unlocked(log_file_id, log_offset, out_key);
            };
        }

        struct segment_info_t {
            uint64_t id{0};
            std::filesystem::path path;
            uint64_t record_count{0};
        };

        using row_ids_t = std::pmr::vector<size_t>;

        void initialize_storage();
        void load_from_disk();
        void apply_merge_recovery_cleanup();
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
        // Reports a hash-index write failure instead of dropping it: the segment record is already
        // durable at that point, so a lost index entry would leave the key unfindable while the
        // statement reported success.
        [[nodiscard]] core::error_t append_snapshot(const value_t& key, const row_ids_t& rows);
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
        void note_write_error(core::error_t err);
        // Opens the keydir file and says why it could not, as a VALUE: open() hands that
        // value back and the construct-and-open ctor aborts on it. Nothing is recorded on
        // the object, so no owner has to know to ask afterwards.
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
        // SOLE owner. It used to be an intrusive_ptr because the index facade held the
        // same object across the actor boundary; nothing outside this store holds it any
        // more, so the reference count went with the sharing (C2c, rule 10).
        std::unique_ptr<disk_hash_table_t> hash_index_;
        // Set when a hash-index write fails on a path whose caller returns void (the direct and
        // bulk inserts, the startup rebuild, segment merge). force_flush() hands it to the caller,
        // which is the first point on those paths that can report anything at all.
        core::error_t pending_write_error_{core::error_t::no_error()};
        uint64_t next_timestamp_{0};
        std::atomic<uint64_t> next_segment_id_{regular_segment_id_start_};
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
        // Allocated on resource_. Empty for a fresh, runtime-created instance
        // (no txn-log to gate).
        std::pmr::set<std::uint64_t> committed_txn_ids_;
        // Set by load_from_disk when a segment's CRC check fails. The
        // factory checks this flag to convert the failure into a
        // core::error_t; the direct ctor asserts.
        bool crc_failure_{false};
    };

} // namespace services::index
