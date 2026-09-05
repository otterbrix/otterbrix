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
    // FAULT-INJECTION SEAM FOR THE BITCASK STORE'S OWN FILES.
    //
    // Everything this store meets from the OUTSIDE -- a segment truncated under it, a
    // corrupted record, a missing file -- a test stages through the filesystem, and the
    // cases in test_bitcask_index_disk.cpp do exactly that. Two failures are not reachable
    // that way: a REFUSED WRITE and a REFUSED fsync on a descriptor this store opened
    // itself. chmod does not reach an open fd, and the block manager's T3 interposer wraps
    // single_file_block_manager_t, which this store does not use.
    //
    // So the seam is here, DEV_MODE only, shaped exactly like services::wal's
    // dev_set_wal_file_interposer: a plain virtual interface (rule 14 -- not std::function),
    // process-wide, consulted once per open_file this translation unit performs. Returning
    // nullptr from wrap() models a file that WILL NOT OPEN and is faithful rather than a
    // stand-in: core::filesystem::open_file answers nullptr for exactly that.
    struct bitcask_file_interposer_t {
        virtual ~bitcask_file_interposer_t() = default;
        virtual std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) = 0;
    };

    void dev_set_bitcask_file_interposer(bitcask_file_interposer_t* interposer); // nullptr = off
    bitcask_file_interposer_t* dev_bitcask_file_interposer();
#endif

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
        // -- the deleted copy ctor suppresses the implicit move -- so the agent builds it
        // in its MEMBER INITIALIZER LIST, from parameters, and the open runs as a separate
        // step afterwards.
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
        //
        // AND IT REFUSES rather than answering short. The keydir walk underneath can meet
        // a page it cannot read, and it used to stop there and hand back what it had --
        // which arrives here as "this key has these rows" and reaches a reader as a row
        // set with rows silently missing from it. bitcask_index_agent_t::read_rows already
        // answers with a core::result_wrapper_t, so the value has somewhere to go.
        [[nodiscard]] core::error_t find(const value_t& value, result& res) const;

        // By-value shorthand, built on resource_. Never on a default-constructed
        // std::pmr::vector, which is std::pmr::get_default_resource() by consequence --
        // and insert()/remove() reach it internally, so that would put the process default
        // resource on the WRITE path.
        [[nodiscard]] core::result_wrapper_t<result> find(const value_t& value) const {
            result res(resource_);
            if (auto read_error = find(value, res); read_error.contains_error()) {
                return read_error;
            }
            return res;
        }

        void drop();
        // Wipe all stored index data IN PLACE, keeping the backing live and writable:
        // subsequent insert/remove repopulate cleanly. NOT the terminal drop -- the
        // files/directory survive (re-initialized empty), the instance stays usable.
        //
        // AND IT REPORTS WHAT IT COULD NOT DO. This used to be void, so its only channel was
        // pending_write_error_ -- which find() does not read -- and a rebuild that refused
        // left the reader with "this key has no rows" over segments that were all still
        // there. Three outcomes travel out of here now: the directory could not be listed
        // (nothing was touched, every row is still readable); an artifact could not be
        // removed (the store is consistent, it just holds more than this call promised); the
        // rebuild could not finish (the keydir is CLOSED, so every later read and write
        // refuses, and the next clear() repairs it).
        [[nodiscard]] core::error_t clear();
        // Returns io_error when the data did not reach the disk. The caller must fail the
        // statement: a discarded failure here means the table and its index disagree, and
        // nothing downstream would ever notice.
        [[nodiscard]] core::error_t force_flush();
        // Refuses when the keydir walk could not finish: a rebuild fed from PART of an
        // index is a rebuild that drops rows without saying so.
        [[nodiscard]] core::error_t load_entries(entries_t& entries) const;

        // COMPACT THE ROTATED SEGMENTS, ON THE CALLER'S THREAD.
        //
        // Rotation only RECORDS that a merge is owed (rotate_active_segment); this is the
        // one place it is paid. It used to be paid by a std::thread this store started for
        // itself, which is why the store also held a shared_mutex: two threads reached the
        // keydir and the segment set, so a second serialization domain had to exist beside
        // the agent's mailbox. There is one owner again, so there is one domain again.
        //
        // The OWNER decides WHEN. bitcask_index_agent_t calls this once, at the end of the
        // write handler it is already inside -- never from the middle of a record append,
        // where a statement writing N segments' worth of rows would pay N merges. A
        // no-merge-owed call is free.
        void merge_pending_segments();
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

        // The whole encoded key of the record at (segment_id, value_offset) — the answer to
        // the one question a truncated keydir entry cannot answer for itself, or the reason
        // it could not be read.
        [[nodiscard]] core::result_wrapper_t<std::pmr::string> load_hash_key_at(uint32_t segment_id,
                                                                               uint64_t value_offset) const;

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
            return [this](uint32_t log_file_id, uint64_t log_offset) -> core::result_wrapper_t<std::pmr::string> {
                return load_hash_key_at(log_file_id, log_offset);
            };
        }

        struct segment_info_t {
            uint64_t id{0};
            std::filesystem::path path;
            uint64_t record_count{0};
            // Where the replay's walk of this segment STOPPED, which is not always its size:
            // a crash inside write_record leaves bytes the scan cannot read as a record, and
            // the walk halts in front of them. Filled in by load_from_disk, read for the
            // ACTIVE segment only (see active_segment_clean_end_).
            uint64_t scan_end{0};
        };

        using row_ids_t = std::pmr::vector<size_t>;

        // The THROWING std::filesystem overload used to sit behind this, which is an
        // exception escaping open() -- rule 2 -- for the one failure the whole open path is
        // built to report as a value.
        [[nodiscard]] core::error_t initialize_storage();
        [[nodiscard]] core::error_t load_from_disk();
        void apply_merge_recovery_cleanup();
        // THE DIRECTORY LISTING IS A READ, AND A READ CAN REFUSE. This used to answer with a
        // plain vector built by the THROWING std::filesystem overloads on a -fno-exceptions
        // build (rule 2), which means a directory it could not enumerate came back as an
        // EMPTY LIST -- indistinguishable from "this index has no segments yet".
        //
        // That undercount used to cost a skipped replay. It now costs the whole index:
        // load_from_disk resets the keydir before it reads this list and rebuilds it from
        // what the list holds, so an unlistable directory would answer with a keydir built
        // from nothing and report success over it. A MISSING directory is still the empty
        // list and still no error -- "there is no index here yet" is an answer; a listing
        // that REFUSED is not.
        [[nodiscard]] core::result_wrapper_t<std::pmr::vector<segment_info_t>> collect_segments() const;
        // OPENING THE ACTIVE SEGMENT IS A STEP THAT CAN REFUSE. It used to abort, and it
        // runs on EVERY start and EVERY rotation, so an unopenable path or a full disk cost
        // the engine its process rather than the index its registration. open() already
        // reports as a value and rotation happens under append_snapshot/append_tombstone,
        // which do too.
        [[nodiscard]] core::error_t open_active_segment();
        [[nodiscard]] core::error_t rotate_active_segment();
        [[nodiscard]] core::error_t rotate_active_segment_if_needed();
        uint64_t allocate_next_segment_id();
        [[nodiscard]] core::error_t merge_immutable_segments();
        [[nodiscard]] core::result_wrapper_t<row_ids_t> current_rows(const value_t& key) const;
        // TRUE = the record at this location is a VALUE record and `rows` now holds its row
        // list. FALSE = it is a TOMBSTONE, i.e. the legitimate "this key has no rows here".
        // AN ERROR = the record could not be read at all: the segment would not open, the
        // keydir pointed inside the header, the header or the payload came up short, or the
        // CRC did not match.
        //
        // The bool used to carry all six outcomes, and every caller read the five failures
        // as the one legal answer -- so an unreadable record arrived at current_rows() as
        // "this key has no rows", and the very next append_snapshot REPLACED the key's row
        // list with a list built from nothing. This is the same three-way shape
        // disk_hash_table_t::erase carries for the same reason.
        [[nodiscard]] core::result_wrapper_t<bool>
        read_rows_at(uint32_t segment_id, uint64_t value_offset, row_ids_t& rows, value_t* out_key = nullptr) const;
        std::string key_bytes_for_hash(const value_t& key, bool* ok = nullptr) const;
        [[nodiscard]] core::error_t erase_all_refs_for_key(std::string_view key_bytes);
        // Reports a hash-index write failure instead of dropping it: the segment record is already
        // durable at that point, so a lost index entry would leave the key unfindable while the
        // statement reported success.
        [[nodiscard]] core::error_t append_snapshot(const value_t& key, const row_ids_t& rows);
        [[nodiscard]] core::error_t append_tombstone(const value_t& key);
        // M3.5: returns no_error() on a clean append, an index_create_fail
        // error if the txn-log file cannot be opened (the only recoverable IO
        // failure on this path; write/sync surface through the file handle).
        [[nodiscard]] core::error_t append_txn_record(uint64_t txn_id,
                                                      uint8_t op_kind,
                                                      const std::vector<std::pair<value_t, size_t>>& values);
        // A TXN LOG THAT CANNOT BE READ IS NOT AN EMPTY ONE. This used to return silently
        // when the log would not open -- every committed frame of the last window vanished
        // from the index for the whole uptime -- while a CORRUPT frame three lines below
        // aborted the process. Both halves are the same event now: recovery refuses, open()
        // hands the reason up, and the engine keeps running without the index.
        [[nodiscard]] core::error_t recover_txn_log();
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
        // Fsync the active segment AND the keydir, and say when one of them refused. On a
        // refusal the store STAYS DIRTY -- clearing the flag would tell the next flush there
        // was nothing left to write -- and force_flush() hands the reason to the caller, so
        // a checkpoint can no longer trim the WAL behind an index that never reached disk.
        [[nodiscard]] core::error_t sync_if_dirty();
        void note_write_error(core::error_t err);
        // A STUMP THAT COULD NOT BE UNDONE CLOSES THIS STORE FOR WRITING, and these two are
        // the closing and the closed door.
        //
        // discard_partial_record is the repair, and when the repair itself refuses -- the
        // truncate would not take, the fsync of it would not take -- the half-written record
        // is STILL at the end of the file and the descriptor is STILL past it. Reporting that
        // as an ordinary io_failure and carrying on is the silent loss the repair exists to
        // prevent, only one statement later: the very next append reads seek_position(),
        // lands BEHIND the stump, and turns it from a tail into an interior frame -- the one
        // shape load_from_disk cannot tell from a truncated tail, so it stops there and every
        // record after it leaves the index without a word.
        //
        // So the store stops taking records instead. READS are untouched: everything written
        // before the stump is intact and findable, and the keydir was never pointed at the
        // record that failed. clear() is the repair door -- it unlinks the segment the stump
        // is in, which is why it is also the one place the seal is lifted.
        [[nodiscard]] core::error_t seal_writes(std::string_view reason);
        [[nodiscard]] core::error_t refuse_if_sealed() const;
        // One door for this store's I/O refusals, so the code and the resource are the same
        // on every one of them -- disk_hash_table_t::io_failure one layer down is the same
        // idea. index_create_fail is the code the index error channel already carries.
        [[nodiscard]] core::error_t io_failure(std::string_view message) const;
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
        // Set by seal_writes, read by refuse_if_sealed, cleared by clear(). See seal_writes.
        bool writes_sealed_{false};
        uint64_t next_timestamp_{0};
        uint64_t next_segment_id_{regular_segment_id_start_};
        uint64_t active_segment_id_{0};
        uint64_t active_segment_records_{0};
        // THE UNREADABLE TAIL OF THE ACTIVE SEGMENT, HANDED FROM THE REPLAY TO THE OPEN.
        //
        // The write path can undo its own stump (discard_partial_record) because it is still
        // running; a POWER CUT inside write_record leaves a byte-identical stump with nobody
        // left to undo it. On the next start load_from_disk walks the segment, stops in front
        // of those bytes and reports success -- correctly, because at that moment the stump
        // IS the tail and there is nothing after it. Then open_active_segment used to seek to
        // file_size(), i.e. PAST the stump, and the first insert after recovery wrote a
        // well-formed record behind it. From that point the stump is an interior frame, and
        // the NEXT start reads its bytes together with the head of the record after it as one
        // header, takes the garbage length for a payload past EOF, takes that for a truncated
        // tail -- and stops, silently, dropping every record written after the crash.
        //
        // So the replay hands over where the records really end and open_active_segment cuts
        // the file back to it, at the one moment when doing so cannot cost anything: nothing
        // has been appended yet, so the bytes being removed are exactly the ones no reader
        // could use. no_tail_to_trim means the replay has nothing to say -- a fresh directory,
        // a rotation's brand-new segment -- and the file is left alone; it is also what the
        // field is set back to once a value has been consumed, so a stale one cannot be
        // applied to a different file.
        static constexpr uint64_t no_tail_to_trim{UINT64_MAX};
        uint64_t active_segment_clean_end_{no_tail_to_trim};
        uint64_t segment_record_limit_{default_segment_record_limit_};
        bool bulk_mode_{false};
        bool bulk_rehash_guard_active_{false};
        bool bulk_prev_rehash_suppressed_{false};
        // A rotation happened and the segments below the active one are owed a merge.
        // Set by rotate_active_segment, paid and cleared by merge_pending_segments, and
        // dropped on the floor by clear()/drop() -- which wipe the very segments it names.
        // There is nothing to DRAIN on those two paths: the flag is only ever read from
        // inside a handler, and a handler is the only thing running.
        bool merge_pending_{false};
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
