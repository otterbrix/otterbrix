#pragma once

#include "base_statistics.hpp"
#include "column_segment.hpp"
#include "column_state.hpp"
#include "segment_tree.hpp"
#include "update_segment.hpp"
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/types/tri_bool.hpp>

namespace components::table {

#ifdef DEV_MODE
    // Test-observable count of segment transitions performed while SOMEONE ELSE still holds a pin on
    // the segment's block. The swap drops that block_handle_t, so an outstanding buffer_handle_t is
    // left pointing at freed memory and unpins it when it is destroyed. Must stay at zero.
    uint64_t transitions_with_live_pin() noexcept;
    // Total transitions performed, so a test can tell "no offending transition" apart from
    // "no transition at all" — a zero-vs-zero comparison proves nothing.
    uint64_t segment_transitions() noexcept;
    void reset_transitions_with_live_pin() noexcept;
#endif

    struct persistent_column_data_t;

    namespace storage {
        class block_manager_t;
        class partial_block_manager_t;
    } // namespace storage

    enum class filter_propagate_result_t : uint8_t
    {
        NO_PRUNING_POSSIBLE = 0,
        ALWAYS_TRUE = 1,
        ALWAYS_FALSE = 2,
        TRUE_OR_NULL = 3,
        FALSE_OR_NULL = 4
    };
    // The storage-scan filter answers in SQL three-valued logic. filter_match_t is the table
    // component's spelling of the shared types::tri_bool_t vocabulary (tri_bool.hpp), so the scan
    // filter and the in-memory predicate evaluator share one definition of TRUE/FALSE/UNKNOWN and
    // cannot drift. A value comparison against a NULL operand is UNKNOWN, not FALSE: the two differ
    // under NOT, so collapsing UNKNOWN into FALSE would let NOT resurrect NULL rows.
    using filter_match_t = types::tri_bool_t;

    constexpr uint64_t MAX_ROW_ID = 1ULL << 55; // 2^55

    // A row group's TOP-LEVEL columns are shared: row_group_t::add_column / remove_column copy the
    // column vector into the ALTER successor's row group, so parent and successor hold the SAME
    // column objects and whichever row group dies last must be the one that frees them. The
    // reference count therefore lives inside the object (boost::intrusive_ref_counter;
    // std::shared_ptr is forbidden — rule 14). column_data_t::create_column allocates every branch
    // with plain `new` (std::make_unique), never from the pmr resource — the resource parameter
    // only feeds the object's internal containers — so the counter's `delete` is the matching
    // deallocation, and the virtual destructor below makes deleting a derived column through a
    // column_data_t* correct.
    //
    // This counter is for the top-level columns ALONE. Columns reached any other way are
    // exclusively owned and leave it at zero, untouched: the nested children
    // (list/array child_column, struct sub_columns) are unique_ptr, and
    // standard_column_data_t::validity is a BY-VALUE member. Never build an intrusive_ptr to one
    // of those — releasing it would `delete` a subobject or double-free a unique_ptr's object.
    class column_data_t : public boost::intrusive_ref_counter<column_data_t> {
        friend class column_segment_t;
        friend class column_data_checkpointer_t;
        friend class column_checkpoint_state_t;

    public:
        column_data_t(std::pmr::memory_resource* resource,
                      storage::block_manager_t& block_manager,
                      uint64_t column_index,
                      int64_t start_row,
                      types::complex_logical_type type,
                      column_data_t* parent);
        virtual ~column_data_t() = default;

        virtual filter_propagate_result_t check_zonemap(column_scan_state& state, table_filter_t& filter);
        filter_propagate_result_t check_segment_zonemap(column_scan_state& state, table_filter_t& filter);

        storage::block_manager_t& block_manager() { return block_manager_; }
        virtual uint64_t max_entry();

        uint64_t allocation_size() const { return allocation_size_; }

        virtual void set_start(int64_t new_start);
        const types::complex_logical_type& root_type() const;
        const types::complex_logical_type& type() const { return type_; }
        bool has_updates() const;
        virtual scan_vector_type
        get_vector_scan_type(column_scan_state& state, uint64_t scan_count, vector::vector_t& result);
        virtual void initialize_scan(column_scan_state& state);
        virtual void initialize_scan_with_offset(column_scan_state& state, int64_t row_idx);
        uint64_t scan(uint64_t vector_index, column_scan_state& state, vector::vector_t& result);
        uint64_t
        scan_committed(uint64_t vector_index, column_scan_state& state, vector::vector_t& result, bool allow_updates);
        virtual uint64_t
        scan(uint64_t vector_index, column_scan_state& state, vector::vector_t& result, uint64_t scan_count);
        virtual uint64_t scan_committed(uint64_t vector_index,
                                        column_scan_state& state,
                                        vector::vector_t& result,
                                        bool allow_updates,
                                        uint64_t scan_count);

        virtual uint64_t scan_count(column_scan_state& state, vector::vector_t& result, uint64_t count);
        // Like scan_count, but tolerates and applies committed updates over the scanned
        // range (scan_count itself asserts no updates). Used by LIST/ARRAY parents whose
        // child elements may carry in-place updates after a row was updated.
        uint64_t scan_count_with_updates(column_scan_state& state, vector::vector_t& result, uint64_t count);

        virtual void select(uint64_t vector_index,
                            column_scan_state& state,
                            vector::vector_t& result,
                            vector::indexing_vector_t& indexing,
                            uint64_t count);
        virtual void select_committed(uint64_t vector_index,
                                      column_scan_state& state,
                                      vector::vector_t& result,
                                      vector::indexing_vector_t& indexing,
                                      uint64_t count,
                                      bool allow_updates);
        virtual void filter_scan(uint64_t vector_index,
                                 column_scan_state& state,
                                 vector::vector_t& result,
                                 vector::indexing_vector_t& indexing,
                                 uint64_t count);
        virtual void filter_scan_committed(uint64_t vector_index,
                                           column_scan_state& state,
                                           vector::vector_t& result,
                                           vector::indexing_vector_t& indexing,
                                           uint64_t count,
                                           bool allow_updates);

        virtual void skip(column_scan_state& state, uint64_t count = vector::DEFAULT_VECTOR_CAPACITY);

        // APPEND chain returns out_of_memory when a segment allocation / pin fails; true on success.
        [[nodiscard]] virtual core::result_wrapper_t<bool> initialize_append(column_append_state& state);
        [[nodiscard]] virtual core::result_wrapper_t<bool>
        append(column_append_state& state, vector::vector_t& vector, uint64_t count);
        [[nodiscard]] virtual core::result_wrapper_t<bool>
        append_data(column_append_state& state, vector::unified_vector_format& uvf, uint64_t count);
        // `start_row` is COLLECTION-ABSOLUTE: the row group's start plus the group-local
        // revert point (row_group_t::revert_append owns that conversion). Every override
        // receives it in that space and keeps rows [start_, start_row). Nested columns
        // convert to their child's coordinates THEMSELVES: LIST/ARRAY children share the
        // parent's start_ but are addressed in ELEMENTS from the row group base.
        // Returns out_of_memory / data_corruption when a rollback read or pin fails; a
        // revert that cannot complete must be REPORTED, not asserted away — a half-reverted
        // column desyncs its offsets from its data on the next append (rule 6).
        [[nodiscard]] virtual core::result_wrapper_t<bool> revert_append(int64_t start_row);

        // `error` carries an out_of_memory error_t when a pin fails during the predicate check;
        // on error the bool return is meaningless and the scan loop stops.
        virtual uint64_t fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result);
        virtual void
        fetch_row(column_fetch_state& state, int64_t row_id, vector::vector_t& result, uint64_t result_idx);

        // Update path returns out_of_memory / data_corruption / io_error; true on success.
        // NOT write_conflict — the update overlay below carries no transaction stamp to
        // conflict with (components/table/update_segment.hpp).
        [[nodiscard]] virtual core::result_wrapper_t<bool>
        update(uint64_t column_index, vector::vector_t& update_vector, int64_t* row_ids, uint64_t update_count);
        [[nodiscard]] virtual core::result_wrapper_t<bool> update_column(const std::vector<uint64_t>& column_path,
                                                                         vector::vector_t& update_vector,
                                                                         int64_t* row_ids,
                                                                         uint64_t update_count,
                                                                         uint64_t depth);

        virtual void get_column_segment_info(uint64_t row_group_index,
                                             std::vector<uint64_t> col_path,
                                             std::vector<column_segment_info>& result);

        // The precondition create_column cannot state itself: a constructor has no way to
        // refuse, so struct_column_data_t used to THROW on an unnamed struct — across the disk
        // agent's mailbox, into a coroutine with an empty unhandled_exception(), i.e. a hang
        // rather than an error (rules 2/9). Ask this about the TYPE first, at a gate that owns
        // an error channel; collection_t::initialize_append is that gate for every write.
        // Recursive over the same three nested shapes create_column dispatches on.
        [[nodiscard]] static core::error_t validate_column_type(const types::complex_logical_type& type,
                                                                std::pmr::memory_resource* resource);

        // Hands back EXCLUSIVE ownership. Nested children (list/array child_column, struct
        // sub_columns) keep it exactly so; a row group adopting the result as one of its
        // shared top-level columns transfers it into the intrusive counter instead — see
        // adopt_column() in row_group.cpp.
        static std::unique_ptr<column_data_t> create_column(std::pmr::memory_resource* resource,
                                                            storage::block_manager_t& block_manager,
                                                            uint64_t column_index,
                                                            int64_t start_row,
                                                            const types::complex_logical_type& type,
                                                            column_data_t* parent = nullptr);

        std::pmr::memory_resource* resource() const noexcept { return resource_; }
        uint64_t count() const noexcept { return count_; }
        int64_t start() const noexcept { return start_; }
        const base_statistics_t& statistics() const noexcept { return statistics_; }
        base_statistics_t& statistics() noexcept { return statistics_; }

        // CHECKPOINT chain returns out_of_memory when pinning a segment buffer fails during flush;
        // the persistent data on success. Flushes this node's own segments, records count_, then
        // hands the persistent record to checkpoint_children (the NVI hook below) so nested
        // columns (LIST/STRUCT/ARRAY) append their child columns' persistent form recursively —
        // and every column WITH a validity child (standard/struct/list/array) persists that
        // child FIRST, so NULL bits survive the checkpoint (see checkpoint_children).
        [[nodiscard]] core::result_wrapper_t<persistent_column_data_t>
        checkpoint(storage::partial_block_manager_t& partial_block_manager);
        // LOAD chain: rebuilds this column node (and, in overrides, its validity child and its
        // nested children) from the checkpointed record. Fed by bytes read from DISK, so every
        // malformed shape — a missing validity child, a validity row count that does not match
        // the column, an oversized segment — is a data_corruption error_t, never an assert
        // (asserts vanish under NDEBUG) and never a silent "assume all-valid" fallback.
        [[nodiscard]] virtual core::result_wrapper_t<bool>
        initialize_column(const persistent_column_data_t& persistent_data);

        // Write-through: re-point every COMPLETE managed (in-memory, non-reloadable) segment of this column
        // to a disk-backed segment so the pool can evict+reload them (bounded memory). Called when a row
        // group is closed (all its column segments are final). A no-op for non-fixed-size / compressed
        // segments. Returns io_error/out_of_memory on failure; true on success.
        // Sub-columns (validity / struct / list / array children) are handled by the subclass override.
        //
        // The re-pointed segments are PACKED into shared 256 KiB blocks via `pbm` (the same segment-packing
        // allocator the checkpoint path uses) so narrow column segments no longer each consume a dedicated
        // block. `pbm.write_to_block` only fills an in-memory block buffer; the CALLER owns `pbm` and MUST
        // call `pbm.flush_partial_blocks()` before any concurrent scan/eviction of a re-pointed segment can
        // occur (else a re-pointed live segment could load() an unflushed block -> data_corruption).
        [[nodiscard]] virtual core::result_wrapper_t<bool> transition_to_disk(storage::partial_block_manager_t& pbm);

        // Compact reclaim: append the ids of disk blocks EXCLUSIVELY owned by this column (and its
        // sub-columns) to `out`, so the caller can mark them free once this collection is replaced by a
        // compacted one. Mirrors the checkpoint_children / initialize_column recursion, NOT the
        // transition_to_disk one: since the checkpoint_children hooks every child (validity is always
        // children[0]; struct fields and list/array elements follow) is a persisted column in its own
        // right, so a RELOADED child sits on real disk blocks even though write-through never descends
        // into nested children. Every subclass with sub-columns therefore overrides this to collect its
        // validity child and its nested children on top of the base walk of its own data_ tree; before
        // F6 struct/list/array had no override and compact orphaned their children's blocks — durably,
        // once a checkpoint moved the root past the one they were loaded from
        // (test_nested_compact_reclaim.cpp).
        virtual void collect_disk_block_ids(std::pmr::vector<uint64_t>& out) const;

    protected:
        // Returns out_of_memory when the new segment's transient memory cannot be registered; true on success.
        [[nodiscard]] core::result_wrapper_t<bool> apend_transient_segment(std::unique_lock<std::mutex>& l,
                                                                           int64_t start_row);

        // Write-through: a just-FILLED transient (managed, block_id >= MAXIMUM_BLOCK) segment at
        // `segment_index` in data_ is written to the table's data file and re-pointed to a fresh disk-backed
        // segment (block_id < MAXIMUM_BLOCK -> is_reloadable()==true), so the pool can evict+reload it ->
        // bounded memory. A no-op for non-fixed-size / compressed segments (a raw block copy would not
        // round-trip losslessly). Returns io_error/out_of_memory on a
        // write/alloc failure; true on success or no-op. Caller MUST hold the tree lock `l`.
        //
        // The re-pointed segment is PACKED into a shared block via `pbm` (segment packing): small segments
        // share a 256 KiB block at distinct offsets instead of each owning a dedicated block. `pbm.write_to_block`
        // only fills an in-memory block buffer -- the CALLER (transition_to_disk's owner) MUST flush `pbm`
        // before the re-pointed segment can be evicted/reloaded (flush-before-evict).
        [[nodiscard]] core::result_wrapper_t<bool> transition_segment_to_disk(std::unique_lock<std::mutex>& l,
                                                                              uint64_t segment_index,
                                                                              storage::partial_block_manager_t& pbm);

        uint64_t
        scan_vector(column_scan_state& state, vector::vector_t& result, uint64_t remaining, scan_vector_type scan_type);
        template<bool SCAN_COMMITTED, bool ALLOW_UPDATES>
        uint64_t
        scan_vector(uint64_t vector_index, column_scan_state& state, vector::vector_t& result, uint64_t target_scan);

        // `state` is here for one reason: allow_updates == false over a column that HAS updates
        // is a refusal (an index build cannot see an update overlay), and this was the only
        // place in the scan chain with nothing to say it on. It goes into state.scan_error, the
        // same channel row_group_t aggregates for every other scan failure.
        void fetch_updates(column_scan_state& state,
                           uint64_t vector_index,
                           vector::vector_t& result,
                           uint64_t result_offset,
                           uint64_t scan_count,
                           bool allow_updates,
                           bool scan_committed);
        void fetch_update_row(int64_t row_id, vector::vector_t& result, uint64_t result_idx);
        [[nodiscard]] core::result_wrapper_t<bool> update_internal(uint64_t column_index,
                                                                   vector::vector_t& update_vector,
                                                                   int64_t* row_ids,
                                                                   uint64_t update_count,
                                                                   vector::vector_t& base_vector);

        uint64_t vector_count(uint64_t vector_index) const;

        int64_t start_;
        std::atomic<uint64_t> count_;
        storage::block_manager_t& block_manager_;

    private:
        // NVI hook of checkpoint(): a column with sub-columns checkpoints each of them and
        // appends its persistent form to `persistent.child_columns`, in the same order
        // initialize_column consumes them on load. Convention (v1, on-disk): the VALIDITY
        // child is always child_columns[0] — standard = [validity], struct = [validity,
        // field...], list/array = [validity, element] — mirroring the in-memory scan-state
        // layout where child_states[0] is validity. Without the persisted validity child the
        // reload manufactured an all-valid bitmap and every checkpointed NULL was lost.
        // Default: no children (only validity_column_data_t itself, which has no sub-columns).
        [[nodiscard]] virtual core::result_wrapper_t<bool>
        checkpoint_children(storage::partial_block_manager_t& partial_block_manager,
                            persistent_column_data_t& persistent);

    protected:
        uint64_t column_index_;
        types::complex_logical_type type_;
        column_data_t* parent_;
        segment_tree_t<column_segment_t> data_;
        // Single-owner: see the proof on data_table_t (components/table/data_table.hpp).
        std::unique_ptr<update_segment_t> updates_;
        uint64_t allocation_size_;
        base_statistics_t statistics_;

        std::pmr::memory_resource* resource_;
    };

} // namespace components::table