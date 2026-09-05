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

    // A row group's TOP-LEVEL columns are shared: add_column/remove_column copy the column vector
    // into the ALTER successor, so parent and successor hold the SAME objects and whichever row
    // group dies last frees them. Count lives inside the object (intrusive_ref_counter; shared_ptr
    // is forbidden); allocated with plain `new`, so `delete` is the matching deallocation.
    // For TOP-LEVEL columns only — nested children (list/array child_column, struct sub_columns)
    // are unique_ptr, and standard_column_data_t::validity is by-value. Never build an
    // intrusive_ptr to one of those: it would delete a subobject or double-free a unique_ptr.
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
        // True once the overlay object EXISTS, not once it changed a value — writing a row back
        // to its own value still flips this true, forever (test_update_overlay_predicate.cpp;
        // nothing ever clears updates_). Consumers over-report in the safe direction (extra
        // zonemap miss, extra scan, an unneeded checkpoint rebuild —
        // table_storage_t::has_pending_update_overlay). A consumer needing "did a row actually
        // change" wants update_segment_t::has_updates(...) instead.
        bool has_update_segment() const;
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
        // `start_row` is COLLECTION-ABSOLUTE (row_group_t::revert_append converts); keeps rows
        // [start_, start_row). Nested columns convert to their child's coordinates themselves
        // (LIST/ARRAY children are addressed in ELEMENTS from the row group base). Returns
        // out_of_memory / data_corruption on a failed rollback read/pin — must be REPORTED, not
        // asserted away, or the column desyncs its offsets on the next append.
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

        // The precondition create_column's constructors cannot state themselves (a throw there
        // would hang across the disk agent's mailbox). Called at a gate that owns an
        // error channel — collection_t::initialize_append, for every write.
        [[nodiscard]] static core::error_t validate_column_type(const types::complex_logical_type& type,
                                                                std::pmr::memory_resource* resource);

        // Hands back EXCLUSIVE ownership. A row group adopting the result as a shared top-level
        // column transfers it into the intrusive counter instead — see adopt_column() in
        // row_group.cpp.
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
        // the persistent data on success. Hands the record to checkpoint_children (NVI hook
        // below) so nested columns append their children's persistent form recursively, validity
        // FIRST, so NULL bits survive the checkpoint.
        [[nodiscard]] core::result_wrapper_t<persistent_column_data_t>
        checkpoint(storage::partial_block_manager_t& partial_block_manager);
        // LOAD chain: rebuilds this column node from the checkpointed record. Fed by DISK bytes,
        // so a malformed shape is a data_corruption error_t, never an assert (vanishes under
        // NDEBUG) or a silent "assume all-valid" fallback.
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
        // sub-columns) to `out`, so the caller can mark them free once this collection is replaced
        // by a compacted one. Mirrors the checkpoint_children/initialize_column recursion, not
        // transition_to_disk's: a reloaded child sits on real disk blocks even though write-through
        // never descends into nested children, so every subclass with sub-columns must override
        // this on top of the base walk. Without it, compact durably orphans a nested column's
        // children's blocks (test_nested_compact_reclaim.cpp).
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

        // `state` lets allow_updates == false over a column that HAS updates report on
        // state.scan_error, the same channel row_group_t aggregates every other scan failure into.
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
        // NVI hook of checkpoint(): appends each sub-column's persistent form to
        // `persistent.child_columns`, in the order initialize_column consumes them. v1
        // convention: child_columns[0] is always VALIDITY. Without it, reload manufactured an
        // all-valid bitmap and every checkpointed NULL was lost. Default: no children.
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