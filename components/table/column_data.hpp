#pragma once

#include "base_statistics.hpp"
#include "column_segment.hpp"
#include "column_state.hpp"
#include "segment_tree.hpp"
#include "update_segment.hpp"

namespace components::table {

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
    constexpr uint64_t MAX_ROW_ID = 1ULL << 55; // 2^55

    class column_data_t {
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

        virtual void scan_committed_range(uint64_t row_group_start,
                                          uint64_t offset_in_row_group,
                                          uint64_t count,
                                          vector::vector_t& result);
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
        virtual void filter(uint64_t vector_index,
                            column_scan_state& state,
                            vector::vector_t& result,
                            vector::indexing_vector_t& indexing,
                            uint64_t& count,
                            const table_filter_t& filter);
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
        virtual void revert_append(int64_t start_row);

        // `error` carries an out_of_memory error_t when a pin fails during the predicate check;
        // on error the bool return is meaningless and the scan loop stops.
        virtual bool check_predicate(int64_t row_id, const table_filter_t* filter, core::error_t& error);
        virtual bool check_validity(int64_t row_id);
        virtual uint64_t fetch(column_scan_state& state, int64_t row_id, vector::vector_t& result);
        virtual void
        fetch_row(column_fetch_state& state, int64_t row_id, vector::vector_t& result, uint64_t result_idx);

        // Update path returns write_conflict / out_of_memory; true on success.
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
        // the persistent data on success.
        [[nodiscard]] core::result_wrapper_t<persistent_column_data_t>
        checkpoint(storage::partial_block_manager_t& partial_block_manager);
        virtual void initialize_column(const persistent_column_data_t& persistent_data);
        void initialize_column_validity(const persistent_column_data_t& persistent_data);

        // Write-through: re-point every COMPLETE managed (in-memory, non-reloadable) segment of this column
        // to a disk-backed segment so the pool can evict+reload them (bounded memory). Called when a row
        // group is closed (all its column segments are final). A no-op for in-memory tables and for
        // non-fixed-size / compressed segments. Returns io_error/out_of_memory on failure; true on success.
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
        // compacted one. Mirrors the transition_to_disk recursion: the standard subclass also collects from
        // its validity child; struct/list/array collect their own data_ blocks only (their children's
        // payloads stay managed, matching base transition_to_disk).
        virtual void collect_disk_block_ids(std::pmr::vector<uint64_t>& out) const;

    protected:
        // Returns out_of_memory when the new segment's transient memory cannot be registered; true on success.
        [[nodiscard]] core::result_wrapper_t<bool> apend_transient_segment(std::unique_lock<std::mutex>& l,
                                                                           int64_t start_row);

        // Write-through: a just-FILLED transient (managed, block_id >= MAXIMUM_BLOCK) segment at
        // `segment_index` in data_ is written to the table's data file and re-pointed to a fresh disk-backed
        // segment (block_id < MAXIMUM_BLOCK -> is_reloadable()==true), so the pool can evict+reload it ->
        // bounded memory. A no-op for in-memory tables (no backing store) and for non-fixed-size / compressed
        // segments (a raw block copy would not round-trip losslessly). Returns io_error/out_of_memory on a
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

        void clear_updates();
        void fetch_updates(uint64_t vector_index,
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
        uint64_t column_index_;
        types::complex_logical_type type_;
        column_data_t* parent_;
        segment_tree_t<column_segment_t> data_;
        mutable std::mutex update_lock_;
        std::unique_ptr<update_segment_t> updates_;
        uint64_t allocation_size_;
        base_statistics_t statistics_;

        std::pmr::memory_resource* resource_;
    };

} // namespace components::table