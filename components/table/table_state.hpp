#pragma once

#include <atomic>
#include <components/vector/data_chunk.hpp>
#include <random>

#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector.hpp>

#include "column_data.hpp"
#include "column_state.hpp"
#include "row_version_manager.hpp"

namespace components::vector {
    class data_chunk_t;
}

namespace components::table {
    template<class T>
    class segment_tree_t;
    class collection_t;
    class data_table_t;
    class table_scan_state;

    enum class table_scan_type : uint8_t
    {
        REGULAR = 0,
        COMMITTED_ROWS = 1,
        LATEST_COMMITTED_ROWS = 4
    };

    class collection_scan_state {
    public:
        explicit collection_scan_state(std::pmr::memory_resource* resource, table_scan_state& parent);

        row_group_t* row_group;
        uint64_t vector_index;
        int64_t max_row_group_row;
        std::vector<column_scan_state> column_scans;
        segment_tree_t<row_group_t>* row_groups;
        int64_t max_row;
        uint64_t batch_index;
        vector::indexing_vector_t valid_indexing;
        transaction_data txn{0, 0};

        // Aggregated buffer-pool OOM raised during the scan. row_group_t copies each column's
        // column_scan_state::scan_error here; the scan loops stop on it. data_table_t::scan /
        // scan_batched keep their void shape and LEAVE the error here for the caller to read
        // via has_error().
        core::error_t scan_error{core::error_t::no_error()};
        bool has_error() const { return scan_error.contains_error(); }

        std::random_device random;

        void initialize(const std::pmr::vector<types::complex_logical_type>& types);
        const std::vector<storage_index_t>& column_ids();
        const table_filter_t* filter();
        bool scan(vector::data_chunk_t& result);
        // Batched scan: emit one data_chunk_t per ≤DEFAULT_VECTOR_CAPACITY rows directly,
        // skipping the accumulate-then-split round-trip. `projected_cols` is a pointer so
        // callers can pass nullptr for full-schema chunks or a non-null vector to use the
        // projected (sparse) chunk constructor.
        void scan_batched(const std::pmr::vector<types::complex_logical_type>& types,
                          const std::vector<size_t>* projected_cols,
                          std::pmr::vector<vector::data_chunk_t>& batches,
                          std::pmr::memory_resource* resource);
        // Single-batch iterator: fills ONE ≤DEFAULT_VECTOR_CAPACITY batch into `result` (one
        // scan_batched iteration), advancing the cursor. Returns true if a non-empty batch was
        // produced, false when the scan is drained (`result` left empty). Used by the fetch-next
        // streaming source so the scan position persists across mailbox round-trips without
        // materializing the whole table (unlike scan(), which drains everything into one chunk).
        bool next_batch(vector::data_chunk_t& result);
        bool scan_committed(vector::data_chunk_t& result, table_scan_type type);

        [[nodiscard]] const std::vector<uint64_t>& visible_to_physical() const noexcept;
        [[nodiscard]] uint64_t physical_column(uint64_t visible) const noexcept;
        [[nodiscard]] uint64_t result_width() const noexcept { return result_width_; }

    private:
        table_scan_state& parent_;
        uint64_t result_width_ = 0;
    };

    class table_scan_state {
    public:
        table_scan_state(std::pmr::memory_resource* resource);
        virtual ~table_scan_state() = default;

        collection_scan_state table_state;
        collection_scan_state local_state;
        const table_filter_t* filter = nullptr;

        void initialize(std::vector<storage_index_t> column_ids, const table_filter_t* table_filter_tree = nullptr);

        const std::vector<storage_index_t>& column_ids();

        [[nodiscard]] const std::vector<uint64_t>& visible_to_physical() const noexcept { return visible_to_physical_; }
        void set_visible_to_physical(std::vector<uint64_t> map) { visible_to_physical_ = std::move(map); }

    private:
        std::vector<storage_index_t> column_ids_;
        std::vector<uint64_t> visible_to_physical_;
    };

    class create_index_scan_state : public table_scan_state {
    public:
        create_index_scan_state(std::pmr::memory_resource* resource)
            : table_scan_state(resource) {}
    };

    struct table_append_state {
        table_append_state(std::pmr::memory_resource* resource)
            : append_state(*this)
            , total_append_count(0)
            , start_row_group(nullptr)
            , hashes(resource, types::logical_type::UBIGINT)
            , cut(resource)
            , piece_cut(resource) {}
        ~table_append_state() = default;

        row_group_append_state append_state;
        // Sequencing token, not a lock: data_table_t::append_lock() sets it and
        // initialize_append refuses to run without it. No mutex needed (one disk agent owns
        // the table, see data_table.hpp), but the ordering guarantee a lock gave is still required.
        bool append_locked{false};
        int64_t row_start;
        int64_t current_row;
        uint64_t total_append_count;
        row_group_t* start_row_group;
        vector::vector_t hashes;
        // The counts of the row group holding row_start, before the session's first row: what
        // collection_t keeps for the transaction's revert of this session.
        append_cut_t cut;
        // The counts of the current row group before the chunk being appended: what its own unwind
        // of a refused chunk cuts back to.
        append_cut_t piece_cut;
    };

    class storage_commit_state {
    public:
        virtual ~storage_commit_state() = default;

        virtual void revert_commit() = 0;
        virtual void flush_commit() = 0;

        virtual void add_row_group_data(data_table_t& table, uint64_t start_index, uint64_t count) = 0;
        virtual bool has_row_group_data() { return false; }
    };

} // namespace components::table