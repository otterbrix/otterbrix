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
    class row_group_segment_tree_t;
    class collection_t;
    class data_table_t;
    class table_scan_state;

    enum class table_scan_type : uint8_t
    {
        REGULAR = 0,
        COMMITTED_ROWS = 1,
        COMMITTED_ROWS_DISALLOW_UPDATES = 2,
        LATEST_COMMITTED_ROWS = 4
    };

    struct column_index_t {
        column_index_t()
            : index_(storage::INVALID_INDEX) {}
        explicit column_index_t(uint64_t index)
            : index_(index) {}
        column_index_t(uint64_t index, std::vector<column_index_t> child_indexes)
            : index_(index)
            , child_indexes_(std::move(child_indexes)) {}

        bool operator==(const column_index_t& rhs) const { return index_ == rhs.index_; }
        bool operator!=(const column_index_t& rhs) const { return index_ != rhs.index_; }
        bool operator<(const column_index_t& rhs) const { return index_ < rhs.index_; }
        uint64_t primary_index() const { return index_; }
        bool has_children() const { return !child_indexes_.empty(); }
        uint64_t child_index_count() const { return child_indexes_.size(); }
        const column_index_t& child_index(uint64_t idx) const { return child_indexes_[idx]; }
        column_index_t& child_index(uint64_t idx) { return child_indexes_[idx]; }
        const std::vector<column_index_t>& child_indexes() const { return child_indexes_; }
        std::vector<column_index_t>& child_indexes() { return child_indexes_; }
        void add_child_index(column_index_t new_index) { child_indexes_.push_back(std::move(new_index)); }
        bool is_row_id_column() const { return index_ == storage::INVALID_INDEX; }

    private:
        uint64_t index_;
        std::vector<column_index_t> child_indexes_;
    };

    class collection_scan_state {
    public:
        explicit collection_scan_state(std::pmr::memory_resource* resource, table_scan_state& parent);

        row_group_t* row_group;
        uint64_t vector_index;
        int64_t max_row_group_row;
        std::vector<column_scan_state> column_scans;
        row_group_segment_tree_t* row_groups;
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

    private:
        table_scan_state& parent_;
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

    private:
        std::vector<storage_index_t> column_ids_;
    };

    class create_index_scan_state : public table_scan_state {
    public:
        create_index_scan_state(std::pmr::memory_resource* resource)
            : table_scan_state(resource) {}

        // Segment-tree lock, NOT one of the per-table locks removed with the single-owner
        // proof: it belongs to row_group_segment_tree_t and is out of that scope.
        std::unique_lock<std::mutex> segment_lock;
    };

    struct table_append_state {
        table_append_state(std::pmr::memory_resource* resource)
            : append_state(*this)
            , total_append_count(0)
            , start_row_group(nullptr)
            , hashes(resource, types::logical_type::UBIGINT) {}
        ~table_append_state() = default;

        row_group_append_state append_state;
        // Sequencing token, not a lock: data_table_t::append_lock() sets it and
        // initialize_append refuses to run without it. It used to be a held mutex; the mutex
        // is gone (a table is reachable from exactly one disk agent, see data_table.hpp), the
        // ordering guarantee it also carried is not.
        bool append_locked{false};
        int64_t row_start;
        int64_t current_row;
        uint64_t total_append_count;
        row_group_t* start_row_group;
        vector::vector_t hashes;
    };

    class storage_commit_state {
    public:
        virtual ~storage_commit_state() = default;

        virtual void revert_commit() = 0;
        virtual void flush_commit() = 0;

        virtual void add_row_group_data(data_table_t& table, uint64_t start_index, uint64_t count) = 0;
        virtual bool has_row_group_data() { return false; }
    };

    enum class constraint_type : uint8_t
    {
        INVALID = 0,
        NOT_NULL = 1,
        CHECK = 2,
        UNIQUE = 3,
        FOREIGN_KEY = 4
    };

    class bound_constraint_t {
    public:
        explicit bound_constraint_t(constraint_type type)
            : type(type){};
        virtual ~bound_constraint_t() = default;

        constraint_type type;

        template<class TARGET>
        TARGET& cast() {
            return reinterpret_cast<TARGET&>(*this);
        }

        template<class TARGET>
        const TARGET& cast() const {
            return reinterpret_cast<const TARGET&>(*this);
        }
    };

    struct constraint_state {
        explicit constraint_state(const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints)
            : bound_constraints(bound_constraints) {}

        const std::vector<std::unique_ptr<bound_constraint_t>>& bound_constraints;
    };

    struct table_delete_state {
        table_delete_state(std::pmr::memory_resource*) {}
        std::unique_ptr<constraint_state> constraint;
        bool has_delete_constraints = false;
        std::vector<storage_index_t> col_ids;
    };

    struct table_update_state {
        std::unique_ptr<constraint_state> constraint;
    };

} // namespace components::table