#pragma once

#include <atomic>
#include <components/vector/data_chunk.hpp>
#include <random>

#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector.hpp>

#include "column_data.hpp"
#include "column_state.hpp"

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
        COMMITTED_ROWS_OMIT_PERMANENTLY_DELETED = 3,
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

    class table_filter_set_t {
    public:
        std::unordered_map<uint64_t, std::unique_ptr<table_filter_t>> filters;

        void push_filter(const column_index_t& col_idx, std::unique_ptr<table_filter_t> filter) {
            filters.emplace(col_idx.primary_index(), std::move(filter));
        }

        bool equals(table_filter_set_t& other) {
            if (filters.size() != other.filters.size()) {
                return false;
            }
            for (auto& entry : filters) {
                auto other_entry = other.filters.find(entry.first);
                if (other_entry == other.filters.end()) {
                    return false;
                }
                if (!entry.second->equals(*other_entry->second)) {
                    return false;
                }
            }
            return true;
        }
        static bool equals(table_filter_set_t* left, table_filter_set_t* right) {
            if (left == right) {
                return true;
            }
            if (!left || !right) {
                return false;
            }
            return left->equals(*right);
        }
    };

    struct scan_filter_t {
        scan_filter_t(uint64_t index, const std::vector<storage_index_t>& column_ids, table_filter_t& filter);

        uint64_t scan_column_index;
        uint64_t table_column_index;
        table_filter_t& filter;
        bool always_true;
    };

    using adaptive_filter_state = std::chrono::time_point<std::chrono::high_resolution_clock>;

    class adaptive_filter_t {
    public:
        explicit adaptive_filter_t(const table_filter_set_t& table_filters);

        std::vector<uint64_t> permutation;

        void adapt_runtime_statistics(double duration);

        adaptive_filter_state begin_filter() const;
        void end_filter(adaptive_filter_state state);

    private:
        bool disable_permutations_ = false;

        uint64_t iteration_count_ = 0;
        uint64_t swap_idx_ = 0;
        uint64_t right_random_border_ = 0;
        uint64_t observe_interval_ = 0;
        uint64_t execute_interval_ = 0;
        double runtime_sum_ = 0;
        double prev_mean_ = 0;
        bool observe_ = false;
        bool warmup_ = false;
        std::vector<uint64_t> swap_likeliness_;
    };

    class scan_filter_info {
    public:
        void initialize(table_filter_set_t& filters, const std::vector<storage_index_t>& column_ids);

        const std::vector<scan_filter_t>& filter_list() const { return filter_list_; }

        adaptive_filter_t* adaptive_filter();
        adaptive_filter_state begin_filter() const;
        void end_filter(adaptive_filter_state state);

        bool has_filters() const;

        bool column_has_filters(uint64_t col_idx);

        void check_all_filters();
        void set_filter_always_true(uint64_t filter_idx);

    private:
        table_filter_set_t* table_filters_ = nullptr;
        std::unique_ptr<adaptive_filter_t> adaptive_filter_;
        std::vector<scan_filter_t> filter_list_;
        std::vector<bool> column_has_filter_;
        std::vector<bool> base_column_has_filter_;
        uint64_t always_true_filters_ = 0;
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

        std::random_device random;

        void initialize(const std::pmr::vector<types::complex_logical_type>& types);
        const std::vector<storage_index_t>& column_ids();
        const table_filter_t* filter();
        bool scan(vector::data_chunk_t& result);
        bool scan_committed(vector::data_chunk_t& result, table_scan_type type);
        bool scan_committed(vector::data_chunk_t& result, std::unique_lock<std::mutex>& l, table_scan_type type);

    private:
        table_scan_state& parent_;
    };

    class table_scan_state {
    public:
        table_scan_state(std::pmr::memory_resource* resource);
        virtual ~table_scan_state() = default;

        collection_scan_state table_state;
        collection_scan_state local_state;
        bool force_fetch_row = false;
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

        std::vector<std::unique_ptr<std::lock_guard<std::mutex>>> locks;
        std::unique_lock<std::mutex> append_lock;
        std::unique_lock<std::mutex> segment_lock;
    };

    struct parallel_table_scan_state_t {
        parallel_table_scan_state_t(std::vector<storage_index_t> col_ids,
                                     const table_filter_t* flt,
                                     uint64_t total_rg)
            : column_ids(std::move(col_ids))
            , filter(flt)
            , total_row_groups(total_rg) {}

        std::vector<storage_index_t> column_ids;
        const table_filter_t* filter;
        uint64_t total_row_groups;
        std::atomic<uint64_t> next_row_group_idx{0};
    };

    struct table_append_state {
        table_append_state(std::pmr::memory_resource* resource)
            : append_state(*this)
            , total_append_count(0)
            , start_row_group(nullptr)
            , hashes(resource, types::logical_type::UBIGINT) {}
        ~table_append_state() = default;

        row_group_append_state append_state;
        std::unique_lock<std::mutex> append_lock;
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