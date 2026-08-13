#pragma once
#include <components/types/tri_bool.hpp>
#include <components/types/types.hpp>
#include <core/date/date_types.hpp>
#include <core/operations_helper.hpp>
#include <core/result_wrapper.hpp>
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include <vector>

#include <components/table/storage/buffer_handle.hpp>

#include <components/execution_context/graph_execution_context.hpp>
#include <components/execution_graph/execution_graph.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/forward.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/parameter_map.hpp>

namespace components::vector {
    class data_chunk_t;
} // namespace components::vector

namespace components::table {
    class row_group_t;
    struct table_append_state;
    struct uncompressed_string_segment_state;

    class column_data_t;

    namespace storage {
        class block_manager_t;
        class buffer_handle_t;
        class block_handle_t;
        struct block_pointer_t;
    } // namespace storage

    class column_segment_t;
    struct column_segment_state;

    struct storage_index_t {
        storage_index_t()
            : index_(storage::INVALID_INDEX) {}
        explicit storage_index_t(uint64_t index)
            : index_(index) {}
        storage_index_t(uint64_t index, std::vector<storage_index_t> child_indexes)
            : index_(index)
            , child_indexes_(std::move(child_indexes)) {}

        bool operator==(const storage_index_t& rhs) const { return index_ == rhs.index_; }
        bool operator!=(const storage_index_t& rhs) const { return index_ != rhs.index_; }
        bool operator<(const storage_index_t& rhs) const { return index_ < rhs.index_; }
        uint64_t primary_index() const { return index_; }
        bool has_children() const { return !child_indexes_.empty(); }
        uint64_t child_index_count() const { return child_indexes_.size(); }
        const storage_index_t& child_index(uint64_t idx) const { return child_indexes_[idx]; }
        storage_index_t& child_index(uint64_t idx) { return child_indexes_[idx]; }
        const std::vector<storage_index_t>& child_indexes() const { return child_indexes_; }
        void add_child_index(storage_index_t new_index) { child_indexes_.push_back(std::move(new_index)); }
        void set_index(uint64_t new_index) { index_ = new_index; }
        bool is_row_id_column() const { return index_ == storage::INVALID_INDEX; }

    private:
        uint64_t index_;
        std::vector<storage_index_t> child_indexes_;
    };

    class table_filter_t {
    public:
        table_filter_t(types::parameter_map_t parameters,
                       graph_execution_context context,
                       std::unique_ptr<execution_graph::execution_graph_t> graph,
                       expressions::condition_kind condition)
            : parameters(std::move(parameters))
            , context(context)
            , graph(std::move(graph))
            , condition(condition) {}

        types::parameter_map_t parameters;
        graph_execution_context context;
        std::unique_ptr<execution_graph::execution_graph_t> graph;
        expressions::condition_kind condition{expressions::condition_kind::always};
    };

    struct column_segment_state {
        virtual ~column_segment_state() = default;

        template<typename TARGET>
        TARGET& cast() {
            return reinterpret_cast<TARGET&>(*this);
        }
        template<typename TARGET>
        const TARGET& cast() const {
            return reinterpret_cast<const TARGET&>(*this);
        }

        std::vector<uint32_t> blocks;
    };

    struct column_append_state {
        column_segment_t* current;
        std::vector<column_append_state> child_appends;
        std::unique_ptr<std::unique_lock<std::mutex>> lock;
        std::unique_ptr<storage::buffer_handle_t> handle;
    };

    struct row_group_append_state {
        explicit row_group_append_state(table_append_state& parent)
            : parent(parent) {}

        table_append_state& parent;
        row_group_t* row_group = nullptr;
        std::unique_ptr<column_append_state[]> states;
        uint64_t offset_in_row_group = 0;
    };
    struct column_scan_state {
        column_segment_t* current = nullptr;
        int64_t row_index = 0;
        int64_t internal_index = 0;
        std::unique_ptr<storage::buffer_handle_t> scan_state;
        std::vector<column_scan_state> child_states;
        bool initialized = false;
        bool segment_checked = false;
        std::vector<std::unique_ptr<storage::buffer_handle_t>> previous_states;
        uint64_t last_offset = 0;
        uint64_t result_offset = 0;
        std::vector<bool> scan_child_column;
        // OOM (buffer-pool exhaustion) raised by a pin() while scanning this column.
        // Leaf segment helpers set it; column_data_t::scan_vector bails on it and
        // row_group_t aggregates it into collection_scan_state::scan_error. Success
        // path leaves it as no_error().
        core::error_t scan_error{core::error_t::no_error()};
        bool has_error() const { return scan_error.contains_error(); }

        void initialize(const types::complex_logical_type& type, const std::vector<storage_index_t>& children);
        void initialize(const types::complex_logical_type& type);
        void next(uint64_t count);
    };

    struct column_fetch_state {
        std::unordered_map<uint64_t, storage::buffer_handle_t> handles;
        std::vector<std::unique_ptr<column_fetch_state>> child_states;
        // OOM raised by the pin() inside get_or_insert_handle(); callers that route
        // through a column_scan_state copy it into scan_error.
        core::error_t fetch_error{core::error_t::no_error()};

        // Returns nullptr and sets fetch_error on buffer-pool exhaustion.
        storage::buffer_handle_t* get_or_insert_handle(column_segment_t& segment);
    };

    struct string_block_t {
        std::shared_ptr<storage::block_handle_t> block;
        uint64_t offset;
        uint64_t size;
        std::unique_ptr<string_block_t> next;
    };

    struct compressed_segment_state {
        virtual ~compressed_segment_state() {}

        virtual std::string segment_info() const { return ""; }

        virtual std::vector<uint32_t> additional_blocks() const { return std::vector<uint32_t>(); }
        template<typename TARGET>
        TARGET& cast() {
            return reinterpret_cast<TARGET&>(*this);
        }
        template<typename TARGET>
        const TARGET& cast() const {
            return reinterpret_cast<const TARGET&>(*this);
        }
    };

    struct uncompressed_string_segment_state : public compressed_segment_state {
        ~uncompressed_string_segment_state() override;

        std::unique_ptr<string_block_t> head;
        std::unordered_map<uint32_t, string_block_t*> overflow_blocks;
        std::vector<uint32_t> on_disk_blocks;

        std::shared_ptr<storage::block_handle_t> handle(storage::block_manager_t& manager, uint32_t block_id);

        void register_block(storage::block_manager_t& manager, uint32_t block_id);

    private:
        std::mutex block_lock_;
        std::unordered_map<uint32_t, std::shared_ptr<storage::block_handle_t>> handles_;
    };

    struct column_segment_info {
        uint64_t row_group_index;
        uint64_t column_id;
        std::string column_path;
        uint64_t segment_idx;
        std::string segment_type;
        int64_t segment_start;
        uint64_t segment_count;
        bool has_updates;
        uint32_t block_id;
        std::vector<uint32_t> additional_blocks;
        uint64_t block_offset;
        std::string segment_info;
    };

} // namespace components::table