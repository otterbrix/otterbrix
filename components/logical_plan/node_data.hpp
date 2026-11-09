#pragma once

#include "node.hpp"

#include <components/casts/cast_function.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::logical_plan {

    using data_t = components::vector::data_chunk_t;
    using chunks_vector_t = std::pmr::vector<components::vector::data_chunk_t>;

    // A VALUES cell whose type differs from the one its chunk column took from the first value. A chunk column holds
    // one type, so the chunk keeps a NULL there and the written value waits here until validation picks the column's
    // type: the INSERT target column's (PostgreSQL transformInsertRow), or the common type of the column when the
    // VALUES list is read as rows (transformValuesClause). `row` counts across the whole chunk batch; `column` is
    // the position in the chunk.
    struct deferred_cell_t {
        uint64_t row{0};
        uint64_t column{0};
        vector::vector_t value; // one row
        casts::cast_t cast;     // stamped by validate: value's type to the column's, empty when they are equal
    };

    using deferred_cells_t = std::pmr::vector<deferred_cell_t>;

    class node_data_t final : public node_t {
    public:
        explicit node_data_t(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk);

        explicit node_data_t(std::pmr::memory_resource* resource, const components::vector::data_chunk_t& chunk);

        explicit node_data_t(std::pmr::memory_resource* resource, chunks_vector_t&& chunks);

        // The raw data as a batch of ≤DEFAULT_VECTOR_CAPACITY chunks (all share the same
        // column shape). Always holds at least one (possibly empty) chunk.
        chunks_vector_t& chunks();
        const chunks_vector_t& chunks() const;

        // The first chunk. Its column shape/types are shared by every chunk, so this is the
        // canonical source for column metadata. For a single-chunk node it is the whole data.
        // Row-spanning access or whole-table mutation must go through chunks().
        components::vector::data_chunk_t& data_chunk();
        const components::vector::data_chunk_t& data_chunk() const;

        // Total rows across all chunks.
        size_t size() const;

        void set_deferred_cells(deferred_cells_t cells) { deferred_cells_ = std::move(cells); }
        const deferred_cells_t& deferred_cells() const { return deferred_cells_; }
        deferred_cells_t& deferred_cells() { return deferred_cells_; }

        [[nodiscard]] core::error_t settle_column(uint64_t column,
                                                  const types::complex_logical_type& type,
                                                  const casts::cast_t& rows_cast,
                                                  const graph_execution_context& context);

    private:
        chunks_vector_t chunks_;
        deferred_cells_t deferred_cells_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_data_ptr = boost::intrusive_ptr<node_data_t>;

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk);

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const components::vector::data_chunk_t& chunk);

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource, chunks_vector_t&& chunks);

} // namespace components::logical_plan
