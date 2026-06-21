#pragma once

#include "node.hpp"

#include <components/vector/data_chunk.hpp>

namespace components::logical_plan {

    using data_t = components::vector::data_chunk_t;
    using chunks_vector_t = std::pmr::vector<components::vector::data_chunk_t>;

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

    private:
        chunks_vector_t chunks_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_data_ptr = boost::intrusive_ptr<node_data_t>;

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk);

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const components::vector::data_chunk_t& chunk);

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource, chunks_vector_t&& chunks);

} // namespace components::logical_plan
