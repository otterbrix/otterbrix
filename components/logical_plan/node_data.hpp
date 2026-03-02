#pragma once

#include "node.hpp"

#include <components/vector/data_chunk.hpp>

namespace components::logical_plan {

    using data_t = components::vector::data_chunk_t;

    class node_data_t final : public node_t {
    public:
        explicit node_data_t(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk);

        explicit node_data_t(std::pmr::memory_resource* resource, const components::vector::data_chunk_t& chunk);

        components::vector::data_chunk_t& data_chunk();
        const components::vector::data_chunk_t& data_chunk() const;

        size_t size() const;

    private:
        data_t data_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_data_ptr = boost::intrusive_ptr<node_data_t>;

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk);

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const components::vector::data_chunk_t& chunk);

} // namespace components::logical_plan
