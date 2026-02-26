#include "node_data.hpp"

#include <sstream>

namespace components::logical_plan {

    node_data_t::node_data_t(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk)
        : node_t(resource, node_type::data_t, {})
        , data_(std::move(chunk)) {}

    node_data_t::node_data_t(std::pmr::memory_resource* resource, const components::vector::data_chunk_t& chunk)
        : node_t(resource, node_type::data_t, {})
        , data_(vector::data_chunk_t(resource, chunk.types(), chunk.size())) {
        chunk.copy(data_, 0);
    }

    components::vector::data_chunk_t& node_data_t::data_chunk() { return data_; }

    const components::vector::data_chunk_t& node_data_t::data_chunk() const { return data_; }

    size_t node_data_t::size() const { return data_.size(); }

    hash_t node_data_t::hash_impl() const { return 0; }

    std::string node_data_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$raw_data: {";
        stream << "$rows: " << size();
        stream << "}";
        return stream.str();
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk) {
        return {new node_data_t{resource, std::move(chunk)}};
    }

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource,
                                     const components::vector::data_chunk_t& chunk) {
        return {new node_data_t{resource, chunk}};
    }

} // namespace components::logical_plan
