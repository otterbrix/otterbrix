#include "node_data.hpp"

#include <sstream>

namespace components::logical_plan {

    node_data_t::node_data_t(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk)
        : node_t(resource, node_type::data_t)
        , chunks_(resource) {
        chunks_.emplace_back(std::move(chunk));
    }

    node_data_t::node_data_t(std::pmr::memory_resource* resource, const components::vector::data_chunk_t& chunk)
        : node_t(resource, node_type::data_t)
        , chunks_(resource) {
        vector::data_chunk_t copy(resource, chunk.types(), chunk.size() == 0 ? 1 : chunk.size());
        chunk.copy(copy, 0);
        chunks_.emplace_back(std::move(copy));
    }

    node_data_t::node_data_t(std::pmr::memory_resource* resource, chunks_vector_t&& chunks)
        : node_t(resource, node_type::data_t)
        , chunks_(std::move(chunks)) {
        // Keep at least one (possibly empty) chunk so data_chunk()/column metadata always
        // have a valid front to return.
        if (chunks_.empty()) {
            chunks_.emplace_back(resource, std::pmr::vector<components::types::complex_logical_type>{resource});
        }
    }

    chunks_vector_t& node_data_t::chunks() { return chunks_; }

    const chunks_vector_t& node_data_t::chunks() const { return chunks_; }

    components::vector::data_chunk_t& node_data_t::data_chunk() { return chunks_.front(); }

    const components::vector::data_chunk_t& node_data_t::data_chunk() const { return chunks_.front(); }

    size_t node_data_t::size() const {
        size_t total = 0;
        for (const auto& chunk : chunks_) {
            total += chunk.size();
        }
        return total;
    }

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

    node_data_ptr make_node_raw_data(std::pmr::memory_resource* resource, chunks_vector_t&& chunks) {
        return {new node_data_t{resource, std::move(chunks)}};
    }

} // namespace components::logical_plan
