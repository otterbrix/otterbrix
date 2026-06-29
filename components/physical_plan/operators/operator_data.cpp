#include "operator_data.hpp"

#include <cassert>

namespace components::operators {

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource,
                                     const std::pmr::vector<types::complex_logical_type>& types,
                                     uint64_t capacity)
        : resource_(resource)
        , chunks_(resource) {
        chunks_.emplace_back(resource, types, capacity);
    }

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk)
        : resource_(resource)
        , chunks_(resource) {
        chunks_.emplace_back(std::move(chunk));
    }

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource, chunks_vector_t&& chunks)
        : resource_(resource)
        , chunks_(std::move(chunks), resource) {}

    operator_data_t::ptr operator_data_t::copy() const {
        chunks_vector_t new_chunks(resource_);
        new_chunks.reserve(chunks_.size());
        for (const auto& chunk : chunks_) {
            vector::data_chunk_t dst{resource_, chunk.types(), chunk.size()};
            chunk.copy(dst, 0);
            new_chunks.emplace_back(std::move(dst));
        }
        return {new operator_data_t(resource_, std::move(new_chunks))};
    }

    std::size_t operator_data_t::size() const {
        std::size_t total = 0;
        for (const auto& c : chunks_) {
            total += c.size();
        }
        return total;
    }

    void operator_data_t::append_chunk(vector::data_chunk_t&& chunk) { chunks_.emplace_back(std::move(chunk)); }

    std::pmr::memory_resource* operator_data_t::resource() const { return resource_; }

} // namespace components::operators
