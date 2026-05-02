#include "operator_data.hpp"

#include <cassert>
#include <components/vector/vector_operations.hpp>

namespace components::operators {

    namespace {
        vector::data_chunk_t merge_chunks(std::pmr::memory_resource* resource,
                                          std::vector<vector::data_chunk_t>&& chunks) {
            if (chunks.empty()) {
                std::pmr::vector<types::complex_logical_type> empty_types(resource);
                return vector::data_chunk_t(resource, empty_types, 0);
            }
            if (chunks.size() == 1) {
                return std::move(chunks.front());
            }
            std::size_t total = 0;
            for (const auto& c : chunks) {
                total += c.size();
            }
            auto types = chunks.front().types();
            vector::data_chunk_t combined(resource, types, total == 0 ? 1 : total);
            uint64_t offset = 0;
            for (auto& c : chunks) {
                if (c.size() == 0) {
                    continue;
                }
                for (uint64_t col = 0; col < c.column_count(); ++col) {
                    vector::vector_ops::copy(c.data[col], combined.data[col], c.size(), 0, offset);
                }
                vector::vector_ops::copy(c.row_ids, combined.row_ids, c.size(), 0, offset);
                offset += c.size();
            }
            combined.set_cardinality(total);
            return combined;
        }
    } // namespace

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource,
                                     const std::pmr::vector<types::complex_logical_type>& types,
                                     uint64_t capacity)
        : resource_(resource) {
        chunks_.emplace_back(resource, types, capacity);
    }

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk)
        : resource_(resource) {
        chunks_.emplace_back(std::move(chunk));
    }

    operator_data_t::operator_data_t(std::pmr::memory_resource* resource, std::vector<vector::data_chunk_t>&& chunks)
        : resource_(resource)
        , chunks_(std::move(chunks)) {}

    operator_data_t::ptr operator_data_t::copy() const {
        std::vector<vector::data_chunk_t> new_chunks;
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

    void operator_data_t::append_chunk(vector::data_chunk_t&& chunk) {
        chunks_.emplace_back(std::move(chunk));
    }

    vector::data_chunk_t& operator_data_t::data_chunk() {
        if (chunks_.size() != 1) {
            auto combined = merge_chunks(resource_, std::move(chunks_));
            chunks_.clear();
            chunks_.emplace_back(std::move(combined));
        }
        return chunks_.front();
    }

    const vector::data_chunk_t& operator_data_t::data_chunk() const {
        return const_cast<operator_data_t*>(this)->data_chunk();
    }

    std::pmr::memory_resource* operator_data_t::resource() const { return resource_; }

    operator_data_ptr split_large_output(std::pmr::memory_resource* resource, operator_data_ptr data) {
        if (!data || data->chunk_count() != 1) {
            return data;
        }
        if (data->size() <= vector::DEFAULT_VECTOR_CAPACITY) {
            return data;
        }
        auto chunks = split_chunk_into_batches(resource, std::move(data->data_chunk()));
        return make_operator_data(resource, std::move(chunks));
    }

    std::vector<vector::data_chunk_t> split_chunk_into_batches(std::pmr::memory_resource* resource,
                                                               vector::data_chunk_t&& chunk) {
        std::vector<vector::data_chunk_t> out;
        const auto total = chunk.size();
        if (total == 0) {
            out.emplace_back(std::move(chunk));
            return out;
        }
        if (total <= vector::DEFAULT_VECTOR_CAPACITY) {
            out.emplace_back(std::move(chunk));
            return out;
        }
        const auto batch_size = vector::DEFAULT_VECTOR_CAPACITY;
        for (uint64_t offset = 0; offset < total; offset += batch_size) {
            auto count = std::min<uint64_t>(batch_size, total - offset);
            out.emplace_back(chunk.partial_copy(resource, offset, count));
        }
        return out;
    }

} // namespace components::operators
