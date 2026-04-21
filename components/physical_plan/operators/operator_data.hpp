#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <memory_resource>
#include <vector>
#include <vector/data_chunk.hpp>

namespace components::operators {

    class operator_data_t : public boost::intrusive_ref_counter<operator_data_t> {
    public:
        using ptr = boost::intrusive_ptr<operator_data_t>;

        operator_data_t(std::pmr::memory_resource* resource,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        uint64_t capacity = vector::DEFAULT_VECTOR_CAPACITY);
        operator_data_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);
        operator_data_t(std::pmr::memory_resource* resource, std::vector<vector::data_chunk_t>&& chunks);

        ptr copy() const;

        // Total rows across all chunks.
        std::size_t size() const;
        std::size_t chunk_count() const { return chunks_.size(); }

        // Multi-chunk API. Each chunk in the vector must contain ≤ DEFAULT_VECTOR_CAPACITY rows.
        std::vector<vector::data_chunk_t>& chunks() { return chunks_; }
        const std::vector<vector::data_chunk_t>& chunks() const { return chunks_; }
        void append_chunk(vector::data_chunk_t&& chunk);

        // Backward-compat single-chunk API. Lazily concatenates all chunks into one
        // the first time it is called; subsequent calls are O(1). Callers that still
        // rely on a single-chunk view go through this accessor.
        vector::data_chunk_t& data_chunk();
        const vector::data_chunk_t& data_chunk() const;

        std::pmr::memory_resource* resource() const;

    private:
        std::pmr::memory_resource* resource_;
        std::vector<vector::data_chunk_t> chunks_;
    };

    using operator_data_ptr = operator_data_t::ptr;

    // Splits a data_chunk_t into ≤DEFAULT_VECTOR_CAPACITY-sized chunks. Input is consumed.
    std::vector<vector::data_chunk_t> split_chunk_into_batches(std::pmr::memory_resource* resource,
                                                               vector::data_chunk_t&& chunk);

    // Splits an operator_data_t whose single chunk exceeds DEFAULT_VECTOR_CAPACITY into
    // multiple ≤DEFAULT_VECTOR_CAPACITY chunks. Returns the input unchanged otherwise.
    boost::intrusive_ptr<operator_data_t> split_large_output(std::pmr::memory_resource* resource,
                                                             boost::intrusive_ptr<operator_data_t> data);

    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource,
                                                const std::pmr::vector<types::complex_logical_type>& types,
                                                uint64_t capacity = vector::DEFAULT_VECTOR_CAPACITY) {
        return {new operator_data_t(resource, types, capacity)};
    }

    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk) {
        return {new operator_data_t(resource, std::move(chunk))};
    }

    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource,
                                                std::vector<vector::data_chunk_t>&& chunks) {
        return {new operator_data_t(resource, std::move(chunks))};
    }

} // namespace components::operators
