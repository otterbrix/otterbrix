#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <memory_resource>
#include <components/vector/data_chunk.hpp>
#include <vector>

namespace components::operators {

    using chunks_vector_t = std::pmr::vector<vector::data_chunk_t>;

    class operator_data_t : public boost::intrusive_ref_counter<operator_data_t> {
    public:
        using ptr = boost::intrusive_ptr<operator_data_t>;

        operator_data_t(std::pmr::memory_resource* resource,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        uint64_t capacity = vector::DEFAULT_VECTOR_CAPACITY);
        operator_data_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);
        operator_data_t(std::pmr::memory_resource* resource, chunks_vector_t&& chunks);

        ptr copy() const;

        // Total rows across all chunks.
        std::size_t size() const;
        std::size_t chunk_count() const { return chunks_.size(); }

        // Multi-chunk API. Each chunk in the vector must contain ≤ DEFAULT_VECTOR_CAPACITY rows.
        chunks_vector_t& chunks() { return chunks_; }
        const chunks_vector_t& chunks() const { return chunks_; }
        void append_chunk(vector::data_chunk_t&& chunk);

        // Backward-compat single-chunk API. Lazily concatenates all chunks into one
        // the first time it is called; subsequent calls are O(1). Callers that still
        // rely on a single-chunk view go through this accessor.
        vector::data_chunk_t& data_chunk();
        const vector::data_chunk_t& data_chunk() const;

        std::pmr::memory_resource* resource() const;

    private:
        std::pmr::memory_resource* resource_;
        chunks_vector_t chunks_;
    };

    using operator_data_ptr = operator_data_t::ptr;

    // Splits a data_chunk_t into ≤DEFAULT_VECTOR_CAPACITY-sized chunks. Input is consumed.
    chunks_vector_t split_chunk_into_batches(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);

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

    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource, chunks_vector_t&& chunks) {
        return {new operator_data_t(resource, std::move(chunks))};
    }

    // Builds the 1-row columnar key carrier used to call manager_disk_t::read_chunks_by_key
    // (and mirrors the scan_by_keys columnar contract). Column j of the returned chunk has
    // type values[j].type() and cell (j, 0) holds values[j] (null cells stay invalid). The
    // result has exactly one row (cardinality 1) and one column per value.
    //
    // The chunk carries no column NAMES: callers pass key column names separately
    // (key_col_names), so values[] must be ordered POSITIONALLY to match key_col_names[] —
    // values[j] is the key for key_col_names[j]. The agent resolves names to storage column
    // indices independently and reads keys.value(ki, 0) by the same positional index ki.
    vector::data_chunk_t make_key_chunk(std::pmr::memory_resource* resource,
                                        std::pmr::vector<types::logical_value_t> values);

    // Builds the N-row columnar key carrier used to call manager_disk_t::read_chunks_by_keys
    // (and mirrors the scan_by_keys columnar contract for a multi-key batch). Row i of the
    // returned chunk is the i-th key-tuple: cell (j, i) holds rows[i][j] (null cells stay
    // invalid). The result has exactly rows.size() rows and one column per key field.
    //
    // Column types come from rows[0]: every key-tuple MUST be positionally aligned to the same
    // key_col_names[] and use the same column types, so rows[i][j].type() == rows[0][j].type()
    // for all i. The chunk carries no column NAMES; callers pass key_col_names separately and
    // the agent resolves names to storage indices once, then reads keys.value(ki, i) by the
    // same positional index ki. An empty rows[] yields a 0-row, 0-column chunk (the read then
    // returns an empty per-key result, matching read_chunks_by_keys' invariant).
    vector::data_chunk_t make_keys_chunk(std::pmr::memory_resource* resource,
                                         const std::pmr::vector<std::pmr::vector<types::logical_value_t>>& rows);

} // namespace components::operators
