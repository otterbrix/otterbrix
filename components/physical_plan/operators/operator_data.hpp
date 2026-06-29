#pragma once

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/vector/data_chunk.hpp>
#include <memory_resource>
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

        std::pmr::memory_resource* resource() const;

    private:
        std::pmr::memory_resource* resource_;
        chunks_vector_t chunks_;
    };

    using operator_data_ptr = operator_data_t::ptr;

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

    // Builds the 1-row columnar key carrier used to call manager_disk_t::read_chunks_by_key (and
    // mirrors the scan_by_keys columnar contract): one column per argument, cardinality 1, with each
    // column's type derived from the argument's C++ type via the same to_logical_type<> mapping
    // logical_value_t uses, and the cell written through the typed vector_t::set_value.
    //
    // The chunk carries no column NAMES: callers pass key column names separately (key_col_names), so
    // the arguments must be ordered POSITIONALLY to match key_col_names[] — the j-th argument is the
    // key for key_col_names[j]. The agent resolves names to storage column indices independently and
    // reads keys.value(ki, 0) by the same positional index ki.
    //
    // Pass string keys as std::string_view: a bare const char* maps to POINTER, not STRING_LITERAL.
    template<typename Val, typename... Vals>
    requires(!std::is_same_v<std::remove_cvref_t<Val>, std::pmr::vector<types::logical_value_t>>) vector::data_chunk_t
        make_key_chunk(std::pmr::memory_resource* resource, Val&& val, Vals&&... vals) {
        std::pmr::vector<types::complex_logical_type> col_types(resource);
        col_types.reserve(1 + sizeof...(Vals));
        col_types.emplace_back(types::to_logical_type<std::remove_cvref_t<Val>>());
        (col_types.emplace_back(types::to_logical_type<std::remove_cvref_t<Vals>>()), ...);
        vector::data_chunk_t chunk(resource, col_types, 1);
        uint64_t col = 0;
        chunk.set_value(col++, uint64_t{0}, std::forward<Val>(val));
        (chunk.set_value(col++, uint64_t{0}, std::forward<Vals>(vals)), ...);
        chunk.set_cardinality(1);
        return chunk;
    }

    // Builds the N-row columnar key carrier for manager_disk_t::read_chunks_by_keys: a single key
    // column (type derived from T) with one row per value — the common probe-by-oid batch. Same
    // positional/no-column-names contract as make_key_chunk; an empty values[] yields a 1-capacity,
    // 0-row chunk (the read then returns an empty per-key result).
    template<typename T>
    requires(!std::is_same_v<std::remove_cvref_t<T>, types::logical_value_t>) vector::data_chunk_t
        make_keys_chunk(std::pmr::memory_resource* resource, const std::pmr::vector<T>& values) {
        std::pmr::vector<types::complex_logical_type> col_types(resource);
        col_types.emplace_back(types::to_logical_type<std::remove_cvref_t<T>>());
        const std::size_t nrows = values.size();
        vector::data_chunk_t chunk(resource, col_types, nrows == 0 ? 1 : nrows);
        for (std::size_t i = 0; i < nrows; ++i) {
            chunk.set_value(uint64_t{0}, static_cast<uint64_t>(i), values[i]);
        }
        chunk.set_cardinality(static_cast<uint64_t>(nrows));
        return chunk;
    }

} // namespace components::operators
