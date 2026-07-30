#pragma once

#include <core/result_wrapper.hpp>

#include <string_view>
#include <vector>

#include <components/base/collection_full_name.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace components::cursor {

    using index_t = int32_t;
    constexpr index_t start_index = -1;

    class cursor_t : public boost::intrusive_ref_counter<cursor_t> {
    public:
        explicit cursor_t(std::pmr::memory_resource* resource);
        explicit cursor_t(std::pmr::memory_resource* resource, const core::error_t& error);
        explicit cursor_t(std::pmr::memory_resource* resource, core::error_t&& error);
        explicit cursor_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);
        explicit cursor_t(std::pmr::memory_resource* resource, std::pmr::vector<vector::data_chunk_t>&& chunks);
        explicit cursor_t(std::pmr::memory_resource* resource, std::pmr::vector<vector::column_schema_t>&& columns);

        // Raw access to the result batch. Row access that may span chunks must go
        // through value()/row() — never index a single chunk by a global row id.
        std::pmr::vector<vector::data_chunk_t>& chunks();
        const std::pmr::vector<vector::data_chunk_t>& chunks() const;

        // The result's column descriptor — one record per output column, {attoid, name, type}.
        // This is the source column_count() reports and the one every binding reads: the C
        // ABI's cursor_column_name / cursor_column_logical_type / cursor_get_value_by_name,
        // the python wrapper, and rust and C# through the C ABI.
        //
        // It is the cursor's OWN carrier, and it has to be (M3-B5): a bare type list cannot
        // name a column — the type's name slot belongs to self-naming types, so a STRUCT
        // column would answer with the type's name. It is a deep copy taken once at
        // construction and never re-synced, which is deliberate and pinned: chunks() hands
        // out a NON-const reference, so a rename after construction moves the chunk's
        // schema and not this (test_cursor.cpp).
        const std::pmr::vector<vector::column_schema_t>& columns() const;

        std::size_t size() const;
        std::size_t column_count() const;

        bool has_next() const;
        void advance();
        index_t current_index() const;

        types::logical_value_t value(uint64_t col_idx) const;
        types::logical_value_t value(uint64_t col_idx, uint64_t row_idx) const;
        std::pmr::vector<types::logical_value_t> row() const;
        std::pmr::vector<types::logical_value_t> row(uint64_t row_idx) const;

        bool is_success() const noexcept;
        bool is_error() const noexcept;
        core::error_t get_error() const;

    private:
        // Result rows as a batch of ≤DEFAULT_VECTOR_CAPACITY chunks (never combined into
        // one oversized chunk). Always holds at least one (possibly empty) chunk so
        // chunks().front() / column metadata have a valid front to return.
        std::size_t size_{};
        index_t current_index_{start_index};
        std::pmr::vector<vector::data_chunk_t> chunks_;
        std::pmr::vector<vector::column_schema_t> columns_;
        core::error_t error_;
    };

    using cursor_t_ptr = boost::intrusive_ptr<cursor_t>;

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, const core::error_t& error);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, core::error_t&& error);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, std::pmr::vector<vector::data_chunk_t>&& chunks);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource,
                             std::pmr::vector<vector::column_schema_t>&& columns);

} // namespace components::cursor
