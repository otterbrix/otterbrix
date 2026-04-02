#pragma once

#include <core/result_wrapper.hpp>

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
        explicit cursor_t(std::pmr::memory_resource* resource,
                          std::pmr::vector<components::types::complex_logical_type>&& types);

        vector::data_chunk_t& chunk_data();
        const vector::data_chunk_t& chunk_data() const;
        std::pmr::vector<components::types::complex_logical_type>& type_data();
        const std::pmr::vector<components::types::complex_logical_type>& type_data() const;

        std::size_t size() const;

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
        std::size_t size_{};
        index_t current_index_{start_index};
        vector::data_chunk_t table_data_;
        std::pmr::vector<components::types::complex_logical_type> type_data_;
        core::error_t error_;
    };

    using cursor_t_ptr = boost::intrusive_ptr<cursor_t>;

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, const core::error_t& error);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, core::error_t&& error);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::types::complex_logical_type>&& types);

} // namespace components::cursor
