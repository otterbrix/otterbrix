#pragma once

#include <vector>

#include <components/base/collection_full_name.hpp>
#include <components/document/document.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

namespace components::cursor {

    using index_t = int32_t;
    constexpr index_t start_index = -1;

    enum class operation_status_t : bool
    {
        success = true,
        failure = false
    };

    enum class error_code_t : int32_t
    {
        other_error = -1,
        none = 0,
        database_already_exists = 1,
        database_not_exists = 2,
        collection_already_exists = 3,
        collection_not_exists = 4,
        index_create_fail = 5,
        collection_dropped = 6,
        sql_parse_error = 7,
        create_physical_plan_error = 8,
        schema_error = 9,
        incompatible_storage_types = 10,
        index_not_exists = 11,
    };

    struct error_t {
        error_code_t type;
        std::string what;

        explicit error_t(error_code_t type);
        explicit error_t(error_code_t type, const std::string& what);
    };

    class cursor_t : public boost::intrusive_ref_counter<cursor_t> {
    public:
        explicit cursor_t(std::pmr::memory_resource* resource);
        explicit cursor_t(std::pmr::memory_resource* resource, const error_t& error);
        explicit cursor_t(std::pmr::memory_resource* resource, operation_status_t op_status);
        explicit cursor_t(std::pmr::memory_resource* resource, std::pmr::vector<document::document_ptr>&& documents);
        explicit cursor_t(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);
        explicit cursor_t(std::pmr::memory_resource* resource,
                          std::pmr::vector<components::types::complex_logical_type>&& types);

        bool uses_table_data() const;

        std::pmr::vector<document::document_ptr>& document_data();
        const std::pmr::vector<document::document_ptr>& document_data() const;
        vector::data_chunk_t& chunk_data();
        const vector::data_chunk_t& chunk_data() const;
        std::pmr::vector<components::types::complex_logical_type>& type_data();
        const std::pmr::vector<components::types::complex_logical_type>& type_data() const;

        std::size_t size() const;
        // std::pmr::vector<std::unique_ptr<sub_cursor_t>>::iterator begin();
        // std::pmr::vector<std::unique_ptr<sub_cursor_t>>::iterator end();

        bool has_next() const;
        document::document_ptr next_document();
        document::document_ptr get_document() const;
        document::document_ptr get_document(std::size_t index) const;

        // types::logical_value_t next_row();
        // types::logical_value_t get_row() const;
        // types::logical_value_t get_row(std::size_t index) const;

        bool is_success() const noexcept;
        bool is_error() const noexcept;
        error_t get_error() const;

        void sort(std::function<bool(document::document_ptr, document::document_ptr)> sorter);
        //void sort(std::function<bool(types::logical_value_t, types::logical_value_t)> sorter);

    private:
        std::size_t size_{};
        index_t current_index_{start_index};
        std::pmr::vector<document::document_ptr> document_data_;
        vector::data_chunk_t table_data_;
        std::pmr::vector<components::types::complex_logical_type> type_data_;
        error_t error_;
        bool success_{true};
        bool uses_table_data_{true};
    };

    using cursor_t_ptr = boost::intrusive_ptr<cursor_t>;

    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, operation_status_t op_status);
    cursor_t_ptr
    make_cursor(std::pmr::memory_resource* resource, error_code_t type, const std::string& what = std::string());
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, std::pmr::vector<document::document_ptr>&& documents);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource, vector::data_chunk_t&& chunk);
    cursor_t_ptr make_cursor(std::pmr::memory_resource* resource,
                             std::pmr::vector<components::types::complex_logical_type>&& types);

} // namespace components::cursor
