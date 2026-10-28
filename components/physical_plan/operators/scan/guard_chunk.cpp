#include "guard_chunk.hpp"

#include <string>

namespace components::operators {

    vector::data_chunk_t make_guard_chunk(std::pmr::memory_resource* resource,
                                          const std::pmr::vector<types::complex_logical_type>& types,
                                          const std::vector<size_t>& projected_cols) {
        if (projected_cols.empty()) {
            return vector::data_chunk_t{resource, types, 0};
        }
        std::pmr::vector<types::complex_logical_type> projected_types(resource);
        projected_types.reserve(projected_cols.size());
        for (size_t column : projected_cols) {
            projected_types.push_back(types[column]);
        }
        return vector::data_chunk_t{resource, projected_types, 0};
    }

    core::error_t check_scan_page(std::pmr::memory_resource* resource,
                                  const std::pmr::string& table,
                                  const std::pmr::vector<types::complex_logical_type>& planned,
                                  const vector::data_chunk_t& page) {
        if (planned.empty()) {
            return core::error_t::no_error();
        }
        bool fits = page.column_count() == planned.size();
        for (uint64_t column = 0; fits && column < planned.size(); ++column) {
            fits = page.data[column].type().type() == planned[column].type();
        }
        if (fits) {
            return core::error_t::no_error();
        }
        std::pmr::string what{"the storage of table \"", resource};
        what += table;
        what += "\" answered a page of other columns";
        return core::error_t{core::error_code_t::invalid_parameter, std::move(what)};
    }

    core::error_t check_chunk_width(std::pmr::memory_resource* resource, const vector::data_chunk_t& chunk) {
        if (chunk.size() <= vector::DEFAULT_VECTOR_CAPACITY) {
            return core::error_t::no_error();
        }
        std::pmr::string what{"a storage answered a chunk of ", resource};
        what += std::to_string(chunk.size());
        what += " rows, more than the ";
        what += std::to_string(vector::DEFAULT_VECTOR_CAPACITY);
        what += " rows of a chunk";
        return core::error_t{core::error_code_t::invalid_parameter, std::move(what)};
    }

} // namespace components::operators
