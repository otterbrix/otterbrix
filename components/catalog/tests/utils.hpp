#pragma once

#include <components/catalog/catalog.hpp>
#include <components/cursor/cursor.hpp>
#include <components/types/types.hpp>

namespace test {
    template<size_t... Idx>
    std::vector<components::types::field_description> enumerate(std::index_sequence<Idx...>) {
        return {components::types::field_description(1 + Idx)...};
    }

    template<size_t Size>
    std::vector<components::types::field_description> n_field_descriptions() {
        return enumerate(std::make_index_sequence<Size>());
    }

    void create_single_column_table(const collection_full_name_t& name,
                                    components::types::complex_logical_type log_t,
                                    components::catalog::catalog& cat,
                                    std::pmr::memory_resource* resource);

    components::types::logical_type front_cursor_type(components::cursor::cursor_t_ptr cursor);
} // namespace test