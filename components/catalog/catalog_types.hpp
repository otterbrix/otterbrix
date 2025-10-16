#pragma once

#include <cassert>
#include <chrono>

#include <components/types/types.hpp>

namespace components::catalog {
    using timestamp = std::chrono::milliseconds;
    using schema_version_t = uint64_t;
    using field_id_t = uint64_t;

    [[nodiscard]] types::complex_logical_type
    create_list(field_id_t field_id, const types::complex_logical_type& type, bool required = true);

    [[nodiscard]] types::complex_logical_type create_struct(const std::vector<types::complex_logical_type>& columns,
                                                            std::vector<types::field_description> descriptions);

    [[nodiscard]] types::complex_logical_type create_map(field_id_t key_id,
                                                         const types::complex_logical_type& key,
                                                         field_id_t value_id,
                                                         const types::complex_logical_type& value,
                                                         bool value_required = true);

    types::map_logical_type_extension& to_map(const types::complex_logical_type& type);
    types::struct_logical_type_extension& to_struct(const types::complex_logical_type& type);
    types::list_logical_type_extension& to_list(const types::complex_logical_type& type);
} // namespace components::catalog
