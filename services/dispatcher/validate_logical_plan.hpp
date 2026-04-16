#pragma once

#include <components/catalog/catalog.hpp>
#include <components/cursor/cursor.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>

namespace services::dispatcher {

    using column_path = std::pmr::vector<size_t>;
    struct type_from_t {
        std::string result_alias;
        components::types::complex_logical_type type;
    };
    struct type_path_t {
        column_path path;
        components::types::complex_logical_type type;
    };

    using named_schema = std::pmr::vector<type_from_t>;
    using type_paths = std::pmr::vector<type_path_t>;

    [[nodiscard]] core::error_t check_namespace_exists(std::pmr::memory_resource* resource,
                                                       const components::catalog::catalog& catalog,
                                                       const components::catalog::table_id& id);
    [[nodiscard]] core::error_t check_collection_exists(std::pmr::memory_resource* resource,
                                                        const components::catalog::catalog& catalog,
                                                        const components::catalog::table_id& id);
    [[nodiscard]] core::error_t check_type_exists(std::pmr::memory_resource* resource,
                                                  const components::catalog::catalog& catalog,
                                                  const std::string& alias);

    [[nodiscard]] core::error_t validate_types(std::pmr::memory_resource* resource,
                                               const components::catalog::catalog& catalog,
                                               components::logical_plan::node_t* node);
    [[nodiscard]] core::result_wrapper_t<named_schema>
    validate_schema(std::pmr::memory_resource* resource,
                    const components::catalog::catalog& catalog,
                    components::logical_plan::node_t* node,
                    const components::logical_plan::storage_parameters& parameters);

} // namespace services::dispatcher