#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/cursor/cursor.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/forward.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/types.hpp>

#include <span>
#include <string_view>

namespace components::catalog {
    class table_id;
}

// Real type lives in services::catalog_resolve; impl::plan_resolve_index_t
// below is an alias (see plan_resolve_index.hpp).
namespace services::catalog_resolve {
    struct plan_resolve_index_t;
} // namespace services::catalog_resolve

namespace services::dispatcher {

    namespace impl {
        using ::services::catalog_resolve::plan_resolve_index_t;
    }

    using column_path = std::pmr::vector<size_t>;
    struct type_from_t {
        std::string result_alias;
        components::types::complex_logical_type type;
        components::expressions::side_t side = components::expressions::side_t::undefined;
    };
    struct type_path_t {
        column_path path;
        components::types::complex_logical_type type;
    };

    using named_schema = std::pmr::vector<type_from_t>;
    using type_paths = std::pmr::vector<type_path_t>;

    // Existence checks — return no_error() on success, an error on failure.
    [[nodiscard]] core::error_t check_namespace_exists(std::pmr::memory_resource* resource,
                                                       const impl::plan_resolve_index_t* idx,
                                                       const components::catalog::table_id& id);
    [[nodiscard]] core::error_t check_collection_exists(std::pmr::memory_resource* resource,
                                                        const impl::plan_resolve_index_t* idx,
                                                        const components::catalog::table_id& id);
    // Probe `alias` against the plan-tree idx (impl::type_md_for) for each
    // dbname in `search_dbnames` in order. Returns no_error() on first hit.
    // If `search_dbnames` is empty, falls back to {"public", "pg_catalog"}.
    [[nodiscard]] core::error_t check_type_exists(std::pmr::memory_resource* resource,
                                                  const impl::plan_resolve_index_t* idx,
                                                  const std::string& alias,
                                                  std::span<const std::string> search_dbnames = {});

    // Validate plan node types against the plan-tree idx.
    [[nodiscard]] core::error_t validate_types(std::pmr::memory_resource* resource,
                                               const impl::plan_resolve_index_t* idx,
                                               components::logical_plan::node_t* node,
                                               core::date::timezone_offset_t session_tz);

    [[nodiscard]] core::result_wrapper_t<named_schema>
    validate_schema(std::pmr::memory_resource* resource,
                    const impl::plan_resolve_index_t* idx,
                    components::logical_plan::node_t* node,
                    const components::logical_plan::storage_parameters& parameters);

} // namespace services::dispatcher
