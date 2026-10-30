#pragma once

#include <components/compute/function.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/storage_address.hpp>
#include <core/result_wrapper.hpp>
#include <services/collection/context_storage.hpp>

#include <string_view>

namespace services::planner {

    // A successful result always holds an operator.
    using plan_result_t = core::result_wrapper_t<components::operators::operator_ptr>;

    // A generator's refusal: `what` says why this node has no physical plan.
    core::error_t plan_refusal(std::pmr::memory_resource* resource, std::string_view what);

    // A node names a table this plan has no resolved oid for.
    core::error_t unresolved_table_refusal(std::pmr::memory_resource* resource, const qualified_name_t& name);

    // Where the rows of `table_oid` live: its resolved storage, the disk for a system catalog table; a table the plan
    // holds no metadata of is refused (internal_error).
    core::result_wrapper_t<components::table::storage_address_t>
    storage_address_of(const context_storage_t& context, components::catalog::oid_t table_oid);
    // connection.database.schema.table of a resolved table, as EXPLAIN names a table of a connection.
    std::pmr::string full_name_of(const context_storage_t& context, components::catalog::oid_t table_oid);
    qualified_name_t qualified_name_of(const components::logical_plan::resolved_table_metadata_t& table);
    // The columns a scan of `table_oid` reads: for a computed table its live columns by chunk position (a dropped
    // field keeps its physical column, which no scan reads), for any other table `projected`.
    std::vector<size_t> scan_columns_of(const context_storage_t& context,
                                        components::catalog::oid_t table_oid,
                                        const std::vector<size_t>& projected);
    // The types of the columns a scan of `table_oid` reads, from the catalog: the visible columns at `projected`
    // (every one when empty), for a computed table the columns at those chunk positions. A table the plan holds no
    // metadata of is refused.
    core::result_wrapper_t<std::pmr::vector<components::types::complex_logical_type>>
    page_types_of(const context_storage_t& context,
                  components::catalog::oid_t table_oid,
                  const std::vector<size_t>& projected);

    plan_result_t create_plan(const context_storage_t& context,
                              const components::compute::function_registry_t& function_registry,
                              const components::logical_plan::node_ptr& node,
                              components::logical_plan::limit_t limit,
                              const components::logical_plan::storage_parameters* params);

} // namespace services::planner
