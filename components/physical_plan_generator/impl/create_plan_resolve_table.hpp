#pragma once

#include <components/logical_plan/node.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <services/collection/context_storage.hpp>

namespace services::planner::impl {

    // Bridge node_catalog_resolve_table_t -> operator_resolve_table_t.
    // Forwards (namespace_oid, relname) into the operator's name-form ctor;
    // the operator itself handles oid resolution via pg_class scan inside
    // its async resume path. When namespace_oid() is INVALID_OID
    // (pre-enrichment) the operator falls back to a relname-only pg_class scan.
    components::operators::operator_ptr create_plan_resolve_table(const context_storage_t& context,
                                                                  const components::logical_plan::node_ptr& node);

} // namespace services::planner::impl
