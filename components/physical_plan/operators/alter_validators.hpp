#pragma once

// ALTER atomic validation: async data-gathering layer.
//
// The pure validators in components/catalog/alter_column_validators.{hpp,cpp} take
// pre-materialised inputs by const-reference; this file provides the async
// helpers that gather those inputs from manager_disk_t. The split keeps the
// pure validators testable without an actor harness while still letting ALTER
// operators short-circuit on validation failure BEFORE any pg_catalog mutation.
//
// These helpers are NOT actors: they are coroutine functions invoked from an
// operator's await_async_and_resume, piggy-backing on its async frame and
// talking to manager_disk_t only via actor_zeta::send. read_rows_by_key has no
// error channel yet, so a scan-side failure degrades to an empty result and the
// downstream pure validator reports the appropriate code.

#include <components/catalog/alter_column_validators.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <core/result_wrapper.hpp>

#include <actor-zeta.hpp>
#include <actor-zeta/detail/future.hpp>

#include <memory_resource>
#include <string>
#include <utility>
#include <vector>

namespace components::operators::alter_validators {

    // Async pg_attribute scan: visible column names for the relation, filtered by
    // attisdropped==false and the MVCC snapshot (added_at <= horizon AND
    // (dropped_at == 0 OR dropped_at > horizon)). Vector is allocated against
    // `resource` and consumed by validate_column_not_duplicate. A scan-side
    // failure returns empty — callers MUST treat empty as "no known visible
    // columns" (worst case: a duplicate slips through to a later consistency error).
    actor_zeta::unique_future<std::pmr::vector<std::string>>
    visible_column_names(std::pmr::memory_resource* resource,
                         actor_zeta::address_t disk_address,
                         components::execution_context_t exec_ctx,
                         components::catalog::oid_t table_oid);

    // Async pg_depend scan: (classid, objid) pairs depending on (refclassid,
    // refobjid, refobjsubid). refobjsubid is the altered column's attnum
    // (0 = whole-relation). Feeds validate_cascade_dependencies for RESTRICT, or
    // the cascade loop for CASCADE.
    // TBD-impl: pg_depend has no refobjsubid yet, so this returns ALL dependents
    // of the refobj (table); callers must refine once the column-grain id lands.
    actor_zeta::unique_future<std::pmr::vector<std::pair<int, components::catalog::oid_t>>>
    scan_cascade_dependents(std::pmr::memory_resource* resource,
                            actor_zeta::address_t disk_address,
                            components::execution_context_t exec_ctx,
                            components::catalog::oid_t ref_classid,
                            components::catalog::oid_t ref_objid,
                            std::int32_t ref_objsubid);

    // Re-export the pure validators so callsites reach pure + async helpers
    // through one `using namespace alter_validators;`.
    using components::catalog::alter_column_validators::encode_default_spec_ec;
    using components::catalog::alter_column_validators::validate_cascade_dependencies;
    using components::catalog::alter_column_validators::validate_column_not_duplicate;
    using components::catalog::alter_column_validators::validate_default_value_evaluatable;
    using components::catalog::alter_column_validators::validate_default_value_type;

} // namespace components::operators::alter_validators
