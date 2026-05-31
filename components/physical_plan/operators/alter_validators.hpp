#pragma once

// ALTER atomic validation helpers (async data-gathering layer).
//
// The pure, actor-free validators in catalog/alter_column_validators.{hpp,cpp}
// take pre-materialised inputs by const-reference. This file provides the
// async, actor-aware data-gathering helpers that populate those inputs by
// mailbox-sending to manager_disk_t. The split keeps the pure validators
// trivially testable (no actor harness needed) while still allowing ALTER
// operators to short-circuit on validation failure BEFORE any pg_catalog
// mutation.
//
// Helpers in this file:
//   1. visible_column_names — pg_attribute scan filtered by MVCC visibility
//      (added_at_commit_id <= snapshot AND
//      (dropped_at_commit_id == 0 OR dropped_at_commit_id > snapshot)).
//   2. scan_cascade_dependents — pg_depend scan for entries that reference a
//      specific (refclassid, refobjid, refobjsubid) tuple. Returns the list of
//      (classid, objid) pairs that must be cascade-dropped, or — for a strict
//      RESTRICT caller — drives a non-empty result into a cascade_required-style
//      error.
//
// No shared state between actors: both helpers are NOT actors; they are
// coroutine functions invoked from within an operator's
// await_async_and_resume — they piggy-back on the operator's existing async
// frame and only ever talk to manager_disk_t through the standard
// actor_zeta::send mailbox API.
//
// No exceptions: all failures are surfaced via core::error_t returned through
// unique_future<core::result<T>>-equivalent shape. The current disk API does
// not yet propagate errors (read_rows_by_key returns the row vector directly),
// so wrapper-level failures degrade to empty result + the downstream pure
// validator reporting the appropriate code (e.g. already_exists / other_error).

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/alter_column_validators.hpp>
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

    // Async pg_attribute scan: returns the list of currently-visible column
    // names for the given relation, filtered by:
    //   * attisdropped boolean tombstone == false, AND
    //   * MVCC snapshot: added_at_commit_id <= snapshot_horizon AND
    //     (dropped_at_commit_id == 0 OR dropped_at_commit_id > snapshot_horizon).
    //
    // The returned vector is allocated against `resource` and consumed by
    // alter_column_validators::validate_column_not_duplicate (pure validator). Returns
    // an empty vector on a scan-side failure — operators MUST treat empty as
    // "no known visible columns" (worst case: a duplicate column slips through
    // validation and surfaces as a later consistency error). This degrades
    // gracefully because the manager_disk_t read path itself currently has no
    // error-channel; once it does, this helper should be upgraded to return
    // unique_future<core::result<...>> instead.
    //
    // Mailbox-only: the only inter-actor interaction is the
    // actor_zeta::send(disk_address, &manager_disk_t::read_rows_by_key, ...).
    actor_zeta::unique_future<std::pmr::vector<std::string>>
    visible_column_names(std::pmr::memory_resource* resource,
                         actor_zeta::address_t disk_address,
                         components::execution_context_t exec_ctx,
                         components::catalog::oid_t table_oid);

    // Async pg_depend scan: returns the list of (classid, objid) pairs that
    // depend on (refclassid, refobjid, refobjsubid). The refobjsubid is the
    // pg_attribute.attnum of the column being altered (0 = whole-relation
    // dependency). The returned vector is allocated against `resource` and
    // consumed by alter_column_validators::validate_cascade_dependencies (pure validator)
    // when the caller is RESTRICT, or fed into the cascade execution loop when
    // the caller is CASCADE.
    //
    // pg_depend layout (per system_table_schemas.cpp):
    //   [0]=classid, [1]=objid, [2]=refclassid, [3]=refobjid, [4]=deptype
    //
    // NOTE: pg_depend currently does NOT carry refobjsubid (the column-level
    // subobject id) — see pg_depend_columns() in system_table_schemas.cpp.
    // Until that field is added, this helper conservatively returns ALL
    // dependents of the refobj (table) and the caller is expected to refine.
    // TBD-impl: pg_depend.refobjsubid for column-grain dependency tracking.
    actor_zeta::unique_future<std::pmr::vector<std::pair<int, components::catalog::oid_t>>>
    scan_cascade_dependents(std::pmr::memory_resource* resource,
                            actor_zeta::address_t disk_address,
                            components::execution_context_t exec_ctx,
                            components::catalog::oid_t ref_classid,
                            components::catalog::oid_t ref_objid,
                            std::int32_t ref_objsubid);

    // Convenience re-exports of the pure validators (defined in
    // components/catalog/alter_column_validators.hpp). Brought into this
    // namespace so ALTER operator callsites can `using namespace
    // alter_validators;` and reach the full 5-helper surface uniformly,
    // without having to remember which validators are pure
    // (catalog::alter_column_validators::) and which are async
    // (operators::alter_validators::).
    using components::catalog::alter_column_validators::encode_default_spec_ec;
    using components::catalog::alter_column_validators::validate_cascade_dependencies;
    using components::catalog::alter_column_validators::validate_column_not_duplicate;
    using components::catalog::alter_column_validators::validate_default_value_evaluatable;
    using components::catalog::alter_column_validators::validate_default_value_type;

} // namespace components::operators::alter_validators
