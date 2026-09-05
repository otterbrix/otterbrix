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
// talking to manager_disk_t only via actor_zeta::send. A scan-side failure is
// returned as a core::error_t: it used to degrade to an empty result, which the
// pure validators read as "no visible columns" and "no dependents" — so a failed
// read let a duplicate column through and made a RESTRICT check pass vacuously.

#include <components/catalog/alter_column_validators.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <actor-zeta.hpp>
#include <actor-zeta/detail/future.hpp>

#include <memory_resource>
#include <string>
#include <vector>

namespace components::operators::alter_validators {

    // Async pg_attribute scan: visible column names for the relation, filtered by
    // attisdropped==false and the MVCC snapshot (added_at <= horizon AND
    // (dropped_at == 0 OR dropped_at > horizon)). Vector is allocated against
    // `resource` and consumed by validate_column_not_duplicate. An empty list means
    // the relation really has no visible columns, which a caller is entitled to trust.
    actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::string>>>
    visible_column_names(std::pmr::memory_resource* resource,
                         actor_zeta::address_t disk_address,
                         components::execution_context_t exec_ctx,
                         components::catalog::oid_t table_oid);

    // Who the relation is, out of the batches a keyed read on pg_class.oid returned:
    // the name a refusal has to quote ("column x of relation y ..."), and the relkind
    // that decides WHICH true sentence the refusal is. An empty name means the batches
    // carried no readable pg_class row — a caller on a refusal path falls back to the
    // oid in its message rather than dropping the refusal — and relkind is then 0,
    // which matches no kind and so picks the ordinary wording.
    //
    // PURE, deliberately: the caller does its own send + co_await on its own frame.
    // This started life as an async helper that did both, and the ALTER refusals whose
    // messages it built arrived at the cursor as unreadable bytes. Every operator here
    // already reads pg_class inline elsewhere; this keeps that one shape and shares
    // only the part that has no lifetime of its own.
    //
    // Callers use it on the REFUSAL path only, so an accepted ALTER pays nothing.
    struct relation_identity_t {
        std::string relname;
        char relkind{0};
    };
    relation_identity_t
    relation_identity_of(const std::pmr::vector<components::vector::data_chunk_t>& pg_class_batches);

    // There is no pg_depend gatherer here. One lived here — scan_cascade_dependents,
    // keyed on (refclassid, refobjid) — and it never had a caller: it dropped
    // pg_depend.deptype, which is the only field that tells a blocking edge from a
    // cascadable one, so the operator that needs the answer reads pg_depend itself.

    // Re-export the pure validators so callsites reach pure + async helpers
    // through one `using namespace alter_validators;`.
    using components::catalog::alter_column_validators::encode_default_spec_ec;
    using components::catalog::alter_column_validators::validate_column_not_duplicate;
    using components::catalog::alter_column_validators::validate_default_value_type;

} // namespace components::operators::alter_validators
