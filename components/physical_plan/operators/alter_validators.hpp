#pragma once

// ALTER atomic validation: async data-gathering layer. The pure validators in
// components/catalog/alter_column_validators.{hpp,cpp} take pre-materialised inputs by const-reference;
// this file gathers those inputs from manager_disk_t, keeping the pure validators testable without an
// actor harness.
//
// These helpers are coroutines invoked from an operator's await_async_and_resume, not actors. A scan-side
// failure returns a core::error_t rather than an empty result: the pure validators read an empty gather as
// "no visible columns" / "no dependents", so degrading a failed read would let a duplicate column through
// or make a RESTRICT check pass vacuously.

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

    // Extracts the relation's name (for a refusal message) and relkind (which picks the refusal's wording)
    // from a keyed pg_class.oid read. An empty name (no readable row) leaves relkind 0, the ordinary-wording
    // case; a caller falls back to the oid in its message rather than dropping the refusal.
    //
    // PURE deliberately — the caller does its own send + co_await — so an accepted ALTER pays nothing; only
    // callers on the refusal path use it.
    struct relation_identity_t {
        std::string relname;
        char relkind{0};
    };
    relation_identity_t
    relation_identity_of(const std::pmr::vector<components::vector::data_chunk_t>& pg_class_batches);

    // deliberately no pg_depend gatherer here: deptype (dropped by a (refclassid, refobjid)
    // key alone) is the only field that tells a blocking edge from a cascadable one, so
    // the operator that needs the answer reads pg_depend itself.

    // Re-export the pure validators so callsites reach pure + async helpers
    // through one `using namespace alter_validators;`.
    using components::catalog::alter_column_validators::encode_default_spec_ec;
    using components::catalog::alter_column_validators::validate_column_not_duplicate;
    using components::catalog::alter_column_validators::validate_default_value_type;

} // namespace components::operators::alter_validators
