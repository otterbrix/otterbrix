#pragma once

// Gathers inputs for alter_column_validators.{hpp,cpp}'s pure validators, testable without an actor harness.
//
// A scan-side failure returns core::error_t, not empty: the validators read empty as "no visible
// columns"/"no dependents," so a failure must not slip a duplicate column or pass RESTRICT vacuously.

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

    // attisdropped==false and the MVCC snapshot (added_at <= horizon AND (dropped_at == 0 OR dropped_at > horizon)).
    actor_zeta::unique_future<core::result_wrapper_t<std::pmr::vector<std::string>>>
    visible_column_names(std::pmr::memory_resource* resource,
                         actor_zeta::address_t disk_address,
                         components::execution_context_t exec_ctx,
                         components::catalog::oid_t table_oid);

    // relkind picks the refusal's wording; an empty name (no readable row) leaves relkind 0, the
    // ordinary fallback. PURE deliberately: the caller awaits it itself, so an accepted ALTER pays nothing.
    struct relation_identity_t {
        std::string relname;
        char relkind{0};
    };
    relation_identity_t
    relation_identity_of(const std::pmr::vector<components::vector::data_chunk_t>& pg_class_batches);

    // Deliberately no pg_depend gatherer: deptype (blocking vs. cascadable) needs a
    // (refclassid, refobjid) keyed read, so the operator that needs it reads pg_depend itself.

    using components::catalog::alter_column_validators::encode_default_spec_ec;
    using components::catalog::alter_column_validators::validate_column_not_duplicate;
    using components::catalog::alter_column_validators::validate_default_value_type;

} // namespace components::operators::alter_validators
