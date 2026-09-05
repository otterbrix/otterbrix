#pragma once

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/catalog/results/ddl_result.hpp>

#include <functional>
#include <memory_resource>
#include <vector>

namespace components::catalog {

    // A single step in a computed drop plan: drop the object identified by (classid, objid).
    struct drop_step_t {
        oid_t classid{INVALID_OID}; // catalog table that owns objid
        oid_t objid{INVALID_OID};   // object to drop
        char deptype{'n'};          // deptype of the pg_depend edge that drove this step
    };

    // Result of planning a DROP operation (CASCADE or RESTRICT).
    struct cascade_plan_t {
        explicit cascade_plan_t(std::pmr::memory_resource* resource)
            : steps(resource) {}

        // DROP succeeded: ordered list of objects to drop (children first, seed last).
        // NEVER EMPTY when status==ok — the seed's own step is always the last entry,
        // under every behavior. An accepted DROP that planned no steps is a statement
        // that deletes nothing and answers success, which is what this used to be on
        // the RESTRICT allow-path. Empty steps therefore only ever accompany a
        // non-ok status (restrict_blocked / cycle_detected).
        std::pmr::vector<drop_step_t> steps;

        // Non-INVALID_OID when RESTRICT is blocked: OID of the blocking dependent.
        oid_t blocking_oid{INVALID_OID};
        ddl_status status{ddl_status::ok};
    };

    // Plan a DROP starting from (seed_classid, seed_oid) with the given behavior.
    //
    // fetch_deps: callback returning all pg_depend rows where (refclassid, refobjid)
    //   matches the given (cls, oid) — i.e., all direct dependents of the seed.
    //   Implemented by disk as a closure over collect_dependents(); the catalog owns
    //   only the traversal logic, not the storage scan.
    //
    // behavior: the three forms collapse through catalog::refuses_on_dependency
    //   (components/catalog/results/ddl_result.hpp), never by comparing against a
    //   single enumerator:
    //     restrict_    — a written RESTRICT. GATE ONLY: if any DIRECT 'n' (normal)
    //                    dependency reaches the seed, the plan comes back
    //                    restrict_blocked with blocking_oid and NO steps. Past the
    //                    gate it falls through to the same order computation as
    //                    CASCADE — refusing is all RESTRICT does differently; it
    //                    never shrinks an accepted drop.
    //     cascade_     — a written CASCADE. No gate, full topological drop order.
    //     unspecified  — the statement named neither word. Resolves to CASCADE in
    //                    this build (owner decision; moving it is GitHub #638).
    cascade_plan_t plan_drop(std::pmr::memory_resource* resource,
                             oid_t seed_classid,
                             oid_t seed_oid,
                             drop_behavior_t behavior,
                             const fetch_deps_fn& fetch_deps);

} // namespace components::catalog
