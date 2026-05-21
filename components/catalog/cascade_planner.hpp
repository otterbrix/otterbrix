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

        // DROP succeeded: ordered list of objects to drop (children first, root last).
        // Empty when behavior==restrict_ and no external dependencies exist.
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
    // behavior: restrict_ → return immediately with blocking_oid if any 'n' dependency
    //           exists; cascade_ → compute full topological drop order.
    cascade_plan_t plan_drop(std::pmr::memory_resource* resource,
                             oid_t seed_classid,
                             oid_t seed_oid,
                             drop_behavior_t behavior,
                             const fetch_deps_fn& fetch_deps);

} // namespace components::catalog
