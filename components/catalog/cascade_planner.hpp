#pragma once

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/catalog/results/ddl_result.hpp>

#include <functional>
#include <memory_resource>
#include <vector>

namespace components::catalog {

    struct drop_step_t {
        oid_t classid{INVALID_OID}; // catalog table that owns objid
        oid_t objid{INVALID_OID};
        char deptype{'n'}; // deptype of the pg_depend edge that drove this step
    };

    // Result of a DROP plan, whether CASCADE or RESTRICT.
    struct cascade_plan_t {
        explicit cascade_plan_t(std::pmr::memory_resource* resource)
            : steps(resource) {}

        // Ordered children-first, seed-last; non-empty iff status==ok.
        std::pmr::vector<drop_step_t> steps;

        // Non-INVALID_OID when RESTRICT is blocked: OID of the blocking dependent.
        oid_t blocking_oid{INVALID_OID};
        ddl_status status{ddl_status::ok};
    };

    // `behavior` collapses through catalog::refuses_on_dependency, not a raw enum comparison:
    //   restrict_ (PostgreSQL parity, #638) is a gate only -- any direct 'n' dependency on the
    //   seed blocks with no steps, else it falls through to cascade_'s order; cascade_ has no
    //   gate, full topological order.
    cascade_plan_t plan_drop(std::pmr::memory_resource* resource,
                             oid_t seed_classid,
                             oid_t seed_oid,
                             drop_behavior_t behavior,
                             const fetch_deps_fn& fetch_deps);

} // namespace components::catalog
