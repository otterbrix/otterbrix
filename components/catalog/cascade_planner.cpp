#include "cascade_planner.hpp"

namespace components::catalog {

    cascade_plan_t plan_drop(std::pmr::memory_resource* resource,
                             oid_t seed_classid,
                             oid_t seed_oid,
                             drop_behavior_t behavior,
                             const fetch_deps_fn& fetch_deps) {
        cascade_plan_t plan{resource};

        if (refuses_on_dependency(behavior)) {
            // RESTRICT only gates on a direct 'n' dependency and refuses outright — it
            // never falls through with an empty plan, which used to read as "success,
            // nothing deleted". Unlike PostgreSQL's findDependentObjects, the gate
            // does not recurse past the seed's direct edges.
            for (const auto& d : fetch_deps(resource, seed_classid, seed_oid)) {
                if (deptype::blocks_restrict(d.deptype)) {
                    plan.status = ddl_status::restrict_blocked;
                    plan.blocking_oid = d.objid;
                    return plan;
                }
            }
        }

        // topological_drop_order emits each dependent once, when finished (not once
        // per reaching edge); the seed itself is appended last for the executor to drop.
        oid_t cycle_at = INVALID_OID;
        auto ordered = topological_drop_order(resource, seed_classid, seed_oid, fetch_deps, cycle_at);
        if (cycle_at != INVALID_OID) {
            plan.status = ddl_status::cycle_detected;
            plan.blocking_oid = cycle_at;
            return plan;
        }
        plan.steps.reserve(ordered.size() + 1);
        for (const auto& d : ordered) {
            plan.steps.push_back({d.classid, d.objid, d.deptype});
        }
        plan.steps.push_back({seed_classid, seed_oid, 'n'});
        return plan;
    }

} // namespace components::catalog
