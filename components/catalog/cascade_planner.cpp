#include "cascade_planner.hpp"

namespace components::catalog {

    cascade_plan_t plan_drop(std::pmr::memory_resource* resource,
                             oid_t seed_classid,
                             oid_t seed_oid,
                             drop_behavior_t behavior,
                             const fetch_deps_fn& fetch_deps) {
        cascade_plan_t plan{resource};

        if (refuses_on_dependency(behavior)) {
            // RESTRICT REFUSES; IT DOES NOT SHRINK THE DROP. The only thing this leg
            // adds over CASCADE is a gate: if a 'n' (normal) dependency reaches the
            // seed, the statement is refused outright and NOTHING is planned. Past the
            // gate the statement is an ordinary drop of the seed and of the
            // auto/internal children that cannot outlive it — so it falls through to
            // the same order computation below rather than returning an empty plan.
            // An empty plan is not "a drop that removed only the children": the
            // executor iterates plan.steps, so zero steps is zero catalog deletes and
            // a DROP that answers success over an object it left fully live.
            //
            // The gate reads the seed's DIRECT edges only. A normal edge landing on
            // one of the auto children is not re-checked here; PostgreSQL's
            // findDependentObjects does recurse, this build does not, and that is
            // stated rather than left to be inferred from the absence of a loop.
            for (const auto& d : fetch_deps(resource, seed_classid, seed_oid)) {
                if (deptype::blocks_restrict(d.deptype)) {
                    plan.status = ddl_status::restrict_blocked;
                    plan.blocking_oid = d.objid;
                    return plan;
                }
            }
        }

        // Compute the full topological drop order via DFS. The walk returns only the
        // seed's transitive dependents, each exactly once (topological_drop_order
        // emits an object when it finishes it, not once per reaching edge); the seed
        // itself is appended last so the executor deletes its catalog rows after
        // everything that pointed at them.
        // On back-edge, cycle_at carries the offending oid — surface as
        // ddl_status::cycle_detected, blocking_oid populated for diagnostics.
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
