#include "cascade_planner.hpp"

namespace components::catalog {

    cascade_plan_t plan_drop(oid_t seed_classid,
                              oid_t seed_oid,
                              services::disk::drop_behavior_t behavior,
                              const fetch_deps_fn& fetch_deps) {
        cascade_plan_t plan;

        auto deps = fetch_deps(seed_classid, seed_oid);

        if (behavior == services::disk::drop_behavior_t::restrict_) {
            // RESTRICT: block if any 'n' (normal) external dependency exists.
            for (const auto& d : deps) {
                if (deptype::blocks_restrict(d.deptype)) {
                    plan.status       = services::disk::ddl_status::restrict_blocked;
                    plan.blocking_oid = d.objid;
                    return plan;
                }
            }
            // No external deps → RESTRICT allows the drop (only auto/internal children).
            return plan;
        }

        // CASCADE: compute full topological drop order via DFS.
        try {
            auto ordered = topological_drop_order(seed_classid, seed_oid, fetch_deps);
            plan.steps.reserve(ordered.size());
            for (const auto& d : ordered) {
                plan.steps.push_back({d.classid, d.objid, d.deptype});
            }
        } catch (const cycle_detected_error& e) {
            plan.status       = services::disk::ddl_status::cycle_detected;
            plan.blocking_oid = e.offending_oid();
        }
        return plan;
    }

} // namespace components::catalog
