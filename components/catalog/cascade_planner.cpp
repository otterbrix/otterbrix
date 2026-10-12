#include "cascade_planner.hpp"

#include <cstdint>
#include <string>
#include <unordered_map>

namespace components::catalog {

    namespace {
        constexpr std::uint64_t object_key(oid_t cls, oid_t oid) noexcept {
            return (static_cast<std::uint64_t>(cls) << 32) | static_cast<std::uint64_t>(oid);
        }

        struct reached_t {
            oid_t objid{INVALID_OID};
            bool owned{false};
        };

        // PostgreSQL 18 findDependentObjects / reportDependentObjects: the whole closure is walked, and an object
        // that no auto or internal edge of the closure reaches is a normal dependent the drop would take along.
        oid_t first_normal_dependent(std::pmr::memory_resource* resource,
                                     oid_t seed_classid,
                                     oid_t seed_oid,
                                     const fetch_deps_fn& fetch_deps) {
            std::pmr::unordered_map<std::uint64_t, std::size_t> index(resource);
            std::pmr::vector<reached_t> reached(resource);
            std::pmr::vector<dependency_t> pending(resource);
            index.emplace(object_key(seed_classid, seed_oid), reached.size());
            reached.push_back({seed_oid, true});
            pending.push_back({seed_classid, seed_oid, deptype::auto_dep});
            for (std::size_t next = 0; next < pending.size(); ++next) {
                const auto from = pending[next];
                for (const auto& dep : fetch_deps(resource, from.classid, from.objid)) {
                    const auto key = object_key(dep.classid, dep.objid);
                    auto it = index.find(key);
                    if (it == index.end()) {
                        it = index.emplace(key, reached.size()).first;
                        reached.push_back({dep.objid, false});
                        pending.push_back(dep);
                    }
                    if (!deptype::blocks_restrict(dep.deptype)) {
                        reached[it->second].owned = true;
                    }
                }
            }
            for (const auto& object : reached) {
                if (!object.owned) {
                    return object.objid;
                }
            }
            return INVALID_OID;
        }
    } // namespace

    cascade_plan_t plan_drop(std::pmr::memory_resource* resource,
                             oid_t seed_classid,
                             oid_t seed_oid,
                             drop_behavior_t behavior,
                             const fetch_deps_fn& fetch_deps) {
        cascade_plan_t plan{resource};

        if (refuses_on_dependency(behavior)) {
            if (const auto blocking = first_normal_dependent(resource, seed_classid, seed_oid, fetch_deps);
                blocking != INVALID_OID) {
                plan.status = ddl_status::restrict_blocked;
                plan.blocking_oid = blocking;
                return plan;
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

    core::error_t
    dependent_objects_error(std::pmr::memory_resource* resource, std::string_view target, oid_t blocking_oid) {
        std::pmr::string msg{"cannot drop ", resource};
        msg.append(target);
        msg.append(" because other objects depend on it\nDETAIL: object with oid ");
        msg.append(std::to_string(blocking_oid));
        msg.append(" depends on ");
        msg.append(target);
        msg.append("\nHINT: Use DROP ... CASCADE to drop the dependent objects too.");
        return core::error_t{core::error_code_t::other_error, std::move(msg)};
    }

} // namespace components::catalog
