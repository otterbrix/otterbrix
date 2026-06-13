#include "dependency_walker.hpp"

namespace components::catalog {

    namespace {
        // DFS traversal with cycle detection via tri-color marks:
        //   white = unvisited, gray = on current stack, black = fully processed.
        // Hitting gray = back-edge = cycle. Hitting black = re-rooted path, skip.
        // On cycle detection, cycle_at is set to the offending oid and recursion
        // unwinds without further work (no exceptions).
        struct walk_state {
            explicit walk_state(std::pmr::memory_resource* resource)
                : gray(resource)
                , black(resource)
                , order(resource)
                , cycle_at(INVALID_OID) {}

            std::pmr::unordered_set<oid_t> gray;
            std::pmr::unordered_set<oid_t> black;
            std::pmr::vector<dependency_t> order; // dependents-first; seed pushed last by caller
            oid_t cycle_at;
        };

        void dfs(walk_state& st,
                 std::pmr::memory_resource* resource,
                 const fetch_deps_fn& fetch_deps,
                 oid_t cls,
                 oid_t oid) {
            if (st.cycle_at != INVALID_OID)
                return; // propagating up after cycle hit
            if (st.black.count(oid))
                return;
            if (st.gray.count(oid)) {
                st.cycle_at = oid;
                return;
            }
            st.gray.insert(oid);

            for (const auto& dep : fetch_deps(resource, cls, oid)) {
                dfs(st, resource, fetch_deps, dep.classid, dep.objid);
                if (st.cycle_at != INVALID_OID)
                    return;
                st.order.push_back(dep);
            }

            st.gray.erase(oid);
            st.black.insert(oid);
        }
    } // namespace

    std::pmr::vector<dependency_t> topological_drop_order(std::pmr::memory_resource* resource,
                                                          oid_t seed_cls,
                                                          oid_t seed_oid,
                                                          const fetch_deps_fn& fetch_deps,
                                                          oid_t& cycle_at) {
        walk_state st{resource};
        dfs(st, resource, fetch_deps, seed_cls, seed_oid);
        cycle_at = st.cycle_at;
        return std::move(st.order);
    }

} // namespace components::catalog