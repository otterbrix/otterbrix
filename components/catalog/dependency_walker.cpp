#include "dependency_walker.hpp"

namespace components::catalog {

    namespace {
        // DFS traversal with cycle detection via tri-color marks:
        //   white = unvisited, gray = on current stack, black = fully processed.
        // Hitting gray = back-edge = cycle. Hitting black = re-rooted path, skip.
        struct walk_state {
            std::unordered_set<oid_t> gray;
            std::unordered_set<oid_t> black;
            std::vector<dependency_t> order; // dependents-first; seed pushed last by caller
        };

        void dfs(walk_state& st,
                 const fetch_deps_fn& fetch_deps,
                 oid_t cls,
                 oid_t oid) {
            if (st.black.count(oid))
                return;
            if (st.gray.count(oid))
                throw cycle_detected_error{oid};
            st.gray.insert(oid);

            for (const auto& dep : fetch_deps(cls, oid)) {
                dfs(st, fetch_deps, dep.classid, dep.objid);
                st.order.push_back(dep);
            }

            st.gray.erase(oid);
            st.black.insert(oid);
        }
    } // namespace

    std::vector<dependency_t>
    topological_drop_order(oid_t seed_cls,
                           oid_t seed_oid,
                           const fetch_deps_fn& fetch_deps) {
        walk_state st;
        dfs(st, fetch_deps, seed_cls, seed_oid);
        return std::move(st.order);
    }

} // namespace components::catalog
