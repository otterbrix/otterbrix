#include "dependency_walker.hpp"

#include <cstdint>

namespace components::catalog {

    namespace {
        // AN OBJECT IS (classid, objid), AND THAT WHOLE PAIR IS THE MARK KEY. Keying
        // the marks on the bare oid collapses two objects that share an oid across
        // two catalogs into one, and the second one is then never emitted at all —
        // a plan silently short one delete, which is a worse answer than the
        // duplicate step this walk used to produce. Today every oid in the build
        // comes from one counter, so the collision cannot occur; the signature
        // takes and returns (classid, objid) pairs and promises nothing about the
        // oid alone, so the walk does not lean on that.
        constexpr std::uint64_t mark_key(oid_t cls, oid_t oid) noexcept {
            return (static_cast<std::uint64_t>(cls) << 32) | static_cast<std::uint64_t>(oid);
        }

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

            std::pmr::unordered_set<std::uint64_t> gray;
            std::pmr::unordered_set<std::uint64_t> black;
            std::pmr::vector<dependency_t> order; // dependents-first; seed pushed last by caller
            oid_t cycle_at;
        };

        // \p via is the edge this node was reached through, or nullptr for the seed
        // (which the caller appends itself). It is what gets emitted once the node
        // is finished, so the order carries the deptype of the DISCOVERY edge.
        void dfs(walk_state& st,
                 std::pmr::memory_resource* resource,
                 const fetch_deps_fn& fetch_deps,
                 oid_t cls,
                 oid_t oid,
                 const dependency_t* via) {
            if (st.cycle_at != INVALID_OID)
                return; // propagating up after cycle hit
            const std::uint64_t key = mark_key(cls, oid);
            if (st.black.count(key))
                return;
            if (st.gray.count(key)) {
                st.cycle_at = oid;
                return;
            }
            st.gray.insert(key);

            for (const auto& dep : fetch_deps(resource, cls, oid)) {
                dfs(st, resource, fetch_deps, dep.classid, dep.objid, &dep);
                if (st.cycle_at != INVALID_OID)
                    return;
            }

            st.gray.erase(key);
            st.black.insert(key);
            // EMITTED ON COMPLETION, NOT ONCE PER INCOMING EDGE. Pushing in the loop
            // above emitted a node once for every edge that reached it, so a diamond
            // — an FK constraint reachable both from its own table and from the table
            // it references — came back twice. Two entries for one object is not a
            // harmless repeat for a caller that judges per-step results: the second
            // occurrence's own catalog row is already gone, so its delete legitimately
            // counts 0 and reads as "the object is missing". Emitting here, right
            // after the node turns black, gives exactly one entry per object and keeps
            // the dependents-before-parent order (every child finished before we got
            // here). The seed carries no incoming edge and is the caller's to append.
            if (via != nullptr) {
                st.order.push_back(*via);
            }
        }
    } // namespace

    std::pmr::vector<dependency_t> topological_drop_order(std::pmr::memory_resource* resource,
                                                          oid_t seed_cls,
                                                          oid_t seed_oid,
                                                          const fetch_deps_fn& fetch_deps,
                                                          oid_t& cycle_at) {
        walk_state st{resource};
        dfs(st, resource, fetch_deps, seed_cls, seed_oid, /*via=*/nullptr);
        cycle_at = st.cycle_at;
        return std::move(st.order);
    }

} // namespace components::catalog