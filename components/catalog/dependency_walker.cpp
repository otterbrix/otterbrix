#include "dependency_walker.hpp"

#include <cstdint>

namespace components::catalog {

    namespace {
        // An object is (classid, objid), and that whole pair is the mark key: keying on the
        // bare oid would collapse two objects sharing an oid across catalogs into one, silently
        // dropping the second from the plan. Today every oid comes from one counter so the
        // collision cannot occur, but the signature takes/returns (classid, objid) pairs and
        // the walk does not lean on that invariant holding.
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
            // EMITTED ON COMPLETION, NOT ONCE PER INCOMING EDGE: pushing in the loop above
            // emitted a node once per edge, so a diamond (an FK constraint reachable both from
            // its own table and from the table it references) came back twice -- and the second
            // occurrence's own catalog row is already gone, so its delete counts 0 and reads as
            // "the object is missing". Emitting here, right after the node turns black, gives
            // one entry per object and keeps dependents-before-parent order. The seed carries
            // no incoming edge and is the caller's to append.
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