#include "dependency_walker.hpp"

#include <cstdint>

namespace components::catalog {

    namespace {
        // (classid, objid) is the mark key, not the bare oid: a bare oid could collapse two catalogs' objects sharing
        // it into one and drop from the plan.
        constexpr std::uint64_t mark_key(oid_t cls, oid_t oid) noexcept {
            return (static_cast<std::uint64_t>(cls) << 32) | static_cast<std::uint64_t>(oid);
        }

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

        void dfs(walk_state& st,
                 std::pmr::memory_resource* resource,
                 const fetch_deps_fn& fetch_deps,
                 oid_t cls,
                 oid_t oid,
                 const dependency_t* via) {
            if (st.cycle_at != INVALID_OID)
                return;
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
            // Emitted on completion, not per incoming edge: a diamond dependency would otherwise come back twice, and
            // the second occurrence's delete reads as "the object is missing".
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