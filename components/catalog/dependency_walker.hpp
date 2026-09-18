#pragma once

#include <components/catalog/catalog_oids.hpp>

#include <functional>
#include <memory_resource>
#include <unordered_set>
#include <vector>

namespace components::catalog {

    // pg_depend.deptype: 'n' (normal) blocks DROP RESTRICT; 'a' (auto) is always cascaded, never blocks.
    namespace deptype {
        inline constexpr char normal = 'n';
        inline constexpr char auto_dep = 'a';

        inline constexpr bool blocks_restrict(char dt) noexcept { return dt == normal; }
    } // namespace deptype

    struct dependency_t {
        oid_t classid{0}; // catalog hosting dependent (e.g. pg_class.oid)
        oid_t objid{0};
        char deptype{'n'};
    };

    // Implemented over manager_disk_t::collect_dependents.
    using fetch_deps_fn =
        std::function<std::pmr::vector<dependency_t>(std::pmr::memory_resource* resource, oid_t cls, oid_t oid)>;

    // Reverse topological order (children before parents), seed excluded (caller appends it
    // last); a set, not a multiset, each (classid, objid) pair appears exactly once (the pair
    // is the identity, not the bare oid). On pg_depend back-edge, sets \p cycle_at to the
    // offending oid and returns a partial order -- caller must check `cycle_at == INVALID_OID`.
    std::pmr::vector<dependency_t> topological_drop_order(std::pmr::memory_resource* resource,
                                                          oid_t seed_cls,
                                                          oid_t seed_oid,
                                                          const fetch_deps_fn& fetch_deps,
                                                          oid_t& cycle_at);

} // namespace components::catalog
