#pragma once

#include <components/catalog/catalog_oids.hpp>

#include <functional>
#include <memory_resource>
#include <unordered_set>
#include <vector>

namespace components::catalog {

    // pg_depend.deptype semantics (subset actually used by otterbrix):
    //   'n' — normal: external dependency; DROP RESTRICT blocks, DROP CASCADE cascades.
    //   'a' — auto:   maintained automatically (e.g. index→table); always
    //                 auto-cascaded, never blocks RESTRICT.
    namespace deptype {
        inline constexpr char normal = 'n';
        // 'auto' is a C++ keyword — use auto_dep for the named constant.
        inline constexpr char auto_dep = 'a';

        // Returns true if this deptype should cause RESTRICT to block.
        // Only 'n' (normal) external dependencies block RESTRICT; 'a' (auto)
        // dependencies are always auto-cascaded with the parent.
        inline constexpr bool blocks_restrict(char dt) noexcept { return dt == normal; }
    } // namespace deptype

    // dependency_walker — topologically traverses pg_depend starting from a (refclassid,
    // refobjid) seed and RETURNS the seed's transitive dependents in reverse topological
    // order (deepest dependents first). It takes no visitor, and the seed is NOT among
    // the returned objects — the caller appends its own step, which is what makes the
    // seed's catalog rows go last. Cycles are detected by tracking the in-flight path
    // and reported through the `cycle_at` out-parameter; NOTHING here throws (rule 2 —
    // this build has no exceptions, so a back-edge cannot be signalled by one).
    // Used by the DROP planner to schedule child drops before parent drops without
    // hanging on pathological pg_depend cycles.
    //
    // This walker DOES NOT itself mutate state — it only computes the drop order. The
    // caller remains responsible for its own MVCC delete + invalidation event emission.
    struct dependency_t {
        oid_t classid{0};  // catalog hosting dependent (e.g. pg_class.oid)
        oid_t objid{0};    // dependent's own oid
        char deptype{'n'}; // 'n' normal, 'a' auto
    };

    // The fetch_deps callback should return all pg_depend rows where (refclassid, refobjid)
    // matches the supplied (cls, oid). Implemented over manager_disk_t::collect_dependents.
    // The memory_resource* is the arena the caller wants the returned vector to live in.
    using fetch_deps_fn =
        std::function<std::pmr::vector<dependency_t>(std::pmr::memory_resource* resource, oid_t cls, oid_t oid)>;

    // Walk from (seed_cls, seed_oid). Returns dependents in reverse topological order:
    // children before parents. The seed itself is NOT in the result — the caller
    // appends it last.
    //
    // A SET, NOT A MULTISET: each reachable object appears EXACTLY ONCE, carrying the
    // deptype of the edge it was first discovered through, however many edges reach
    // it. Callers that judge a per-object outcome ("this step's own catalog row
    // deleted 0 rows") depend on that: a repeated object's second delete legitimately
    // counts 0 and would read as a missing object.
    //
    // "Object" here means the WHOLE (classid, objid) pair, which is also what the
    // visited marks are keyed on. Two objects sharing an oid in two catalogs are two
    // entries, not one — collapsing them would drop the second out of the result
    // silently, and a caller cannot delete what the plan does not name.
    //
    // On pg_depend back-edge, sets \p cycle_at to the offending oid and returns a
    // partial order — caller must check `cycle_at == INVALID_OID` before using the
    // result. INVALID_OID = success.
    std::pmr::vector<dependency_t> topological_drop_order(std::pmr::memory_resource* resource,
                                                          oid_t seed_cls,
                                                          oid_t seed_oid,
                                                          const fetch_deps_fn& fetch_deps,
                                                          oid_t& cycle_at);

} // namespace components::catalog
