#pragma once

#include <components/catalog/catalog_oids.hpp>

#include <functional>
#include <stdexcept>
#include <unordered_set>
#include <vector>

namespace components::catalog {

    // pg_depend.deptype semantics (subset actually used by otterbrix):
    //   'n' — normal: external dependency; DROP RESTRICT blocks, DROP CASCADE cascades.
    //   'a' — auto:   maintained automatically (e.g. index→table); always
    //                 auto-cascaded, never blocks RESTRICT.
    namespace deptype {
        inline constexpr char normal   = 'n';
        // 'auto' is a C++ keyword — use auto_dep for the named constant.
        inline constexpr char auto_dep = 'a';

        // Returns true if this deptype should cause RESTRICT to block.
        // Only 'n' (normal) external dependencies block RESTRICT; 'a' (auto)
        // dependencies are always auto-cascaded with the parent.
        inline constexpr bool blocks_restrict(char dt) noexcept {
            return dt == normal;
        }
    } // namespace deptype

    // dependency_walker — topologically traverses pg_depend starting from a (refclassid,
    // refobjid) seed, invoking a visitor on each transitive dependent in REVERSE topological
    // order (deepest dependents first, seed last). Detects cycles by tracking the in-flight
    // path; throws cycle_detected_error with the offending oid when a back-edge is found.
    // Used by ddl_drop_* CASCADE paths to schedule child drops before parent drops without
    // hanging on pathological pg_depend cycles (no FKs in our schema → cycles ARE pathological).
    //
    // This walker DOES NOT itself mutate state — it only computes the drop order. Each ddl_drop_*
    // remains responsible for its own MVCC delete + invalidation event emission.
    struct dependency_t {
        oid_t classid{0};   // catalog hosting dependent (e.g. pg_class.oid)
        oid_t objid{0};     // dependent's own oid
        char deptype{'n'};   // 'n' normal, 'a' auto
    };

    class cycle_detected_error : public std::runtime_error {
    public:
        explicit cycle_detected_error(oid_t at)
            : std::runtime_error("pg_depend cycle detected at oid " + std::to_string(at))
            , offending_oid_(at) {}
        oid_t offending_oid() const noexcept { return offending_oid_; }

    private:
        oid_t offending_oid_;
    };

    // The fetch_deps callback should return all pg_depend rows where (refclassid, refobjid)
    // matches the supplied (cls, oid). Implemented over manager_disk_t::collect_dependents.
    using fetch_deps_fn =
        std::function<std::vector<dependency_t>(oid_t cls, oid_t oid)>;

    // Walk from (seed_cls, seed_oid). Returns dependents in reverse topological order:
    // children before parents, seed at end. Throws cycle_detected_error on back-edges.
    std::vector<dependency_t>
    topological_drop_order(oid_t seed_cls,
                           oid_t seed_oid,
                           const fetch_deps_fn& fetch_deps);

} // namespace components::catalog
