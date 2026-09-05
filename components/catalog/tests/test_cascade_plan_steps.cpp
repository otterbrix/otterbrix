// ============================================================================
// WHAT A DROP PLAN OWES ITS CALLER: ONE STEP PER OBJECT, AND THE SEED AMONG THEM.
//
// plan_drop is the only thing standing between a DROP statement and the catalog
// rows it is supposed to delete: operator_dynamic_cascade_delete_t walks
// plan.steps and issues a per-classid delete template for each one. A plan that
// comes back with no step for the seed is therefore not "a drop that dropped
// only the children" — it is a statement that deletes NOTHING and answers
// success, because the operator has nothing to iterate and runs straight to
// mark_executed().
//
// These are unit tests over the planner alone: fetch_deps is a plain lambda over
// a fixed edge table, so nothing here depends on disk, actors or timing.
// ============================================================================

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/cascade_planner.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <core/pmr.hpp>

#include <memory_resource>
#include <vector>

using namespace components::catalog;

namespace {

    constexpr oid_t kClass = well_known_oid::pg_class_table;
    constexpr oid_t kConstraint = well_known_oid::pg_constraint_table;

    struct edge_t {
        oid_t ref_cls;
        oid_t ref_oid;
        dependency_t dep;
    };

    // fetch_deps over a fixed edge table: every row whose (refclassid, refobjid)
    // matches the query, in insertion order — the shape manager_disk_t's
    // collect_dependents closure produces.
    fetch_deps_fn make_fetch(const std::vector<edge_t>& edges) {
        return [&edges](std::pmr::memory_resource* mr, oid_t cls, oid_t oid) {
            std::pmr::vector<dependency_t> out{mr};
            for (const auto& e : edges) {
                if (e.ref_cls == cls && e.ref_oid == oid) {
                    out.push_back(e.dep);
                }
            }
            return out;
        };
    }

    bool has_step(const cascade_plan_t& plan, oid_t cls, oid_t oid) {
        for (const auto& s : plan.steps) {
            if (s.classid == cls && s.objid == oid) {
                return true;
            }
        }
        return false;
    }

    std::size_t count_step(const cascade_plan_t& plan, oid_t cls, oid_t oid) {
        std::size_t n = 0;
        for (const auto& s : plan.steps) {
            if (s.classid == cls && s.objid == oid) {
                ++n;
            }
        }
        return n;
    }

} // namespace

// ---------------------------------------------------------------------------
// The allow-path of RESTRICT must still be a DROP.
// ---------------------------------------------------------------------------
TEST_CASE("catalog::cascade_plan::restrict_with_no_dependencies_still_drops_the_seed") {
    core::pmr::otterbrix_resource resource;
    const std::vector<edge_t> edges{}; // nothing depends on the seed

    auto plan = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::restrict_, make_fetch(edges));

    // RESTRICT has nothing to refuse here, so the statement is allowed...
    REQUIRE(plan.status == ddl_status::ok);
    // ...and "allowed" has to mean the seed's own catalog rows get deleted.
    INFO("steps planned: " << plan.steps.size());
    REQUIRE(plan.steps.size() == 1);
    CHECK(plan.steps.back().classid == kClass);
    CHECK(plan.steps.back().objid == oid_t{16400});
}

// ---------------------------------------------------------------------------
// The auto/internal children the allow-path's own comment says it is allowing
// have to be in the plan too, seed last.
// ---------------------------------------------------------------------------
TEST_CASE("catalog::cascade_plan::restrict_drops_the_auto_children_it_allows") {
    core::pmr::otterbrix_resource resource;
    const std::vector<edge_t> edges{
        {kClass, oid_t{16400}, {kClass, oid_t{16401}, deptype::auto_dep}}, // an index on the table
    };

    auto plan = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::restrict_, make_fetch(edges));

    REQUIRE(plan.status == ddl_status::ok);
    INFO("steps planned: " << plan.steps.size());
    REQUIRE(plan.steps.size() == 2);
    CHECK(has_step(plan, kClass, oid_t{16401}));
    // Seed last: the object's own rows go after everything that pointed at them.
    CHECK(plan.steps.back().objid == oid_t{16400});
}

// ---------------------------------------------------------------------------
// Control: the refusing half of RESTRICT is correct today and stays correct.
// ---------------------------------------------------------------------------
TEST_CASE("catalog::cascade_plan::restrict_still_refuses_on_a_normal_dependency") {
    core::pmr::otterbrix_resource resource;
    const std::vector<edge_t> edges{
        {kClass, oid_t{16400}, {kConstraint, oid_t{16402}, deptype::normal}},
    };

    auto plan = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::restrict_, make_fetch(edges));

    REQUIRE(plan.status == ddl_status::restrict_blocked);
    CHECK(plan.blocking_oid == oid_t{16402});
    // A refused drop plans nothing: the operator must not delete a single row.
    CHECK(plan.steps.empty());
}

// ---------------------------------------------------------------------------
// The walker owes a SET, not a multiset: an object reachable through two edges
// is one object and must be planned once.
// ---------------------------------------------------------------------------
TEST_CASE("catalog::cascade_plan::a_diamond_dependent_is_planned_once") {
    core::pmr::otterbrix_resource resource;
    // seed 16400 -> {16401, 16402}; both -> 16403 (the FK-diamond shape: a
    // constraint reachable from its own table AND from the table it references).
    const std::vector<edge_t> edges{
        {kClass, oid_t{16400}, {kClass, oid_t{16401}, deptype::auto_dep}},
        {kClass, oid_t{16400}, {kClass, oid_t{16402}, deptype::auto_dep}},
        {kClass, oid_t{16401}, {kConstraint, oid_t{16403}, deptype::auto_dep}},
        {kClass, oid_t{16402}, {kConstraint, oid_t{16403}, deptype::auto_dep}},
    };

    auto plan = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::cascade_, make_fetch(edges));

    REQUIRE(plan.status == ddl_status::ok);
    INFO("steps planned: " << plan.steps.size());
    CHECK(count_step(plan, kConstraint, oid_t{16403}) == 1);
    REQUIRE(plan.steps.size() == 4);
    CHECK(plan.steps.back().objid == oid_t{16400});
}

// ---------------------------------------------------------------------------
// Control: a cycle is still surfaced, not walked forever.
// ---------------------------------------------------------------------------
TEST_CASE("catalog::cascade_plan::a_back_edge_is_reported_as_a_cycle") {
    core::pmr::otterbrix_resource resource;
    const std::vector<edge_t> edges{
        {kClass, oid_t{16400}, {kClass, oid_t{16401}, deptype::auto_dep}},
        {kClass, oid_t{16401}, {kClass, oid_t{16400}, deptype::auto_dep}},
    };

    auto plan = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::cascade_, make_fetch(edges));

    REQUIRE(plan.status == ddl_status::cycle_detected);
    CHECK(plan.blocking_oid == oid_t{16400});
    CHECK(plan.steps.empty());
}

// ---------------------------------------------------------------------------
// THE THIRD FORM. `unspecified` is what a statement that wrote NEITHER word
// carries. Owner decision (2026-09-04): it keeps this build's historical CASCADE
// meaning, so the tree's bare DROP statements are untouched by wiring the two
// written words up; moving the default is GitHub #638. These two cases are the
// pin on that decision — if #638 flips it, THEY are what turn red, not ~205
// statements failing somewhere downstream.
// ---------------------------------------------------------------------------
TEST_CASE("catalog::cascade_plan::the_unwritten_form_still_means_cascade") {
    core::pmr::otterbrix_resource resource;
    // A NORMAL dependency: the one thing a written RESTRICT refuses.
    const std::vector<edge_t> edges{
        {kClass, oid_t{16400}, {kConstraint, oid_t{16402}, deptype::normal}},
    };

    auto written = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::restrict_, make_fetch(edges));
    REQUIRE(written.status == ddl_status::restrict_blocked);

    // The same statement without the word is NOT refused — it cascades.
    auto unwritten = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::unspecified, make_fetch(edges));
    REQUIRE(unwritten.status == ddl_status::ok);
    CHECK(has_step(unwritten, kConstraint, oid_t{16402}));
    REQUIRE(unwritten.steps.size() == 2);
    CHECK(unwritten.steps.back().objid == oid_t{16400});

    // ...and it produces exactly what a written CASCADE would.
    auto cascaded = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::cascade_, make_fetch(edges));
    REQUIRE(cascaded.steps.size() == unwritten.steps.size());
    for (std::size_t i = 0; i < cascaded.steps.size(); ++i) {
        CHECK(cascaded.steps[i].classid == unwritten.steps[i].classid);
        CHECK(cascaded.steps[i].objid == unwritten.steps[i].objid);
    }
}

TEST_CASE("catalog::cascade_plan::only_a_written_restrict_refuses") {
    // refuses_on_dependency is the ONE place the three forms collapse into the
    // two things a planner can do; pin it directly so a future edit that adds a
    // fourth form has to come through here.
    CHECK(refuses_on_dependency(drop_behavior_t::restrict_));
    CHECK_FALSE(refuses_on_dependency(drop_behavior_t::cascade_));
    CHECK_FALSE(refuses_on_dependency(drop_behavior_t::unspecified));
}

// ---------------------------------------------------------------------------
// AN OBJECT IS (classid, objid), AND THE WALK HAS TO REMEMBER IT AS ONE.
//
// The visited marks used to be keyed by the bare oid, so two objects that share
// an oid across two catalogs collapsed into one mark. That is not the same
// failure the diamond had: a duplicated step was a step too MANY and the
// executor's own-row judgement caught it, while a collapsed mark silently drops
// the second object out of the plan entirely — a DROP CASCADE that answers
// success having left one of its dependents' catalog rows in place.
//
// This build allocates oids from one counter, so the state is unreachable
// today. The walk is keyed on the whole identity anyway: the caller hands it
// (classid, objid) pairs and gets (classid, objid) steps back, so nothing in
// its signature promises the oid alone is unique, and a future per-catalog
// counter must not turn that unwritten assumption into missing deletes.
// ---------------------------------------------------------------------------
TEST_CASE("catalog::cascade_plan::two_objects_sharing_an_oid_are_two_steps") {
    core::pmr::otterbrix_resource resource;
    // Same oid 16401 in two different catalogs: two distinct objects.
    const std::vector<edge_t> edges{
        {kClass, oid_t{16400}, {kClass, oid_t{16401}, deptype::auto_dep}},
        {kClass, oid_t{16400}, {kConstraint, oid_t{16401}, deptype::auto_dep}},
    };

    auto plan = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::cascade_, make_fetch(edges));

    REQUIRE(plan.status == ddl_status::ok);
    INFO("steps planned: " << plan.steps.size());
    CHECK(has_step(plan, kClass, oid_t{16401}));
    CHECK(has_step(plan, kConstraint, oid_t{16401}));
    REQUIRE(plan.steps.size() == 3);
    CHECK(plan.steps.back().objid == oid_t{16400});
}
