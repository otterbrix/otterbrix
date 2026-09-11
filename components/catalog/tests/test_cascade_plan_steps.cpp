// operator_dynamic_cascade_delete_t walks plan.steps and deletes one classid template per step,
// so a plan missing a step for the seed would silently drop nothing while reporting success;
// these are unit tests over the planner alone, with fetch_deps a plain lambda (no disk/actors/timing).

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

}

TEST_CASE("catalog::cascade_plan::restrict_with_no_dependencies_still_drops_the_seed") {
    core::pmr::otterbrix_resource resource;
    const std::vector<edge_t> edges{};

    auto plan = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::restrict_, make_fetch(edges));

    REQUIRE(plan.status == ddl_status::ok);
    INFO("steps planned: " << plan.steps.size());
    REQUIRE(plan.steps.size() == 1);
    CHECK(plan.steps.back().classid == kClass);
    CHECK(plan.steps.back().objid == oid_t{16400});
}

TEST_CASE("catalog::cascade_plan::restrict_drops_the_auto_children_it_allows") {
    core::pmr::otterbrix_resource resource;
    const std::vector<edge_t> edges{
        {kClass, oid_t{16400}, {kClass, oid_t{16401}, deptype::auto_dep}},
    };

    auto plan = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::restrict_, make_fetch(edges));

    REQUIRE(plan.status == ddl_status::ok);
    INFO("steps planned: " << plan.steps.size());
    REQUIRE(plan.steps.size() == 2);
    CHECK(has_step(plan, kClass, oid_t{16401}));
    // Seed last: the object's own rows go after everything that pointed at them.
    CHECK(plan.steps.back().objid == oid_t{16400});
}

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

TEST_CASE("catalog::cascade_plan::a_diamond_dependent_is_planned_once") {
    core::pmr::otterbrix_resource resource;
    // seed 16400 -> {16401, 16402}; both -> 16403 (the diamond: one constraint reachable two ways).
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

// DROP with neither word written means RESTRICT, matching PostgreSQL: drop_behavior_of maps the
// grammar's shared DROP_RESTRICT token to restrict_ directly.
TEST_CASE("catalog::cascade_plan::the_unwritten_form_means_restrict") {
    core::pmr::otterbrix_resource resource;
    // A NORMAL dependency: the one thing RESTRICT refuses.
    const std::vector<edge_t> edges{
        {kClass, oid_t{16400}, {kConstraint, oid_t{16402}, deptype::normal}},
    };

    auto bare = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::restrict_, make_fetch(edges));
    REQUIRE(bare.status == ddl_status::restrict_blocked);
    CHECK(bare.blocking_oid == oid_t{16402});
    CHECK(bare.steps.empty());

    auto cascaded = plan_drop(&resource, kClass, oid_t{16400}, drop_behavior_t::cascade_, make_fetch(edges));
    REQUIRE(cascaded.status == ddl_status::ok);
    CHECK(has_step(cascaded, kConstraint, oid_t{16402}));
    REQUIRE(cascaded.steps.size() == 2);
    CHECK(cascaded.steps.back().objid == oid_t{16400});
}

TEST_CASE("catalog::cascade_plan::only_a_written_restrict_refuses") {
    // Pins the one place behavior collapses to a bool, so a future third form must touch here.
    CHECK(refuses_on_dependency(drop_behavior_t::restrict_));
    CHECK_FALSE(refuses_on_dependency(drop_behavior_t::cascade_));
}

// Visited marks are keyed on (classid, objid), not the bare oid: a mark keyed on oid alone
// would collapse two objects sharing an oid across catalogs into one, silently dropping the
// second from the plan. Unreachable today (oids come from one global counter) but the walk
// makes no promise the oid alone is unique, so a future per-catalog counter must not regress this.
TEST_CASE("catalog::cascade_plan::two_objects_sharing_an_oid_are_two_steps") {
    core::pmr::otterbrix_resource resource;
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
