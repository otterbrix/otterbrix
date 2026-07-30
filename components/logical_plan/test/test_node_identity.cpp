#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_check_constraint.hpp>
#include <components/logical_plan/node_sequence.hpp>

#include <memory_resource>
#include <string>
#include <vector>

using namespace components::logical_plan;

// node_check_constraint_t once re-declared table_oid()/set_table_oid() and a
// private table_oid_ that SHADOWED the non-virtual node_t members: every writer
// (planner.cpp's rewrite_insert / rewrite_update) stamped through the derived
// type, the base field stayed INVALID_OID, and node_t::table_oid_dependencies_()
// — which reads the base field — never saw the constraint node's table. The node
// now has only the base members; this pins that a stamped oid is visible through
// the base sub-object and reaches the dependency walk.
TEST_CASE("logical_plan::node_check_constraint_t reports its table oid as a dependency") {
    auto* resource = std::pmr::get_default_resource();
    constexpr components::catalog::oid_t table_oid = 16400;

    auto cc = boost::intrusive_ptr(new node_check_constraint_t(resource,
                                                               core::dbname_t{},
                                                               core::relname_t{},
                                                               std::vector<std::string>{"id"}));
    // Stamped exactly as components/planner/planner.cpp does it: through the
    // derived pointer type the planner already holds.
    cc->set_table_oid(table_oid);

    // Sanity: the derived accessor round-trips.
    CHECK(cc->table_oid() == table_oid);

    // The same oid read through the base sub-object -- the only field the
    // dependency walker looks at.
    const node_t* as_base = cc.get();
    CHECK(as_base->table_oid() == table_oid);

    // Plan shape: check-constraint sink hanging under a sequence root, mirroring
    // the planner's rewrite where the constraint node wraps the DML pipeline.
    auto root = boost::intrusive_ptr(new node_sequence_t(resource));
    root->append_child(cc);

    const auto deps = root->table_oid_dependencies();
    REQUIRE(deps.count(table_oid) == 1);
}
