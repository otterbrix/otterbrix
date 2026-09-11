#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/planner/planner.hpp>
#include <core/pmr.hpp>

#include <cstddef>
#include <string>
#include <vector>

// An unresolved DDL target (INVALID_OID left by enrich_logical_plan when the named object doesn't exist)
// must be refused by the planner rewrite, not passed through: rewrite_create_index skipping the rewrite
// would report success for an index never created, and rewrite_drop_index emitting only the teardown
// marker would report success for a DROP over garbage (its no-row-deleted check never fires — no delete
// spec is emitted). Both are silent successes; these cases pin the refusal.

namespace {

    using components::catalog::oid_t;

} // namespace

TEST_CASE("components::planner::ddl_unresolved::create_index_on_a_missing_table_is_refused") {
    auto resource = core::pmr::otterbrix_resource();
    auto node = components::logical_plan::make_node_create_index(&resource, core::indexname_t{std::string{"idx"}});
    node->set_dbname(std::string{"db"});
    node->set_relname(std::string{"no_such_table"});
    // enrich left namespace_oid()/table_oid() at INVALID_OID: the table is not in the catalog.

    const std::size_t need = components::planner::compute_oid_demand(node.get());
    REQUIRE(need == 1);

    components::planner::planner_t planner;
    auto rewritten = planner.create_plan(&resource, node, std::vector<oid_t>{oid_t{16384}}, need);

    INFO("CREATE INDEX on a table enrich could not resolve must refuse, not pass a dead marker through");
    REQUIRE(rewritten.has_error());
    REQUIRE(rewritten.error().type == core::error_code_t::table_not_exists);
}

TEST_CASE("components::planner::ddl_unresolved::drop_index_on_a_missing_index_is_refused") {
    auto resource = core::pmr::otterbrix_resource();
    auto node = components::logical_plan::make_node_drop(&resource, components::logical_plan::drop_target_kind::index);
    node->set_dbname(std::string{"db"});
    node->set_relname(std::string{"t"});
    node->set_index_name(std::string{"no_such_index"});
    // enrich left index_oid() at INVALID_OID: no pg_class row answered to the name.

    const std::size_t need = components::planner::compute_oid_demand(node.get());
    REQUIRE(need == 0);

    components::planner::planner_t planner;
    auto rewritten = planner.create_plan(&resource, node, std::vector<oid_t>{}, need);

    INFO("DROP INDEX on an index enrich could not resolve must refuse, not report success");
    REQUIRE(rewritten.has_error());
    REQUIRE(rewritten.error().type == core::error_code_t::index_not_exists);
}

TEST_CASE("components::planner::ddl_unresolved::drop_index_if_exists_on_a_missing_index_is_a_noop_success") {
    auto resource = core::pmr::otterbrix_resource();
    auto node = components::logical_plan::make_node_drop(&resource, components::logical_plan::drop_target_kind::index);
    node->set_dbname(std::string{"db"});
    node->set_relname(std::string{"t"});
    node->set_index_name(std::string{"no_such_index"});
    node->set_missing_ok(true); // `DROP INDEX IF EXISTS ...`

    components::planner::planner_t planner;
    auto rewritten = planner.create_plan(&resource, node, std::vector<oid_t>{}, 0);

    INFO("IF EXISTS is the ONE form PostgreSQL lets pass on a missing index: an empty "
         "sequence — nothing to scrub, nothing to tear down, success");
    REQUIRE_FALSE(rewritten.has_error());
    REQUIRE(rewritten.value() != nullptr);
    REQUIRE(rewritten.value()->type() == components::logical_plan::node_type::sequence_t);
    REQUIRE(rewritten.value()->children().empty());
}
