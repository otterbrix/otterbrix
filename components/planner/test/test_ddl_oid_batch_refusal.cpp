#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/oid_batch.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/planner/planner.hpp>
#include <components/table/column_definition.hpp>
#include <components/types/types.hpp>
#include <core/pmr.hpp>

#include <cstddef>
#include <string>
#include <vector>

// A DDL rewrite must never stamp a catalog row with an OID nothing allocated: if the batch runs out
// mid-rewrite (a refused allocation round, or compute_oid_demand drifting from the rewrite_* functions),
// an assert inside oid_batch_t is no guard — it's gone under NDEBUG, leaving a read past the end of the
// vector written into a durable catalog row. These cases pin the refusal instead.

namespace {

    using components::catalog::oid_t;

    std::vector<components::table::column_definition_t> two_columns() {
        std::vector<components::table::column_definition_t> cols;
        cols.emplace_back("id", components::types::complex_logical_type(components::types::logical_type::BIGINT));
        cols.emplace_back("payload",
                          components::types::complex_logical_type(components::types::logical_type::STRING_LITERAL));
        return cols;
    }

    // A fresh node per case: create_plan MUTATES the node it rewrites (it stamps table_oid and
    // attoids and clears the constraint children), so no two cases may share one.
    components::logical_plan::node_create_collection_ptr make_create_table(std::pmr::memory_resource* resource) {
        return components::logical_plan::make_node_create_collection(resource,
                                                                     core::relname_t{std::string{"t"}},
                                                                     two_columns(),
                                                                     {});
    }

} // namespace

TEST_CASE("components::planner::ddl_oid_batch::a_full_batch_is_rewritten_and_stamps_the_table_oid") {
    auto resource = core::pmr::otterbrix_resource();
    auto node = make_create_table(&resource);

    const std::size_t need = components::planner::compute_oid_demand(node.get());
    REQUIRE(need == 3); // pg_class oid + one attoid per column

    const std::vector<oid_t> batch{16384, 16385, 16386};
    components::planner::planner_t planner;
    auto rewritten = planner.create_plan(&resource, node, batch, need);

    REQUIRE_FALSE(rewritten.has_error());
    REQUIRE(rewritten.value() != nullptr);
    // The rewrite keeps the create node as child 0 and stamps it with the pg_class oid it
    // just minted — the identity the physical plan generator hands to storage.
    REQUIRE(node->table_oid() == batch.front());
}

TEST_CASE("components::planner::ddl_oid_batch::a_round_that_delivered_fewer_oids_than_asked_is_refused") {
    auto resource = core::pmr::otterbrix_resource();
    auto node = make_create_table(&resource);

    const std::size_t need = components::planner::compute_oid_demand(node.get());
    REQUIRE(need == 3);

    // What a refused allocation round looks like to this call: the batch is short (here,
    // empty — the value both failure branches of the round answer with).
    components::planner::planner_t planner;
    auto rewritten = planner.create_plan(&resource, node, std::vector<oid_t>{}, need);

    INFO("a short allocation round must refuse the statement, not rewrite it from a batch that ran out");
    REQUIRE(rewritten.has_error());
    REQUIRE(rewritten.error().type == core::error_code_t::io_error);
    // Nothing was minted onto the node.
    REQUIRE(node->table_oid() == components::catalog::INVALID_OID);
}

TEST_CASE("components::planner::ddl_oid_batch::a_rewrite_that_consumes_more_than_the_demand_is_refused") {
    auto resource = core::pmr::otterbrix_resource();
    auto node = make_create_table(&resource);

    // Demand and batch agree, but the rewrite needs one more OID than either says — the drift case,
    // where the batch runs out INSIDE walk_ddl.
    const std::vector<oid_t> batch{16384, 16385};
    components::planner::planner_t planner;
    auto rewritten = planner.create_plan(&resource, node, batch, batch.size());

    INFO("a rewrite that outruns its batch must refuse, not stamp INVALID_OID into the catalog");
    REQUIRE(rewritten.has_error());
    REQUIRE(rewritten.error().type == core::error_code_t::create_physical_plan_error);
}

TEST_CASE("components::planner::ddl_oid_batch::a_demand_of_zero_with_an_empty_batch_is_a_success") {
    auto resource = core::pmr::otterbrix_resource();
    auto node = components::logical_plan::make_node_drop(&resource,
                                                        components::logical_plan::drop_target_kind::database);

    // DROP consumes no OID, so its caller hands the planner an empty batch — normal, not a failure.
    const std::size_t need = components::planner::compute_oid_demand(node.get());
    REQUIRE(need == 0);

    components::planner::planner_t planner;
    auto rewritten = planner.create_plan(&resource, node, std::vector<oid_t>{}, need);

    REQUIRE_FALSE(rewritten.has_error());
    REQUIRE(rewritten.value() != nullptr);
}
