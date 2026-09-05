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

// A DDL REWRITE MUST NEVER STAMP A CATALOG ROW WITH AN OID NOTHING ALLOCATED.
//
// The DDL path pre-allocates OIDs in one disk round and then rewrites the statement into
// pg_class / pg_attribute / pg_depend rows synchronously, taking each identity out of that
// batch. Two things could hand the rewrite fewer OIDs than it consumes:
//
//   * the allocation round refuses (it reported BOTH of its failures as an EMPTY vector, and
//     nothing compared what came back with the demand that had just been computed for it);
//   * compute_oid_demand and the rewrite_* functions drift apart — they live in different
//     files and their counts are kept equal by hand.
//
// Either way the batch runs out mid-rewrite, and the old guard was an assert inside
// oid_batch_t: gone under NDEBUG, leaving a read PAST THE END of the vector whose result was
// written into a DURABLE catalog. These cases pin the refusal instead — and pin that a demand
// of zero stays a success, because DROP / ALTER TABLE really do rewrite without an OID.

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
    // empty — the exact value both failure branches of the round used to answer with).
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

    // The demand and the batch agree, so the size check passes — and the rewrite still needs
    // one more OID than either of them says. This is the drift case: the batch runs out INSIDE
    // walk_ddl, which is what the assert used to "guard" and what NDEBUG turned into an
    // out-of-bounds read feeding a durable pg_attribute row.
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

    // DROP consumes no OID, so its caller runs no allocation round at all and hands the
    // planner an empty batch. That is the normal shape for DROP / ALTER TABLE / a CREATE
    // MATERIALIZED VIEW with no inferred columns — it must not be mistaken for a failure.
    const std::size_t need = components::planner::compute_oid_demand(node.get());
    REQUIRE(need == 0);

    components::planner::planner_t planner;
    auto rewritten = planner.create_plan(&resource, node, std::vector<oid_t>{}, need);

    REQUIRE_FALSE(rewritten.has_error());
    REQUIRE(rewritten.value() != nullptr);
}
