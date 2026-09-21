#include <catch2/catch_test_macros.hpp>
#include <components/logical_plan/node_transaction.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using components::logical_plan::node_transaction_t;
using components::logical_plan::node_type;
using components::logical_plan::transaction_op;

TEST_CASE("components::sql::transaction::spellings_map_to_their_operation") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);

    const auto operation_of = [&](const char* query) {
        auto statement = linitial(raw_parser(&arena_resource, query));
        transform::transformer local(&resource, query);
        auto result = local.transform(transform::pg_cell_to_node_cast(statement));
        REQUIRE_FALSE(result.get_error().contains_error());
        auto node = result.node_ptr();
        REQUIRE(node->type() == node_type::transaction_t);
        return static_cast<const node_transaction_t*>(node.get())->op();
    };

    REQUIRE(operation_of("BEGIN;") == transaction_op::begin);
    REQUIRE(operation_of("START TRANSACTION;") == transaction_op::begin);
    REQUIRE(operation_of("COMMIT;") == transaction_op::commit);
    REQUIRE(operation_of("END;") == transaction_op::commit);
    REQUIRE(operation_of("ROLLBACK;") == transaction_op::abort);
    REQUIRE(operation_of("ABORT;") == transaction_op::abort);
}

TEST_CASE("components::sql::transaction::mode_lists_are_refused") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);

    const auto refused = [&](const char* query) {
        auto statement = linitial(raw_parser(&arena_resource, query));
        transform::transformer local(&resource, query);
        return local.transform(transform::pg_cell_to_node_cast(statement)).get_error().contains_error();
    };

    REQUIRE(refused("BEGIN READ ONLY;"));
    REQUIRE(refused("BEGIN ISOLATION LEVEL SERIALIZABLE;"));
    REQUIRE(refused("START TRANSACTION ISOLATION LEVEL READ COMMITTED;"));
    // Not vacuous: without a mode list the same statements transform.
    REQUIRE_FALSE(refused("BEGIN;"));
    REQUIRE_FALSE(refused("START TRANSACTION;"));
}
