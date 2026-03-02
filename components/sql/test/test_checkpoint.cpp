#include <catch2/catch.hpp>
#include <components/logical_plan/node_checkpoint.hpp>
#include <components/logical_plan/node_vacuum.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::logical_plan;
using namespace components::sql::transform;

TEST_CASE("components::sql::checkpoint") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("CHECKPOINT") {
        auto stmt = raw_parser(&arena_resource, "CHECKPOINT")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == node_type::checkpoint_t);
        REQUIRE(node->to_string() == "$checkpoint");
    }

    SECTION("CHECKPOINT;") {
        auto stmt = raw_parser(&arena_resource, "CHECKPOINT;")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == node_type::checkpoint_t);
        REQUIRE(node->to_string() == "$checkpoint");
    }
}

TEST_CASE("components::sql::vacuum") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("VACUUM") {
        auto stmt = raw_parser(&arena_resource, "VACUUM")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == node_type::vacuum_t);
        REQUIRE(node->to_string() == "$vacuum");
    }

    SECTION("VACUUM;") {
        auto stmt = raw_parser(&arena_resource, "VACUUM;")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == node_type::vacuum_t);
        REQUIRE(node->to_string() == "$vacuum");
    }
}
