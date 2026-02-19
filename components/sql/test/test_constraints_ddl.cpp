#include <catch2/catch.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_drop_sequence.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_drop_view.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_drop_macro.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::logical_plan;
using namespace components::types;
using namespace components::sql::transform;
using namespace components::table;

TEST_CASE("components::sql::constraints::not_null_and_default") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("CREATE TABLE with NOT NULL") {
        auto stmt = raw_parser(&arena_resource,
            "CREATE TABLE db.tbl (id INTEGER NOT NULL, name TEXT)")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);

        const auto& col_defs = data->column_definitions();
        REQUIRE(col_defs.size() == 2);
        REQUIRE(col_defs[0].name() == "id");
        REQUIRE(col_defs[0].is_not_null() == true);
        REQUIRE(col_defs[1].name() == "name");
        REQUIRE(col_defs[1].is_not_null() == false);
    }

    SECTION("CREATE TABLE with DEFAULT") {
        auto stmt = raw_parser(&arena_resource,
            "CREATE TABLE db.tbl (id INTEGER, name TEXT DEFAULT 'unknown')")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);

        const auto& col_defs = data->column_definitions();
        REQUIRE(col_defs.size() == 2);
        REQUIRE(col_defs[0].name() == "id");
        REQUIRE(col_defs[0].has_default_value() == false);
        REQUIRE(col_defs[1].name() == "name");
        REQUIRE(col_defs[1].has_default_value() == true);
        REQUIRE(col_defs[1].default_value().value<std::string_view>() == "unknown");
    }

    SECTION("CREATE TABLE with NOT NULL and DEFAULT combined") {
        auto stmt = raw_parser(&arena_resource,
            "CREATE TABLE db.tbl (id INTEGER NOT NULL, score DOUBLE DEFAULT 0)")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);

        const auto& col_defs = data->column_definitions();
        REQUIRE(col_defs.size() == 2);
        REQUIRE(col_defs[0].name() == "id");
        REQUIRE(col_defs[0].is_not_null() == true);
        REQUIRE(col_defs[0].has_default_value() == false);
        REQUIRE(col_defs[1].name() == "score");
        REQUIRE(col_defs[1].has_default_value() == true);
    }

    SECTION("CREATE TABLE with PRIMARY KEY column-level") {
        auto stmt = raw_parser(&arena_resource,
            "CREATE TABLE db.tbl (id INTEGER PRIMARY KEY, name TEXT)")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);

        const auto& col_defs = data->column_definitions();
        REQUIRE(col_defs.size() == 2);
        // PRIMARY KEY implies NOT NULL
        REQUIRE(col_defs[0].name() == "id");
        REQUIRE(col_defs[0].is_not_null() == true);
    }

    SECTION("CREATE TABLE with table-level PRIMARY KEY") {
        auto stmt = raw_parser(&arena_resource,
            "CREATE TABLE db.tbl (id INTEGER, name TEXT, PRIMARY KEY (id))")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);

        const auto& constraints = data->constraints();
        REQUIRE(constraints.size() == 1);
        REQUIRE(constraints[0].type == table_constraint_type::PRIMARY_KEY);
        REQUIRE(constraints[0].columns.size() == 1);
        REQUIRE(constraints[0].columns[0] == "id");
    }

    SECTION("CREATE TABLE with table-level UNIQUE") {
        auto stmt = raw_parser(&arena_resource,
            "CREATE TABLE db.tbl (id INTEGER, email TEXT, UNIQUE (email))")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);

        const auto& constraints = data->constraints();
        REQUIRE(constraints.size() == 1);
        REQUIRE(constraints[0].type == table_constraint_type::UNIQUE);
        REQUIRE(constraints[0].columns.size() == 1);
        REQUIRE(constraints[0].columns[0] == "email");
    }
}

TEST_CASE("components::sql::sequence") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("CREATE SEQUENCE basic") {
        auto stmt = raw_parser(&arena_resource,
            "CREATE SEQUENCE db.my_seq")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == node_type::create_sequence_t);
        REQUIRE(node->to_string() == "$create_sequence: db.my_seq");
    }

    SECTION("CREATE SEQUENCE with options") {
        auto stmt = raw_parser(&arena_resource,
            "CREATE SEQUENCE db.my_seq START 10 INCREMENT 2")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == node_type::create_sequence_t);
        auto seq = reinterpret_cast<node_create_sequence_ptr&>(node);
        REQUIRE(seq->start() == 10);
        REQUIRE(seq->increment() == 2);
    }

    SECTION("DROP SEQUENCE") {
        auto stmt = raw_parser(&arena_resource,
            "DROP SEQUENCE db.my_seq")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == node_type::drop_sequence_t);
        REQUIRE(node->to_string() == "$drop_sequence: db.my_seq");
    }
}

TEST_CASE("components::sql::view") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);

    SECTION("CREATE VIEW") {
        transform::transformer transformer(&resource);
        auto stmt = raw_parser(&arena_resource,
            "CREATE VIEW db.my_view AS SELECT * FROM db.tbl")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == node_type::create_view_t);
        REQUIRE(node->to_string() == "$create_view: db.my_view");
    }

    SECTION("CREATE VIEW with raw_sql extracts query") {
        const char* sql = "CREATE VIEW db.my_view AS SELECT id, name FROM db.tbl WHERE id > 10";
        transform::transformer transformer(&resource, sql);
        auto stmt = raw_parser(&arena_resource, sql)->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto view_node = boost::static_pointer_cast<node_create_view_t>(result.node);
        REQUIRE(view_node->type() == node_type::create_view_t);
        REQUIRE(view_node->query_sql() == "SELECT id, name FROM db.tbl WHERE id > 10");
    }

    SECTION("DROP VIEW") {
        transform::transformer transformer(&resource);
        auto stmt = raw_parser(&arena_resource,
            "DROP VIEW db.my_view")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == node_type::drop_view_t);
        REQUIRE(node->to_string() == "$drop_view: db.my_view");
    }
}

TEST_CASE("components::sql::macro") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("DROP FUNCTION (macro)") {
        auto stmt = raw_parser(&arena_resource,
            "DROP FUNCTION db.my_macro()")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == node_type::drop_macro_t);
        REQUIRE(node->to_string() == "$drop_macro: db.my_macro");
    }

    SECTION("DROP FUNCTION simple name") {
        auto stmt = raw_parser(&arena_resource,
            "DROP FUNCTION my_macro()")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());
        auto node = result.node;
        REQUIRE(node->type() == node_type::drop_macro_t);
    }
}
