#include <catch2/catch.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::logical_plan;
using namespace components::types;
using namespace components::sql::transform;

#define TEST_TRANSFORMER_OK(QUERY, EXPECTED)                                                                           \
    SECTION(QUERY) {                                                                                                   \
        auto stmt = raw_parser(&arena_resource, QUERY)->lst.front().data;                                              \
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());             \
        auto node = result.node;                                                                                       \
        REQUIRE(node->to_string() == EXPECTED);                                                                        \
    }

#define TEST_TRANSFORMER_ERROR(QUERY, RESULT)                                                                          \
    SECTION(QUERY) {                                                                                                   \
        auto create = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        bool exception_thrown = false;                                                                                 \
        try {                                                                                                          \
            transformer.transform(pg_cell_to_node_cast(create));                                                       \
        } catch (const parser_exception_t& e) {                                                                        \
            exception_thrown = true;                                                                                   \
            REQUIRE(std::string_view{e.what()} == RESULT);                                                             \
        }                                                                                                              \
        REQUIRE(exception_thrown);                                                                                     \
    }

#define TEST_TRANSFORMER_EXPECT_SCHEMA(QUERY, CHECK_FN)                                                                \
    SECTION(QUERY) {                                                                                                   \
        auto stmt = linitial(raw_parser(&arena_resource, QUERY));                                                      \
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(stmt)).finalize());             \
        auto node = result.node;                                                                                       \
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);                                               \
        const auto& schema = data->schema();                                                                           \
        CHECK_FN(schema);                                                                                              \
    }

namespace {
    template<typename T>
    bool contains(const std::pmr::vector<complex_logical_type>& schema, T&& pred) {
        return std::find_if(schema.begin(), schema.end(), std::move(pred)) != schema.end();
    }
} // namespace

TEST_CASE("sql::database") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_TRANSFORMER_OK("CREATE DATABASE db_name", R"_($create_database: db_name)_");
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name;", R"_($create_database: db_name)_");
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name;          ", R"_($create_database: db_name)_");
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name; -- comment", R"_($create_database: db_name)_");
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name; /* multiline\ncomments */", R"_($create_database: db_name)_");
    TEST_TRANSFORMER_OK("CREATE /* comment */ DATABASE db_name;", R"_($create_database: db_name)_");
    TEST_TRANSFORMER_OK("DROP DATABASE db_name;", R"_($drop_database: db_name)_");
}

TEST_CASE("sql::table") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("create with uuid") {
        auto create = raw_parser(&arena_resource, "CREATE TABLE uuid.db_name.schema.table_name()")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(create)).finalize());
        auto node = result.node;
        REQUIRE(node->to_string() == R"_($create_collection: db_name.table_name)_");
        REQUIRE(node->collection_full_name().unique_identifier == "uuid");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("create with schema") {
        auto create = raw_parser(&arena_resource, "CREATE TABLE db_name.schema.table_name()")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(create)).finalize());
        auto node = result.node;
        REQUIRE(node->to_string() == R"_($create_collection: db_name.table_name)_");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    TEST_TRANSFORMER_OK("CREATE TABLE db_name.table_name()", R"_($create_collection: db_name.table_name)_");
    TEST_TRANSFORMER_OK("CREATE TABLE table_name()", R"_($create_collection: .table_name)_");

    SECTION("drop with uuid") {
        auto drop = raw_parser(&arena_resource, "DROP TABLE uuid.db_name.schema.table_name")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(drop)).finalize());
        auto node = result.node;
        REQUIRE(node->to_string() == R"_($drop_collection: db_name.table_name)_");
        REQUIRE(node->collection_full_name().unique_identifier == "uuid");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("drop with schema") {
        auto drop = raw_parser(&arena_resource, "DROP TABLE db_name.schema.table_name")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(drop)).finalize());
        auto node = result.node;
        REQUIRE(node->to_string() == R"_($drop_collection: db_name.table_name)_");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    TEST_TRANSFORMER_OK("DROP TABLE db_name.table_name", R"_($drop_collection: db_name.table_name)_");
    TEST_TRANSFORMER_OK("DROP TABLE table_name", R"_($drop_collection: .table_name)_");

    TEST_TRANSFORMER_EXPECT_SCHEMA("CREATE TABLE table_name(test integer, test1 string)",
                                   [](const std::pmr::vector<complex_logical_type>& sch) {
                                       REQUIRE(contains(sch, [](const complex_logical_type& type) {
                                           return type.alias() == "test" && type.type() == logical_type::INTEGER;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& type) {
                                           return type.alias() == "test1" &&
                                                  type.type() == logical_type::STRING_LITERAL;
                                       }));
                                   });

    TEST_TRANSFORMER_EXPECT_SCHEMA(
        "CREATE TABLE table_name(t1 blob, t2 uint, t3 uhugeint, t4 timestamp_sec, t5 decimal(5, 4))",
        [](const std::pmr::vector<complex_logical_type>& sch) {
            REQUIRE(contains(sch, [](const complex_logical_type& t) {
                return t.alias() == "t1" && t.type() == logical_type::BLOB;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& t) {
                return t.alias() == "t2" && t.type() == logical_type::UINTEGER;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& t) {
                return t.alias() == "t3" && t.type() == logical_type::UHUGEINT;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& t) {
                return t.alias() == "t4" && t.type() == logical_type::TIMESTAMP_SEC;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& t) {
                if (t.type() != logical_type::DECIMAL)
                    return false;
                auto decimal = static_cast<decimal_logical_type_extension*>(t.extension());
                return t.alias() == "t5" && decimal->width() == 5 && decimal->scale() == 4;
            }));
        });

    TEST_TRANSFORMER_EXPECT_SCHEMA(
        "CREATE TABLE table_name(t1 decimal(51, 3)[10], t2 int[100], t3 boolean[8])",
        [](const std::pmr::vector<complex_logical_type>& sch) {
            REQUIRE(contains(sch, [](const complex_logical_type& type) {
                if (type.type() != logical_type::ARRAY)
                    return false;
                auto array = static_cast<array_logical_type_extension*>(type.extension());
                if (array->internal_type() != logical_type::DECIMAL)
                    return false;
                auto decimal = static_cast<decimal_logical_type_extension*>(array->internal_type().extension());
                return type.alias() == "t1" && decimal->width() == 51 && decimal->scale() == 3 && array->size() == 10;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& type) {
                if (type.type() != logical_type::ARRAY)
                    return false;
                auto array = static_cast<array_logical_type_extension*>(type.extension());
                return type.alias() == "t2" && array->internal_type() == logical_type::INTEGER && array->size() == 100;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& type) {
                if (type.type() != logical_type::ARRAY)
                    return false;
                auto array = static_cast<array_logical_type_extension*>(type.extension());
                return type.alias() == "t3" && array->internal_type() == logical_type::BOOLEAN && array->size() == 8;
            }));
        });

    TEST_TRANSFORMER_EXPECT_SCHEMA("CREATE TABLE table_name(t1 float, t2 double, t3 float[100])",
                                   [](const std::pmr::vector<complex_logical_type>& sch) {
                                       REQUIRE(contains(sch, [](const complex_logical_type& type) {
                                           return type.alias() == "t1" && type.type() == logical_type::FLOAT;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& type) {
                                           return type.alias() == "t2" && type.type() == logical_type::DOUBLE;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& type) {
                                           if (type.type() != logical_type::ARRAY)
                                               return false;
                                           auto array = static_cast<array_logical_type_extension*>(type.extension());
                                           return type.alias() == "t3" &&
                                                  array->internal_type() == logical_type::FLOAT && array->size() == 100;
                                       }));
                                   });

    SECTION("incorrect types") {
        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (just_name decimal)",
                               R"_(Incorrect modifiers for DECIMAL, width and scale required)_");

        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (just_name decimal(10))",
                               R"_(Incorrect modifiers for DECIMAL, width and scale required)_");

        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (just_name decimal(correct, expressions))",
                               R"_(Incorrect width or scale for DECIMAL, must be integer)_");

        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (just_name decimal(10, 5, something))",
                               R"_(Incorrect modifiers for DECIMAL, width and scale required)_");
    }
}

TEST_CASE("sql::index") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("create with uuid") {
        auto create =
            raw_parser(&arena_resource, "CREATE INDEX some_idx ON uuid.db.schema.table (field);")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(create)).finalize());
        auto node = result.node;
        REQUIRE(node->to_string() == R"_($create_index: db.table name:some_idx[ field ] type:single)_");
        REQUIRE(node->collection_full_name().unique_identifier == "uuid");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("create with schema") {
        auto create =
            raw_parser(&arena_resource, "CREATE INDEX some_idx ON db.schema.table (field);")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(create)).finalize());
        auto node = result.node;
        REQUIRE(node->to_string() == R"_($create_index: db.table name:some_idx[ field ] type:single)_");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    TEST_TRANSFORMER_OK("CREATE INDEX some_idx ON db.table (field);",
                        R"_($create_index: db.table name:some_idx[ field ] type:single)_");

    SECTION("drop with uuid") {
        auto drop = raw_parser(&arena_resource, "DROP INDEX uuid.db.schema.table.some_idx")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(drop)).finalize());
        auto node = result.node;
        REQUIRE(node->to_string() == R"_($drop_index: db.table name:some_idx)_");
        REQUIRE(node->collection_full_name().unique_identifier == "uuid");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("drop with schema") {
        auto drop = raw_parser(&arena_resource, "DROP INDEX db.schema.table.some_idx")->lst.front().data;
        auto result = std::get<result_view>(transformer.transform(pg_cell_to_node_cast(drop)).finalize());
        auto node = result.node;
        REQUIRE(node->to_string() == R"_($drop_index: db.table name:some_idx)_");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    TEST_TRANSFORMER_OK("DROP INDEX db.table.some_idx", R"_($drop_index: db.table name:some_idx)_");
}

TEST_CASE("sql::types") {
    auto resource = std::pmr::synchronized_pool_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_TRANSFORMER_OK("CREATE TYPE custom_type_name AS (f1 int, f2 string);",
                        R"_($create_type: name: custom_type_name, fields:[ f1 f2 ])_");

    TEST_TRANSFORMER_OK("CREATE TYPE custom_enum AS ENUM ('f1', 'f2', 'f3');",
                        R"_($create_type: name: custom_enum, fields:[ f1=0 f2=1 f3=2 ])_");

    TEST_TRANSFORMER_OK("DROP TYPE custom_type_name", R"_($drop_type: name: custom_type_name)_");

    TEST_TRANSFORMER_OK("CREATE TABLE table_ (custom_type_name custom_type);", R"_($create_collection: .table_)_");

    TEST_TRANSFORMER_OK("INSERT INTO table_ (custom_type_name) VALUES (ROW('text', 42))",
                        R"_($insert: {$raw_data: {$rows: 1}})_");
}
