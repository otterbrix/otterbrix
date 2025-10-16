#include <catch2/catch.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/parser.h>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::sql;
using namespace components::logical_plan;
using namespace components::types;

#define TEST_TRANSFORMER_ERROR(QUERY, RESULT)                                                                          \
    SECTION(QUERY) {                                                                                                   \
        auto resource = std::pmr::synchronized_pool_resource();                                                        \
        auto create = linitial(raw_parser(QUERY));                                                                     \
        transform::transformer transformer(&resource);                                                                 \
        components::logical_plan::parameter_node_t agg(&resource);                                                     \
        bool exception_thrown = false;                                                                                 \
        try {                                                                                                          \
            auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &agg);                          \
        } catch (const parser_exception_t& e) {                                                                        \
            exception_thrown = true;                                                                                   \
            REQUIRE(std::string_view{e.what()} == RESULT);                                                             \
        }                                                                                                              \
        REQUIRE(exception_thrown);                                                                                     \
    }

namespace {
    template<typename T>
    bool contains(const std::pmr::vector<complex_logical_type>& schema, T&& pred) {
        return std::find_if(schema.begin(), schema.end(), std::move(pred)) != schema.end();
    }
} // namespace

TEST_CASE("sql::database") {
    auto resource = std::pmr::synchronized_pool_resource();
    components::sql::transform::transformer transformer(&resource);
    components::logical_plan::parameter_node_t statement(&resource);

    SECTION("create") {
        auto create = raw_parser("CREATE DATABASE db_name")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_database: db_name)_");
    }

    SECTION("create;") {
        auto create = raw_parser("CREATE DATABASE db_name;")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_database: db_name)_");
    }

    SECTION("create; ") {
        auto create = raw_parser("CREATE DATABASE db_name;          ")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_database: db_name)_");
    }

    SECTION("create; --") {
        auto create = raw_parser("CREATE DATABASE db_name; -- comment")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_database: db_name)_");
    }

    SECTION("create; /*") {
        auto create = raw_parser("CREATE DATABASE db_name; /* multiline\n"
                                 "comments */")
                          ->lst.front()
                          .data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_database: db_name)_");
    }

    SECTION("create; /* mid */") {
        auto create = raw_parser("CREATE /* comment */ DATABASE db_name;")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_database: db_name)_");
    }

    SECTION("drop") {
        auto drop = raw_parser("DROP DATABASE db_name;")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_database: db_name)_");
    }
}

TEST_CASE("sql::table") {
    auto resource = std::pmr::synchronized_pool_resource();
    transform::transformer transformer(&resource);
    components::logical_plan::parameter_node_t statement(&resource);

    SECTION("create with uuid") {
        auto create = raw_parser("CREATE TABLE uuid.db_name.schema.table_name()")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_collection: db_name.table_name)_");
        REQUIRE(node->collection_full_name().unique_identifier == "uuid");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("create with schema") {
        auto create = raw_parser("CREATE TABLE db_name.schema.table_name()")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_collection: db_name.table_name)_");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("create") {
        auto create = raw_parser("CREATE TABLE db_name.table_name()")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_collection: db_name.table_name)_");
    }

    SECTION("create without database") {
        auto create = raw_parser("CREATE TABLE table_name()")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_collection: .table_name)_");
    }

    SECTION("drop with uuid") {
        auto drop = raw_parser("DROP TABLE uuid.db_name.schema.table_name")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_collection: db_name.table_name)_");
        REQUIRE(node->collection_full_name().unique_identifier == "uuid");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("drop with schema") {
        auto drop = raw_parser("DROP TABLE db_name.schema.table_name")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_collection: db_name.table_name)_");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("drop") {
        auto drop = raw_parser("DROP TABLE db_name.table_name")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_collection: db_name.table_name)_");
    }

    SECTION("drop without database") {
        auto drop = raw_parser("DROP TABLE table_name")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_collection: .table_name)_");
    }

    SECTION("typed columns") {
        auto create = linitial(raw_parser("CREATE TABLE table_name(test integer, test1 string)"));
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        const auto& sch = data->schema();

        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            return type.alias() == "test" && type.type() == logical_type::INTEGER;
        }));
        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            return type.alias() == "test1" && type.type() == logical_type::STRING_LITERAL;
        }));
    }

    SECTION("more typed columns") {
        auto create = linitial(
            raw_parser("CREATE TABLE table_name(t1 blob, t2 uint, t3 uhugeint, t4 timestamp_sec, t5 decimal(5, 4))"));
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        const auto& sch = data->schema();

        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            return type.alias() == "t1" && type.type() == logical_type::BLOB;
        }));
        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            return type.alias() == "t2" && type.type() == logical_type::UINTEGER;
        }));
        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            return type.alias() == "t3" && type.type() == logical_type::UHUGEINT;
        }));
        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            return type.alias() == "t4" && type.type() == logical_type::TIMESTAMP_SEC;
        }));
        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            if (type.type() != logical_type::DECIMAL) {
                return false;
            }
            auto decimal = static_cast<decimal_logical_type_extension*>(type.extension());
            return type.alias() == "t5" && decimal->width() == 5 && decimal->scale() == 4;
        }));
    }

    SECTION("arrays & decimal") {
        auto create =
            linitial(raw_parser("CREATE TABLE table_name(t1 decimal(51, 3)[10], t2 int[100], t3 boolean[8])"));
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        const auto& sch = data->schema();

        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            if (type.type() != logical_type::ARRAY) {
                return false;
            }

            auto array = static_cast<array_logical_type_extension*>(type.extension());
            if (array->internal_type() != logical_type::DECIMAL) {
                return false;
            }

            auto decimal = static_cast<decimal_logical_type_extension*>(array->internal_type().extension());
            return type.alias() == "t1" && decimal->width() == 51, decimal->scale() == 3 && array->size() == 10;
        }));

        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            if (type.type() != logical_type::ARRAY) {
                return false;
            }
            auto array = static_cast<array_logical_type_extension*>(type.extension());
            return type.alias() == "t2" && array->internal_type() == logical_type::INTEGER && array->size() == 100;
        }));

        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            if (type.type() != logical_type::ARRAY) {
                return false;
            }
            auto array = static_cast<array_logical_type_extension*>(type.extension());
            return type.alias() == "t3" && array->internal_type() == logical_type::BOOLEAN && array->size() == 8;
        }));
    }

    SECTION("float") {
        auto create = linitial(raw_parser("CREATE TABLE table_name(t1 float, t2 double, t3 float[100])"));
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);
        const auto& sch = data->schema();

        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            return type.alias() == "t1" && type.type() == logical_type::FLOAT;
        }));
        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            return type.alias() == "t2" && type.type() == logical_type::DOUBLE;
        }));
        REQUIRE(contains(sch, [](const complex_logical_type& type) {
            if (type.type() != logical_type::ARRAY) {
                return false;
            }

            auto array = static_cast<array_logical_type_extension*>(type.extension());
            return type.alias() == "t3" && array->internal_type() == logical_type::FLOAT && array->size() == 100;
        }));
    }

    SECTION("incorrect types") {
        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (just_name struct)", R"_(Unknown type for column: just_name)_");

        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (just_name list)", R"_(Unknown type for column: just_name)_");

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
    transform::transformer transformer(&resource);
    components::logical_plan::parameter_node_t statement(&resource);

    SECTION("create with uuid") {
        auto create = raw_parser("CREATE INDEX some_idx ON uuid.db.schema.table (field);")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_index: db.table name:some_idx[ field ] type:single)_");
        REQUIRE(node->collection_full_name().unique_identifier == "uuid");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("create with schema") {
        auto create = raw_parser("CREATE INDEX some_idx ON db.schema.table (field);")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_index: db.table name:some_idx[ field ] type:single)_");
        REQUIRE(node->collection_full_name().schema == "schema");
    }

    SECTION("create") {
        auto create = raw_parser("CREATE INDEX some_idx ON db.table (field);")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(create), &statement);
        REQUIRE(node->to_string() == R"_($create_index: db.table name:some_idx[ field ] type:single)_");
    }

    SECTION("drop with uuid") {
        auto drop = raw_parser("DROP INDEX uuid.db.schema.table.some_idx")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_index: db.table name:some_idx)_");
        REQUIRE(node->collection_full_name().unique_identifier == "uuid");
        REQUIRE(node->collection_full_name().schema == "schema");
    }
    SECTION("drop with schema") {
        auto drop = raw_parser("DROP INDEX db.schema.table.some_idx")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_index: db.table name:some_idx)_");
        REQUIRE(node->collection_full_name().schema == "schema");
    }
    SECTION("drop") {
        auto drop = raw_parser("DROP INDEX db.table.some_idx")->lst.front().data;
        auto node = transformer.transform(transform::pg_cell_to_node_cast(drop), &statement);
        REQUIRE(node->to_string() == R"_($drop_index: db.table name:some_idx)_");
    }
}
