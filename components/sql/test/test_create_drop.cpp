#include <catch2/catch_test_macros.hpp>
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
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();                                    \
        REQUIRE(!result.has_error());                                                                                  \
        auto node = result.value().sub_queries.back();                                                                 \
        REQUIRE(node->to_string() == EXPECTED);                                                                        \
    }

#define TEST_TRANSFORMER_ERROR(QUERY, RESULT)                                                                          \
    SECTION(QUERY) {                                                                                                   \
        auto create = linitial(raw_parser(&arena_resource, QUERY));                                                    \
        REQUIRE(transformer.transform(pg_cell_to_node_cast(create)).has_error());                                      \
    }

#define TEST_TRANSFORMER_EXPECT_SCHEMA(QUERY, CHECK_FN)                                                                \
    SECTION(QUERY) {                                                                                                   \
        auto stmt = linitial(raw_parser(&arena_resource, QUERY));                                                      \
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();                                    \
        REQUIRE(!result.has_error());                                                                                  \
        auto node = result.value().sub_queries.back();                                                                 \
        auto data = reinterpret_cast<node_create_collection_ptr&>(node);                                               \
        const auto& columns = data->column_definitions();                                                              \
        CHECK_FN(columns);                                                                                             \
    }

namespace {
    // M3-B5: the column list a CREATE TABLE carries is a list of column DEFINITIONS, each of
    // which states its own name.
    template<typename T>
    bool contains(const std::vector<components::table::column_definition_t>& columns, T&& pred) {
        return std::find_if(columns.begin(), columns.end(), std::move(pred)) != columns.end();
    }
} // namespace

TEST_CASE("components::sql::database") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_TRANSFORMER_OK("CREATE DATABASE db_name", R"_($sequence[2])_");
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name;", R"_($sequence[2])_");
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name;          ", R"_($sequence[2])_");
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name; -- comment", R"_($sequence[2])_");
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name; /* multiline\ncomments */", R"_($sequence[2])_");
    TEST_TRANSFORMER_OK("CREATE /* comment */ DATABASE db_name;", R"_($sequence[2])_");
    // DROP DATABASE is wrapped by the transformer in sequence_t(resolve_ns, drop)
    // so result.sub_queries.back()->to_string() returns the sequence wrapper. Underlying drop_database
    // carries only namespace_oid (INVALID_OID/0 at parse time).
    TEST_TRANSFORMER_OK("DROP DATABASE db_name;", R"_($sequence[2])_");
}

TEST_CASE("components::sql::table") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("create with uuid") {
        auto create = raw_parser(&arena_resource, "CREATE TABLE uuid.db_name.schema.table_name()")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(create)).finalize()));
        REQUIRE(result.sub_queries.back()->to_string() == R"_($sequence[2])_");
    }

    SECTION("create with schema") {
        auto create = raw_parser(&arena_resource, "CREATE TABLE db_name.schema.table_name()")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(create)).finalize()));
        REQUIRE(result.sub_queries.back()->to_string() == R"_($sequence[2])_");
    }

    TEST_TRANSFORMER_OK("CREATE TABLE db_name.table_name()", R"_($sequence[2])_");
    TEST_TRANSFORMER_OK("CREATE TABLE table_name()", R"_($create_collection: table_name)_");

    // DROP TABLE is wrapped in sequence_t(resolve_ns?, resolve_table?, drop_collection).
    // The drop node itself carries no user-typed names; routing is OID-only
    // after enrich stamps namespace_oid + table_oid.
    SECTION("drop with uuid") {
        auto drop = raw_parser(&arena_resource, "DROP TABLE uuid.db_name.schema.table_name")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        // result.sub_queries.back() is the wrapping sequence_t: 1 resolve_ns + 1 resolve_table + 1 drop = 3 children.
        REQUIRE(result.sub_queries.back()->to_string() == R"_($sequence[3])_");
    }

    SECTION("drop with schema") {
        auto drop = raw_parser(&arena_resource, "DROP TABLE db_name.schema.table_name")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        REQUIRE(result.sub_queries.back()->to_string() == R"_($sequence[3])_");
    }

    TEST_TRANSFORMER_OK("DROP TABLE db_name.table_name", R"_($sequence[3])_");
    // No db prefix → only resolve_table sibling (no resolve_namespace), so 2 children.
    TEST_TRANSFORMER_OK("DROP TABLE table_name", R"_($sequence[2])_");

    TEST_TRANSFORMER_EXPECT_SCHEMA("CREATE TABLE table_name(test integer, test1 string)",
                                   [](const std::vector<components::table::column_definition_t>& sch) {
                                       REQUIRE(contains(sch, [](const components::table::column_definition_t& type) {
                                           return type.name() == "test" && type.type().type() == logical_type::INTEGER;
                                       }));
                                       REQUIRE(contains(sch, [](const components::table::column_definition_t& type) {
                                           return type.name() == "test1" &&
                                                  type.type().type() == logical_type::STRING_LITERAL;
                                       }));
                                   });

    TEST_TRANSFORMER_EXPECT_SCHEMA(
        "CREATE TABLE table_name(t1 blob, t2 uint, t3 uhugeint, t4 timestamp, t5 decimal(5, 4))",
        [](const std::vector<components::table::column_definition_t>& sch) {
            REQUIRE(contains(sch, [](const components::table::column_definition_t& t) {
                return t.name() == "t1" && t.type().type() == logical_type::BLOB;
            }));
            REQUIRE(contains(sch, [](const components::table::column_definition_t& t) {
                return t.name() == "t2" && t.type().type() == logical_type::UINTEGER;
            }));
            REQUIRE(contains(sch, [](const components::table::column_definition_t& t) {
                return t.name() == "t3" && t.type().type() == logical_type::UHUGEINT;
            }));
            REQUIRE(contains(sch, [](const components::table::column_definition_t& t) {
                return t.name() == "t4" && t.type().type() == logical_type::TIMESTAMP;
            }));
            REQUIRE(contains(sch, [](const components::table::column_definition_t& t) {
                if (t.type().type() != logical_type::DECIMAL)
                    return false;
                auto decimal = static_cast<decimal_logical_type_extension*>(t.type().extension());
                return t.name() == "t5" && decimal->width() == 5 && decimal->scale() == 4;
            }));
        });

    TEST_TRANSFORMER_EXPECT_SCHEMA(
        "CREATE TABLE table_name(t1 decimal(21, 3)[10], t2 int[100], t3 boolean[8])",
        [](const std::vector<components::table::column_definition_t>& sch) {
            REQUIRE(contains(sch, [](const components::table::column_definition_t& type) {
                if (type.type().type() != logical_type::ARRAY)
                    return false;
                auto array = static_cast<array_logical_type_extension*>(type.type().extension());
                if (array->internal_type().type() != logical_type::DECIMAL)
                    return false;
                auto decimal = static_cast<decimal_logical_type_extension*>(array->internal_type().extension());
                return type.name() == "t1" && decimal->width() == 21 && decimal->scale() == 3 && array->size() == 10;
            }));
            REQUIRE(contains(sch, [](const components::table::column_definition_t& type) {
                if (type.type().type() != logical_type::ARRAY)
                    return false;
                auto array = static_cast<array_logical_type_extension*>(type.type().extension());
                return type.name() == "t2" && array->internal_type() == logical_type::INTEGER && array->size() == 100;
            }));
            REQUIRE(contains(sch, [](const components::table::column_definition_t& type) {
                if (type.type().type() != logical_type::ARRAY)
                    return false;
                auto array = static_cast<array_logical_type_extension*>(type.type().extension());
                return type.name() == "t3" && array->internal_type() == logical_type::BOOLEAN && array->size() == 8;
            }));
        });

    TEST_TRANSFORMER_EXPECT_SCHEMA("CREATE TABLE table_name(t1 float, t2 double, t3 float[100])",
                                   [](const std::vector<components::table::column_definition_t>& sch) {
                                       REQUIRE(contains(sch, [](const components::table::column_definition_t& type) {
                                           return type.name() == "t1" && type.type().type() == logical_type::FLOAT;
                                       }));
                                       REQUIRE(contains(sch, [](const components::table::column_definition_t& type) {
                                           return type.name() == "t2" && type.type().type() == logical_type::DOUBLE;
                                       }));
                                       REQUIRE(contains(sch, [](const components::table::column_definition_t& type) {
                                           if (type.type().type() != logical_type::ARRAY)
                                               return false;
                                           auto array = static_cast<array_logical_type_extension*>(type.type().extension());
                                           return type.name() == "t3" &&
                                                  array->internal_type() == logical_type::FLOAT && array->size() == 100;
                                       }));
                                   });

    TEST_TRANSFORMER_EXPECT_SCHEMA("CREATE TABLE table_name("
                                   "  t1 DATE,"
                                   "  t2 TIME,"
                                   "  t3 TIME WITH TIME ZONE,"
                                   "  t4 TIMESTAMP,"
                                   "  t5 TIMESTAMP WITH TIME ZONE,"
                                   "  t6 INTERVAL"
                                   ")",
                                   [](const std::vector<components::table::column_definition_t>& sch) {
                                       REQUIRE(contains(sch, [](const components::table::column_definition_t& t) {
                                           return t.name() == "t1" && t.type().type() == logical_type::DATE;
                                       }));
                                       REQUIRE(contains(sch, [](const components::table::column_definition_t& t) {
                                           return t.name() == "t2" && t.type().type() == logical_type::TIME;
                                       }));
                                       REQUIRE(contains(sch, [](const components::table::column_definition_t& t) {
                                           return t.name() == "t3" && t.type().type() == logical_type::TIME_TZ;
                                       }));
                                       REQUIRE(contains(sch, [](const components::table::column_definition_t& t) {
                                           return t.name() == "t4" && t.type().type() == logical_type::TIMESTAMP;
                                       }));
                                       REQUIRE(contains(sch, [](const components::table::column_definition_t& t) {
                                           return t.name() == "t5" && t.type().type() == logical_type::TIMESTAMP_TZ;
                                       }));
                                       REQUIRE(contains(sch, [](const components::table::column_definition_t& t) {
                                           return t.name() == "t6" && t.type().type() == logical_type::INTERVAL;
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

    // A width/scale pair is user input and reaches complex_logical_type::create_decimal
    // unchecked. Three ways that went wrong, all from a plain CREATE TABLE:
    //   * width 50 has no storage type -> decimal_storage_type threw, and a throw out of the
    //     transformer's caller is an empty unhandled_exception() under NDEBUG;
    //   * width 522 was narrowed to uint8_t FIRST, wrapping to 10, and was accepted as a
    //     perfectly ordinary DECIMAL(10, 2) -- a silently different column type;
    //   * scale > width was caught only by an assert, erased in Release.
    SECTION("decimal width and scale are validated") {
        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (x decimal(50, 2))", "");
        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (x decimal(522, 2))", "");
        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (x decimal(2, 5))", "");
        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (x decimal(0, 0))", "");
        TEST_TRANSFORMER_ERROR("CREATE TABLE table_name (x decimal(-1, 0))", "");
    }

    // The widest spec that does have a storage type still works.
    TEST_TRANSFORMER_EXPECT_SCHEMA("CREATE TABLE table_name (x decimal(38, 10));",
                                   [](const std::vector<components::table::column_definition_t>& sch) {
                                       REQUIRE(contains(sch, [](const components::table::column_definition_t& t) {
                                           return t.name() == "x" && t.type().type() == logical_type::DECIMAL;
                                       }));
                                   });
}

TEST_CASE("components::sql::index") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    SECTION("create with uuid") {
        auto create =
            raw_parser(&arena_resource, "CREATE INDEX some_idx ON uuid.db.schema.table (field);")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(create)).finalize()));
        REQUIRE(result.sub_queries.back()->to_string() == R"_($sequence[3])_");
    }

    SECTION("create with schema") {
        auto create =
            raw_parser(&arena_resource, "CREATE INDEX some_idx ON db.schema.table (field);")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(create)).finalize()));
        REQUIRE(result.sub_queries.back()->to_string() == R"_($sequence[3])_");
    }

    TEST_TRANSFORMER_OK("CREATE INDEX some_idx ON db.table (field);", R"_($sequence[3])_");

    // DROP INDEX is wrapped in sequence_t(resolve_ns, resolve_table_parent,
    // resolve_table_index, drop_index). The drop node carries no user-typed names.
    SECTION("drop with uuid") {
        auto drop = raw_parser(&arena_resource, "DROP INDEX uuid.db.schema.table.some_idx")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        REQUIRE(result.sub_queries.back()->to_string() == R"_($sequence[4])_");
    }

    SECTION("drop with schema") {
        auto drop = raw_parser(&arena_resource, "DROP INDEX db.schema.table.some_idx")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        REQUIRE(result.sub_queries.back()->to_string() == R"_($sequence[4])_");
    }

    TEST_TRANSFORMER_OK("DROP INDEX db.table.some_idx", R"_($sequence[4])_");
}

TEST_CASE("components::sql::types") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // CREATE TYPE is wrapped in sequence_t(resolve_ns?, resolve_field_types..., create_type).
    TEST_TRANSFORMER_OK("CREATE TYPE custom_type_name AS (f1 int, f2 string);", R"_($sequence[3])_");

    TEST_TRANSFORMER_OK("CREATE TYPE custom_enum AS ENUM ('f1', 'f2', 'f3');", R"_($sequence[3])_");

    // DROP TYPE is wrapped in sequence_t(resolve_ns, resolve_type, drop_type).
    TEST_TRANSFORMER_OK("DROP TYPE custom_type_name", R"_($sequence[3])_");

    // CREATE TABLE with a custom type is wrapped in sequence_t(resolve_type, create_collection).
    TEST_TRANSFORMER_OK("CREATE TABLE table_ (custom_type_name custom_type);", R"_($sequence[2])_");

    // INSERT is wrapped in sequence_t(resolve_table, resolve_constraint,
    // insert) — no dbname so no resolve_namespace.
    TEST_TRANSFORMER_OK("INSERT INTO table_ (custom_type_name) VALUES (ROW('text', 42))", R"_($sequence[3])_");
}
