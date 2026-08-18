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

// Entries registered on one resolve slot (0 when the plan needs no lookup of
// that kind).
inline std::size_t entry_count(const node_catalog_resolve_ptr& node) { return node ? node->entries().size() : 0; }

// The transformer does not wrap plans: sub_queries.back() IS the consumer node,
// and the catalog lookups it needs live on the plan. Assert both — this is what
// the old "$sequence[N]" child-count assertions were really pinning down.
#define TEST_TRANSFORMER_OK(QUERY, ROOT_TYPE, NS_COUNT, TBL_COUNT)                                                     \
    SECTION(QUERY) {                                                                                                   \
        auto stmt = raw_parser(&arena_resource, QUERY)->lst.front().data;                                              \
        auto result = transformer.transform(pg_cell_to_node_cast(stmt)).finalize();                                    \
        REQUIRE(!result.has_error());                                                                                  \
        const auto& plan = result.value();                                                                             \
        REQUIRE(plan.sub_queries.back()->type() == ROOT_TYPE);                                                         \
        REQUIRE(entry_count(plan.catalog_resolves.namespaces) == NS_COUNT);                                            \
        REQUIRE(entry_count(plan.catalog_resolves.tables) == TBL_COUNT);                                               \
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
        const auto& schema = data->schema();                                                                           \
        CHECK_FN(schema);                                                                                              \
    }

namespace {
    template<typename T>
    bool contains(const std::pmr::vector<complex_logical_type>& schema, T&& pred) {
        return std::find_if(schema.begin(), schema.end(), std::move(pred)) != schema.end();
    }
} // namespace

TEST_CASE("components::sql::database") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    TEST_TRANSFORMER_OK("CREATE DATABASE db_name", node_type::create_database_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name;", node_type::create_database_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name;          ", node_type::create_database_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name; -- comment", node_type::create_database_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE DATABASE db_name; /* multiline\ncomments */", node_type::create_database_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE /* comment */ DATABASE db_name;", node_type::create_database_t, 1, 0);
    // DROP DATABASE registers only its namespace lookup; the drop node carries the
    // dbname and gets namespace_oid pasted on by enrich.
    TEST_TRANSFORMER_OK("DROP DATABASE db_name;", node_type::drop_t, 1, 0);
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
        REQUIRE(result.sub_queries.back()->type() == node_type::create_collection_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
    }

    SECTION("create with schema") {
        auto create = raw_parser(&arena_resource, "CREATE TABLE db_name.schema.table_name()")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(create)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::create_collection_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
    }

    TEST_TRANSFORMER_OK("CREATE TABLE db_name.table_name()", node_type::create_collection_t, 1, 0);
    TEST_TRANSFORMER_OK("CREATE TABLE table_name()", node_type::create_collection_t, 0, 0);

    // DROP TABLE registers a namespace + table lookup; the drop node carries the
    // names and gets namespace_oid + table_oid pasted on by enrich.
    SECTION("drop with uuid") {
        auto drop = raw_parser(&arena_resource, "DROP TABLE uuid.db_name.schema.table_name")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::drop_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 1);
    }

    SECTION("drop with schema") {
        auto drop = raw_parser(&arena_resource, "DROP TABLE db_name.schema.table_name")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::drop_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 1);
    }

    TEST_TRANSFORMER_OK("DROP TABLE db_name.table_name", node_type::drop_t, 1, 1);
    // No db prefix → the table lookup carries an empty dbname, and no namespace
    // lookup is registered at all.
    TEST_TRANSFORMER_OK("DROP TABLE table_name", node_type::drop_t, 0, 1);

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
        "CREATE TABLE table_name(t1 blob, t2 uint, t3 uhugeint, t4 timestamp, t5 decimal(5, 4))",
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
                return t.alias() == "t4" && t.type() == logical_type::TIMESTAMP;
            }));
            REQUIRE(contains(sch, [](const complex_logical_type& t) {
                if (t.type() != logical_type::DECIMAL)
                    return false;
                auto decimal = static_cast<decimal_logical_type_extension*>(t.extension());
                return t.alias() == "t5" && decimal->width() == 5 && decimal->scale() == 4;
            }));
        });

    TEST_TRANSFORMER_EXPECT_SCHEMA(
        "CREATE TABLE table_name(t1 decimal(21, 3)[10], t2 int[100], t3 boolean[8])",
        [](const std::pmr::vector<complex_logical_type>& sch) {
            REQUIRE(contains(sch, [](const complex_logical_type& type) {
                if (type.type() != logical_type::ARRAY)
                    return false;
                auto array = static_cast<array_logical_type_extension*>(type.extension());
                if (array->internal_type().type() != logical_type::DECIMAL)
                    return false;
                auto decimal = static_cast<decimal_logical_type_extension*>(array->internal_type().extension());
                return type.alias() == "t1" && decimal->width() == 21 && decimal->scale() == 3 && array->size() == 10;
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

    TEST_TRANSFORMER_EXPECT_SCHEMA("CREATE TABLE table_name("
                                   "  t1 DATE,"
                                   "  t2 TIME,"
                                   "  t3 TIME WITH TIME ZONE,"
                                   "  t4 TIMESTAMP,"
                                   "  t5 TIMESTAMP WITH TIME ZONE,"
                                   "  t6 INTERVAL"
                                   ")",
                                   [](const std::pmr::vector<complex_logical_type>& sch) {
                                       REQUIRE(contains(sch, [](const complex_logical_type& t) {
                                           return t.alias() == "t1" && t.type() == logical_type::DATE;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& t) {
                                           return t.alias() == "t2" && t.type() == logical_type::TIME;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& t) {
                                           return t.alias() == "t3" && t.type() == logical_type::TIME_TZ;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& t) {
                                           return t.alias() == "t4" && t.type() == logical_type::TIMESTAMP;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& t) {
                                           return t.alias() == "t5" && t.type() == logical_type::TIMESTAMP_TZ;
                                       }));
                                       REQUIRE(contains(sch, [](const complex_logical_type& t) {
                                           return t.alias() == "t6" && t.type() == logical_type::INTERVAL;
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
        REQUIRE(result.sub_queries.back()->type() == node_type::create_index_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 1);
    }

    SECTION("create with schema") {
        auto create =
            raw_parser(&arena_resource, "CREATE INDEX some_idx ON db.schema.table (field);")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(create)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::create_index_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 1);
    }

    TEST_TRANSFORMER_OK("CREATE INDEX some_idx ON db.table (field);", node_type::create_index_t, 1, 1);

    // DROP INDEX names TWO pg_class rows — the parent table and the index — so it
    // registers two table lookups, and the drop node carries both names.
    SECTION("drop with uuid") {
        auto drop = raw_parser(&arena_resource, "DROP INDEX uuid.db.schema.table.some_idx")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::drop_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 2);
    }

    SECTION("drop with schema") {
        auto drop = raw_parser(&arena_resource, "DROP INDEX db.schema.table.some_idx")->lst.front().data;
        auto result = ([](auto _w) {
            REQUIRE_FALSE(_w.has_error());
            return _w.value();
        }(transformer.transform(pg_cell_to_node_cast(drop)).finalize()));
        REQUIRE(result.sub_queries.back()->type() == node_type::drop_t);
        REQUIRE(entry_count(result.catalog_resolves.namespaces) == 1);
        REQUIRE(entry_count(result.catalog_resolves.tables) == 2);
    }

    TEST_TRANSFORMER_OK("DROP INDEX db.table.some_idx", node_type::drop_t, 1, 2);
}

TEST_CASE("components::sql::types") {
    auto resource = core::pmr::otterbrix_resource();
    std::pmr::monotonic_buffer_resource arena_resource(&resource);
    transform::transformer transformer(&resource);

    // CREATE TYPE is wrapped in sequence_t(resolve_ns?, resolve_field_types..., create_type).
    TEST_TRANSFORMER_OK("CREATE TYPE custom_type_name AS (f1 int, f2 string);", node_type::create_type_t, 1, 0);

    TEST_TRANSFORMER_OK("CREATE TYPE custom_enum AS ENUM ('f1', 'f2', 'f3');", node_type::create_type_t, 1, 0);

    // DROP TYPE is wrapped in sequence_t(resolve_ns, resolve_type, drop_type).
    TEST_TRANSFORMER_OK("DROP TYPE custom_type_name", node_type::drop_t, 1, 0);

    // CREATE TABLE with a custom type is wrapped in sequence_t(resolve_type, create_collection).
    TEST_TRANSFORMER_OK("CREATE TABLE table_ (custom_type_name custom_type);", node_type::create_collection_t, 0, 0);

    // INSERT is wrapped in sequence_t(resolve_table, resolve_constraint,
    // insert) — no dbname so no resolve_namespace.
    TEST_TRANSFORMER_OK("INSERT INTO table_ (custom_type_name) VALUES (ROW('text', 42))", node_type::insert_t, 0, 1);
}
