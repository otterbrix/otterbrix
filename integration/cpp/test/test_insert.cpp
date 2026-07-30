#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/transformer/utils.hpp>
#include <components/tests/generaty.hpp>
#include <components/tests/temp_dir.hpp>
#include <core/operations_helper.hpp>
#include <variant>

static const database_name_t table_database_name = "table_testdatabase";
static const collection_name_t table_collection_name_simple = "table_testcollection_simple";
static const collection_name_t table_collection_name_not_null = "table_testcollection_not_null";
static const collection_name_t table_collection_name_null_defaults = "table_testcollection_null_defaults";
static const collection_name_t table_collection_name_value_defaults = "table_testcollection_value_defaults";
static const collection_name_t table_collection_name_value_defaults_not_null =
    "table_testcollection_value_defaults_not_null";

using namespace components;
using namespace cursor;
using key = expressions::key_t;
static constexpr int kNumInserts = 100;

TEST_CASE("integration::cpp::test_collection::insert") {
    auto config = test_create_config(test_temp_path("test_collection_insert"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // A column's name sits BESIDE its type in the schema record (M3-B5), so the duplicate
    // below is a second record and not a second copy of a self-naming type.
    auto schema = gen_schema(dispatcher->resource());
    schema.insert(schema.begin(), schema.front().clone(dispatcher->resource()));
    schema[1].name = "count_duplicate";

    std::vector<table::column_definition_t> columns_simple;
    std::vector<table::column_definition_t> columns_not_null;
    std::vector<table::column_definition_t> columns_null_defaults;
    std::vector<table::column_definition_t> columns_value_defaults;
    std::vector<table::column_definition_t> columns_value_defaults_not_null;

    INFO("set up column definitions");
    {
        columns_simple.reserve(schema.size());
        columns_not_null.reserve(schema.size());
        columns_null_defaults.reserve(schema.size());
        columns_value_defaults.reserve(schema.size());
        columns_value_defaults_not_null.reserve(schema.size());

        for (const auto& column : schema) {
            columns_simple.emplace_back(std::string{column.name}, column.type);
        }
        for (const auto& column : schema) {
            columns_not_null.emplace_back(std::string{column.name}, column.type, true);
        }
        for (const auto& column : schema) {
            columns_null_defaults.emplace_back(std::string{column.name},
                                               column.type,
                                               false,
                                               types::logical_value_t{dispatcher->resource(), types::logical_type::NA});
        }
        // Fill loop for columns_value_defaults. Without it the CREATE TABLE
        // call gets an empty column vector → relkind='g' (computing) table,
        // and the subsequent explicit-column INSERTs fail validation
        // because the catalog has UNKNOWN types until the first INSERT
        // registers pg_computed_column rows.
        for (const auto& column : schema) {
            columns_value_defaults.emplace_back(std::string{column.name},
                                                column.type,
                                                false,
                                                types::logical_value_t{dispatcher->resource(), column.type});
        }
        for (const auto& column : schema) {
            columns_value_defaults_not_null.emplace_back(std::string{column.name},
                                                         column.type,
                                                         true,
                                                         types::logical_value_t{dispatcher->resource(), column.type});
        }
    }

    INFO("initialization");
    {
        auto create_collection = [&](const collection_name_t& collection,
                                     const std::vector<table::column_definition_t>& columns) {
            auto session = otterbrix::session_id_t();
            test_create_collection(dispatcher, session, table_database_name, collection, columns);
        };

        {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session, "CREATE DATABASE " + table_database_name + ";");
        }
        create_collection(table_collection_name_simple, columns_simple);
        create_collection(table_collection_name_not_null, columns_not_null);
        create_collection(table_collection_name_null_defaults, columns_null_defaults);
        create_collection(table_collection_name_value_defaults, columns_value_defaults);
        create_collection(table_collection_name_value_defaults_not_null, columns_value_defaults_not_null);
    }

    INFO("full insert");
    {
        // is the same for all
        auto full_insert = [&](const collection_name_t& collection) {
            auto chunk = gen_data_chunk(kNumInserts, 0, schema, dispatcher->resource());
            auto ins = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
                dispatcher->resource(),
                table_database_name,
                collection,
                logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        };

        full_insert(table_collection_name_simple);
        full_insert(table_collection_name_not_null);
        full_insert(table_collection_name_null_defaults);
        full_insert(table_collection_name_value_defaults);
        full_insert(table_collection_name_value_defaults_not_null);
    }

    INFO("reordered insert");
    {
        // is the same for all
        auto swapped_schema = components::vector::clone_schema(dispatcher->resource(), schema);
        std::swap(swapped_schema[0], swapped_schema[1]);

        auto reordered_insert = [&](const collection_name_t& collection) {
            auto chunk = gen_data_chunk(kNumInserts, 0, swapped_schema, dispatcher->resource());
            auto ins = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
                dispatcher->resource(),
                table_database_name,
                collection,
                logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        };

        reordered_insert(table_collection_name_simple);
        reordered_insert(table_collection_name_not_null);
        reordered_insert(table_collection_name_null_defaults);
        reordered_insert(table_collection_name_value_defaults);
        reordered_insert(table_collection_name_value_defaults_not_null);
    }

    INFO("insert with conversions");
    {
        // is the same for all
        auto changed_schema = components::vector::clone_schema(dispatcher->resource(), schema);
        changed_schema[0].name = "count_but_integer";
        changed_schema[0].type = types::complex_logical_type{types::logical_type::INTEGER};

        auto insert_with_conversion = [&](const collection_name_t& collection) {
            auto chunk = gen_data_chunk(kNumInserts, 0, changed_schema, dispatcher->resource());
            auto ins = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
                dispatcher->resource(),
                table_database_name,
                collection,
                logical_plan::make_node_insert(dispatcher->resource(), std::move(chunk)));
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        };

        insert_with_conversion(table_collection_name_simple);
        insert_with_conversion(table_collection_name_not_null);
        insert_with_conversion(table_collection_name_null_defaults);
        insert_with_conversion(table_collection_name_value_defaults);
        insert_with_conversion(table_collection_name_value_defaults_not_null);
    }

    INFO("partial insert");
    {
        // is the same for all
        auto partial_schema = components::vector::clone_schema(dispatcher->resource(), schema);
        partial_schema.erase(partial_schema.begin() + 1);
        std::pmr::vector<expressions::key_t> fields(dispatcher->resource());
        fields.reserve(partial_schema.size());
        for (const auto& column : partial_schema) {
            fields.emplace_back(dispatcher->resource(), std::string{column.name});
        }

        auto partial_insert = [&](const collection_name_t& collection) {
            auto chunk = gen_data_chunk(kNumInserts, 0, partial_schema, dispatcher->resource());
            auto ins = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
                dispatcher->resource(),
                table_database_name,
                collection,
                logical_plan::make_node_insert(dispatcher->resource(),
                                               std::move(chunk),
                                               std::pmr::vector<expressions::key_t>{fields}));
            auto session = otterbrix::session_id_t();
            return dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
        };
        auto select_all = [&](const collection_name_t& collection) {
            auto session = otterbrix::session_id_t();
            return dispatcher->execute_sql(session, "SELECT * FROM " + table_database_name + "." + collection + ";");
        };

        INFO("table_collection_name_simple");
        {
            {
                auto cur = partial_insert(table_collection_name_simple);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts);
                // column[1] will be filled with 100 nulls
            }
            {
                auto cur = select_all(table_collection_name_simple);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 4);
                for (size_t i = 0; i < kNumInserts * 3; i++) {
                    REQUIRE_FALSE(cur->chunks().front().data[1].is_null(i));
                }
                for (size_t i = kNumInserts * 3; i < kNumInserts * 4; i++) {
                    REQUIRE(cur->chunks().front().data[1].is_null(i));
                }
            }
        }
        INFO("table_collection_name_not_null");
        {
            {
                auto cur = partial_insert(table_collection_name_not_null);
                REQUIRE(cur->is_error());
                // column[1] can not be filled with nulls, total count will be kNumInserts * 3
            }
            {
                auto cur = select_all(table_collection_name_not_null);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 3);
                for (size_t i = 0; i < kNumInserts * 3; i++) {
                    REQUIRE_FALSE(cur->chunks().front().data[1].is_null(i));
                }
            }
        }
        INFO("table_collection_name_null_defaults");
        {
            {
                auto cur = partial_insert(table_collection_name_null_defaults);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts);
                // column[1] will be filled with 100 nulls
            }
            {
                auto cur = select_all(table_collection_name_null_defaults);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 4);
                for (size_t i = 0; i < kNumInserts * 3; i++) {
                    REQUIRE_FALSE(cur->chunks().front().data[1].is_null(i));
                }
                for (size_t i = kNumInserts * 3; i < kNumInserts * 4; i++) {
                    REQUIRE(cur->chunks().front().data[1].is_null(i));
                }
            }
        }
        INFO("table_collection_name_value_defaults");
        {
            {
                auto cur = partial_insert(table_collection_name_value_defaults);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts);
                // column[1] will be filled with 100 default values (PostgreSQL
                // semantic: DEFAULT applies for omitted columns regardless of
                // nullability — see test_persistence::disk_partial_insert).
            }
            {
                auto cur = select_all(table_collection_name_value_defaults);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 4);
                for (size_t i = 0; i < kNumInserts * 3; i++) {
                    REQUIRE_FALSE(cur->chunks().front().data[1].is_null(i));
                }
                auto val = types::logical_value_t{dispatcher->resource(), cur->chunks().front().data[1].type()};
                for (size_t i = kNumInserts * 3; i < kNumInserts * 4; i++) {
                    REQUIRE(cur->value(1, i) == val);
                }
            }
        }
        INFO("table_collection_name_value_defaults_not_null");
        {
            {
                auto cur = partial_insert(table_collection_name_value_defaults_not_null);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts);
                // column[1] will be filled with 100 nulls
            }
            {
                auto cur = select_all(table_collection_name_value_defaults_not_null);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 4);
                for (size_t i = 0; i < kNumInserts * 3; i++) {
                    REQUIRE_FALSE(cur->chunks().front().data[1].is_null(i));
                }
                auto val = types::logical_value_t{dispatcher->resource(), cur->chunks().front().data[1].type()};
                for (size_t i = kNumInserts * 3; i < kNumInserts * 4; i++) {
                    REQUIRE(cur->value(1, i) == val);
                }
            }
        }
    }

    INFO("partial insert in reverse order");
    {
        // is the same for all
        auto reversed_partial_schema = components::vector::clone_schema(dispatcher->resource(), schema);
        reversed_partial_schema.erase(reversed_partial_schema.begin() + 1);
        std::reverse(reversed_partial_schema.begin(), reversed_partial_schema.end());
        std::pmr::vector<expressions::key_t> fields(dispatcher->resource());
        fields.reserve(reversed_partial_schema.size());
        for (const auto& column : reversed_partial_schema) {
            fields.emplace_back(dispatcher->resource(), std::string{column.name});
        }

        auto reversed_partial_insert = [&](const collection_name_t& collection) {
            auto chunk = gen_data_chunk(kNumInserts, 0, reversed_partial_schema, dispatcher->resource());
            auto ins = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
                dispatcher->resource(),
                table_database_name,
                collection,
                logical_plan::make_node_insert(dispatcher->resource(),
                                               std::move(chunk),
                                               std::pmr::vector<expressions::key_t>{fields}));
            auto session = otterbrix::session_id_t();
            return dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
        };
        auto select_all = [&](const collection_name_t& collection) {
            auto session = otterbrix::session_id_t();
            return dispatcher->execute_sql(session, "SELECT * FROM " + table_database_name + "." + collection + ";");
        };

        INFO("table_collection_name_simple");
        {
            {
                auto cur = reversed_partial_insert(table_collection_name_simple);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts);
                // column[1] will be filled with 100 nulls
            }
            {
                auto cur = select_all(table_collection_name_simple);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 5);
                for (size_t i = 0; i < kNumInserts * 3; i++) {
                    REQUIRE_FALSE(cur->chunks().front().data[1].is_null(i));
                }
                for (size_t i = kNumInserts * 3; i < kNumInserts * 5; i++) {
                    REQUIRE(cur->chunks().front().data[1].is_null(i));
                }
            }
        }
        INFO("table_collection_name_not_null");
        {
            {
                auto cur = reversed_partial_insert(table_collection_name_not_null);
                REQUIRE(cur->is_error());
                // column[1] can not be filled with nulls, total count will be kNumInserts * 3
            }
            {
                auto cur = select_all(table_collection_name_not_null);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 3);
                for (size_t i = 0; i < kNumInserts * 3; i++) {
                    REQUIRE_FALSE(cur->chunks().front().data[1].is_null(i));
                }
            }
        }
        INFO("table_collection_name_null_defaults");
        {
            {
                auto cur = reversed_partial_insert(table_collection_name_null_defaults);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts);
                // column[1] will be filled with 100 nulls
            }
            {
                auto cur = select_all(table_collection_name_null_defaults);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 5);
                for (size_t i = 0; i < kNumInserts * 3; i++) {
                    REQUIRE_FALSE(cur->chunks().front().data[1].is_null(i));
                }
                for (size_t i = kNumInserts * 3; i < kNumInserts * 5; i++) {
                    REQUIRE(cur->chunks().front().data[1].is_null(i));
                }
            }
        }
        INFO("table_collection_name_value_defaults");
        {
            {
                auto cur = reversed_partial_insert(table_collection_name_value_defaults);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts);
                // column[1] gets default value for omitted (PostgreSQL semantic).
            }
            {
                auto cur = select_all(table_collection_name_value_defaults);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 5);
                for (size_t i = 0; i < kNumInserts * 3; i++) {
                    REQUIRE_FALSE(cur->chunks().front().data[1].is_null(i));
                }
                auto val = types::logical_value_t{dispatcher->resource(), cur->chunks().front().data[1].type()};
                for (size_t i = kNumInserts * 3; i < kNumInserts * 5; i++) {
                    REQUIRE(cur->value(1, i) == val);
                }
            }
        }
        INFO("table_collection_name_value_defaults_not_null");
        {
            {
                auto cur = reversed_partial_insert(table_collection_name_value_defaults_not_null);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts);
                // column[1] will be filled with 100 nulls
            }
            {
                auto cur = select_all(table_collection_name_value_defaults_not_null);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 5);
                for (size_t i = 0; i < kNumInserts * 3; i++) {
                    REQUIRE_FALSE(cur->chunks().front().data[1].is_null(i));
                }
                auto val = types::logical_value_t{dispatcher->resource(), cur->chunks().front().data[1].type()};
                for (size_t i = kNumInserts * 3; i < kNumInserts * 5; i++) {
                    REQUIRE(cur->value(1, i) == val);
                }
            }
        }
    }

    INFO("invalid key in insert");
    {
        // is the same for all
        std::pmr::vector<expressions::key_t> fields(dispatcher->resource());
        fields.reserve(schema.size());
        for (const auto& column : schema) {
            fields.emplace_back(dispatcher->resource(), "invalid_key_" + std::string{column.name});
        }

        auto invalid_keys_insert = [&](const collection_name_t& collection) {
            auto chunk = gen_data_chunk(kNumInserts, 0, schema, dispatcher->resource());
            auto ins = components::sql::transform::maybe_wrap_with_catalog_resolve_table(
                dispatcher->resource(),
                table_database_name,
                collection,
                logical_plan::make_node_insert(dispatcher->resource(),
                                               std::move(chunk),
                                               std::pmr::vector<expressions::key_t>{fields}));
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(
                session,
                components::logical_plan::execution_plan_t{dispatcher->resource(), ins, nullptr});
            REQUIRE(cur->is_error());
        };

        invalid_keys_insert(table_collection_name_simple);
        invalid_keys_insert(table_collection_name_not_null);
        invalid_keys_insert(table_collection_name_null_defaults);
        invalid_keys_insert(table_collection_name_value_defaults);
        invalid_keys_insert(table_collection_name_value_defaults_not_null);
    }
}

// `INSERT INTO t (<named columns>) SELECT <more columns>` — the transformer's
// arity check lives in the VALUES path only, so an INSERT ... SELECT reaches the
// validator with key_translation() shorter than the incoming schema. The validator
// then walked key_translation()[i] for i < incoming.size(): an out-of-bounds read
// whenever the incoming width happened to equal the table width. PostgreSQL rejects
// this shape ("INSERT has more expressions than target columns").
TEST_CASE("integration::cpp::test_collection::insert_select_column_count_mismatch") {
    auto config = test_create_config(test_temp_path("test_insert_select_arity"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    for (const auto* sql : {"CREATE DATABASE ins;",
                            "CREATE TABLE ins.src (x BIGINT, y BIGINT);",
                            "CREATE TABLE ins.dst (p BIGINT, q BIGINT);",
                            "INSERT INTO ins.src (x, y) VALUES (1, 2);"}) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    INFO("1 target column, 2 source columns, 2 table columns -> rejected, not an OOB read");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO ins.dst (p) SELECT x, y FROM ins.src;");
        REQUIRE(cur->is_error());
    }

    INFO("the rejected statement inserted nothing");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM ins.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    INFO("the matching-arity form still works");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO ins.dst (p, q) SELECT x, y FROM ins.src;");
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT p, q FROM ins.dst;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 1);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 2);
    }

    INFO("a narrower target list than the table is still a partial insert, not an error");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO ins.dst (p) SELECT x FROM ins.src;");
        REQUIRE(cur->is_success());
    }
}
