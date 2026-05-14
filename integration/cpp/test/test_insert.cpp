#include "test_config.hpp"
#include <catch2/catch.hpp>
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
#include <components/tests/generaty.hpp>
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
    auto config = test_create_config("/tmp/test_collection_insert");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;

    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto types = gen_data_chunk(0, dispatcher->resource()).types();
    types.emplace(types.begin(), types.front());
    types[1].set_alias("count_duplicate");

    std::vector<table::column_definition_t> columns_simple;
    std::vector<table::column_definition_t> columns_not_null;
    std::vector<table::column_definition_t> columns_null_defaults;
    std::vector<table::column_definition_t> columns_value_defaults;
    std::vector<table::column_definition_t> columns_value_defaults_not_null;

    INFO("set up column definitions") {
        columns_simple.reserve(types.size());
        columns_not_null.reserve(types.size());
        columns_null_defaults.reserve(types.size());
        columns_value_defaults.reserve(types.size());
        columns_value_defaults_not_null.reserve(types.size());

        for (const auto& type : types) {
            columns_simple.emplace_back(type.alias(), type);
        }
        for (const auto& type : types) {
            columns_not_null.emplace_back(type.alias(), type, true);
        }
        for (const auto& type : types) {
            columns_null_defaults.emplace_back(type.alias(),
                                               type,
                                               false,
                                               types::logical_value_t{dispatcher->resource(), types::logical_type::NA});
        }
        // Phase 11.E: missing fill loop for columns_value_defaults. Without it the
        // CREATE TABLE call gets an empty column vector → relkind='g' (computing)
        // table, and the subsequent explicit-column INSERTs fail validation
        // because the catalog has UNKNOWN types until the first INSERT registers
        // pg_computed_column rows.
        for (const auto& type : types) {
            columns_value_defaults.emplace_back(type.alias(),
                                                type,
                                                false,
                                                types::logical_value_t{dispatcher->resource(), type});
        }
        for (const auto& type : types) {
            columns_value_defaults_not_null.emplace_back(type.alias(),
                                                         type,
                                                         true,
                                                         types::logical_value_t{dispatcher->resource(), type});
        }
    }

    INFO("initialization") {
        auto create_table = [&](const collection_name_t& collection, const char* col_spec) {
            auto session = otterbrix::session_id_t();
            dispatcher->execute_sql(session,
                fmt::format("CREATE TABLE {}.{}({});", table_database_name, collection, col_spec));
        };

        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, table_database_name);
        }
        create_table(table_collection_name_simple,
                     "count_duplicate bigint, count bigint, count_str string, count_double double, "
                     "count_bool bool, count_array ubigint[5], count_decimal decimal(15,7)");
        create_table(table_collection_name_not_null,
                     "count_duplicate bigint NOT NULL, count bigint NOT NULL, count_str string NOT NULL, "
                     "count_double double NOT NULL, count_bool bool NOT NULL, "
                     "count_array ubigint[5] NOT NULL, count_decimal decimal(15,7) NOT NULL");
        create_table(table_collection_name_null_defaults,
                     "count_duplicate bigint DEFAULT NULL, count bigint DEFAULT NULL, "
                     "count_str string DEFAULT NULL, count_double double DEFAULT NULL, "
                     "count_bool bool DEFAULT NULL, count_array ubigint[5] DEFAULT NULL, "
                     "count_decimal decimal(15,7) DEFAULT NULL");
        create_table(table_collection_name_value_defaults,
                     "count_duplicate bigint DEFAULT 0, count bigint DEFAULT 0, "
                     "count_str string DEFAULT '', count_double double DEFAULT 0.0, "
                     "count_bool bool DEFAULT false, count_array ubigint[5] DEFAULT [0,0,0,0,0], "
                     "count_decimal decimal(15,7) DEFAULT 0");
        create_table(table_collection_name_value_defaults_not_null,
                     "count_duplicate bigint NOT NULL DEFAULT 0, count bigint NOT NULL DEFAULT 0, "
                     "count_str string NOT NULL DEFAULT '', count_double double NOT NULL DEFAULT 0.0, "
                     "count_bool bool NOT NULL DEFAULT false, "
                     "count_array ubigint[5] NOT NULL DEFAULT [0,0,0,0,0], "
                     "count_decimal decimal(15,7) NOT NULL DEFAULT 0");
    }

    INFO("full insert") {
        // All 7 columns in declared order (count_duplicate, count, ...).
        auto full_insert = [&](const collection_name_t& collection) {
            std::vector<std::string> rows;
            rows.reserve(kNumInserts);
            for (int i = 1; i <= kNumInserts; ++i) {
                rows.push_back(fmt::format("({}, {}, '{}', {}, {}, [{}, {}, {}, {}, {}], {})",
                                           i, i, i, i + 0.1,
                                           (i % 2 != 0) ? "true" : "false",
                                           i, i + 1, i + 2, i + 3, i + 4,
                                           i));
            }
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("INSERT INTO {}.{} (count_duplicate, count, count_str, count_double, "
                            "count_bool, count_array, count_decimal) VALUES {};",
                            table_database_name, collection, fmt::join(rows, ", ")));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        };

        full_insert(table_collection_name_simple);
        full_insert(table_collection_name_not_null);
        full_insert(table_collection_name_null_defaults);
        full_insert(table_collection_name_value_defaults);
        full_insert(table_collection_name_value_defaults_not_null);
    }

    INFO("reordered insert") {
        // Column list swaps positions 0/1 — storage must align by alias.
        auto reordered_insert = [&](const collection_name_t& collection) {
            std::vector<std::string> rows;
            rows.reserve(kNumInserts);
            for (int i = 1; i <= kNumInserts; ++i) {
                rows.push_back(fmt::format("({}, {}, '{}', {}, {}, [{}, {}, {}, {}, {}], {})",
                                           i, i, i, i + 0.1,
                                           (i % 2 != 0) ? "true" : "false",
                                           i, i + 1, i + 2, i + 3, i + 4,
                                           i));
            }
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("INSERT INTO {}.{} (count, count_duplicate, count_str, count_double, "
                            "count_bool, count_array, count_decimal) VALUES {};",
                            table_database_name, collection, fmt::join(rows, ", ")));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        };

        reordered_insert(table_collection_name_simple);
        reordered_insert(table_collection_name_not_null);
        reordered_insert(table_collection_name_null_defaults);
        reordered_insert(table_collection_name_value_defaults);
        reordered_insert(table_collection_name_value_defaults_not_null);
    }

    INFO("insert with conversions") {
        // First column renamed (count_duplicate → count_but_integer): no
        // matching catalog column, so storage must accept the row via
        // positional fallback / type coercion. Values stay in 1..100 so
        // INTEGER↔BIGINT coercion is safe.
        auto insert_with_conversion = [&](const collection_name_t& collection) {
            std::vector<std::string> rows;
            rows.reserve(kNumInserts);
            for (int i = 1; i <= kNumInserts; ++i) {
                rows.push_back(fmt::format("({}, {}, '{}', {}, {}, [{}, {}, {}, {}, {}], {})",
                                           i, i, i, i + 0.1,
                                           (i % 2 != 0) ? "true" : "false",
                                           i, i + 1, i + 2, i + 3, i + 4,
                                           i));
            }
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("INSERT INTO {}.{} (count_but_integer, count, count_str, count_double, "
                            "count_bool, count_array, count_decimal) VALUES {};",
                            table_database_name, collection, fmt::join(rows, ", ")));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == kNumInserts);
        };

        insert_with_conversion(table_collection_name_simple);
        insert_with_conversion(table_collection_name_not_null);
        insert_with_conversion(table_collection_name_null_defaults);
        insert_with_conversion(table_collection_name_value_defaults);
        insert_with_conversion(table_collection_name_value_defaults_not_null);
    }

    INFO("partial insert") {
        // 6 columns — drop "count" (position 1). Storage either fills with
        // default / null depending on the column definition (NOT NULL,
        // DEFAULT NULL, value DEFAULT).
        auto partial_insert = [&](const collection_name_t& collection) {
            std::vector<std::string> rows;
            rows.reserve(kNumInserts);
            for (int i = 1; i <= kNumInserts; ++i) {
                rows.push_back(fmt::format("({}, '{}', {}, {}, [{}, {}, {}, {}, {}], {})",
                                           i, i, i + 0.1,
                                           (i % 2 != 0) ? "true" : "false",
                                           i, i + 1, i + 2, i + 3, i + 4,
                                           i));
            }
            auto session = otterbrix::session_id_t();
            return dispatcher->execute_sql(session,
                fmt::format("INSERT INTO {}.{} (count_duplicate, count_str, count_double, count_bool, "
                            "count_array, count_decimal) VALUES {};",
                            table_database_name, collection, fmt::join(rows, ", ")));
        };
        auto select_all = [&](const collection_name_t& collection) {
            auto session = otterbrix::session_id_t();
            return dispatcher->execute_sql(session, "SELECT * FROM " + table_database_name + "." + collection + ";");
        };

        INFO("table_collection_name_simple") {
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
                    REQUIRE_FALSE(cur->chunk_data().data[1].is_null(i));
                }
                for (size_t i = kNumInserts * 3; i < kNumInserts * 4; i++) {
                    REQUIRE(cur->chunk_data().data[1].is_null(i));
                }
            }
        }
        INFO("table_collection_name_not_null") {
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
                    REQUIRE_FALSE(cur->chunk_data().data[1].is_null(i));
                }
            }
        }
        INFO("table_collection_name_null_defaults") {
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
                    REQUIRE_FALSE(cur->chunk_data().data[1].is_null(i));
                }
                for (size_t i = kNumInserts * 3; i < kNumInserts * 4; i++) {
                    REQUIRE(cur->chunk_data().data[1].is_null(i));
                }
            }
        }
        INFO("table_collection_name_value_defaults") {
            {
                auto cur = partial_insert(table_collection_name_value_defaults);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts);
                // column[1] will be filled with 100 default values (PostgreSQL
                // semantic: DEFAULT applies for omitted columns regardless of
                // nullability — see test_persistence::disk_partial_insert and
                // docs/remaining-5-tests-plan.md "Group 4 / Plan").
            }
            {
                auto cur = select_all(table_collection_name_value_defaults);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 4);
                for (size_t i = 0; i < kNumInserts * 3; i++) {
                    REQUIRE_FALSE(cur->chunk_data().data[1].is_null(i));
                }
                auto val = types::logical_value_t{dispatcher->resource(), cur->chunk_data().data[1].type()};
                for (size_t i = kNumInserts * 3; i < kNumInserts * 4; i++) {
                    REQUIRE(cur->chunk_data().value(1, i) == val);
                }
            }
        }
        INFO("table_collection_name_value_defaults_not_null") {
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
                    REQUIRE_FALSE(cur->chunk_data().data[1].is_null(i));
                }
                auto val = types::logical_value_t{dispatcher->resource(), cur->chunk_data().data[1].type()};
                for (size_t i = kNumInserts * 3; i < kNumInserts * 4; i++) {
                    REQUIRE(cur->chunk_data().value(1, i) == val);
                }
            }
        }
    }

    INFO("partial insert in reverse order") {
        // Same 6 columns as partial insert, but column list reversed.
        auto reversed_partial_insert = [&](const collection_name_t& collection) {
            std::vector<std::string> rows;
            rows.reserve(kNumInserts);
            for (int i = 1; i <= kNumInserts; ++i) {
                rows.push_back(fmt::format("({}, [{}, {}, {}, {}, {}], {}, {}, '{}', {})",
                                           i,
                                           i, i + 1, i + 2, i + 3, i + 4,
                                           (i % 2 != 0) ? "true" : "false",
                                           i + 0.1, i,
                                           i));
            }
            auto session = otterbrix::session_id_t();
            return dispatcher->execute_sql(session,
                fmt::format("INSERT INTO {}.{} (count_decimal, count_array, count_bool, count_double, "
                            "count_str, count_duplicate) VALUES {};",
                            table_database_name, collection, fmt::join(rows, ", ")));
        };
        auto select_all = [&](const collection_name_t& collection) {
            auto session = otterbrix::session_id_t();
            return dispatcher->execute_sql(session, "SELECT * FROM " + table_database_name + "." + collection + ";");
        };

        INFO("table_collection_name_simple") {
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
                    REQUIRE_FALSE(cur->chunk_data().data[1].is_null(i));
                }
                for (size_t i = kNumInserts * 3; i < kNumInserts * 5; i++) {
                    REQUIRE(cur->chunk_data().data[1].is_null(i));
                }
            }
        }
        INFO("table_collection_name_not_null") {
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
                    REQUIRE_FALSE(cur->chunk_data().data[1].is_null(i));
                }
            }
        }
        INFO("table_collection_name_null_defaults") {
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
                    REQUIRE_FALSE(cur->chunk_data().data[1].is_null(i));
                }
                for (size_t i = kNumInserts * 3; i < kNumInserts * 5; i++) {
                    REQUIRE(cur->chunk_data().data[1].is_null(i));
                }
            }
        }
        INFO("table_collection_name_value_defaults") {
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
                    REQUIRE_FALSE(cur->chunk_data().data[1].is_null(i));
                }
                auto val = types::logical_value_t{dispatcher->resource(), cur->chunk_data().data[1].type()};
                for (size_t i = kNumInserts * 3; i < kNumInserts * 5; i++) {
                    REQUIRE(cur->chunk_data().value(1, i) == val);
                }
            }
        }
        INFO("table_collection_name_value_defaults_not_null") {
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
                    REQUIRE_FALSE(cur->chunk_data().data[1].is_null(i));
                }
                auto val = types::logical_value_t{dispatcher->resource(), cur->chunk_data().data[1].type()};
                for (size_t i = kNumInserts * 3; i < kNumInserts * 5; i++) {
                    REQUIRE(cur->chunk_data().value(1, i) == val);
                }
            }
        }
    }

    INFO("invalid key in insert") {
        // All column names prefixed with "invalid_key_" — no match in catalog.
        auto invalid_keys_insert = [&](const collection_name_t& collection) {
            std::vector<std::string> rows;
            rows.reserve(kNumInserts);
            for (int i = 1; i <= kNumInserts; ++i) {
                rows.push_back(fmt::format("({}, {}, '{}', {}, {}, [{}, {}, {}, {}, {}], {})",
                                           i, i, i, i + 0.1,
                                           (i % 2 != 0) ? "true" : "false",
                                           i, i + 1, i + 2, i + 3, i + 4,
                                           i));
            }
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session,
                fmt::format("INSERT INTO {}.{} (invalid_key_count_duplicate, invalid_key_count, "
                            "invalid_key_count_str, invalid_key_count_double, invalid_key_count_bool, "
                            "invalid_key_count_array, invalid_key_count_decimal) VALUES {};",
                            table_database_name, collection, fmt::join(rows, ", ")));
            REQUIRE(cur->is_error());
        };

        invalid_keys_insert(table_collection_name_simple);
        invalid_keys_insert(table_collection_name_not_null);
        invalid_keys_insert(table_collection_name_null_defaults);
        invalid_keys_insert(table_collection_name_value_defaults);
        invalid_keys_insert(table_collection_name_value_defaults_not_null);
    }
}
