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
        auto create_collection = [&](const collection_name_t& collection,
                                     const std::vector<table::column_definition_t>& columns) {
            auto session = otterbrix::session_id_t();
            dispatcher->create_collection(session, table_database_name, collection, columns);
        };

        {
            auto session = otterbrix::session_id_t();
            dispatcher->create_database(session, table_database_name);
        }
        create_collection(table_collection_name_simple, columns_simple);
        create_collection(table_collection_name_not_null, columns_not_null);
        create_collection(table_collection_name_null_defaults, columns_null_defaults);
        create_collection(table_collection_name_value_defaults, columns_value_defaults);
        create_collection(table_collection_name_value_defaults_not_null, columns_value_defaults_not_null);
    }

    INFO("full insert") {
        // is the same for all
        auto full_insert = [&](const collection_name_t& collection) {
            auto chunk = gen_data_chunk(kNumInserts, 0, types, dispatcher->resource());
            auto ins = logical_plan::make_node_insert(dispatcher->resource(),
                                                      {table_database_name, collection},
                                                      std::move(chunk));
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, ins);
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
        // is the same for all
        auto swapped_types = types;
        std::swap(swapped_types[0], swapped_types[1]);

        auto reordered_insert = [&](const collection_name_t& collection) {
            auto chunk = gen_data_chunk(kNumInserts, 0, swapped_types, dispatcher->resource());
            auto ins = logical_plan::make_node_insert(dispatcher->resource(),
                                                      {table_database_name, collection},
                                                      std::move(chunk));
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, ins);
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
        // is the same for all
        auto changed_types = types;
        changed_types[0] = types::complex_logical_type{types::logical_type::INTEGER, "count_but_integer"};

        auto insert_with_conversion = [&](const collection_name_t& collection) {
            auto chunk = gen_data_chunk(kNumInserts, 0, changed_types, dispatcher->resource());
            auto ins = logical_plan::make_node_insert(dispatcher->resource(),
                                                      {table_database_name, collection},
                                                      std::move(chunk));
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, ins);
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
        // is the same for all
        auto partial_types = types;
        partial_types.erase(partial_types.begin() + 1);
        std::pmr::vector<expressions::key_t> fields(dispatcher->resource());
        fields.reserve(partial_types.size());
        for (const auto& type : partial_types) {
            fields.emplace_back(dispatcher->resource(), type.alias());
        }

        auto partial_insert = [&](const collection_name_t& collection) {
            auto chunk = gen_data_chunk(kNumInserts, 0, partial_types, dispatcher->resource());
            auto ins = logical_plan::make_node_insert(dispatcher->resource(),
                                                      {table_database_name, collection},
                                                      std::move(chunk),
                                                      std::pmr::vector<expressions::key_t>{fields});
            auto session = otterbrix::session_id_t();
            return dispatcher->execute_plan(session, ins);
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
                // column[1] will be filled with the value default (not null)
            }
            {
                auto cur = select_all(table_collection_name_value_defaults);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 4);
                for (size_t i = 0; i < kNumInserts * 4; i++) {
                    REQUIRE_FALSE(cur->chunk_data().data[1].is_null(i));
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
        // is the same for all
        auto reversed_partial_types = types;
        reversed_partial_types.erase(reversed_partial_types.begin() + 1);
        std::reverse(reversed_partial_types.begin(), reversed_partial_types.end());
        std::pmr::vector<expressions::key_t> fields(dispatcher->resource());
        fields.reserve(reversed_partial_types.size());
        for (const auto& type : reversed_partial_types) {
            fields.emplace_back(dispatcher->resource(), type.alias());
        }

        auto reversed_partial_insert = [&](const collection_name_t& collection) {
            auto chunk = gen_data_chunk(kNumInserts, 0, reversed_partial_types, dispatcher->resource());
            auto ins = logical_plan::make_node_insert(dispatcher->resource(),
                                                      {table_database_name, collection},
                                                      std::move(chunk),
                                                      std::pmr::vector<expressions::key_t>{fields});
            auto session = otterbrix::session_id_t();
            return dispatcher->execute_plan(session, ins);
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
                // column[1] will be filled with the value default (not null)
            }
            {
                auto cur = select_all(table_collection_name_value_defaults);
                REQUIRE(cur->is_success());
                REQUIRE(cur->size() == kNumInserts * 5);
                for (size_t i = 0; i < kNumInserts * 5; i++) {
                    REQUIRE_FALSE(cur->chunk_data().data[1].is_null(i));
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
        // is the same for all
        std::pmr::vector<expressions::key_t> fields(dispatcher->resource());
        fields.reserve(types.size());
        for (const auto& type : types) {
            fields.emplace_back(dispatcher->resource(), "invalid_key_" + type.alias());
        }

        auto invalid_keys_insert = [&](const collection_name_t& collection) {
            auto chunk = gen_data_chunk(kNumInserts, 0, types, dispatcher->resource());
            auto ins = logical_plan::make_node_insert(dispatcher->resource(),
                                                      {table_database_name, collection},
                                                      std::move(chunk),
                                                      std::pmr::vector<expressions::key_t>{fields});
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_plan(session, ins);
            REQUIRE(cur->is_error());
        };

        invalid_keys_insert(table_collection_name_simple);
        invalid_keys_insert(table_collection_name_not_null);
        invalid_keys_insert(table_collection_name_null_defaults);
        invalid_keys_insert(table_collection_name_value_defaults);
        invalid_keys_insert(table_collection_name_value_defaults_not_null);
    }
}
