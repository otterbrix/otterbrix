// row_group_t::evaluate_predicate materialises every column a pushed predicate binds by
// point-fetching it, and the column_fetch_state it fetches through owns two things that are
// PER-COLUMN in intent: the child states a nested column reads its fields through, and the
// pin handles that keep a fetched string's bytes addressable. One state shared across all
// bound columns aliases the first; making it per-ITERATION would break the second, because
// the chunk is handed to run_graph AFTER the loop and a borrowed string_view outlives a pin
// released at the end of the iteration that took it.
//
// The shape that satisfies both is the one row_group_t::fetch_row already uses: ONE state
// whose lifetime spans the graph run, and a per-column CHILD of it. This test exercises a
// predicate that binds two long-string columns at once -- long, because a string_t of 12
// bytes or fewer is inlined and never points into a block, so short values would not touch
// the pins this is about.

#include <catch2/catch_test_macros.hpp>
#include <components/expressions/execution_dag_builder.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <cstdio>
#include <string>
#include <unistd.h>

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    std::string predicate_state_db_path() {
        static std::string path = "/tmp/test_otterbrix_predicate_fetch_state_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    const std::string& predicate_state_fresh_db_path() {
        static const std::string path = (std::remove(predicate_state_db_path().c_str()), predicate_state_db_path());
        return path;
    }

    struct predicate_env {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::single_file_block_manager_t block_manager;

        predicate_env()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, predicate_state_fresh_db_path()) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~predicate_env() { std::remove(predicate_state_db_path().c_str()); }
    };

    // 40 characters: well past the 12-byte inline limit of string_t, so the value lives in a
    // block and the fetched view borrows a pin.
    std::string wide_string(char tag, size_t index) {
        std::string s(1, tag);
        s += "-";
        s += std::string(6 - std::to_string(index).size(), '0');
        s += std::to_string(index);
        s += "-payload-that-does-not-fit-inline!!";
        return s;
    }

} // anonymous namespace

TEST_CASE("components::table::predicate::two_long_string_columns_in_one_predicate_read_their_own_values") {
    predicate_env env;

    constexpr size_t rows = 300;

    std::vector<column_definition_t> columns;
    columns.emplace_back("a", complex_logical_type(logical_type::STRING_LITERAL));
    columns.emplace_back("b", complex_logical_type(logical_type::STRING_LITERAL));
    auto table = std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "t");

    auto types = table->copy_types();
    {
        auto chunk = data_chunk_t(&env.resource, types, rows);
        for (size_t i = 0; i < rows; i++) {
            chunk.data[0].set_value(i, logical_value_t(&env.resource, wide_string('a', i)));
            chunk.data[1].set_value(i, logical_value_t(&env.resource, wide_string('b', i)));
        }
        chunk.set_cardinality(rows);
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data{0, 0});
    }

    // a >= wide_string('a', 100) AND b < wide_string('b', 200) — both bound columns are long
    // strings, so evaluate_predicate fetches both through the state under test.
    namespace expr = components::expressions;
    const core::parameter_id_t lo_id{0};
    const core::parameter_id_t hi_id{1};
    parameter_map_t parameters(&env.resource);
    parameters.emplace(lo_id, logical_value_t{&env.resource, wide_string('a', 100)});
    parameters.emplace(hi_id, logical_value_t{&env.resource, wide_string('b', 200)});

    auto column_key = [&](size_t ordinal) {
        expr::key_t key{&env.resource};
        key.set_path(std::pmr::vector<size_t>{{ordinal}, std::pmr::polymorphic_allocator<size_t>{&env.resource}});
        return key;
    };
    auto predicate = expr::make_compare_union_expression(&env.resource, expr::compare_type::union_and);
    predicate->append_child(expr::make_compare_expression(&env.resource, expr::compare_type::gte, column_key(0), lo_id));
    predicate->append_child(expr::make_compare_expression(&env.resource, expr::compare_type::lt, column_key(1), hi_id));

    auto built = expr::build_condition_graph(&env.resource, parameters, predicate.get(), table->copy_types());
    REQUIRE_FALSE(built.has_error());
    table_filter_t filter{parameters,
                          components::graph_execution_context{},
                          std::move(built.value()),
                          expr::condition_kind::computed};

    std::vector<storage_index_t> column_ids;
    column_ids.emplace_back(0);
    column_ids.emplace_back(1);
    table_scan_state scan_state(&env.resource);
    table->initialize_scan(scan_state, column_ids, &filter);

    size_t produced = 0;
    for (;;) {
        auto result = data_chunk_t(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        table->scan(result, scan_state);
        REQUIRE_FALSE(scan_state.table_state.scan_error.contains_error());
        if (result.size() == 0) {
            break;
        }
        for (size_t row = 0; row < result.size(); row++) {
            auto a_cell = result.data[0].value(row);
            auto b_cell = result.data[1].value(row);
            const auto a = a_cell.value<std::string_view>();
            const auto b = b_cell.value<std::string_view>();
            INFO("row " << (produced + row) << " a=" << a << " b=" << b);
            // Each column must hold ITS OWN value: 'a' rows carry the a-tag, 'b' rows the
            // b-tag, and the two must agree on the index encoded in them.
            REQUIRE(a.substr(0, 2) == "a-");
            REQUIRE(b.substr(0, 2) == "b-");
            REQUIRE(a.substr(2) == b.substr(2));
            REQUIRE(a >= wide_string('a', 100));
            REQUIRE(b < wide_string('b', 200));
        }
        produced += result.size();
    }

    INFO("rows surviving the two-column predicate: " << produced);
    REQUIRE(produced == 100); // indices 100..199
}
