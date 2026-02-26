#include <catch2/catch.hpp>
#include <set>
#include <components/table/data_table.hpp>
#include <components/table/row_group.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    struct test_env {
        std::pmr::synchronized_pool_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::in_memory_block_manager_t block_manager;

        test_env()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, storage::DEFAULT_BLOCK_ALLOC_SIZE) {}
    };

    std::unique_ptr<data_table_t> make_int_table(test_env& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", complex_logical_type(logical_type::BIGINT));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "test");
    }

    void append_rows(data_table_t& table, test_env& env, int64_t start, uint64_t count) {
        auto types = table.copy_types();
        auto chunk = data_chunk_t(&env.resource, types, count);
        for (uint64_t i = 0; i < count; i++) {
            chunk.data[0].set_value(i, logical_value_t(&env.resource, start + static_cast<int64_t>(i)));
        }
        chunk.set_cardinality(count);

        table_append_state state(&env.resource);
        table.append_lock(state);
        table.initialize_append(state);
        table.append(chunk, state);
        table.finalize_append(state, transaction_data{0, 0});
    }

} // anonymous namespace

TEST_CASE("parallel scan: each call gets different row group") {
    test_env env;
    auto table = make_int_table(env);

    // Append 4 batches of 1024 rows each -> 4 row groups
    constexpr uint64_t rows_per_rg = DEFAULT_VECTOR_CAPACITY;
    constexpr int num_row_groups = 4;
    for (int i = 0; i < num_row_groups; i++) {
        append_rows(*table, env, static_cast<int64_t>(static_cast<uint64_t>(i) * rows_per_rg), rows_per_rg);
    }

    REQUIRE(table->row_group()->total_rows() == num_row_groups * rows_per_rg);

    std::vector<storage_index_t> column_ids;
    column_ids.emplace_back(0);

    auto parallel_state = table->create_parallel_scan_state(column_ids);
    REQUIRE(parallel_state->total_row_groups == num_row_groups);

    auto types = table->copy_types();
    uint64_t total_rows_scanned = 0;
    int chunks_retrieved = 0;

    for (int i = 0; i < num_row_groups; i++) {
        table_scan_state local_state(&env.resource);
        data_chunk_t result(&env.resource, types, rows_per_rg);
        bool got_chunk = table->next_parallel_chunk(*parallel_state, local_state, result);
        REQUIRE(got_chunk);
        REQUIRE(result.size() > 0);
        total_rows_scanned += result.size();
        chunks_retrieved++;
    }

    // 5th call should return false — no more row groups
    {
        table_scan_state local_state(&env.resource);
        data_chunk_t result(&env.resource, types, rows_per_rg);
        bool got_chunk = table->next_parallel_chunk(*parallel_state, local_state, result);
        REQUIRE_FALSE(got_chunk);
    }

    REQUIRE(chunks_retrieved == num_row_groups);
    REQUIRE(total_rows_scanned == num_row_groups * rows_per_rg);
}

TEST_CASE("parallel scan: atomic index increments correctly") {
    test_env env;
    auto table = make_int_table(env);

    // Append 3 row groups
    constexpr uint64_t rows_per_rg = DEFAULT_VECTOR_CAPACITY;
    for (int i = 0; i < 3; i++) {
        append_rows(*table, env, static_cast<int64_t>(static_cast<uint64_t>(i) * rows_per_rg), rows_per_rg);
    }

    std::vector<storage_index_t> column_ids;
    column_ids.emplace_back(0);

    auto parallel_state = table->create_parallel_scan_state(column_ids);

    // Verify atomic counter starts at 0 and increments
    REQUIRE(parallel_state->next_row_group_idx.load() == 0);

    auto types = table->copy_types();
    table_scan_state local_state(&env.resource);
    data_chunk_t result(&env.resource, types, rows_per_rg);

    table->next_parallel_chunk(*parallel_state, local_state, result);
    REQUIRE(parallel_state->next_row_group_idx.load() == 1);

    table->next_parallel_chunk(*parallel_state, local_state, result);
    REQUIRE(parallel_state->next_row_group_idx.load() == 2);

    table->next_parallel_chunk(*parallel_state, local_state, result);
    REQUIRE(parallel_state->next_row_group_idx.load() == 3);

    // No more row groups
    bool got = table->next_parallel_chunk(*parallel_state, local_state, result);
    REQUIRE_FALSE(got);
    REQUIRE(parallel_state->next_row_group_idx.load() == 4);
}

TEST_CASE("parallel scan: empty table returns false immediately") {
    test_env env;
    auto table = make_int_table(env);

    std::vector<storage_index_t> column_ids;
    column_ids.emplace_back(0);

    auto parallel_state = table->create_parallel_scan_state(column_ids);
    REQUIRE(parallel_state->total_row_groups == 0);

    auto types = table->copy_types();
    table_scan_state local_state(&env.resource);
    data_chunk_t result(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
    bool got = table->next_parallel_chunk(*parallel_state, local_state, result);
    REQUIRE_FALSE(got);
}

TEST_CASE("parallel scan: all row values are correct") {
    test_env env;
    auto table = make_int_table(env);

    constexpr uint64_t rows_per_rg = DEFAULT_VECTOR_CAPACITY;
    constexpr int num_row_groups = 3;
    for (int i = 0; i < num_row_groups; i++) {
        append_rows(*table, env, static_cast<int64_t>(static_cast<uint64_t>(i) * rows_per_rg), rows_per_rg);
    }

    std::vector<storage_index_t> column_ids;
    column_ids.emplace_back(0);
    auto parallel_state = table->create_parallel_scan_state(column_ids);

    auto types = table->copy_types();
    std::set<int64_t> all_values;

    for (int i = 0; i < num_row_groups; i++) {
        table_scan_state local_state(&env.resource);
        data_chunk_t result(&env.resource, types, rows_per_rg);
        bool got = table->next_parallel_chunk(*parallel_state, local_state, result);
        REQUIRE(got);
        result.data[0].flatten(result.size());
        auto data = result.data[0].data<int64_t>();
        for (uint64_t j = 0; j < result.size(); j++) {
            all_values.insert(data[j]);
        }
    }

    // Verify all values 0..3071 are present
    REQUIRE(all_values.size() == num_row_groups * rows_per_rg);
    REQUIRE(*all_values.begin() == 0);
    REQUIRE(*all_values.rbegin() == static_cast<int64_t>(num_row_groups * rows_per_rg - 1));
}

TEST_CASE("parallel scan: two independent scans on same table") {
    test_env env;
    auto table = make_int_table(env);

    constexpr uint64_t rows_per_rg = DEFAULT_VECTOR_CAPACITY;
    constexpr int num_row_groups = 4;
    for (int i = 0; i < num_row_groups; i++) {
        append_rows(*table, env, static_cast<int64_t>(static_cast<uint64_t>(i) * rows_per_rg), rows_per_rg);
    }

    std::vector<storage_index_t> column_ids;
    column_ids.emplace_back(0);

    // Create two independent parallel scan states
    auto parallel_state_a = table->create_parallel_scan_state(column_ids);
    auto parallel_state_b = table->create_parallel_scan_state(column_ids);

    auto types = table->copy_types();
    uint64_t total_a = 0;
    uint64_t total_b = 0;

    // Interleave scans from both states
    for (int i = 0; i < num_row_groups; i++) {
        {
            table_scan_state local_state(&env.resource);
            data_chunk_t result(&env.resource, types, rows_per_rg);
            if (table->next_parallel_chunk(*parallel_state_a, local_state, result)) {
                total_a += result.size();
            }
        }
        {
            table_scan_state local_state(&env.resource);
            data_chunk_t result(&env.resource, types, rows_per_rg);
            if (table->next_parallel_chunk(*parallel_state_b, local_state, result)) {
                total_b += result.size();
            }
        }
    }

    REQUIRE(total_a == num_row_groups * rows_per_rg);
    REQUIRE(total_b == num_row_groups * rows_per_rg);
}

TEST_CASE("parallel scan: copy_segments provides snapshot") {
    test_env env;
    auto table = make_int_table(env);

    constexpr uint64_t rows_per_rg = DEFAULT_VECTOR_CAPACITY;
    for (int i = 0; i < 3; i++) {
        append_rows(*table, env, static_cast<int64_t>(static_cast<uint64_t>(i) * rows_per_rg), rows_per_rg);
    }

    auto* tree = table->row_group()->row_group_tree();
    auto segments = tree->copy_segments();

    REQUIRE(segments.size() == 3);
    for (uint64_t i = 0; i < segments.size(); i++) {
        REQUIRE(segments[i] != nullptr);
        REQUIRE(segments[i]->count == rows_per_rg);
    }
}
