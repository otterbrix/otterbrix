#include <catch2/catch.hpp>

#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <core/file/local_file_system.hpp>

namespace {

struct table_test_env_t {
    std::pmr::synchronized_pool_resource resource;
    core::filesystem::local_file_system_t fs;
    components::table::storage::buffer_pool_t buffer_pool;
    components::table::storage::standard_buffer_manager_t buffer_manager;
    components::table::storage::in_memory_block_manager_t block_manager;

    table_test_env_t()
        : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
        , buffer_manager(&resource, fs, buffer_pool)
        , block_manager(buffer_manager, components::table::storage::DEFAULT_BLOCK_ALLOC_SIZE) {}
};

void append_chunk(components::table::data_table_t& table, components::vector::data_chunk_t& chunk) {
    components::table::table_append_state state(chunk.resource());
    table.append_lock(state);
    table.initialize_append(state);
    table.append(chunk, state);
    table.finalize_append(state, components::table::transaction_data{0, 0});
}

} // namespace

TEST_CASE("components::table::logical_storage_stats scalar columns count fixed, varlen, and validity bytes") {
    using namespace components::table;
    using namespace components::types;
    using namespace components::vector;

    table_test_env_t env;

    std::vector<column_definition_t> columns;
    columns.emplace_back("id", logical_type::BIGINT);
    columns.emplace_back("note", logical_type::STRING_LITERAL);

    data_table_t table(&env.resource, env.block_manager, std::move(columns), "logical_stats_scalar");

    const std::string overflow_value(40, 'x');
    data_chunk_t chunk(&env.resource, table.copy_types(), 3);
    chunk.set_cardinality(3);
    chunk.set_value(0, 0, logical_value_t{&env.resource, int64_t{10}});
    chunk.set_value(1, 0, logical_value_t{&env.resource, std::string("abc")});
    chunk.set_value(0, 1, logical_value_t{&env.resource, int64_t{20}});
    chunk.set_value(1, 1, logical_value_t{&env.resource, nullptr});
    chunk.set_value(0, 2, logical_value_t{&env.resource, int64_t{30}});
    chunk.set_value(1, 2, logical_value_t{&env.resource, overflow_value});
    append_chunk(table, chunk);

    const auto stats = table.logical_storage_stats();
    const auto types = table.copy_types();
    const auto validity_bytes = validity_mask_t::validity_mask_size(3);
    const auto expected_fixed_bytes = 3 * types[0].size();
    const auto expected_varlen_bytes = uint64_t{3 + overflow_value.size()};
    const auto expected_validity_bytes = 2 * validity_bytes;

    REQUIRE(stats.rows == 3);
    REQUIRE(stats.fixed_value_bytes == expected_fixed_bytes);
    REQUIRE(stats.varlen_value_bytes == expected_varlen_bytes);
    REQUIRE(stats.validity_bytes == expected_validity_bytes);
    REQUIRE(stats.logical_input_bytes() == expected_fixed_bytes + expected_varlen_bytes + expected_validity_bytes);
}

TEST_CASE("components::table::logical_storage_stats nested columns include child values and child validity") {
    using namespace components::table;
    using namespace components::types;
    using namespace components::vector;

    table_test_env_t env;

    const auto list_type = complex_logical_type::create_list(logical_type::STRING_LITERAL, "tags");
    const auto array_type = complex_logical_type::create_array(logical_type::BIGINT, 2, "metrics");

    std::vector<column_definition_t> columns;
    columns.emplace_back("tags", list_type);
    columns.emplace_back("metrics", array_type);

    data_table_t table(&env.resource, env.block_manager, std::move(columns), "logical_stats_nested");

    data_chunk_t chunk(&env.resource, table.copy_types(), 2);
    chunk.set_cardinality(2);
    chunk.set_value(0,
                    0,
                    logical_value_t::create_list(
                        &env.resource,
                        logical_type::STRING_LITERAL,
                        std::vector<logical_value_t>{
                            logical_value_t{&env.resource, std::string("ab")},
                            logical_value_t{&env.resource, std::string("cdef")},
                        }));
    chunk.set_value(1,
                    0,
                    logical_value_t::create_array(
                        &env.resource,
                        logical_type::BIGINT,
                        std::vector<logical_value_t>{
                            logical_value_t{&env.resource, int64_t{1}},
                            logical_value_t{&env.resource, int64_t{2}},
                        }));
    chunk.set_value(0, 1, logical_value_t{&env.resource, nullptr});
    chunk.set_value(1,
                    1,
                    logical_value_t::create_array(
                        &env.resource,
                        logical_type::BIGINT,
                        std::vector<logical_value_t>{
                            logical_value_t{&env.resource, int64_t{3}},
                            logical_value_t{&env.resource, int64_t{4}},
                        }));
    append_chunk(table, chunk);

    const auto stats = table.logical_storage_stats();
    const auto types = table.copy_types();
    const auto top_level_validity_bytes = validity_mask_t::validity_mask_size(2);
    const auto child_list_validity_bytes = validity_mask_t::validity_mask_size(2);
    const auto child_array_validity_bytes = validity_mask_t::validity_mask_size(4);
    const auto expected_fixed_bytes = 2 * types[0].size() + 4 * types[1].child_type().size();
    const auto expected_varlen_bytes = uint64_t{2 + 4};
    const auto expected_validity_bytes =
        top_level_validity_bytes + child_list_validity_bytes + top_level_validity_bytes + child_array_validity_bytes;

    REQUIRE(stats.rows == 2);
    REQUIRE(stats.fixed_value_bytes == expected_fixed_bytes);
    REQUIRE(stats.varlen_value_bytes == expected_varlen_bytes);
    REQUIRE(stats.validity_bytes == expected_validity_bytes);
    REQUIRE(stats.logical_input_bytes() == expected_fixed_bytes + expected_varlen_bytes + expected_validity_bytes);
}
