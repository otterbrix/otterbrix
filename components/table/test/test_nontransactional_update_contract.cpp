// The two-argument data_table_t::update is NOT a transactional update: no transaction id in
// the signature, so there is nothing to compare a conflicting writer against. Its callers
// (WAL replay before the scheduler; the pg_attribute commit-id stamp below the durable
// commit marker) can neither roll back nor race a second writer on the same row.
//
// test_storage_update_rollback.cpp pins the no-undo and no-snapshot halves with
// [!shouldfail] sentinels. This pins the remaining clause — NO CONFLICT DETECTION: two
// writers over the same row both succeed, the conflict is reported to no one, and the last
// writer silently wins. A third caller without the callers' serialization guarantee gets
// exactly this behaviour; if conflict detection is ever added to this leg, this case goes
// RED and the [!shouldfail] convention of the sibling file applies.

#include <catch2/catch_test_macros.hpp>
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
namespace tstorage = components::table::storage;

namespace {

    std::string ntu_db_path() {
        static std::string path = "/tmp/test_otterbrix_nontxn_update_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    struct ntu_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        tstorage::buffer_pool_t buffer_pool;
        tstorage::standard_buffer_manager_t buffer_manager;
        tstorage::single_file_block_manager_t block_manager;

        ntu_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, (std::remove(ntu_db_path().c_str()), ntu_db_path())) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~ntu_env_t() { std::remove(ntu_db_path().c_str()); }
    };

    std::unique_ptr<data_table_t> make_table(ntu_env_t& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", complex_logical_type(logical_type::BIGINT));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "t");
    }

    void append_row(data_table_t& table, ntu_env_t& env, int64_t value) {
        auto types = table.copy_types();
        data_chunk_t chunk(&env.resource, types, 1);
        chunk.data[0].set_value(0, logical_value_t(&env.resource, value));
        chunk.set_cardinality(1);
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    core::result_wrapper_t<std::pair<int64_t, uint64_t>>
    update_in_place(data_table_t& table, ntu_env_t& env, int64_t row_id, int64_t new_value) {
        auto types = table.copy_types();
        vector_t row_ids(&env.resource, complex_logical_type(logical_type::BIGINT), 1);
        row_ids.set_value(0, logical_value_t(&env.resource, row_id));
        data_chunk_t payload(&env.resource, types, 1);
        payload.data[0].set_value(0, logical_value_t(&env.resource, new_value));
        payload.set_cardinality(1);
        auto state = table.initialize_update({});
        return table.update(nontransactional_update_access_t::for_test(), *state, row_ids, payload);
    }

    int64_t scan_first_value(data_table_t& table, ntu_env_t& env) {
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);
        table_scan_state state(&env.resource);
        table.initialize_scan(state, column_ids);
        auto types = table.copy_types();
        data_chunk_t result(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        table.scan(result, state);
        REQUIRE(result.size() == 1);
        return result.data[0].value(0).value<int64_t>();
    }

} // namespace

TEST_CASE("components::table::update_segment::two_writers_over_one_row_conflict_with_no_one") {
    ntu_env_t env;
    auto table = make_table(env);
    append_row(*table, env, 1);

    // Writer A and writer B, same row, no transaction identity anywhere. On the
    // delete-stamp + append path a second writer draws write_conflict; here both land.
    auto first = update_in_place(*table, env, /*row_id=*/0, /*new_value=*/100);
    REQUIRE_FALSE(first.has_error());
    REQUIRE(first.value().second == 1);

    auto second = update_in_place(*table, env, /*row_id=*/0, /*new_value=*/200);
    INFO("the second writer must succeed silently today — this leg has no conflict channel");
    REQUIRE_FALSE(second.has_error());
    REQUIRE(second.value().second == 1);
    REQUIRE(second.value().first == 0); // {0, count}: the pair carries no conflict report

    // Last writer wins; writer A's value is gone with no diagnostic anywhere.
    const int64_t seen = scan_first_value(*table, env);
    INFO("row reads " << seen << " — writer A's 100 was overwritten with nothing reported");
    REQUIRE(seen == 200);
}
