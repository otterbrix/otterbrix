// Measures the in-place storage update (data_table_t::update) reached only from agent_disk.cpp's
// direct_update_sync (WAL replay, pg_attribute patches) -- not the three-argument SQL UPDATE path.
// The two [!shouldfail] cases assert what a version-chained update WOULD do; they must go red once
// implemented -- the signal to drop the tag, not weaken it.

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

namespace {

    std::string update_rollback_db_path() {
        static std::string path = "/tmp/test_otterbrix_storage_update_rollback_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    const std::string& update_rollback_fresh_db_path() {
        static const std::string path = (std::remove(update_rollback_db_path().c_str()), update_rollback_db_path());
        return path;
    }

    struct update_env {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::single_file_block_manager_t block_manager;

        update_env()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, update_rollback_fresh_db_path()) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~update_env() { std::remove(update_rollback_db_path().c_str()); }
    };

    std::unique_ptr<data_table_t> make_one_column_table(update_env& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", complex_logical_type(logical_type::BIGINT));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "t");
    }

    void append_committed_row(data_table_t& table, update_env& env, int64_t value) {
        auto types = table.copy_types();
        auto chunk = data_chunk_t(&env.resource, types, 1);
        chunk.data[0].set_value(0, logical_value_t(&env.resource, value));
        chunk.set_cardinality(1);

        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    core::result_wrapper_t<std::pair<int64_t, uint64_t>>
    update_in_place(data_table_t& table, update_env& env, int64_t row_id, int64_t new_value) {
        auto types = table.copy_types();
        auto row_ids = vector_t(&env.resource, complex_logical_type(logical_type::BIGINT), 1);
        row_ids.set_value(0, logical_value_t(&env.resource, row_id));
        auto payload = data_chunk_t(&env.resource, types, 1);
        payload.data[0].set_value(0, logical_value_t(&env.resource, new_value));
        payload.set_cardinality(1);
        auto state = table.initialize_update({});
        return table.update(*state, row_ids, payload);
    }

    int64_t scan_first_value(data_table_t& table, update_env& env, transaction_data txn) {
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);
        table_scan_state scan_state(&env.resource);
        table.initialize_scan(scan_state, column_ids);
        scan_state.table_state.txn = txn;
        auto types = table.copy_types();
        auto result = data_chunk_t(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        table.scan(result, scan_state);
        REQUIRE(result.size() == 1);
        return result.data[0].value(0).value<int64_t>();
    }

} // anonymous namespace

TEST_CASE("components::table::update_segment::snapshot_predating_an_in_place_update_still_reads_the_old_value",
          "[!shouldfail]") {
    update_env env;
    auto table = make_one_column_table(env);
    append_committed_row(*table, env, 1);

    const transaction_data reader{TRANSACTION_ID_START + 5, 10};
    REQUIRE(scan_first_value(*table, env, reader) == 1);

    auto updated = update_in_place(*table, env, /*row_id=*/0, /*new_value=*/99);
    REQUIRE_FALSE(updated.has_error());
    REQUIRE(updated.value().second == 1);

    const int64_t seen = scan_first_value(*table, env, reader);
    INFO("snapshot reader saw " << seen << ", a version-chained update would show 1");
    REQUIRE(seen == 1);
}

TEST_CASE("components::table::update_segment::an_abandoned_in_place_update_leaves_the_row_at_its_old_value",
          "[!shouldfail]") {
    update_env env;
    auto table = make_one_column_table(env);
    append_committed_row(*table, env, 1);

    const uint64_t txn_id = TRANSACTION_ID_START + 7;
    auto updated = update_in_place(*table, env, /*row_id=*/0, /*new_value=*/99);
    REQUIRE_FALSE(updated.has_error());

    table->revert_all_deletes(txn_id);
    REQUIRE_FALSE(table->revert_append(1, 0).has_error());

    const int64_t seen = scan_first_value(*table, env, transaction_data{TRANSACTION_ID_START + 8, 20});
    INFO("after the abandoned statement the row reads " << seen << ", a rollback would leave 1");
    REQUIRE(seen == 1);
}

// update() must refuse a superseded table, or it silently writes into a collection no reader reopens.
TEST_CASE("components::table::update_segment::updating_a_superseded_table_is_refused") {
    update_env env;
    auto table = make_one_column_table(env);
    append_committed_row(*table, env, 1);

    column_definition_t added("added", complex_logical_type(logical_type::BIGINT));
    auto successor = std::make_unique<data_table_t>(*table, added);

    auto updated = update_in_place(*table, env, /*row_id=*/0, /*new_value=*/99);
    REQUIRE(updated.has_error());
    INFO("error: " << updated.error().what.c_str());
    REQUIRE(updated.error().type == core::error_code_t::write_conflict);
}

// Control (not [!shouldfail]): the overlay IS applied, so the two cases above fail on "no MVCC."
TEST_CASE("components::table::update_segment::an_in_place_update_is_published_to_the_next_reader") {
    update_env env;
    auto table = make_one_column_table(env);
    append_committed_row(*table, env, 1);

    auto updated = update_in_place(*table, env, /*row_id=*/0, /*new_value=*/99);
    REQUIRE_FALSE(updated.has_error());
    REQUIRE(updated.value().second == 1);

    REQUIRE(scan_first_value(*table, env, transaction_data{TRANSACTION_ID_START + 9, 30}) == 99);
}
