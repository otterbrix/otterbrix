#include <catch2/catch.hpp>
#include <components/table/data_table.hpp>
#include <components/table/transaction_manager.hpp>
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

    void append_rows_txn(data_table_t& table, test_env& env, int64_t start, uint64_t count, transaction_data txn) {
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
        table.finalize_append(state, txn);
    }

    uint64_t scan_count(data_table_t& table, test_env& env) {
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);

        table_scan_state scan_state(&env.resource);
        table.initialize_scan(scan_state, column_ids);

        auto types = table.copy_types();
        auto result = data_chunk_t(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        uint64_t total = 0;
        table.scan(result, scan_state);
        total += result.size();
        return total;
    }

    uint64_t scan_count_txn(data_table_t& table, test_env& env, transaction_data txn) {
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);

        table_scan_state scan_state(&env.resource);
        table.initialize_scan(scan_state, column_ids);
        scan_state.table_state.txn = txn;

        auto types = table.copy_types();
        auto result = data_chunk_t(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        uint64_t total = 0;
        table.scan(result, scan_state);
        total += result.size();
        return total;
    }

} // anonymous namespace

TEST_CASE("components::table::mvcc::append_commit_visible") {
    test_env env;
    auto table = make_int_table(env);

    transaction_manager_t mgr;
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    append_rows_txn(*table, env, 0, 10, txn.data());

    auto commit_id = mgr.commit(session);
    table->commit_append(commit_id, 0, 10);

    auto count = scan_count(*table, env);
    REQUIRE(count == 10);
}

TEST_CASE("components::table::mvcc::append_revert_invisible") {
    test_env env;
    auto table = make_int_table(env);

    transaction_manager_t mgr;
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    append_rows_txn(*table, env, 0, 10, txn.data());

    mgr.abort(session);
    table->revert_append(0, 10);

    auto count = scan_count(*table, env);
    REQUIRE(count == 0);
}

TEST_CASE("components::table::mvcc::append_without_txn_backward_compat") {
    test_env env;
    auto table = make_int_table(env);

    append_rows(*table, env, 0, 100);

    auto count = scan_count(*table, env);
    REQUIRE(count == 100);
}

TEST_CASE("components::table::mvcc::cleanup_versions") {
    test_env env;
    auto table = make_int_table(env);

    transaction_manager_t mgr;
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    append_rows_txn(*table, env, 0, 10, txn.data());
    auto commit_id = mgr.commit(session);
    table->commit_append(commit_id, 0, 10);

    auto lowest = mgr.lowest_active_start_time();
    table->cleanup_versions(lowest);

    auto count = scan_count(*table, env);
    REQUIRE(count == 10);
}

TEST_CASE("components::table::mvcc::multiple_txn_appends") {
    test_env env;
    auto table = make_int_table(env);

    transaction_manager_t mgr;

    // Transaction 1: append 10 rows
    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 10, txn1.data());
    auto cid1 = mgr.commit(s1);
    table->commit_append(cid1, 0, 10);

    // Transaction 2: append 5 more rows
    auto s2 = components::session::session_id_t::generate_uid();
    auto& txn2 = mgr.begin_transaction(s2);
    append_rows_txn(*table, env, 10, 5, txn2.data());
    auto cid2 = mgr.commit(s2);
    table->commit_append(cid2, 10, 5);

    auto count = scan_count(*table, env);
    REQUIRE(count == 15);
}

TEST_CASE("components::table::mvcc::delete_rows_txn_commit_all_deletes") {
    test_env env;
    auto table = make_int_table(env);

    // Append 10 rows (non-txn, immediately visible)
    append_rows(*table, env, 0, 10);
    REQUIRE(scan_count(*table, env) == 10);

    // Begin transaction and delete 5 rows
    transaction_manager_t mgr;
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    // Build row_ids vector (BIGINT) with values 0..4
    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 5);
    for (uint64_t i = 0; i < 5; i++) {
        row_ids_chunk.data[0].set_value(i, logical_value_t(&env.resource, static_cast<int64_t>(i)));
    }
    row_ids_chunk.set_cardinality(5);

    auto txn_id = txn.data().transaction_id;

    table_delete_state del_state(&env.resource);
    table->delete_rows(del_state, row_ids_chunk.data[0], 5, txn_id);

    // Commit: finalize all deletes for this txn
    // Note: mgr.commit() erases txn from active_ map, so txn ref becomes dangling
    auto commit_id = mgr.commit(session);
    table->commit_all_deletes(txn_id, commit_id);

    // Scan should see only 5 rows
    REQUIRE(scan_count(*table, env) == 5);
}

TEST_CASE("components::table::mvcc::delete_rows_txn_without_commit_visible") {
    test_env env;
    auto table = make_int_table(env);

    // Append 10 rows (non-txn, immediately visible)
    append_rows(*table, env, 0, 10);
    REQUIRE(scan_count(*table, env) == 10);

    // Begin transaction and delete 5 rows
    transaction_manager_t mgr;
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    // Build row_ids vector (BIGINT) with values 0..4
    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 5);
    for (uint64_t i = 0; i < 5; i++) {
        row_ids_chunk.data[0].set_value(i, logical_value_t(&env.resource, static_cast<int64_t>(i)));
    }
    row_ids_chunk.set_cardinality(5);

    auto txn_id = txn.data().transaction_id;

    table_delete_state del_state(&env.resource);
    table->delete_rows(del_state, row_ids_chunk.data[0], 5, txn_id);

    // Abort — don't commit deletes (mgr.abort erases txn, so txn ref becomes dangling)
    mgr.abort(session);

    // Non-txn scan should still see all 10 rows (deleted[] has txn_id, not commit_id)
    REQUIRE(scan_count(*table, env) == 10);
}

TEST_CASE("components::table::mvcc::cleanup_committed_deletes") {
    test_env env;
    auto table = make_int_table(env);

    // Append 10 rows (non-txn, immediately visible)
    append_rows(*table, env, 0, 10);
    REQUIRE(scan_count(*table, env) == 10);

    // Delete all 10 rows via transaction
    transaction_manager_t mgr;
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 10);
    for (uint64_t i = 0; i < 10; i++) {
        row_ids_chunk.data[0].set_value(i, logical_value_t(&env.resource, static_cast<int64_t>(i)));
    }
    row_ids_chunk.set_cardinality(10);

    auto txn_id = txn.data().transaction_id;
    table_delete_state del_state(&env.resource);
    table->delete_rows(del_state, row_ids_chunk.data[0], 10, txn_id);

    auto commit_id = mgr.commit(session);
    table->commit_all_deletes(txn_id, commit_id);

    // After commit, scan should see 0 rows
    REQUIRE(scan_count(*table, env) == 0);

    // cleanup_versions should succeed (committed deletes should not block cleanup)
    auto lowest = mgr.lowest_active_start_time();
    table->cleanup_versions(lowest);

    // committed_row_count should reflect the deletes
    // (Verify through scan — still 0 rows)
    REQUIRE(scan_count(*table, env) == 0);
}

TEST_CASE("components::table::mvcc::cleanup_partial_deletes") {
    test_env env;
    auto table = make_int_table(env);

    // Append 10 rows (non-txn, immediately visible)
    append_rows(*table, env, 0, 10);
    REQUIRE(scan_count(*table, env) == 10);

    // Delete 5 rows via transaction
    transaction_manager_t mgr;
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 5);
    for (uint64_t i = 0; i < 5; i++) {
        row_ids_chunk.data[0].set_value(i, logical_value_t(&env.resource, static_cast<int64_t>(i)));
    }
    row_ids_chunk.set_cardinality(5);

    auto txn_id = txn.data().transaction_id;
    table_delete_state del_state(&env.resource);
    table->delete_rows(del_state, row_ids_chunk.data[0], 5, txn_id);

    auto commit_id = mgr.commit(session);
    table->commit_all_deletes(txn_id, commit_id);

    // 5 rows visible
    REQUIRE(scan_count(*table, env) == 5);

    // cleanup_versions should succeed now
    auto lowest = mgr.lowest_active_start_time();
    table->cleanup_versions(lowest);

    // Still 5 rows visible after cleanup
    REQUIRE(scan_count(*table, env) == 5);
}

TEST_CASE("components::table::mvcc::compact_after_delete") {
    test_env env;
    auto table = make_int_table(env);

    // Append 100 rows
    append_rows(*table, env, 0, 100);
    REQUIRE(scan_count(*table, env) == 100);

    // Delete 50 rows (0..49)
    transaction_manager_t mgr;
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 50);
    for (uint64_t i = 0; i < 50; i++) {
        row_ids_chunk.data[0].set_value(i, logical_value_t(&env.resource, static_cast<int64_t>(i)));
    }
    row_ids_chunk.set_cardinality(50);

    auto txn_id = txn.data().transaction_id;
    table_delete_state del_state(&env.resource);
    table->delete_rows(del_state, row_ids_chunk.data[0], 50, txn_id);

    auto commit_id = mgr.commit(session);
    table->commit_all_deletes(txn_id, commit_id);

    // 50 rows visible
    REQUIRE(scan_count(*table, env) == 50);

    // Compact: physically remove deleted rows
    table->compact();

    // Still 50 rows visible
    REQUIRE(scan_count(*table, env) == 50);

    // Total rows should now be 50 (reduced allocation)
    REQUIRE(table->row_group()->total_rows() == 50);
}

TEST_CASE("components::table::mvcc::uncommitted_rows_invisible_to_other_txn") {
    test_env env;
    auto table = make_int_table(env);

    transaction_manager_t mgr;

    // Txn1 appends 10 rows, does NOT commit
    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 10, txn1.data());

    // Txn2 scans — should see 0 rows (txn1 uncommitted)
    auto s2 = components::session::session_id_t::generate_uid();
    auto& txn2 = mgr.begin_transaction(s2);
    REQUIRE(scan_count_txn(*table, env, txn2.data()) == 0);

    // Commit txn1
    auto commit_id = mgr.commit(s1);
    table->commit_append(commit_id, 0, 10);

    // Txn3 scans — should see 10 rows
    auto s3 = components::session::session_id_t::generate_uid();
    auto& txn3 = mgr.begin_transaction(s3);
    REQUIRE(scan_count_txn(*table, env, txn3.data()) == 10);

    mgr.abort(s2);
    mgr.abort(s3);
}

TEST_CASE("components::table::mvcc::delete_not_visible_until_commit") {
    test_env env;
    auto table = make_int_table(env);

    // Append 10 rows (non-txn, immediately visible)
    append_rows(*table, env, 0, 10);
    REQUIRE(scan_count(*table, env) == 10);

    transaction_manager_t mgr;

    // Txn1 deletes rows 0..4 (does NOT commit yet)
    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);

    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 5);
    for (uint64_t i = 0; i < 5; i++) {
        row_ids_chunk.data[0].set_value(i, logical_value_t(&env.resource, static_cast<int64_t>(i)));
    }
    row_ids_chunk.set_cardinality(5);

    auto txn_id = txn1.data().transaction_id;
    table_delete_state del_state(&env.resource);
    table->delete_rows(del_state, row_ids_chunk.data[0], 5, txn_id);

    // Txn2 scans — should still see 10 rows (delete uncommitted)
    auto s2 = components::session::session_id_t::generate_uid();
    auto& txn2 = mgr.begin_transaction(s2);
    REQUIRE(scan_count_txn(*table, env, txn2.data()) == 10);
    mgr.abort(s2);

    // Commit delete
    auto commit_id = mgr.commit(s1);
    table->commit_all_deletes(txn_id, commit_id);

    // Txn3 scans — should see 5 rows
    auto s3 = components::session::session_id_t::generate_uid();
    auto& txn3 = mgr.begin_transaction(s3);
    REQUIRE(scan_count_txn(*table, env, txn3.data()) == 5);
    mgr.abort(s3);
}

TEST_CASE("components::table::mvcc::txn_sees_own_writes") {
    test_env env;
    auto table = make_int_table(env);

    transaction_manager_t mgr;

    // Txn1 appends 5 rows
    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 5, txn1.data());

    // Same txn scans — should see 5 rows (own writes)
    REQUIRE(scan_count_txn(*table, env, txn1.data()) == 5);

    // Different txn scans — should see 0 rows (txn1 uncommitted)
    auto s2 = components::session::session_id_t::generate_uid();
    auto& txn2 = mgr.begin_transaction(s2);
    REQUIRE(scan_count_txn(*table, env, txn2.data()) == 0);

    mgr.abort(s1);
    table->revert_append(0, 5);
    mgr.abort(s2);
}
