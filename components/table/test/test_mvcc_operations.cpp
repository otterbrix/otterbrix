#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/transaction_manager.hpp>
#include <core/file/local_file_system.hpp>
#include <cstdio>
#include <string>
#include <unistd.h>

#include <set>

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    // B4: the fixture runs on a real .otbx. It used to hold the file-less block manager, whose
    // every I/O virtual now aborts; the block_manager_t predicate that made that safe is gone
    // along with the in-memory table mode it named. The row counts here span more than one row
    // group, so closing one writes its segments through to the file — this fixture reaches the
    // disk path for real. The substrate is all that changes: not one assertion below is about it.
    std::string mvcc_operations_db_path() {
        static std::string path = "/tmp/test_otterbrix_mvcc_operations_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    // Removes any leftover from an earlier process that died holding this pid, then names the
    // file. Called from the member-init list, so the removal precedes the manager's open.
    const std::string& mvcc_operations_fresh_db_path() {
        static const std::string path = (std::remove(mvcc_operations_db_path().c_str()), mvcc_operations_db_path());
        return path;
    }

    struct test_env {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::single_file_block_manager_t block_manager;

        test_env()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, mvcc_operations_fresh_db_path()) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~test_env() { std::remove(mvcc_operations_db_path().c_str()); }
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
            chunk.data[0].set_value(i, start + static_cast<int64_t>(i));
        }
        chunk.set_cardinality(count);

        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    void append_rows_txn(data_table_t& table, test_env& env, int64_t start, uint64_t count, transaction_data txn) {
        auto types = table.copy_types();
        auto chunk = data_chunk_t(&env.resource, types, count);
        for (uint64_t i = 0; i < count; i++) {
            chunk.data[0].set_value(i, start + static_cast<int64_t>(i));
        }
        chunk.set_cardinality(count);

        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
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

    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    append_rows_txn(*table, env, 0, 10, txn.data());

    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_append(commit_id, 0, 10);

    auto count = scan_count(*table, env);
    REQUIRE(count == 10);
}

TEST_CASE("components::table::mvcc::append_revert_invisible") {
    test_env env;
    auto table = make_int_table(env);

    transaction_manager_t mgr(&env.resource);
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

    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    append_rows_txn(*table, env, 0, 10, txn.data());
    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_append(commit_id, 0, 10);

    auto lowest = mgr.lowest_active_start_time();
    table->cleanup_versions(lowest);

    auto count = scan_count(*table, env);
    REQUIRE(count == 10);
}

TEST_CASE("components::table::mvcc::multiple_txn_appends") {
    test_env env;
    auto table = make_int_table(env);

    transaction_manager_t mgr(&env.resource);

    // Transaction 1: append 10 rows
    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 10, txn1.data());
    auto cid1 = mgr.commit(s1);
    mgr.publish(cid1);
    table->commit_append(cid1, 0, 10);

    // Transaction 2: append 5 more rows
    auto s2 = components::session::session_id_t::generate_uid();
    auto& txn2 = mgr.begin_transaction(s2);
    append_rows_txn(*table, env, 10, 5, txn2.data());
    auto cid2 = mgr.commit(s2);
    mgr.publish(cid2);
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
    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    // Build row_ids vector (BIGINT) with values 0..4
    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 5);
    for (uint64_t i = 0; i < 5; i++) {
        row_ids_chunk.data[0].set_value(i, static_cast<int64_t>(i));
    }
    row_ids_chunk.set_cardinality(5);

    auto txn_id = txn.data().transaction_id;

    table_delete_state del_state(&env.resource);
    table->delete_rows(del_state, row_ids_chunk.data[0], 5, txn_id);

    // Commit: finalize all deletes for this txn
    // Note: mgr.commit() erases txn from active_ map, so txn ref becomes dangling
    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
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
    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    // Build row_ids vector (BIGINT) with values 0..4
    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 5);
    for (uint64_t i = 0; i < 5; i++) {
        row_ids_chunk.data[0].set_value(i, static_cast<int64_t>(i));
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
    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 10);
    for (uint64_t i = 0; i < 10; i++) {
        row_ids_chunk.data[0].set_value(i, static_cast<int64_t>(i));
    }
    row_ids_chunk.set_cardinality(10);

    auto txn_id = txn.data().transaction_id;
    table_delete_state del_state(&env.resource);
    table->delete_rows(del_state, row_ids_chunk.data[0], 10, txn_id);

    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
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
    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 5);
    for (uint64_t i = 0; i < 5; i++) {
        row_ids_chunk.data[0].set_value(i, static_cast<int64_t>(i));
    }
    row_ids_chunk.set_cardinality(5);

    auto txn_id = txn.data().transaction_id;
    table_delete_state del_state(&env.resource);
    table->delete_rows(del_state, row_ids_chunk.data[0], 5, txn_id);

    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
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
    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);

    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 50);
    for (uint64_t i = 0; i < 50; i++) {
        row_ids_chunk.data[0].set_value(i, static_cast<int64_t>(i));
    }
    row_ids_chunk.set_cardinality(50);

    auto txn_id = txn.data().transaction_id;
    table_delete_state del_state(&env.resource);
    table->delete_rows(del_state, row_ids_chunk.data[0], 50, txn_id);

    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_all_deletes(txn_id, commit_id);

    // 50 rows visible
    REQUIRE(scan_count(*table, env) == 50);

    // Compact: physically remove deleted rows. No other snapshot is active and
    // the delete is published, so the watermark green-lights the rebuild.
    REQUIRE(table->compact(mgr.compact_watermark()));

    // Still 50 rows visible
    REQUIRE(scan_count(*table, env) == 50);

    // Total rows should now be 50 (reduced allocation)
    REQUIRE(table->row_group()->total_rows() == 50);
}

TEST_CASE("components::table::mvcc::uncommitted_rows_invisible_to_other_txn") {
    test_env env;
    auto table = make_int_table(env);

    transaction_manager_t mgr(&env.resource);

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
    mgr.publish(commit_id);
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

    transaction_manager_t mgr(&env.resource);

    // Txn1 deletes rows 0..4 (does NOT commit yet)
    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);

    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 5);
    for (uint64_t i = 0; i < 5; i++) {
        row_ids_chunk.data[0].set_value(i, static_cast<int64_t>(i));
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
    mgr.publish(commit_id);
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

    transaction_manager_t mgr(&env.resource);

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

namespace {

    // Distinct row VALUES (not just counts): compact() rebuilds the collection, so a
    // dropped old version and a leaked new version can cancel out in a bare count.
    std::set<int64_t> scan_values_txn(data_table_t& table, test_env& env, transaction_data txn) {
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);

        table_scan_state scan_state(&env.resource);
        table.initialize_scan(scan_state, column_ids);
        scan_state.table_state.txn = txn;

        auto types = table.copy_types();
        std::set<int64_t> values;
        while (true) {
            auto result = data_chunk_t(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
            table.scan(result, scan_state);
            if (result.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < result.size(); i++) {
                values.insert(result.data[0].get_value<int64_t>(i));
            }
        }
        return values;
    }

    void delete_row0_txn(data_table_t& table, test_env& env, uint64_t txn_id) {
        std::pmr::vector<complex_logical_type> id_type(&env.resource);
        id_type.emplace_back(logical_type::BIGINT);
        auto row_ids_chunk = data_chunk_t(&env.resource, id_type, 1);
        row_ids_chunk.data[0].set_value(0, static_cast<int64_t>(0));
        row_ids_chunk.set_cardinality(1);

        table_delete_state del_state(&env.resource);
        table.delete_rows(del_state, row_ids_chunk.data[0], 1, txn_id);
    }

    std::set<int64_t> make_range(int64_t first, int64_t last) {
        std::set<int64_t> s;
        for (int64_t v = first; v <= last; v++) {
            s.insert(v);
        }
        return s;
    }

} // anonymous namespace

// MVCC violation: compact() must not collapse version history that an OLDER
// active snapshot still needs. txn2's snapshot predates the row-0 update (delete +
// replacement append, both committed AND published) — after compact, txn2 must
// still see the PRE-update version of row 0 and must NOT see the replacement.
TEST_CASE("components::table::mvcc::compact_preserves_old_snapshot_view") {
    test_env env;
    auto table = make_int_table(env);
    transaction_manager_t mgr(&env.resource);

    // txn1: insert rows 0..9, commit, publish, storage-stamp.
    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 10, txn1.data());
    auto c1 = mgr.commit(s1);
    mgr.publish(c1);
    table->commit_append(c1, 0, 10);

    // txn2: snapshot BEFORE the update — must keep seeing {0..9} forever.
    auto s2 = components::session::session_id_t::generate_uid();
    auto& txn2 = mgr.begin_transaction(s2);
    REQUIRE(scan_values_txn(*table, env, txn2.data()) == make_range(0, 9));

    // txn3: "update" row 0 — delete it and append replacement value 100;
    // commit, storage-stamp BOTH sides, publish. Fully published: only txn2's
    // older snapshot still needs the pre-update version.
    auto s3 = components::session::session_id_t::generate_uid();
    auto& txn3 = mgr.begin_transaction(s3);
    auto txn3_id = txn3.data().transaction_id;
    delete_row0_txn(*table, env, txn3_id);
    append_rows_txn(*table, env, 100, 1, txn3.data()); // physical row 10
    auto c3 = mgr.commit(s3);
    table->commit_all_deletes(txn3_id, c3);
    table->commit_append(c3, 10, 1);
    mgr.publish(c3);

    // Sanity: a fresh snapshot sees the post-update state.
    auto s4 = components::session::session_id_t::generate_uid();
    auto& txn4 = mgr.begin_transaction(s4);
    auto expected_new = make_range(1, 9);
    expected_new.insert(100);
    REQUIRE(scan_values_txn(*table, env, txn4.data()) == expected_new);

    // Compact while txn2's older snapshot is still active: the watermark sits
    // below c3, so the rebuild must be refused.
    REQUIRE_FALSE(table->compact(mgr.compact_watermark()));

    // MVCC promise: txn2 still sees the pre-update view — row 0 alive, no 100.
    REQUIRE(scan_values_txn(*table, env, txn2.data()) == make_range(0, 9));
    // And the fresh snapshot keeps the post-update view.
    REQUIRE(scan_values_txn(*table, env, txn4.data()) == expected_new);

    mgr.abort(s2);
    mgr.abort(s4);

    // With every old snapshot gone the watermark reaches c3: compact proceeds
    // and reclaims the dead pre-update version.
    REQUIRE(table->compact(mgr.compact_watermark()));
    REQUIRE(table->row_group()->total_rows() == 10);
    auto s6 = components::session::session_id_t::generate_uid();
    auto& txn6 = mgr.begin_transaction(s6);
    REQUIRE(scan_values_txn(*table, env, txn6.data()) == expected_new);
    mgr.abort(s6);
}

// MVCC violation: the mid-update in-flight window. txn3 committed (commit_id
// allocated, still in in_flight_commits_ — publish() pending), the DELETE side is
// already storage-stamped with the commit_id but the replacement append is NOT yet
// commit_append-stamped (storage_publish in flight). A compact() fired in this
// window (checkpoint path) sees the delete as committed and the replacement as
// uncommitted — the row vanishes entirely for every snapshot.
TEST_CASE("components::table::mvcc::compact_in_flight_commit_window") {
    test_env env;
    auto table = make_int_table(env);
    transaction_manager_t mgr(&env.resource);

    // Baseline rows 0..9, committed + published + stamped.
    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 10, txn1.data());
    auto c1 = mgr.commit(s1);
    mgr.publish(c1);
    table->commit_append(c1, 0, 10);

    // txn3 updates row 0: delete + replacement append (value 100).
    auto s3 = components::session::session_id_t::generate_uid();
    auto& txn3 = mgr.begin_transaction(s3);
    auto txn3_id = txn3.data().transaction_id;
    delete_row0_txn(*table, env, txn3_id);
    append_rows_txn(*table, env, 100, 1, txn3.data()); // physical row 10

    // Commit allocates c3 and leaves it IN FLIGHT (no publish yet). Stamp only
    // the delete side — the replacement's commit_append is still in flight.
    auto c3 = mgr.commit(s3);
    table->commit_all_deletes(txn3_id, c3);

    // A snapshot taken inside the window holds c3 in in_flight_snapshot: it must
    // see the OLD version of row 0 and must not see the replacement.
    auto s4 = components::session::session_id_t::generate_uid();
    auto& txn4 = mgr.begin_transaction(s4);
    REQUIRE(scan_values_txn(*table, env, txn4.data()) == make_range(0, 9));

    // Checkpoint-path compact fires inside the window: c3 is in flight, so the
    // watermark sits below it and the rebuild must be refused.
    REQUIRE_FALSE(table->compact(mgr.compact_watermark()));

    // The row must NOT vanish: txn4 still sees the pre-update version.
    REQUIRE(scan_values_txn(*table, env, txn4.data()) == make_range(0, 9));

    // Finish the commit pipeline: stamp the replacement, publish.
    table->commit_append(c3, 10, 1);
    mgr.publish(c3);

    // Fresh snapshot sees the post-update state — replacement present, row 0 gone.
    auto s5 = components::session::session_id_t::generate_uid();
    auto& txn5 = mgr.begin_transaction(s5);
    auto expected_new = make_range(1, 9);
    expected_new.insert(100);
    REQUIRE(scan_values_txn(*table, env, txn5.data()) == expected_new);

    mgr.abort(s4);
    mgr.abort(s5);

    // Window closed, snapshots gone: compact proceeds and reclaims the old row 0.
    REQUIRE(table->compact(mgr.compact_watermark()));
    REQUIRE(table->row_group()->total_rows() == 10);
    auto s6 = components::session::session_id_t::generate_uid();
    auto& txn6 = mgr.begin_transaction(s6);
    REQUIRE(scan_values_txn(*table, env, txn6.data()) == expected_new);
    mgr.abort(s6);
}

namespace {

    // Two-column (BIGINT, BIGINT) table for the direct revert_append storage regression.
    std::unique_ptr<data_table_t> make_int2_table(test_env& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("b", complex_logical_type(logical_type::BIGINT));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "test2");
    }

    // Append `count` rows onto the two-column table: column a = start+i, column b = (start+i)*10.
    void append_rows2(data_table_t& table, test_env& env, int64_t start, uint64_t count) {
        auto types = table.copy_types();
        auto chunk = data_chunk_t(&env.resource, types, count);
        for (uint64_t i = 0; i < count; i++) {
            auto v = start + static_cast<int64_t>(i);
            chunk.data[0].set_value(i, logical_value_t(&env.resource, v));
            chunk.data[1].set_value(i, logical_value_t(&env.resource, v * 10));
        }
        chunk.set_cardinality(count);

        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    // Ordered sequential scan of BOTH columns across every chunk. Order is the physical
    // append order, so a desynced/stale column tail surfaces as a wrong pair (or a size mismatch).
    std::vector<std::pair<int64_t, int64_t>> scan_pairs(data_table_t& table, test_env& env) {
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);
        column_ids.emplace_back(1);

        table_scan_state scan_state(&env.resource);
        table.initialize_scan(scan_state, column_ids);

        auto types = table.copy_types();
        std::vector<std::pair<int64_t, int64_t>> rows;
        while (true) {
            auto result = data_chunk_t(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
            table.scan(result, scan_state);
            if (result.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < result.size(); i++) {
                rows.emplace_back(result.data[0].value(i).value<int64_t>(), result.data[1].value(i).value<int64_t>());
            }
        }
        return rows;
    }

} // anonymous namespace

// DIRECT storage-level regression for row_group_t::revert_append column truncation.
// revert_append must truncate every COLUMN's segments (get_column(c).revert_append) along with
// the row-group / version count. A revert that moves only the row-group count leaves a stale
// column tail desynced from it: a subsequent scan over-reads the stale rows (heap-buffer-overflow
// in fetch_row) and a re-append lands past the reverted boundary, corrupting both columns. This
// drives the exact revert-then-reappend path directly at the storage layer (no DML), asserting
// the reverted count, the surviving column values after the revert, and correct values after
// re-append.
TEST_CASE("components::table::mvcc::revert_append_truncates_columns_direct") {
    test_env env;
    auto table = make_int2_table(env);

    // Append 100 rows across two BIGINT columns: a=i, b=i*10.
    append_rows2(*table, env, 0, 100);
    REQUIRE(table->row_group()->total_rows() == 100);

    // Revert the tail: keep rows [0,40), drop the last 60. Both the row-group count AND every
    // column segment must truncate to 40.
    table->revert_append(40, 60);
    REQUIRE(table->row_group()->total_rows() == 40);

    {
        // No stale column rows may leak past the revert, and the survivors must be intact.
        auto rows = scan_pairs(*table, env);
        REQUIRE(rows.size() == 40);
        for (uint64_t i = 0; i < 40; i++) {
            REQUIRE(rows[i].first == static_cast<int64_t>(i));
            REQUIRE(rows[i].second == static_cast<int64_t>(i) * 10);
        }
    }

    // Re-append onto the reverted table with DISTINCT values (a=1000..1029) so a missed column
    // truncation is OBSERVABLE: without it the re-appended rows land AFTER the stale column tail,
    // and logical rows [40,70) read the STALE originals (a=40..69) instead of 1000..1029.
    append_rows2(*table, env, 1000, 30);
    REQUIRE(table->row_group()->total_rows() == 70);

    {
        auto rows = scan_pairs(*table, env);
        REQUIRE(rows.size() == 70);
        // Survivors [0,40) unchanged.
        for (uint64_t i = 0; i < 40; i++) {
            REQUIRE(rows[i].first == static_cast<int64_t>(i));
            REQUIRE(rows[i].second == static_cast<int64_t>(i) * 10);
        }
        // Re-appended [40,70) must read the NEW 1000.. values, not a stale reverted tail.
        for (uint64_t j = 0; j < 30; j++) {
            const int64_t v = 1000 + static_cast<int64_t>(j);
            REQUIRE(rows[40 + j].first == v);
            REQUIRE(rows[40 + j].second == v * 10);
        }
    }
}

// Issue #552 family: an aborted MVCC update (delete-stamp + append) followed by the
// failed-statement revert (physical append revert + delete un-stamp) must restore the
// original row intact, and a subsequent COMMITTED update of that row must yield exactly
// the new version. The second phase also pins validity_mask_t::set_valid bit semantics:
// the revert's BIT reset used to clobber a whole 64-bit entry per "bit", leaving the
// failed append's NULL bit set — the re-appended row then read as NULL forever.
TEST_CASE("components::table::mvcc::aborted_update_revert_restores_row") {
    test_env env;
    std::vector<column_definition_t> columns;
    columns.emplace_back("id", complex_logical_type(logical_type::BIGINT));
    columns.emplace_back("val", complex_logical_type(logical_type::STRING_LITERAL));
    auto table = std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "t");

    auto types = table->copy_types();
    // Row 0: (1, 'p1'), committed immediately.
    {
        auto chunk = data_chunk_t(&env.resource, types, 1);
        chunk.data[0].set_value(0, logical_value_t(&env.resource, int64_t(1)));
        chunk.data[1].set_value(0, logical_value_t(&env.resource, std::string("p1")));
        chunk.set_cardinality(1);
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data{0, 0});
    }

    // Failed-statement txn: stamp delete on row 0, append (77, NULL).
    const uint64_t txn_id = TRANSACTION_ID_START + 5;
    const transaction_data txn{txn_id, 100};
    {
        auto del_state = table->initialize_delete({});
        auto row_ids = vector_t(&env.resource, complex_logical_type(logical_type::BIGINT), 1);
        row_ids.set_value(0, logical_value_t(&env.resource, int64_t(0)));
        REQUIRE(table->delete_rows(*del_state, row_ids, 1, txn_id) == 1);
    }
    int64_t appended_start = 0;
    {
        auto chunk = data_chunk_t(&env.resource, types, 1);
        chunk.data[0].set_value(0, logical_value_t(&env.resource, int64_t(77)));
        chunk.data[1].set_null(0, true);
        chunk.set_cardinality(1);
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        appended_start = state.current_row;
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, txn);
    }
    REQUIRE(appended_start == 1);

    // Failed-statement revert: physical append revert + delete un-stamp.
    table->revert_append(appended_start, 1);
    table->revert_all_deletes(txn_id);

    // Reader: later txn.
    auto scan_once = [&](uint64_t reader_txn, uint64_t reader_start) {
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);
        column_ids.emplace_back(1);
        table_scan_state scan_state(&env.resource);
        table->initialize_scan(scan_state, column_ids);
        scan_state.table_state.txn = transaction_data{reader_txn, reader_start};
        auto result = data_chunk_t(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
        table->scan(result, scan_state);
        return result;
    };
    {
        auto result = scan_once(TRANSACTION_ID_START + 6, 101);
        REQUIRE(result.size() == 1);
        INFO("id=" << result.data[0].value(0).value<int64_t>());
        REQUIRE(result.data[0].value(0).value<int64_t>() == 1);
        REQUIRE(result.data[1].validity().row_is_valid(0));
        REQUIRE_FALSE(result.data[1].value(0).is_null());
    }

    // Second, SUCCESSFUL update: (1, 'renamed') — delete-stamp row0 + append, then commit.
    const uint64_t txn2 = TRANSACTION_ID_START + 7;
    {
        auto del_state = table->initialize_delete({});
        auto row_ids = vector_t(&env.resource, complex_logical_type(logical_type::BIGINT), 1);
        row_ids.set_value(0, logical_value_t(&env.resource, int64_t(0)));
        REQUIRE(table->delete_rows(*del_state, row_ids, 1, txn2) == 1);
    }
    int64_t start2 = 0;
    {
        auto chunk = data_chunk_t(&env.resource, types, 1);
        chunk.data[0].set_value(0, logical_value_t(&env.resource, int64_t(1)));
        chunk.data[1].set_value(0, logical_value_t(&env.resource, std::string("renamed")));
        chunk.set_cardinality(1);
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table->append_lock(state).has_error());
        REQUIRE_FALSE(table->initialize_append(state).has_error());
        start2 = state.current_row;
        REQUIRE_FALSE(table->append(chunk, state).has_error());
        table->finalize_append(state, transaction_data{txn2, 102});
    }
    REQUIRE(start2 == 1);
    table->commit_append(103, start2, 1);
    table->commit_all_deletes(txn2, 103);

    {
        auto result = scan_once(TRANSACTION_ID_START + 8, 104);
        REQUIRE(result.size() == 1);
        INFO("id=" << result.data[0].value(0).value<int64_t>());
        REQUIRE(result.data[0].value(0).value<int64_t>() == 1);
        INFO("val validity=" << result.data[1].validity().row_is_valid(0));
        REQUIRE(result.data[1].validity().row_is_valid(0));
        auto v = result.data[1].value(0);
        REQUIRE_FALSE(v.is_null());
        REQUIRE(v.value<std::string_view>() == "renamed");
    }
}

// ---------------------------------------------------------------------------
// A6: mixed addressing in row_version_manager_t::vector_info_.
//
// The append path (append_version_info / commit_append / revert_append /
// cleanup_append and the counters committed_deleted_count / has_version_above)
// addresses vector_info_ with GROUP-LOCAL vector indices (slot 0 = the group's
// first vector), while the scan path (collection_scan_state::vector_index),
// the delete path (version_delete_state::delete_row) and point-fetch
// (row_version_manager_t::fetch) address it with collection-ABSOLUTE indices
// (slot N for the group starting at row N*1024). With the default
// row_group_size == DEFAULT_VECTOR_CAPACITY every row group past the first
// reads/writes a DIFFERENT slot than the append path wrote. The existing MVCC
// tests run on 10..100 rows — one row group — and are structurally blind to
// this. The three tests below drive the same operations past row 1024.
// ---------------------------------------------------------------------------

// txn1 appends 10 rows into the SECOND row group (rows 1024..1033) and does not
// commit. A concurrent snapshot (txn2) must still count exactly the 1024
// committed rows: the uncommitted rows' insert stamps must be consulted, not
// bypassed because the scan asked a vector_info_ slot the append never wrote.
TEST_CASE("components::table::mvcc::uncommitted_rows_invisible_in_second_row_group") {
    test_env env;
    auto table = make_int_table(env);

    // Fill row group 0 exactly (row_group_size == DEFAULT_VECTOR_CAPACITY).
    append_rows(*table, env, 0, 1024);
    REQUIRE(scan_count(*table, env) == 1024);

    transaction_manager_t mgr(&env.resource);
    auto session1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(session1);
    // Lands in row group 1 (start = 1024). NOT committed.
    append_rows_txn(*table, env, 1024, 10, txn1.data());

    auto session2 = components::session::session_id_t::generate_uid();
    auto& txn2 = mgr.begin_transaction(session2);
    REQUIRE(scan_count_txn(*table, env, txn2.data()) == 1024);

    mgr.abort(session2);
    mgr.abort(session1);
}

// A committed DELETE of a row past 1024 must be reflected by
// committed_row_count exactly as the scan reflects it. The tombstone lands in
// the slot the (absolute-addressed) delete path picked; the counter walks the
// group-local slots and must find it there.
TEST_CASE("components::table::mvcc::committed_row_count_after_delete_past_1024") {
    test_env env;
    auto table = make_int_table(env);

    append_rows(*table, env, 0, 1024);
    append_rows(*table, env, 1024, 10);
    REQUIRE(scan_count(*table, env) == 1034);
    REQUIRE(table->row_group()->committed_row_count() == 1034);

    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);
    auto txn_id = txn.data().transaction_id;

    // Row id 1030 lives in row group 1 (start = 1024).
    auto del_state = table->initialize_delete({});
    auto row_ids = vector_t(&env.resource, complex_logical_type(logical_type::BIGINT), 1);
    row_ids.set_value(0, logical_value_t(&env.resource, int64_t(1030)));
    REQUIRE(table->delete_rows(*del_state, row_ids, 1, txn_id) == 1);

    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_all_deletes(txn_id, commit_id);

    // Scan and counter must agree on the committed tombstone.
    REQUIRE(scan_count(*table, env) == 1033);
    REQUIRE(table->row_group()->committed_row_count() == 1033);
}

// A PENDING (uncommitted) DELETE of a row past 1024 must refuse compact():
// the pending stamp is above any watermark, and has_version_above must see it
// in whatever slot the delete path wrote it to.
TEST_CASE("components::table::mvcc::compact_refused_while_delete_past_1024_pending") {
    test_env env;
    auto table = make_int_table(env);

    append_rows(*table, env, 0, 1024);
    append_rows(*table, env, 1024, 10);
    REQUIRE(scan_count(*table, env) == 1034);

    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);
    auto txn_id = txn.data().transaction_id;

    // Pending delete of row 1030 (row group 1) — no commit.
    auto del_state = table->initialize_delete({});
    auto row_ids = vector_t(&env.resource, complex_logical_type(logical_type::BIGINT), 1);
    row_ids.set_value(0, logical_value_t(&env.resource, int64_t(1030)));
    REQUIRE(table->delete_rows(*del_state, row_ids, 1, txn_id) == 1);

    REQUIRE_FALSE(table->compact(mgr.compact_watermark()));

    // Abort path: un-stamp and verify everything is visible again.
    mgr.abort(session);
    table->revert_all_deletes(txn_id);
    REQUIRE(scan_count(*table, env) == 1034);
}

namespace {

    // Nested-column revert_append coordinate regression (F3 family). Every logical row `r`
    // seeded with `base` carries fully determined content, so a stale child tail after a
    // revert is OBSERVABLE as wrong CONTENT, not just a wrong count:
    //   LIST  column: length r % 3 (empties included), element j = base + r * 100 + j
    //   ARRAY column: NESTED_ARRAY_SIZE elements,      element j = base + r * 100 + 50 + j
    constexpr uint64_t NESTED_ARRAY_SIZE = 4;

    uint64_t nested_list_length(uint64_t row) { return row % 3; }

    std::unique_ptr<data_table_t> make_list_table(test_env& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("l", complex_logical_type::create_list(logical_type::UBIGINT));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "list_revert");
    }

    std::unique_ptr<data_table_t> make_array_table(test_env& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("id", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("a", complex_logical_type::create_array(logical_type::UBIGINT, NESTED_ARRAY_SIZE));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "array_revert");
    }

    void append_list_rows(data_table_t& table, test_env& env, uint64_t row_begin, uint64_t count, uint64_t base) {
        auto types = table.copy_types();
        auto chunk = data_chunk_t(&env.resource, types, count);
        chunk.set_cardinality(count);
        for (uint64_t i = 0; i < count; i++) {
            const uint64_t r = row_begin + i;
            chunk.set_value(0, i, static_cast<int64_t>(r));
            std::vector<uint64_t> list;
            list.reserve(nested_list_length(r));
            for (uint64_t j = 0; j < nested_list_length(r); j++) {
                list.emplace_back(base + r * 100 + j);
            }
            chunk.set_value(1, i, list);
        }
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    void append_array_rows(data_table_t& table, test_env& env, uint64_t row_begin, uint64_t count, uint64_t base) {
        auto types = table.copy_types();
        auto chunk = data_chunk_t(&env.resource, types, count);
        chunk.set_cardinality(count);
        for (uint64_t i = 0; i < count; i++) {
            const uint64_t r = row_begin + i;
            chunk.set_value(0, i, static_cast<int64_t>(r));
            std::vector<uint64_t> arr;
            arr.reserve(NESTED_ARRAY_SIZE);
            for (uint64_t j = 0; j < NESTED_ARRAY_SIZE; j++) {
                arr.emplace_back(base + r * 100 + 50 + j);
            }
            chunk.set_value(1, i, arr);
        }
        table_append_state state(&env.resource);
        REQUIRE_FALSE(table.append_lock(state).has_error());
        REQUIRE_FALSE(table.initialize_append(state).has_error());
        REQUIRE_FALSE(table.append(chunk, state).has_error());
        table.finalize_append(state, transaction_data{0, 0});
    }

    // Full ordered scan; rows below `new_from` must carry base 0 content, rows at or past it
    // base `new_base` content (new_from == total when nothing was re-appended yet).
    void verify_list_rows(data_table_t& table, test_env& env, uint64_t total, uint64_t new_from, uint64_t new_base) {
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);
        column_ids.emplace_back(1);
        table_scan_state scan_state(&env.resource);
        table.initialize_scan(scan_state, column_ids);
        auto types = table.copy_types();
        uint64_t row = 0;
        while (true) {
            auto result = data_chunk_t(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
            table.scan(result, scan_state);
            if (result.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < result.size(); i++, row++) {
                REQUIRE(result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                const uint64_t base = row < new_from ? 0 : new_base;
                auto lv = result.data[1].value(i);
                REQUIRE(lv.type().type() == logical_type::LIST);
                REQUIRE(lv.children().size() == nested_list_length(row));
                for (uint64_t j = 0; j < nested_list_length(row); j++) {
                    REQUIRE(lv.children()[j].value<uint64_t>() == base + row * 100 + j);
                }
            }
        }
        REQUIRE(row == total);
    }

    void verify_array_rows(data_table_t& table, test_env& env, uint64_t total, uint64_t new_from, uint64_t new_base) {
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);
        column_ids.emplace_back(1);
        table_scan_state scan_state(&env.resource);
        table.initialize_scan(scan_state, column_ids);
        auto types = table.copy_types();
        uint64_t row = 0;
        while (true) {
            auto result = data_chunk_t(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
            table.scan(result, scan_state);
            if (result.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < result.size(); i++, row++) {
                REQUIRE(result.data[0].value(i).value<int64_t>() == static_cast<int64_t>(row));
                const uint64_t base = row < new_from ? 0 : new_base;
                auto av = result.data[1].value(i);
                REQUIRE(av.type().type() == logical_type::ARRAY);
                REQUIRE(av.children().size() == NESTED_ARRAY_SIZE);
                for (uint64_t j = 0; j < NESTED_ARRAY_SIZE; j++) {
                    REQUIRE(av.children()[j].value<uint64_t>() == base + row * 100 + 50 + j);
                }
            }
        }
        REQUIRE(row == total);
    }

} // anonymous namespace

// F3: row_group_t::revert_append hands every column a COLLECTION-ABSOLUTE row number
// (this->start + group-local revert point). A LIST column must convert that into its
// CHILD element space: the stored offsets are cumulative element counts within the row
// group, and the child column shares the parent's start_. The old code compared the
// RELATIVE surviving count (max_entry()) against the ABSOLUTE start_, so for any row
// group with start_ > 0 the child was never truncated; the next append then seeded its
// offsets past the stale child tail, and rows past the revert boundary read stale
// elements that belonged to the reverted rows.
TEST_CASE("components::table::mvcc::revert_append_list_child_row_group_1") {
    test_env env;
    auto table = make_list_table(env);

    // Fill row group 0 completely, then 40 rows into row group 1 (rows 1024..1063).
    append_list_rows(*table, env, 0, 1024, 0);
    append_list_rows(*table, env, 1024, 40, 0);
    REQUIRE(table->row_group()->total_rows() == 1064);

    // Failed-statement revert of the last 20 rows: keep [0, 1044).
    table->revert_append(1044, 20);
    REQUIRE(table->row_group()->total_rows() == 1044);

    // Survivors intact — content, not just counts.
    verify_list_rows(*table, env, 1044, 1044, 0);

    // Re-append 12 rows with a DISTINCT base so a stale child tail is observable:
    // without the child truncation, rows [1044, 1056) read the reverted rows' elements.
    append_list_rows(*table, env, 1044, 12, 1'000'000);
    REQUIRE(table->row_group()->total_rows() == 1056);
    verify_list_rows(*table, env, 1056, 1044, 1'000'000);
}

// F3: same coordinate confusion on the ARRAY leg. The child holds array_size() elements
// per row and shares the parent's start_, so the child's absolute truncation row is
// start_ + surviving_rows * array_size. The old code passed start_row * array_size —
// far past the child's end for any row group with start_ > 0 (Debug builds abort on the
// exact-boundary assert in column_data_t::revert_append; release builds silently keep
// the stale child tail, and a re-append lands its elements after it).
TEST_CASE("components::table::mvcc::revert_append_array_child_row_group_1") {
    test_env env;
    auto table = make_array_table(env);

    append_array_rows(*table, env, 0, 1024, 0);
    append_array_rows(*table, env, 1024, 40, 0);
    REQUIRE(table->row_group()->total_rows() == 1064);

    table->revert_append(1044, 20);
    REQUIRE(table->row_group()->total_rows() == 1044);

    verify_array_rows(*table, env, 1044, 1044, 0);

    append_array_rows(*table, env, 1044, 12, 1'000'000);
    REQUIRE(table->row_group()->total_rows() == 1056);
    verify_array_rows(*table, env, 1056, 1044, 1'000'000);
}

// ---------------------------------------------------------------------------
// VACUUM must not resurrect committed deletes.
//
// row_version_manager_t::cleanup_append processes a vector ONLY when it is FULL
// (vcount == DEFAULT_VECTOR_CAPACITY), and the default row_group_size is
// DEFAULT_VECTOR_CAPACITY too — so the vectors that reach chunk_info::cleanup at all
// are exactly the full row groups of a table. Every MVCC test above runs on 10..100
// rows and is turned away by that guard before cleanup is ever called, which is why
// the cases below insist on a FULL 1024-row vector.
//
// The hazard cleanup_append carries: `cleanup() == true` INSTALLS `result` into the
// slot, and `result` may be empty. An empty slot is not "no history left", it is
// "every row here is visible" — row_version_manager_t::indexing_vector returns
// max_count and fetch returns true when get_chunk_info gives nullptr. Dropping a
// committed delete stamp therefore un-deletes the rows for every reader, with no
// crash and no restart in the way.
// ---------------------------------------------------------------------------

namespace {

    // Commits a DELETE of the contiguous row-id range [first_row, first_row+count) the way
    // the DELETE statement does: delete_rows stamps the pending txn id, commit+publish
    // allocate the commit id, commit_all_deletes stamps it in.
    void delete_range_committed(data_table_t& table,
                                test_env& env,
                                transaction_manager_t& mgr,
                                int64_t first_row,
                                uint64_t count) {
        auto session = components::session::session_id_t::generate_uid();
        auto& txn = mgr.begin_transaction(session);
        auto txn_id = txn.data().transaction_id;

        auto row_ids = vector_t(&env.resource, complex_logical_type(logical_type::BIGINT), count);
        for (uint64_t i = 0; i < count; i++) {
            row_ids.set_value(i, logical_value_t(&env.resource, first_row + static_cast<int64_t>(i)));
        }
        auto del_state = table.initialize_delete({});
        REQUIRE(table.delete_rows(*del_state, row_ids, count, txn_id) == count);

        auto commit_id = mgr.commit(session);
        mgr.publish(commit_id);
        table.commit_all_deletes(txn_id, commit_id);
    }

    // Full ordered drain of the visible set. These cases check CONTENTS, not only the
    // count: a resurrected row is recognisable only by its value, and a count alone
    // cannot tell "the deleted rows came back" from "different rows survived".
    std::vector<int64_t> scan_all_values(data_table_t& table, test_env& env) {
        std::vector<storage_index_t> column_ids;
        column_ids.emplace_back(0);

        table_scan_state scan_state(&env.resource);
        table.initialize_scan(scan_state, column_ids);

        auto types = table.copy_types();
        std::vector<int64_t> values;
        while (true) {
            auto result = data_chunk_t(&env.resource, types, DEFAULT_VECTOR_CAPACITY);
            table.scan(result, scan_state);
            if (result.size() == 0) {
                break;
            }
            for (uint64_t i = 0; i < result.size(); i++) {
                values.push_back(result.data[0].get_value<int64_t>(i));
            }
        }
        return values;
    }

    std::vector<int64_t> make_vector_range(int64_t first, int64_t last) {
        std::vector<int64_t> v;
        for (int64_t x = first; x <= last; x++) {
            v.push_back(x);
        }
        return v;
    }

} // anonymous namespace

// Leg 1 — PARTIAL deletes in a full vector. ONE cleanup_versions pass is enough:
// chunk_vector_info::cleanup used to fall through its partial-delete branch to
// `return true` while leaving `result` empty, so the vector's 500 committed delete
// stamps were thrown away and the rows became visible again.
TEST_CASE("components::table::mvcc::vacuum_keeps_partial_committed_deletes") {
    test_env env;
    auto table = make_int_table(env);
    transaction_manager_t mgr(&env.resource);

    // Exactly one FULL row group / one FULL vector: values 0..1023 at row ids 0..1023.
    append_rows(*table, env, 0, 1024);
    REQUIRE(scan_all_values(*table, env) == make_vector_range(0, 1023));

    // Committed DELETE of the first 500 rows.
    delete_range_committed(*table, env, mgr, 0, 500);
    const auto survivors = make_vector_range(500, 1023);
    REQUIRE(scan_all_values(*table, env) == survivors);

    // The count goes first so a resurrection reads as "524 became 1024" rather than as a
    // 1024-element diff; the contents then say WHICH rows are there.
    auto check_survivors = [&] {
        auto visible = scan_all_values(*table, env);
        REQUIRE(visible.size() == survivors.size());
        REQUIRE(visible == survivors);
    };

    // VACUUM. No other transaction is active, so the horizon is already past the
    // delete's commit — exactly the state that made cleanup drop the stamps.
    table->cleanup_versions(mgr.lowest_active_start_time());
    check_survivors();

    // A second pass must be just as harmless.
    table->cleanup_versions(mgr.lowest_active_start_time());
    check_survivors();
}

// Leg 2 — a FULLY deleted vector. It takes TWO cleanup_versions passes: the first
// legitimately collapses the 1024-stamp chunk_vector_info into a chunk_constant_info
// that keeps the delete_id; the second then hit chunk_constant_info::cleanup, which
// returned true on a committed delete_id with an empty `result` and brought all 1024
// rows back at once.
TEST_CASE("components::table::mvcc::vacuum_keeps_fully_deleted_vector_deleted") {
    test_env env;
    auto table = make_int_table(env);
    transaction_manager_t mgr(&env.resource);

    append_rows(*table, env, 0, 1024);
    REQUIRE(scan_all_values(*table, env) == make_vector_range(0, 1023));

    delete_range_committed(*table, env, mgr, 0, 1024);
    REQUIRE(scan_all_values(*table, env).size() == 0);

    // First VACUUM: the vector collapses to a constant. The rows stay deleted.
    table->cleanup_versions(mgr.lowest_active_start_time());
    REQUIRE(scan_all_values(*table, env).size() == 0);

    // Second VACUUM: the constant must keep its delete stamp too.
    table->cleanup_versions(mgr.lowest_active_start_time());
    REQUIRE(scan_all_values(*table, env).size() == 0);

    // And a third, in case the collapse is ever staged differently.
    table->cleanup_versions(mgr.lowest_active_start_time());
    REQUIRE(scan_all_values(*table, env).size() == 0);
}

// The other half of the contract, driven straight at chunk_info::cleanup: GC must still
// RECLAIM what it legitimately may, or tightening it would just be a leak. `true` with an
// empty `result` drops the slot; `true` with a `result` replaces it; `false` keeps what is
// there. Insert history visible to every reader is droppable — the fact of a delete is not.
TEST_CASE("components::table::mvcc::cleanup_still_reclaims_insert_only_history") {
    constexpr uint64_t kLowest = 1000;
    constexpr uint64_t kOldCommit = 10;

    // (a) A full vector with no delete at all: the case cleanup exists for. The 1024
    // insert stamps are visible to everyone, so the whole slot goes.
    {
        chunk_vector_info info(0);
        info.append(0, DEFAULT_VECTOR_CAPACITY, kOldCommit);
        std::unique_ptr<chunk_info> result;
        REQUIRE(info.cleanup(kLowest, result));
        REQUIRE(result == nullptr);
    }

    // (b) A fully deleted vector still collapses 1024 stamps into one constant — but the
    // constant must KEEP the delete, so the rows stay invisible to a see-all-committed reader.
    {
        chunk_vector_info info(0);
        info.append(0, DEFAULT_VECTOR_CAPACITY, kOldCommit);
        info.any_deleted = true;
        for (uint64_t i = 0; i < DEFAULT_VECTOR_CAPACITY; i++) {
            info.deleted[i] = kOldCommit + 1;
        }
        std::unique_ptr<chunk_info> result;
        REQUIRE(info.cleanup(kLowest, result));
        REQUIRE(result != nullptr);
        REQUIRE(result->type == chunk_info_type::CONSTANT_INFO);
        REQUIRE(result->cast<chunk_constant_info>().delete_id == kOldCommit + 1);
        REQUIRE_FALSE(result->fetch(transaction_data{}, 0));
        REQUIRE_FALSE(result->fetch(transaction_data{}, DEFAULT_VECTOR_CAPACITY - 1));
    }

    // (b2) The same vector deleted by TWO transactions. A constant can carry ONE delete
    // stamp, and neither of the two is right for the rows the other deleted, so the
    // collapse is off and the per-row stamps stay.
    {
        chunk_vector_info info(0);
        info.append(0, DEFAULT_VECTOR_CAPACITY, kOldCommit);
        info.any_deleted = true;
        for (uint64_t i = 0; i < DEFAULT_VECTOR_CAPACITY; i++) {
            info.deleted[i] = (i % 2 == 0) ? kOldCommit + 1 : kOldCommit + 2;
        }
        std::unique_ptr<chunk_info> result;
        REQUIRE_FALSE(info.cleanup(kLowest, result));
        REQUIRE(result == nullptr);
    }

    // (c) revert_all_deletes leaves any_deleted set as a conservative hint with not one
    // surviving stamp. Nothing is being hidden, so the slot is still reclaimable.
    {
        chunk_vector_info info(0);
        info.append(0, DEFAULT_VECTOR_CAPACITY, kOldCommit);
        info.any_deleted = true;
        std::unique_ptr<chunk_info> result;
        REQUIRE(info.cleanup(kLowest, result));
        REQUIRE(result == nullptr);
    }

    // (d) A delete-free CONSTANT slot is reclaimable on the same terms.
    {
        chunk_constant_info info(0);
        info.insert_id = kOldCommit;
        std::unique_ptr<chunk_info> result;
        REQUIRE(info.cleanup(kLowest, result));
        REQUIRE(result == nullptr);
    }

    // (e) Refusals that already held and must keep holding: an insert newer than the
    // floor, and a delete still pending under a live txn id.
    {
        chunk_vector_info info(0);
        info.append(0, DEFAULT_VECTOR_CAPACITY, kLowest + 1);
        std::unique_ptr<chunk_info> result;
        REQUIRE_FALSE(info.cleanup(kLowest, result));
        REQUIRE(result == nullptr);
    }
    {
        chunk_vector_info info(0);
        info.append(0, DEFAULT_VECTOR_CAPACITY, kOldCommit);
        info.any_deleted = true;
        info.deleted[7] = TRANSACTION_ID_START + 3;
        std::unique_ptr<chunk_info> result;
        REQUIRE_FALSE(info.cleanup(kLowest, result));
        REQUIRE(result == nullptr);
    }
    {
        chunk_constant_info info(0);
        info.insert_id = kOldCommit;
        info.delete_id = TRANSACTION_ID_START + 3;
        std::unique_ptr<chunk_info> result;
        REQUIRE_FALSE(info.cleanup(kLowest, result));
        REQUIRE(result == nullptr);
    }
}
