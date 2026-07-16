#include <catch2/catch_test_macros.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/in_memory_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/transaction_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <set>

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    struct test_env {
        core::pmr::otterbrix_resource resource;
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

// Issue #567. cleanup_versions over a FULL (DEFAULT_VECTOR_CAPACITY) vector holding COMMITTED
// PARTIAL deletes must not drop that vector's version info: its per-row delete marks are the only
// record of which rows are tombstoned, and a null vector_info reads as "all rows live", so dropping
// it resurrects the deleted rows. The caller only cleans FULL vectors, so the pre-existing 10-row
// cleanup test (a sub-capacity vector) never reached this branch.
TEST_CASE("components::table::mvcc::cleanup_versions_keeps_partial_deletes_in_full_vector") {
    test_env env;
    auto table = make_int_table(env);

    // Vector 0 (rows 0..DEFAULT_VECTOR_CAPACITY-1) must be exactly full; a few rows spill into
    // vector 1 so the delete+cleanup below acts on a genuinely full vector.
    constexpr uint64_t full = DEFAULT_VECTOR_CAPACITY;
    constexpr uint64_t spill = 476;
    append_rows(*table, env, 0, full);
    append_rows(*table, env, static_cast<int64_t>(full), spill);
    REQUIRE(scan_count(*table, env) == full + spill);

    // Commit a partial delete of the full vector (its first 200 rows).
    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);
    constexpr uint64_t ndel = 200;
    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, ndel);
    for (uint64_t i = 0; i < ndel; i++) {
        row_ids_chunk.data[0].set_value(i, static_cast<int64_t>(i));
    }
    row_ids_chunk.set_cardinality(ndel);
    auto txn_id = txn.data().transaction_id;
    table_delete_state del_state(&env.resource);
    table->delete_rows(del_state, row_ids_chunk.data[0], ndel, txn_id);
    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_all_deletes(txn_id, commit_id);
    REQUIRE(scan_count(*table, env) == full + spill - ndel);

    // With no active txns the deletes are visible-to-all and old enough, so cleanup_versions
    // processes vector 0. It must keep the tombstones: the count stays the same, not resurrect.
    table->cleanup_versions(mgr.lowest_active_start_time());
    REQUIRE(scan_count(*table, env) == full + spill - ndel);
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
                rows.emplace_back(result.data[0].value(i).value<int64_t>(),
                                  result.data[1].value(i).value<int64_t>());
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

// Abort retires appended rows IN PLACE: {insert 0, delete 0} — invisible to every
// snapshot, counted committed-dead, reclaimable — while the rows keep their
// physical ids (positional WAL records and their replay depend on placement).
TEST_CASE("components::table::mvcc::abort_append_dead_in_place") {
    test_env env;
    auto table = make_int_table(env);

    // A committed base so the aborted rows sit between live neighbours.
    append_rows(*table, env, 0, 5);

    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    auto& txn = mgr.begin_transaction(session);
    append_rows_txn(*table, env, 100, 10, txn.data());

    // More committed rows after: the aborted range must not swallow them.
    append_rows(*table, env, 200, 5);

    mgr.abort(session);
    table->abort_append(5, 10);

    // Placement intact, contents dead.
    REQUIRE(table->row_group()->total_rows() == 20);
    REQUIRE(scan_count(*table, env) == 10);

    // The dead stamps are below every watermark: compaction proceeds and
    // reclaims exactly the aborted rows.
    REQUIRE(table->compact(std::numeric_limits<uint64_t>::max()));
    REQUIRE(table->row_group()->total_rows() == 10);
    REQUIRE(scan_count(*table, env) == 10);
}

// Version slots are row-group-LOCAL. If the scan's cumulative vector_index or
// the delete path's collection-absolute row ids were forwarded into the slot
// array unconverted, every row group after the first would read — and the
// delete stamps would write — the wrong slot: pending appends beyond the first
// row group would scan as committed-live for every snapshot, and delete stamps
// would be invisible to the dead-count/compaction walks. All probes act on row
// groups past the first (row group size == DEFAULT_VECTOR_CAPACITY here).
TEST_CASE("components::table::mvcc::version_slots_are_row_group_local") {
    test_env env;
    auto table = make_int_table(env);

    constexpr uint64_t cap = DEFAULT_VECTOR_CAPACITY;
    for (int64_t i = 0; i < 3; i++) {
        append_rows(*table, env, i * static_cast<int64_t>(cap), cap);
    }
    REQUIRE(scan_count(*table, env) == 3 * cap);

    transaction_manager_t mgr(&env.resource);

    // Pending appends land in the fourth row group; a fresh snapshot must not
    // see them until commit.
    auto writer_session = components::session::session_id_t::generate_uid();
    auto& writer = mgr.begin_transaction(writer_session);
    append_rows_txn(*table, env, 5000, 10, writer.data());

    auto reader_session = components::session::session_id_t::generate_uid();
    auto& reader = mgr.begin_transaction(reader_session);
    REQUIRE(scan_count_txn(*table, env, reader.data()) == 3 * cap);
    mgr.abort(reader_session);

    auto write_cid = mgr.commit(writer_session);
    mgr.publish(write_cid);
    table->commit_append(write_cid, 3 * cap, 10);
    REQUIRE(scan_count(*table, env) == 3 * cap + 10);

    // Committed deletes in the second row group tombstone in place...
    constexpr uint64_t ndel = 200;
    auto del_session = components::session::session_id_t::generate_uid();
    auto& del_txn = mgr.begin_transaction(del_session);
    std::pmr::vector<complex_logical_type> id_type(&env.resource);
    id_type.emplace_back(logical_type::BIGINT);
    auto row_ids_chunk = data_chunk_t(&env.resource, id_type, ndel);
    for (uint64_t i = 0; i < ndel; i++) {
        row_ids_chunk.data[0].set_value(i, static_cast<int64_t>(cap + 100 + i));
    }
    row_ids_chunk.set_cardinality(ndel);
    auto del_txn_id = del_txn.data().transaction_id;
    table_delete_state del_state(&env.resource);
    table->delete_rows(del_state, row_ids_chunk.data[0], ndel, del_txn_id);
    auto del_cid = mgr.commit(del_session);
    mgr.publish(del_cid);
    table->commit_all_deletes(del_txn_id, del_cid);
    REQUIRE(scan_count(*table, env) == 3 * cap + 10 - ndel);

    // ...and the dead-count/compaction walks see those stamps: compaction
    // reclaims exactly the deleted rows.
    REQUIRE(table->compact(std::numeric_limits<uint64_t>::max()));
    REQUIRE(table->row_group()->total_rows() == 3 * cap + 10 - ndel);
    REQUIRE(scan_count(*table, env) == 3 * cap + 10 - ndel);
}
