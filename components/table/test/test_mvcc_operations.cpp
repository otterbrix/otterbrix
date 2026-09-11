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

namespace {
    // delete_rows answers a refusal, not just a count: result_wrapper_t::value() asserts the
    // channel was cleared first, so a test that wants the number has to clear it.
    uint64_t deleted_or_fail(core::result_wrapper_t<uint64_t> r) {
        REQUIRE_FALSE(r.has_error());
        return r.value();
    }
} // namespace

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    std::string mvcc_operations_db_path() {
        static std::string path = "/tmp/test_otterbrix_mvcc_operations_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    // Comma operator: remove stale file before naming it, ahead of the manager's open.
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
    REQUIRE_FALSE(table->revert_append(0, 10).has_error());

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

    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 10, txn1.data());
    auto cid1 = mgr.commit(s1);
    mgr.publish(cid1);
    table->commit_append(cid1, 0, 10);

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

    append_rows(*table, env, 0, 10);
    REQUIRE(scan_count(*table, env) == 10);

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
    REQUIRE_FALSE(table->delete_rows(del_state, row_ids_chunk.data[0], 5, txn_id).has_error());

    // mgr.commit() erases txn from active_map, so `txn` is dangling from here on
    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_all_deletes(txn_id, commit_id);

    REQUIRE(scan_count(*table, env) == 5);
}

TEST_CASE("components::table::mvcc::delete_rows_txn_without_commit_visible") {
    test_env env;
    auto table = make_int_table(env);

    append_rows(*table, env, 0, 10);
    REQUIRE(scan_count(*table, env) == 10);

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
    REQUIRE_FALSE(table->delete_rows(del_state, row_ids_chunk.data[0], 5, txn_id).has_error());

    mgr.abort(session); // also erases txn from active_map; leave it uncommitted

    REQUIRE(scan_count(*table, env) == 10); // deleted[] carries txn_id, not commit_id, so a non-txn scan skips it
}

TEST_CASE("components::table::mvcc::cleanup_committed_deletes") {
    test_env env;
    auto table = make_int_table(env);

    append_rows(*table, env, 0, 10);
    REQUIRE(scan_count(*table, env) == 10);

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
    REQUIRE_FALSE(table->delete_rows(del_state, row_ids_chunk.data[0], 10, txn_id).has_error());

    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_all_deletes(txn_id, commit_id);

    REQUIRE(scan_count(*table, env) == 0);

    auto lowest = mgr.lowest_active_start_time();
    table->cleanup_versions(lowest);

    REQUIRE(scan_count(*table, env) == 0);
}

TEST_CASE("components::table::mvcc::cleanup_partial_deletes") {
    test_env env;
    auto table = make_int_table(env);

    append_rows(*table, env, 0, 10);
    REQUIRE(scan_count(*table, env) == 10);

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
    REQUIRE_FALSE(table->delete_rows(del_state, row_ids_chunk.data[0], 5, txn_id).has_error());

    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_all_deletes(txn_id, commit_id);

    REQUIRE(scan_count(*table, env) == 5);

    auto lowest = mgr.lowest_active_start_time();
    table->cleanup_versions(lowest);

    REQUIRE(scan_count(*table, env) == 5);
}

TEST_CASE("components::table::mvcc::compact_after_delete") {
    test_env env;
    auto table = make_int_table(env);

    append_rows(*table, env, 0, 100);
    REQUIRE(scan_count(*table, env) == 100);

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
    REQUIRE_FALSE(table->delete_rows(del_state, row_ids_chunk.data[0], 50, txn_id).has_error());

    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_all_deletes(txn_id, commit_id);

    REQUIRE(scan_count(*table, env) == 50);

    // no other snapshot active and the delete is published, so the watermark green-lights the rebuild
    REQUIRE(table->compact(mgr.compact_watermark()));

    REQUIRE(scan_count(*table, env) == 50);
    REQUIRE(table->row_group()->total_rows() == 50);
}

TEST_CASE("components::table::mvcc::uncommitted_rows_invisible_to_other_txn") {
    test_env env;
    auto table = make_int_table(env);

    transaction_manager_t mgr(&env.resource);

    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 10, txn1.data());

    auto s2 = components::session::session_id_t::generate_uid();
    auto& txn2 = mgr.begin_transaction(s2);
    REQUIRE(scan_count_txn(*table, env, txn2.data()) == 0);

    auto commit_id = mgr.commit(s1);
    mgr.publish(commit_id);
    table->commit_append(commit_id, 0, 10);

    auto s3 = components::session::session_id_t::generate_uid();
    auto& txn3 = mgr.begin_transaction(s3);
    REQUIRE(scan_count_txn(*table, env, txn3.data()) == 10);

    mgr.abort(s2);
    mgr.abort(s3);
}

TEST_CASE("components::table::mvcc::delete_not_visible_until_commit") {
    test_env env;
    auto table = make_int_table(env);

    append_rows(*table, env, 0, 10);
    REQUIRE(scan_count(*table, env) == 10);

    transaction_manager_t mgr(&env.resource);

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
    REQUIRE_FALSE(table->delete_rows(del_state, row_ids_chunk.data[0], 5, txn_id).has_error());

    auto s2 = components::session::session_id_t::generate_uid();
    auto& txn2 = mgr.begin_transaction(s2);
    REQUIRE(scan_count_txn(*table, env, txn2.data()) == 10);
    mgr.abort(s2);

    auto commit_id = mgr.commit(s1);
    mgr.publish(commit_id);
    table->commit_all_deletes(txn_id, commit_id);

    auto s3 = components::session::session_id_t::generate_uid();
    auto& txn3 = mgr.begin_transaction(s3);
    REQUIRE(scan_count_txn(*table, env, txn3.data()) == 5);
    mgr.abort(s3);
}

TEST_CASE("components::table::mvcc::txn_sees_own_writes") {
    test_env env;
    auto table = make_int_table(env);

    transaction_manager_t mgr(&env.resource);

    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 5, txn1.data());

    REQUIRE(scan_count_txn(*table, env, txn1.data()) == 5);

    auto s2 = components::session::session_id_t::generate_uid();
    auto& txn2 = mgr.begin_transaction(s2);
    REQUIRE(scan_count_txn(*table, env, txn2.data()) == 0);

    mgr.abort(s1);
    REQUIRE_FALSE(table->revert_append(0, 5).has_error());
    mgr.abort(s2);
}

namespace {

    // distinct row VALUES, not counts: compact() can drop+leak versions, cancelling out in a bare count
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
        REQUIRE_FALSE(table.delete_rows(del_state, row_ids_chunk.data[0], 1, txn_id).has_error());
    }

    std::set<int64_t> make_range(int64_t first, int64_t last) {
        std::set<int64_t> s;
        for (int64_t v = first; v <= last; v++) {
            s.insert(v);
        }
        return s;
    }

} // anonymous namespace

// compact() must not collapse version history an OLDER snapshot still needs: txn2 predates a published update of row 0
TEST_CASE("components::table::mvcc::compact_preserves_old_snapshot_view") {
    test_env env;
    auto table = make_int_table(env);
    transaction_manager_t mgr(&env.resource);

    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 10, txn1.data());
    auto c1 = mgr.commit(s1);
    mgr.publish(c1);
    table->commit_append(c1, 0, 10);

    auto s2 = components::session::session_id_t::generate_uid();
    auto& txn2 = mgr.begin_transaction(s2);
    REQUIRE(scan_values_txn(*table, env, txn2.data()) == make_range(0, 9));

    auto s3 = components::session::session_id_t::generate_uid();
    auto& txn3 = mgr.begin_transaction(s3);
    auto txn3_id = txn3.data().transaction_id;
    delete_row0_txn(*table, env, txn3_id);
    append_rows_txn(*table, env, 100, 1, txn3.data()); // physical row 10
    auto c3 = mgr.commit(s3);
    table->commit_all_deletes(txn3_id, c3);
    table->commit_append(c3, 10, 1);
    mgr.publish(c3);

    auto s4 = components::session::session_id_t::generate_uid();
    auto& txn4 = mgr.begin_transaction(s4);
    auto expected_new = make_range(1, 9);
    expected_new.insert(100);
    REQUIRE(scan_values_txn(*table, env, txn4.data()) == expected_new);

    REQUIRE_FALSE(table->compact(mgr.compact_watermark())); // txn2 still active, watermark sits below c3

    REQUIRE(scan_values_txn(*table, env, txn2.data()) == make_range(0, 9));
    REQUIRE(scan_values_txn(*table, env, txn4.data()) == expected_new);

    mgr.abort(s2);
    mgr.abort(s4);

    REQUIRE(table->compact(mgr.compact_watermark())); // every old snapshot gone; watermark reaches c3
    REQUIRE(table->row_group()->total_rows() == 10);
    auto s6 = components::session::session_id_t::generate_uid();
    auto& txn6 = mgr.begin_transaction(s6);
    REQUIRE(scan_values_txn(*table, env, txn6.data()) == expected_new);
    mgr.abort(s6);
}

// The mid-update in-flight window: DELETE side stamped, replacement append not; compact() must see both as uncommitted
TEST_CASE("components::table::mvcc::compact_in_flight_commit_window") {
    test_env env;
    auto table = make_int_table(env);
    transaction_manager_t mgr(&env.resource);

    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 10, txn1.data());
    auto c1 = mgr.commit(s1);
    mgr.publish(c1);
    table->commit_append(c1, 0, 10);

    auto s3 = components::session::session_id_t::generate_uid();
    auto& txn3 = mgr.begin_transaction(s3);
    auto txn3_id = txn3.data().transaction_id;
    delete_row0_txn(*table, env, txn3_id);
    append_rows_txn(*table, env, 100, 1, txn3.data()); // physical row 10

    auto c3 = mgr.commit(s3); // c3 stays IN FLIGHT (no publish yet); stamp only the delete side
    table->commit_all_deletes(txn3_id, c3);

    auto s4 = components::session::session_id_t::generate_uid();
    auto& txn4 = mgr.begin_transaction(s4);
    REQUIRE(scan_values_txn(*table, env, txn4.data()) == make_range(0, 9));

    REQUIRE_FALSE(table->compact(mgr.compact_watermark())); // c3 in flight, watermark sits below it

    REQUIRE(scan_values_txn(*table, env, txn4.data()) == make_range(0, 9)); // row must not vanish

    table->commit_append(c3, 10, 1);
    mgr.publish(c3);

    auto s5 = components::session::session_id_t::generate_uid();
    auto& txn5 = mgr.begin_transaction(s5);
    auto expected_new = make_range(1, 9);
    expected_new.insert(100);
    REQUIRE(scan_values_txn(*table, env, txn5.data()) == expected_new);

    mgr.abort(s4);
    mgr.abort(s5);

    REQUIRE(table->compact(mgr.compact_watermark())); // window closed, snapshots gone
    REQUIRE(table->row_group()->total_rows() == 10);
    auto s6 = components::session::session_id_t::generate_uid();
    auto& txn6 = mgr.begin_transaction(s6);
    REQUIRE(scan_values_txn(*table, env, txn6.data()) == expected_new);
    mgr.abort(s6);
}

namespace {

    std::unique_ptr<data_table_t> make_int2_table(test_env& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("a", complex_logical_type(logical_type::BIGINT));
        columns.emplace_back("b", complex_logical_type(logical_type::BIGINT));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "test2");
    }

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

    std::vector<std::pair<int64_t, int64_t>> scan_pairs(data_table_t& table, test_env& env) { // a desync = wrong pair
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

// A revert that moves only the row-group count desyncs columns: a scan over-reads stale rows (fetch_row overflow)
TEST_CASE("components::table::mvcc::revert_append_truncates_columns_direct") {
    test_env env;
    auto table = make_int2_table(env);

    append_rows2(*table, env, 0, 100);
    REQUIRE(table->row_group()->total_rows() == 100);

    REQUIRE_FALSE(table->revert_append(40, 60).has_error()); // keep [0,40), drop the last 60
    REQUIRE(table->row_group()->total_rows() == 40);

    {
        auto rows = scan_pairs(*table, env);
        REQUIRE(rows.size() == 40);
        for (uint64_t i = 0; i < 40; i++) {
            REQUIRE(rows[i].first == static_cast<int64_t>(i));
            REQUIRE(rows[i].second == static_cast<int64_t>(i) * 10);
        }
    }

    // distinct values (a=1000..1029): a missed truncation would read STALE originals (a=40..69) in [40,70)
    append_rows2(*table, env, 1000, 30);
    REQUIRE(table->row_group()->total_rows() == 70);

    {
        auto rows = scan_pairs(*table, env);
        REQUIRE(rows.size() == 70);
        for (uint64_t i = 0; i < 40; i++) {
            REQUIRE(rows[i].first == static_cast<int64_t>(i));
            REQUIRE(rows[i].second == static_cast<int64_t>(i) * 10);
        }
        for (uint64_t j = 0; j < 30; j++) {
            const int64_t v = 1000 + static_cast<int64_t>(j);
            REQUIRE(rows[40 + j].first == v);
            REQUIRE(rows[40 + j].second == v * 10);
        }
    }
}

// Issue #552 family: an aborted MVCC update must restore the original row, and a later COMMITTED update must yield
// exactly the new version; also pins validity_mask_t::set_valid (a 64-bit-per-"bit" clear would leave a stuck NULL).
TEST_CASE("components::table::mvcc::aborted_update_revert_restores_row") {
    test_env env;
    std::vector<column_definition_t> columns;
    columns.emplace_back("id", complex_logical_type(logical_type::BIGINT));
    columns.emplace_back("val", complex_logical_type(logical_type::STRING_LITERAL));
    auto table = std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "t");

    auto types = table->copy_types();
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

    const uint64_t txn_id = TRANSACTION_ID_START + 5;
    const transaction_data txn{txn_id, 100};
    {
        auto del_state = table->initialize_delete({});
        auto row_ids = vector_t(&env.resource, complex_logical_type(logical_type::BIGINT), 1);
        row_ids.set_value(0, logical_value_t(&env.resource, int64_t(0)));
        REQUIRE(deleted_or_fail(table->delete_rows(*del_state, row_ids, 1, txn_id)) == 1);
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

    REQUIRE_FALSE(table->revert_append(appended_start, 1).has_error());
    table->revert_all_deletes(txn_id);

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

    const uint64_t txn2 = TRANSACTION_ID_START + 7;
    {
        auto del_state = table->initialize_delete({});
        auto row_ids = vector_t(&env.resource, complex_logical_type(logical_type::BIGINT), 1);
        row_ids.set_value(0, logical_value_t(&env.resource, int64_t(0)));
        REQUIRE(deleted_or_fail(table->delete_rows(*del_state, row_ids, 1, txn2)) == 1);
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

// vector_info_ addressing disagrees past row group 0: append uses GROUP-LOCAL, scan/delete/fetch use ABSOLUTE
TEST_CASE("components::table::mvcc::uncommitted_rows_invisible_in_second_row_group") {
    test_env env;
    auto table = make_int_table(env);

    append_rows(*table, env, 0, 1024); // fills row group 0 exactly (row_group_size == DEFAULT_VECTOR_CAPACITY)
    REQUIRE(scan_count(*table, env) == 1024);

    transaction_manager_t mgr(&env.resource);
    auto session1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(session1);
    append_rows_txn(*table, env, 1024, 10, txn1.data()); // lands in row group 1; NOT committed

    auto session2 = components::session::session_id_t::generate_uid();
    auto& txn2 = mgr.begin_transaction(session2);
    REQUIRE(scan_count_txn(*table, env, txn2.data()) == 1024);

    mgr.abort(session2);
    mgr.abort(session1);
}

// committed_row_count (group-local) must agree with the scan on a tombstone the delete path wrote past row 1024
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

    auto del_state = table->initialize_delete({});
    auto row_ids = vector_t(&env.resource, complex_logical_type(logical_type::BIGINT), 1);
    row_ids.set_value(0, logical_value_t(&env.resource, int64_t(1030))); // lives in row group 1
    REQUIRE(deleted_or_fail(table->delete_rows(*del_state, row_ids, 1, txn_id)) == 1);

    auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_all_deletes(txn_id, commit_id);

    REQUIRE(scan_count(*table, env) == 1033);
    REQUIRE(table->row_group()->committed_row_count() == 1033);
}

// a pending delete past row 1024 must refuse compact(): has_version_above must see the stamp wherever it landed
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

    auto del_state = table->initialize_delete({});
    auto row_ids = vector_t(&env.resource, complex_logical_type(logical_type::BIGINT), 1);
    row_ids.set_value(0, logical_value_t(&env.resource, int64_t(1030))); // pending, no commit
    REQUIRE(deleted_or_fail(table->delete_rows(*del_state, row_ids, 1, txn_id)) == 1);

    REQUIRE_FALSE(table->compact(mgr.compact_watermark()));

    mgr.abort(session);
    table->revert_all_deletes(txn_id);
    REQUIRE(scan_count(*table, env) == 1034);
}

namespace {

    // every row's content is a function of `base`, so a stale child tail is observable content, not just a wrong count
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

// revert_append hands every column an ABSOLUTE row number, but a LIST child's offsets are cumulative counts sharing
// the parent's start_ — comparing the RELATIVE surviving count against it leaves the child untruncated for start_ > 0
TEST_CASE("components::table::mvcc::revert_append_list_child_row_group_1") {
    test_env env;
    auto table = make_list_table(env);

    append_list_rows(*table, env, 0, 1024, 0); // fills row group 0, then 40 rows into row group 1
    append_list_rows(*table, env, 1024, 40, 0);
    REQUIRE(table->row_group()->total_rows() == 1064);

    REQUIRE_FALSE(table->revert_append(1044, 20).has_error()); // keep [0, 1044)
    REQUIRE(table->row_group()->total_rows() == 1044);

    verify_list_rows(*table, env, 1044, 1044, 0);

    // distinct base: without truncation, [1044,1056) would read the reverted rows' elements
    append_list_rows(*table, env, 1044, 12, 1'000'000);
    REQUIRE(table->row_group()->total_rows() == 1056);
    verify_list_rows(*table, env, 1056, 1044, 1'000'000);
}

// Same confusion on the ARRAY leg: start_row * array_size lands past the child's end for start_ > 0
TEST_CASE("components::table::mvcc::revert_append_array_child_row_group_1") {
    test_env env;
    auto table = make_array_table(env);

    append_array_rows(*table, env, 0, 1024, 0);
    append_array_rows(*table, env, 1024, 40, 0);
    REQUIRE(table->row_group()->total_rows() == 1064);

    REQUIRE_FALSE(table->revert_append(1044, 20).has_error());
    REQUIRE(table->row_group()->total_rows() == 1044);

    verify_array_rows(*table, env, 1044, 1044, 0);

    append_array_rows(*table, env, 1044, 12, 1'000'000);
    REQUIRE(table->row_group()->total_rows() == 1056);
    verify_array_rows(*table, env, 1056, 1044, 1'000'000);
}

// VACUUM must not resurrect deletes: `cleanup()==true` installing an empty `result` means "every row visible",
// un-deleting for every reader. cleanup_append only collapses a FULL vector, so the cases below insist on 1024 rows.

namespace {

    // mirrors the DELETE statement: stamp pending txn id, commit+publish allocate the commit id, then stamp it in
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
        REQUIRE(deleted_or_fail(table.delete_rows(*del_state, row_ids, count, txn_id)) == count);

        auto commit_id = mgr.commit(session);
        mgr.publish(commit_id);
        table.commit_all_deletes(txn_id, commit_id);
    }

    // contents, not just count: a count alone can't tell "deleted rows came back" from "different rows survived"
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

// PARTIAL deletes: falling through the partial-delete branch to `true, empty result` would resurrect all 500
TEST_CASE("components::table::mvcc::vacuum_keeps_partial_committed_deletes") {
    test_env env;
    auto table = make_int_table(env);
    transaction_manager_t mgr(&env.resource);

    append_rows(*table, env, 0, 1024);
    REQUIRE(scan_all_values(*table, env) == make_vector_range(0, 1023));

    delete_range_committed(*table, env, mgr, 0, 500);
    const auto survivors = make_vector_range(500, 1023);
    REQUIRE(scan_all_values(*table, env) == survivors);

    // size check first: a resurrection then reads as "524 became 1024", not a 1024-element diff
    auto check_survivors = [&] {
        auto visible = scan_all_values(*table, env);
        REQUIRE(visible.size() == survivors.size());
        REQUIRE(visible == survivors);
    };

    table->cleanup_versions(mgr.lowest_active_start_time());
    check_survivors();

    table->cleanup_versions(mgr.lowest_active_start_time()); // a second pass must be just as harmless
    check_survivors();
}

// A FULLY deleted vector takes TWO passes: pass 1 collapses it to a constant; only pass 2 reaches
// chunk_constant_info::cleanup, where an empty `result` on a committed delete_id would resurrect all 1024.
TEST_CASE("components::table::mvcc::vacuum_keeps_fully_deleted_vector_deleted") {
    test_env env;
    auto table = make_int_table(env);
    transaction_manager_t mgr(&env.resource);

    append_rows(*table, env, 0, 1024);
    REQUIRE(scan_all_values(*table, env) == make_vector_range(0, 1023));

    delete_range_committed(*table, env, mgr, 0, 1024);
    REQUIRE(scan_all_values(*table, env).size() == 0);

    table->cleanup_versions(mgr.lowest_active_start_time()); // collapses the vector to a constant
    REQUIRE(scan_all_values(*table, env).size() == 0);

    table->cleanup_versions(mgr.lowest_active_start_time()); // the constant must keep its delete stamp too
    REQUIRE(scan_all_values(*table, env).size() == 0);

    table->cleanup_versions(mgr.lowest_active_start_time());
    REQUIRE(scan_all_values(*table, env).size() == 0);
}

// The other half, at chunk_info::cleanup: `true`+empty drops the slot, `true`+result replaces it, `false` keeps it.
// Insert history is droppable; a delete is not.
TEST_CASE("components::table::mvcc::cleanup_still_reclaims_insert_only_history") {
    constexpr uint64_t kLowest = 1000;
    constexpr uint64_t kOldCommit = 10;

    // no delete at all: 1024 insert stamps visible to everyone, so the whole slot goes
    {
        chunk_vector_info info(0);
        info.append(0, DEFAULT_VECTOR_CAPACITY, kOldCommit);
        std::unique_ptr<chunk_info> result;
        REQUIRE(info.cleanup(kLowest, result));
        REQUIRE(result == nullptr);
    }

    // fully deleted: collapses to one constant, but it must KEEP the delete_id
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

    // deleted by TWO transactions: a constant carries only ONE stamp, so collapse is off
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

    // revert_all_deletes leaves any_deleted as a conservative hint with no surviving stamp
    {
        chunk_vector_info info(0);
        info.append(0, DEFAULT_VECTOR_CAPACITY, kOldCommit);
        info.any_deleted = true;
        std::unique_ptr<chunk_info> result;
        REQUIRE(info.cleanup(kLowest, result));
        REQUIRE(result == nullptr);
    }

    // a delete-free CONSTANT slot is reclaimable on the same terms
    {
        chunk_constant_info info(0);
        info.insert_id = kOldCommit;
        std::unique_ptr<chunk_info> result;
        REQUIRE(info.cleanup(kLowest, result));
        REQUIRE(result == nullptr);
    }

    // refusals that already held: an insert newer than the floor, a delete still pending under a live txn id
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

// SILENT WRONG ANSWER: an unrelated commit publishing a LARGER id can jump published_horizon_ past c_del while it's
// still in_flight_commits_, so the deferred index-delete sweep (services/index/manager_index.cpp) reaps row 0's index
// entry while the table still hands it back. End-to-end form: services/index/tests/test_index_delete_horizon.cpp.
TEST_CASE("components::table::mvcc::index_sweep_floor_in_publish_window") {
    test_env env;
    auto table = make_int_table(env);
    transaction_manager_t mgr(&env.resource);

    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 10, txn1.data());
    auto c1 = mgr.commit(s1);
    mgr.publish(c1);
    table->commit_append(c1, 0, 10);

    auto s_del = components::session::session_id_t::generate_uid();
    auto& txn_del = mgr.begin_transaction(s_del);
    auto txn_del_id = txn_del.data().transaction_id;
    delete_row0_txn(*table, env, txn_del_id);
    auto c_del = mgr.commit(s_del); // stamped, but publish() has NOT run yet
    table->commit_all_deletes(txn_del_id, c_del);

    auto s_oth = components::session::session_id_t::generate_uid(); // larger id drags published_horizon_ past c_del
    mgr.begin_transaction(s_oth);
    auto c_oth = mgr.commit(s_oth);
    REQUIRE(c_oth > c_del);
    mgr.publish(c_oth);

    auto s_read = components::session::session_id_t::generate_uid(); // snapshot_horizon==c_oth, in_flight{c_del}
    auto& reader = mgr.begin_transaction(s_read);

    REQUIRE(scan_values_txn(*table, env, reader.data()) == make_range(0, 9));

    const auto broadcast = mgr.lowest_active_snapshot_horizon(); // c_del must NOT qualify while this reads row 0
    REQUIRE_FALSE(c_del <= broadcast);

    mgr.abort(s_read);
    mgr.publish(c_del);
    auto s_after = components::session::session_id_t::generate_uid();
    auto& after = mgr.begin_transaction(s_after);
    REQUIRE(scan_values_txn(*table, env, after.data()) == make_range(1, 9));
    mgr.abort(s_after);
    REQUIRE(c_del <= mgr.lowest_active_snapshot_horizon());
}

// An orphaned commit_id blocks compaction of every table (see test_transaction_manager.cpp's
// "orphaned_commit_pins_horizon_forever"): with no live transactions the only floor is
// min(in_flight_commits_) - 1. The orphan here writes nothing, to isolate the horizon effect.
TEST_CASE("components::table::mvcc::orphaned_commit_blocks_compaction") {
    test_env env;
    auto table = make_int_table(env);
    transaction_manager_t mgr(&env.resource);

    auto s1 = components::session::session_id_t::generate_uid();
    auto& txn1 = mgr.begin_transaction(s1);
    append_rows_txn(*table, env, 0, 10, txn1.data());
    auto c1 = mgr.commit(s1);
    mgr.publish(c1);
    table->commit_append(c1, 0, 10);

    auto s_lost = components::session::session_id_t::generate_uid(); // dies at an early exit
    mgr.begin_transaction(s_lost);
    const auto c_lost = mgr.commit(s_lost); // commit() already dropped it from active_

    // a REAL delete, after the orphan (else its id would sit below the pinned floor and compact would succeed wrongly)
    auto s_del = components::session::session_id_t::generate_uid();
    auto& txn_del = mgr.begin_transaction(s_del);
    auto txn_del_id = txn_del.data().transaction_id;
    delete_row0_txn(*table, env, txn_del_id);
    const auto c_del = mgr.commit(s_del);
    REQUIRE(c_del > c_lost);
    table->commit_all_deletes(txn_del_id, c_del);
    mgr.publish(c_del);

    REQUIRE_FALSE(mgr.has_active_transactions());

    auto s_before = components::session::session_id_t::generate_uid();
    auto& before = mgr.begin_transaction(s_before);
    const auto expected = make_range(1, 9);
    REQUIRE(scan_values_txn(*table, env, before.data()) == expected);
    mgr.abort(s_before);

    REQUIRE(mgr.compact_watermark() == c_lost - 1); // stuck one below the orphan, refusing the rebuild
    REQUIRE_FALSE(table->compact(mgr.compact_watermark()));
    REQUIRE(table->row_group()->total_rows() == 10);

    const auto horizon_before = mgr.published_horizon();
    mgr.discard(c_lost); // the cure: one erase; published_horizon_ does not move
    REQUIRE(mgr.published_horizon() == horizon_before);
    REQUIRE(mgr.compact_watermark() == c_del);

    REQUIRE(table->compact(mgr.compact_watermark()));
    REQUIRE(table->row_group()->total_rows() == 9);

    auto s_after = components::session::session_id_t::generate_uid();
    auto& after = mgr.begin_transaction(s_after);
    REQUIRE(scan_values_txn(*table, env, after.data()) == expected);
    mgr.abort(s_after);
}

// cleanup_versions gates on lowest_active_start_time, which IGNORES in-flight commits: an unpublished commit already
// left active_, so lowest can exceed its id and collapse the version slot to "visible to all" without the gate.
TEST_CASE("components::table::mvcc::cleanup_must_not_publish_an_in_flight_commit") {
    test_env env;
    auto table = make_int_table(env);
    transaction_manager_t mgr(&env.resource);

    // a FULL vector (cleanup_append only collapses full ones); commit without publish stays in-flight
    auto sw = components::session::session_id_t::generate_uid();
    auto& wtxn = mgr.begin_transaction(sw);
    append_rows_txn(*table, env, 0, 1024, wtxn.data());
    auto commit_id = mgr.commit(sw);
    table->commit_append(commit_id, 0, 1024);

    auto sr = components::session::session_id_t::generate_uid();
    auto& rtxn = mgr.begin_transaction(sr);
    REQUIRE(scan_count_txn(*table, env, rtxn.data()) == 0);

    table->cleanup_versions(mgr.lowest_active_start_time());

    REQUIRE(scan_count_txn(*table, env, rtxn.data()) == 0); // must STILL not see the unpublished commit

    mgr.publish(commit_id);
    mgr.abort(sr);
}

// Published AFTER the reader starts, so only the reader's own snapshot still carries it — the gate must
// check that per-txn half too.
TEST_CASE("components::table::mvcc::cleanup_honours_a_readers_in_flight_snapshot") {
    test_env env;
    auto table = make_int_table(env);
    transaction_manager_t mgr(&env.resource);

    auto sw = components::session::session_id_t::generate_uid();
    auto& wtxn = mgr.begin_transaction(sw);
    append_rows_txn(*table, env, 0, 1024, wtxn.data());
    auto commit_id = mgr.commit(sw);
    table->commit_append(commit_id, 0, 1024);

    auto sr = components::session::session_id_t::generate_uid();
    auto& rtxn = mgr.begin_transaction(sr);
    REQUIRE(scan_count_txn(*table, env, rtxn.data()) == 0);

    mgr.publish(commit_id);

    table->cleanup_versions(mgr.lowest_active_start_time());

    REQUIRE(scan_count_txn(*table, env, rtxn.data()) == 0);
    mgr.abort(sr);
}
