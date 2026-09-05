// The point fetch by row_id and MVCC visibility, at the table layer.
//
// storage_t::fetch is the leaf of the "index -> fetch by row_id" route, and the only place that
// route asks the visibility question at all — row_version_manager_t::fetch is the predicate
// written for exactly this. These cases pin the predicate to the route, in both directions:
//
//   * a row a transaction has deleted (its own uncommitted delete, or a committed one below the
//     reader's snapshot) must NOT come back under SNAPSHOT;
//   * a row deleted by SOMEONE ELSE'S still-open transaction must STILL come back — an
//     uncommitted delete hides nothing from anyone but its own author;
//   * RAW ignores all of it, which is what the CREATE INDEX backfill needs: it reads deleted
//     rows on purpose to recover their old key columns.
//
// EVERY ROW UNDER TEST IS PAST 1024. Version slots are addressed GROUP-LOCALLY while the point
// fetch names collection-ABSOLUTE row ids, and row_version_manager_t::fetch keeps the absolute
// contract and rebases inside itself. A row in the first row group has start == 0 and cannot
// tell a correct rebase from a missing one, so the whole class of bug is invisible below 1024.
//
// The fetch result is also the ONLY report of which rows came back: the produced chunk's row_ids
// name exactly the rows it carries, in order, so a skipped row is visible to the caller instead
// of masked by a request-shaped stamp.

#include <catch2/catch_test_macros.hpp>

#include <components/storage/table_storage_adapter.hpp>
#include <components/table/data_table.hpp>
#include <components/table/storage/buffer_pool.hpp>
#include <components/table/storage/single_file_block_manager.hpp>
#include <components/table/storage/standard_buffer_manager.hpp>
#include <components/table/table_state.hpp>
#include <components/table/transaction_manager.hpp>
#include <core/file/local_file_system.hpp>

#include <cstdio>
#include <string>
#include <unistd.h>

using namespace components::types;
using namespace components::vector;
using namespace components::table;

namespace {

    std::string fetch_visibility_db_path() {
        static std::string path = "/tmp/test_otterbrix_fetch_visibility_" + std::to_string(::getpid()) + ".otbx";
        return path;
    }

    const std::string& fetch_visibility_fresh_db_path() {
        static const std::string path = (std::remove(fetch_visibility_db_path().c_str()), fetch_visibility_db_path());
        return path;
    }

    struct fetch_env_t {
        core::pmr::otterbrix_resource resource;
        core::filesystem::local_file_system_t fs;
        storage::buffer_pool_t buffer_pool;
        storage::standard_buffer_manager_t buffer_manager;
        storage::single_file_block_manager_t block_manager;

        fetch_env_t()
            : buffer_pool(&resource, uint64_t(1) << 32, false, uint64_t(1) << 24)
            , buffer_manager(&resource, fs, buffer_pool)
            , block_manager(buffer_manager, fs, fetch_visibility_fresh_db_path()) {
            REQUIRE_FALSE(block_manager.create_new_database().has_error());
        }

        ~fetch_env_t() { std::remove(fetch_visibility_db_path().c_str()); }
    };

    // Row group size is DEFAULT_VECTOR_CAPACITY (1024), so 3072 rows == three groups
    // and every row id used below sits in the SECOND or THIRD.
    constexpr uint64_t kRows = 3072;
    constexpr int64_t kProbe = 1500; // group 1, local row 476

    std::unique_ptr<data_table_t> make_table(fetch_env_t& env) {
        std::vector<column_definition_t> columns;
        columns.emplace_back("value", complex_logical_type(logical_type::BIGINT));
        return std::make_unique<data_table_t>(&env.resource, env.block_manager, std::move(columns), "fetch_visibility");
    }

    // Appends `count` committed rows (txn 0 == visible to everyone) in
    // DEFAULT_VECTOR_CAPACITY batches, so the row groups close exactly on 1024.
    void seed_committed(data_table_t& table, fetch_env_t& env, uint64_t count) {
        auto types = table.copy_types();
        for (uint64_t base = 0; base < count; base += DEFAULT_VECTOR_CAPACITY) {
            const uint64_t n = std::min<uint64_t>(DEFAULT_VECTOR_CAPACITY, count - base);
            auto chunk = data_chunk_t(&env.resource, types, n);
            for (uint64_t i = 0; i < n; i++) {
                chunk.data[0].set_value(i, static_cast<int64_t>(base + i));
            }
            chunk.set_cardinality(n);

            table_append_state state(&env.resource);
            REQUIRE_FALSE(table.append_lock(state).has_error());
            REQUIRE_FALSE(table.initialize_append(state).has_error());
            REQUIRE_FALSE(table.append(chunk, state).has_error());
            table.finalize_append(state, transaction_data{0, 0});
        }
    }

    void delete_row(data_table_t& table, fetch_env_t& env, int64_t row_id, uint64_t txn_id) {
        std::pmr::vector<complex_logical_type> id_type(&env.resource);
        id_type.emplace_back(logical_type::BIGINT);
        auto ids = data_chunk_t(&env.resource, id_type, 1);
        ids.data[0].set_value(0, row_id);
        ids.set_cardinality(1);

        table_delete_state del_state(&env.resource);
        REQUIRE(table.delete_rows(del_state, ids.data[0], 1, txn_id) == 1);
    }

    struct fetched_t {
        uint64_t rows{0};
        std::pmr::vector<int64_t> row_ids;
        std::pmr::vector<int64_t> values;

        explicit fetched_t(std::pmr::memory_resource* r)
            : row_ids(r)
            , values(r) {}
    };

    // The single call under test. Everything else in this file is scaffolding.
    fetched_t fetch_rows(components::storage::storage_t& storage,
                         fetch_env_t& env,
                         data_table_t& table,
                         const std::pmr::vector<int64_t>& request,
                         transaction_data txn,
                         fetch_visibility_t visibility) {
        auto types = table.copy_types();
        data_chunk_t out(&env.resource, types, request.size());
        vector_t row_ids(&env.resource, logical_type::BIGINT, request.size());
        for (std::size_t i = 0; i < request.size(); ++i) {
            row_ids.data<int64_t>()[i] = request[i];
        }

        auto fetch_r = storage.fetch(out, row_ids, request.size(), {}, txn, visibility);
        REQUIRE_FALSE(fetch_r.has_error());

        fetched_t result(&env.resource);
        result.rows = out.size();
        for (uint64_t i = 0; i < out.size(); ++i) {
            result.row_ids.push_back(out.row_ids.data<int64_t>()[i]);
            const auto cell = out.value(0, i);
            result.values.push_back(cell.value<int64_t>());
        }
        return result;
    }

} // namespace

// The author of an UNCOMMITTED delete must not read the row back through the point
// fetch, and nobody else may lose it. Both halves ride the same call, differing only
// in the transaction_data handed to it.
TEST_CASE("components::table::fetch_visibility::uncommitted_delete_hides_only_from_its_author") {
    fetch_env_t env;
    auto table = make_table(env);
    seed_committed(*table, env, kRows);

    components::storage::table_storage_adapter_t adapter(*table, &env.resource);
    components::storage::storage_t& storage = adapter;

    transaction_manager_t mgr(&env.resource);
    auto deleter_session = components::session::session_id_t::generate_uid();
    const auto deleter = mgr.begin_transaction(deleter_session).data();
    auto reader_session = components::session::session_id_t::generate_uid();
    const auto reader = mgr.begin_transaction(reader_session).data();

    std::pmr::vector<int64_t> one(&env.resource);
    one.push_back(kProbe);

    INFO("before the delete both transactions see the row");
    REQUIRE(fetch_rows(storage, env, *table, one, deleter, fetch_visibility_t::SNAPSHOT).rows == 1);
    REQUIRE(fetch_rows(storage, env, *table, one, reader, fetch_visibility_t::SNAPSHOT).rows == 1);

    delete_row(*table, env, kProbe, deleter.transaction_id);

    INFO("the author of the uncommitted delete must NOT read its own deleted row back");
    {
        // Without the visibility check: storage_fetch never asked row_version_manager_t::fetch, so the
        // deleted row came back with its payload intact (`1 == 0`).
        auto got = fetch_rows(storage, env, *table, one, deleter, fetch_visibility_t::SNAPSHOT);
        REQUIRE(got.rows == 0);
    }

    INFO("an uncommitted delete hides the row from NOBODY else");
    {
        auto got = fetch_rows(storage, env, *table, one, reader, fetch_visibility_t::SNAPSHOT);
        REQUIRE(got.rows == 1);
        REQUIRE(got.row_ids.front() == kProbe);
        REQUIRE(got.values.front() == kProbe);
    }
}

// A COMMITTED delete hides the row from a default (see-everything-committed)
// transaction_data — which is exactly why an empty transaction_data cannot double as
// "raw": it is a snapshot, not a bypass. RAW is the bypass, and the backfill needs it.
TEST_CASE("components::table::fetch_visibility::raw_still_reads_committed_deleted_rows") {
    fetch_env_t env;
    auto table = make_table(env);
    seed_committed(*table, env, kRows);

    components::storage::table_storage_adapter_t adapter(*table, &env.resource);
    components::storage::storage_t& storage = adapter;

    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    const auto txn_id = mgr.begin_transaction(session).data().transaction_id;
    delete_row(*table, env, kProbe, txn_id);
    const auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_all_deletes(txn_id, commit_id);

    std::pmr::vector<int64_t> one(&env.resource);
    one.push_back(kProbe);

    INFO("SNAPSHOT with an EMPTY transaction_data still honours the committed delete");
    {
        // Without the visibility check: the committed tombstone was never consulted, so the row came
        // back (`1 == 0`). An empty transaction_data means "see all COMMITTED rows", and
        // this delete IS committed.
        auto got = fetch_rows(storage, env, *table, one, transaction_data{}, fetch_visibility_t::SNAPSHOT);
        REQUIRE(got.rows == 0);
    }

    INFO("RAW reads the deleted row anyway — the CREATE INDEX backfill depends on this");
    {
        auto got = fetch_rows(storage, env, *table, one, transaction_data{}, fetch_visibility_t::RAW);
        REQUIRE(got.rows == 1);
        REQUIRE(got.row_ids.front() == kProbe);
        REQUIRE(got.values.front() == kProbe);
    }
}

// The produced chunk must REPORT which rows it carries. A skipped row (invisible, or
// naming no row group at all) shortens the answer, and the row_ids stamped on it name
// the surviving rows in order — instead of the request, which was the assumption the
// old code stamped in place of the fact.
TEST_CASE("components::table::fetch_visibility::the_answer_names_the_rows_it_carries") {
    fetch_env_t env;
    auto table = make_table(env);
    seed_committed(*table, env, kRows);

    components::storage::table_storage_adapter_t adapter(*table, &env.resource);
    components::storage::storage_t& storage = adapter;

    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    const auto txn_id = mgr.begin_transaction(session).data().transaction_id;
    delete_row(*table, env, kProbe, txn_id);
    const auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_all_deletes(txn_id, commit_id);

    std::pmr::vector<int64_t> request(&env.resource);
    request.push_back(kProbe - 1);
    request.push_back(kProbe);
    request.push_back(kProbe + 1);
    // Past the end of the table: it resolves to no row group at all.
    request.push_back(static_cast<int64_t>(kRows) + 10);

    auto got = fetch_rows(storage, env, *table, request, transaction_data{}, fetch_visibility_t::SNAPSHOT);
    // Without the visibility check: cardinality was 3 (the tombstoned row survived) and the row_ids
    // were memcpy'd from the REQUEST, so slot 1 named a row the chunk did not carry.
    REQUIRE(got.rows == 2);
    REQUIRE(got.row_ids.size() == 2);
    REQUIRE(got.row_ids[0] == kProbe - 1);
    REQUIRE(got.row_ids[1] == kProbe + 1);
    REQUIRE(got.values[0] == kProbe - 1);
    REQUIRE(got.values[1] == kProbe + 1);
}
