// Point fetch by row_id + MVCC visibility, via row_version_manager_t::fetch (the predicate
// storage_t::fetch consults on the "index -> fetch by row_id" route): an uncommitted delete
// hides the row from its own author only; RAW bypasses visibility entirely (what CREATE INDEX
// backfill needs, to recover a deleted row's old key columns); and the result's row_ids name
// exactly the rows it carries.
//
// EVERY ROW UNDER TEST IS PAST 1024: fetch rebases group-local version slots against a
// collection-absolute row id, and row group 0 (start == 0) can't tell a correct rebase from a
// missing one.

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
            table.finalize_append(state, transaction_data::committed());
        }
    }

    void delete_row(data_table_t& table, fetch_env_t& env, int64_t row_id, uint64_t txn_id) {
        std::pmr::vector<complex_logical_type> id_type(&env.resource);
        id_type.emplace_back(logical_type::BIGINT);
        auto ids = data_chunk_t(&env.resource, id_type, 1);
        ids.data[0].set_value(0, row_id);
        ids.set_cardinality(1);

        table_delete_state del_state(&env.resource);
        REQUIRE(deleted_or_fail(table.delete_rows(del_state, ids.data[0], 1, txn_id)) == 1);
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

// Both directions ride the same fetch_rows call, differing only in the transaction_data.
TEST_CASE("components::table::fetch_visibility::uncommitted_delete_hides_only_from_its_author") {
    fetch_env_t env;
    auto table = make_table(env);
    seed_committed(*table, env, kRows);

    components::storage::table_storage_adapter_t adapter(*table, &env.resource);
    components::storage::storage_t& storage = adapter;

    transaction_manager_t mgr(&env.resource);
    auto deleter_session = components::session::session_id_t::generate_uid();
    const auto deleter = mgr.begin_transaction(deleter_session, transaction_scope_t::statement).data();
    auto reader_session = components::session::session_id_t::generate_uid();
    const auto reader = mgr.begin_transaction(reader_session, transaction_scope_t::statement).data();

    std::pmr::vector<int64_t> one(&env.resource);
    one.push_back(kProbe);

    INFO("before the delete both transactions see the row");
    REQUIRE(fetch_rows(storage, env, *table, one, deleter, fetch_visibility_t::SNAPSHOT).rows == 1);
    REQUIRE(fetch_rows(storage, env, *table, one, reader, fetch_visibility_t::SNAPSHOT).rows == 1);

    delete_row(*table, env, kProbe, deleter.transaction_id);

    INFO("the author of the uncommitted delete must NOT read its own deleted row back");
    {
        // Pre-fix this returned rows==1 (payload intact): fetch never consulted
        // row_version_manager_t::fetch.
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

// An empty transaction_data is a snapshot (sees all committed), not a RAW bypass — the
// distinction this test pins.
TEST_CASE("components::table::fetch_visibility::raw_still_reads_committed_deleted_rows") {
    fetch_env_t env;
    auto table = make_table(env);
    seed_committed(*table, env, kRows);

    components::storage::table_storage_adapter_t adapter(*table, &env.resource);
    components::storage::storage_t& storage = adapter;

    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    const auto txn_id = mgr.begin_transaction(session, transaction_scope_t::statement).data().transaction_id;
    delete_row(*table, env, kProbe, txn_id);
    const auto commit_id = mgr.commit(session);
    mgr.publish(commit_id);
    table->commit_all_deletes(txn_id, commit_id);

    std::pmr::vector<int64_t> one(&env.resource);
    one.push_back(kProbe);

    INFO("SNAPSHOT with an EMPTY transaction_data still honours the committed delete");
    {
        // Pre-fix this returned rows==1: the committed tombstone was never consulted.
        auto got = fetch_rows(storage, env, *table, one, transaction_data::committed(), fetch_visibility_t::SNAPSHOT);
        REQUIRE(got.rows == 0);
    }

    INFO("RAW reads the deleted row anyway — the CREATE INDEX backfill depends on this");
    {
        auto got = fetch_rows(storage, env, *table, one, transaction_data::committed(), fetch_visibility_t::RAW);
        REQUIRE(got.rows == 1);
        REQUIRE(got.row_ids.front() == kProbe);
        REQUIRE(got.values.front() == kProbe);
    }
}

// row_ids on the result must name the SURVIVING rows, not a copy of the request (the old
// assumption).
TEST_CASE("components::table::fetch_visibility::the_answer_names_the_rows_it_carries") {
    fetch_env_t env;
    auto table = make_table(env);
    seed_committed(*table, env, kRows);

    components::storage::table_storage_adapter_t adapter(*table, &env.resource);
    components::storage::storage_t& storage = adapter;

    transaction_manager_t mgr(&env.resource);
    auto session = components::session::session_id_t::generate_uid();
    const auto txn_id = mgr.begin_transaction(session, transaction_scope_t::statement).data().transaction_id;
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

    auto got = fetch_rows(storage, env, *table, request, transaction_data::committed(), fetch_visibility_t::SNAPSHOT);
    // Pre-fix: cardinality 3 (tombstoned row survived) with row_ids memcpy'd from the request,
    // so slot 1 named a row the chunk did not actually carry.
    REQUIRE(got.rows == 2);
    REQUIRE(got.row_ids.size() == 2);
    REQUIRE(got.row_ids[0] == kProbe - 1);
    REQUIRE(got.row_ids[1] == kProbe + 1);
    REQUIRE(got.values[0] == kProbe - 1);
    REQUIRE(got.values[1] == kProbe + 1);
}
