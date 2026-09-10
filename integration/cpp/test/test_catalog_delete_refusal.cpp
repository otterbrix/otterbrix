#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/table/test/fault_injection_file.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/wal/wal_page.hpp>

#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <thread>

// Refuse the WAL write behind DROP's catalog scrub (DEV_MODE seam, services/wal/wal_page.hpp)
// and assert CONTENT after COMMIT, not ROLLBACK, so a scrub that silently no-oped couldn't pass.

using namespace components;

namespace {

    const std::string kTableName = "wide_refusal_t";
    // Against a 4064-byte WAL page, this table's pg_attribute scrub cannot fit in one page.
    constexpr int kColumns = 700;

    class wal_fault_scope_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_fault_scope_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_fault_scope_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }

        wal_fault_scope_t(const wal_fault_scope_t&) = delete;
        wal_fault_scope_t& operator=(const wal_fault_scope_t&) = delete;

        std::string faulty_marker;
        otterbrix_test::fault_plan_t plan;

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            const auto name = path.string();
            if (inner != nullptr && !faulty_marker.empty() && name.find(faulty_marker) != std::string::npos) {
                return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan);
            }
            return inner;
        }
    };

    class delete_refusal_spaces_t final : public otterbrix::base_otterbrix_t {
    public:
        explicit delete_refusal_spaces_t(const configuration::config& config)
            : otterbrix::base_otterbrix_t(config) {
            components::compute::function_registry_t::reset_default();
        }

        services::disk::manager_disk_t* disk() noexcept { return manager_disk_.get(); }
    };

    // "the read refused" — distinct from every honest row count, including zero.
    constexpr std::size_t kReadRefused = static_cast<std::size_t>(-1);

    // Reads the catalog as a snapshot that sees every COMMITTED row.
    template<typename Key>
    core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
    catalog_chunks_with(delete_refusal_spaces_t& space, catalog::oid_t table_oid, std::uint64_t key_col, Key key) {
        auto* resource = space.disk()->resource();
        auto td = table::transaction_data::committed();
        execution_context_t exec_ctx{otterbrix::session_id_t{}, td, {}};
        std::pmr::vector<std::uint64_t> key_cols(resource);
        key_cols.emplace_back(key_col);
        auto [_, fut] = actor_zeta::otterbrix::send(space.disk()->address(),
                                                    &services::disk::manager_disk_t::read_chunks_by_key,
                                                    exec_ctx,
                                                    table_oid,
                                                    std::move(key_cols),
                                                    components::operators::make_key_chunk(resource, key),
                                                    std::pmr::vector<std::uint64_t>{resource});
        for (int i = 0; i < 2000000 && !fut.is_ready(); ++i) {
            std::this_thread::yield();
        }
        REQUIRE(fut.is_ready());
        return std::move(fut).take_ready();
    }

    template<typename Key>
    std::size_t
    catalog_rows_with(delete_refusal_spaces_t& space, catalog::oid_t table_oid, std::uint64_t key_col, Key key) {
        auto batches = catalog_chunks_with(space, table_oid, key_col, key);
        if (batches.has_error()) {
            return kReadRefused;
        }
        std::size_t rows = 0;
        for (const auto& chunk : batches.value()) {
            rows += static_cast<std::size_t>(chunk.size());
        }
        return rows;
    }

    std::size_t pg_class_rows_named(delete_refusal_spaces_t& space, const std::string& name) {
        return catalog_rows_with(space,
                                 catalog::well_known_oid::pg_class_table,
                                 catalog::pg_class_col::relname,
                                 std::string_view{name});
    }

    catalog::oid_t table_oid_named(delete_refusal_spaces_t& space, const std::string& name) {
        auto batches = catalog_chunks_with(space,
                                           catalog::well_known_oid::pg_class_table,
                                           catalog::pg_class_col::relname,
                                           std::string_view{name});
        REQUIRE_FALSE(batches.has_error());
        for (const auto& chunk : batches.value()) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (!chunk.is_null(0, i)) {
                    return static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                }
            }
        }
        return catalog::INVALID_OID;
    }

    // added_at_commit_id is column 10 of the LIVE row, dropped_at_commit_id column 11 of the TOMBSTONE row.
    struct column_rows_t {
        std::size_t live = 0;
        std::size_t tombstones = 0;
        std::int64_t added_at_commit_id = 0;
        std::int64_t dropped_at_commit_id = 0;
    };

    column_rows_t
    pg_attribute_rows_for(delete_refusal_spaces_t& space, catalog::oid_t table_oid, std::string_view attname) {
        auto batches = catalog_chunks_with(space,
                                           catalog::well_known_oid::pg_attribute_table,
                                           catalog::pg_attribute_col::attrelid,
                                           table_oid);
        REQUIRE_FALSE(batches.has_error());
        column_rows_t out{};
        for (const auto& chunk : batches.value()) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(2, i)) {
                    continue;
                }
                // Bind before comparing: get_value<string_view> points into the chunk's own buffer.
                const auto name_cell = chunk.get_value<std::string_view>(2, i);
                if (name_cell != attname) {
                    continue;
                }
                const bool dropped = !chunk.is_null(7, i) && chunk.get_value<bool>(7, i);
                if (dropped) {
                    ++out.tombstones;
                    if (chunk.column_count() > 11 && !chunk.is_null(11, i)) {
                        out.dropped_at_commit_id = chunk.get_value<std::int64_t>(11, i);
                    }
                    continue;
                }
                ++out.live;
                if (chunk.column_count() > 10 && !chunk.is_null(10, i)) {
                    out.added_at_commit_id = chunk.get_value<std::int64_t>(10, i);
                }
            }
        }
        return out;
    }

    void seed_plain_table(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& name) {
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE del;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE del." + name + " (a bigint, b bigint);")->is_success());
    }

    void seed_wide_table(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE del;")->is_success());
        std::string sql = "CREATE TABLE del." + kTableName + " (";
        for (int i = 0; i < kColumns; ++i) {
            if (i != 0) {
                sql += ", ";
            }
            sql += "c" + std::to_string(i) + " bigint";
        }
        sql += ");";
        REQUIRE(test_helpers::exec(dispatcher, sql)->is_success());
    }

} // namespace

TEST_CASE("integration::cpp::test_catalog_delete_refusal::drop_table_fails_when_the_catalog_delete_is_refused") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/drop_table");
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_"; // WAL segment files only; the .otbx files stay untouched

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    seed_wide_table(dispatcher);

    REQUIRE(pg_class_rows_named(space, kTableName) == 1);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());

    fault.plan.fail_writes_from = fault.plan.writes_seen + 1;
    const auto writes_before = fault.plan.writes_seen;

    auto cur = dispatcher->execute_sql(txn, "DROP TABLE del." + kTableName + ";");

    INFO("a DROP whose catalog scrub the journal refused must FAIL, not report success");
    CHECK(cur->is_error());
    CHECK(fault.plan.writes_seen > writes_before);

    fault.plan.fail_writes_from = 0;
    auto committed = dispatcher->execute_sql(txn, "COMMIT;");
    INFO("COMMIT after the refused DROP TABLE: "
         << (committed->is_error() ? std::string(committed->get_error().what.c_str()) : std::string("success")));

    const auto rows = pg_class_rows_named(space, kTableName);
    INFO("pg_class rows named '" << kTableName << "' after the refused DROP TABLE + COMMIT: " << rows);
    CHECK(rows == 1);
}

// The collapse guard: a change that made every DROP TABLE fail would satisfy the case above too.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::a_healthy_drop_table_scrubs_the_catalog") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/healthy");
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    seed_wide_table(dispatcher);
    CHECK(pg_class_rows_named(space, kTableName) == 1);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "DROP TABLE del." + kTableName + ";")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "COMMIT;")->is_success());

    CHECK(pg_class_rows_named(space, kTableName) == 0);
}

// delete_pg_catalog_rows_inner's scan must carry ctx->txn, or it can't see an unpublished row.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::a_column_added_and_dropped_in_one_transaction_is_dropped") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/add_drop_in_txn");
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "add_drop_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);
    REQUIRE(pg_attribute_rows_for(space, table_oid, "c").live == 0);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());

    auto dropped = dispatcher->execute_sql(txn, "ALTER TABLE del." + table + " DROP COLUMN c;");
    INFO("DROP COLUMN of a column ADDed in the same transaction: "
         << (dropped->is_error() ? std::string(dropped->get_error().what.c_str()) : std::string("success")));
    CHECK(dropped->is_success());

    auto committed = dispatcher->execute_sql(txn, "COMMIT;");
    INFO("COMMIT after the in-transaction ADD + DROP COLUMN: "
         << (committed->is_error() ? std::string(committed->get_error().what.c_str()) : std::string("success")));
    CHECK(committed->is_success());

    // The DROP's tombstone legitimately stays behind: attnum is never reused.
    const auto rows = pg_attribute_rows_for(space, table_oid, "c");
    INFO("pg_attribute rows for column 'c' after COMMIT: live=" << rows.live << " tombstones=" << rows.tombstones);
    CHECK(rows.live == 0);
}

// The collapse guard for the case above, split across two autocommit statements.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::a_column_added_and_dropped_in_autocommit_is_dropped") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/add_drop_autocommit");
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "add_drop_ac_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);

    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());
    REQUIRE(pg_attribute_rows_for(space, table_oid, "c").live == 1);

    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " DROP COLUMN c;")->is_success());
    CHECK(pg_attribute_rows_for(space, table_oid, "c").live == 0);
}

// update_pg_attribute_commit_id_field_inner must scan with ctx.txn, or use_inserted_version
// silently skips in-transaction rows (floor: components/table/test/test_update_merge.cpp).
TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_in_transaction_add_column_row_carries_its_commit_id") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/added_at_backfill");
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "added_at_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "COMMIT;")->is_success());

    const auto rows = pg_attribute_rows_for(space, table_oid, "c");
    CHECK(rows.live == 1);
    CHECK(rows.added_at_commit_id != 0);
}

TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_autocommit_add_column_row_carries_its_commit_id") {
    const std::filesystem::path dir =
        integration_fixture_path("test_catalog_delete_refusal/added_at_backfill_autocommit");
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "added_at_auto_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);

    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());

    const auto rows = pg_attribute_rows_for(space, table_oid, "c");
    CHECK(rows.live == 1);
    CHECK(rows.added_at_commit_id != 0);

    // CREATE TABLE passes added_at_commit_id=0 on purpose ("always visible"); nothing backfills it.
    CHECK(pg_attribute_rows_for(space, table_oid, "a").added_at_commit_id == 0);
}

// One ALTER alone cannot reach merge_update_loop_internal's leg; a second patch does, hence two here.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::two_added_columns_each_carry_their_own_commit_id") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/added_at_backfill_twice");
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "added_at_twice_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);

    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " ADD COLUMN d bigint;")->is_success());

    const auto c = pg_attribute_rows_for(space, table_oid, "c");
    const auto d = pg_attribute_rows_for(space, table_oid, "d");
    CHECK(c.live == 1);
    CHECK(d.live == 1);
    CHECK(c.added_at_commit_id != 0);
    CHECK(d.added_at_commit_id != 0);
    CHECK(d.added_at_commit_id > c.added_at_commit_id);

    // A third ALTER runs the merge leg again: it used to come back with a DANGLING attname.
    // Floor: test_update_merge.cpp, a_merged_string_update_owns_its_bytes.
    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " ADD COLUMN e bigint;")->is_success());
    const auto e = pg_attribute_rows_for(space, table_oid, "e");
    CHECK(e.live == 1);
    CHECK(e.added_at_commit_id > d.added_at_commit_id);

    for (const char* col : {"a", "b", "c", "d", "e"}) {
        INFO("column " << col);
        REQUIRE(
            test_helpers::exec(dispatcher, std::string("SELECT ") + col + " FROM del." + table + ";")->is_success());
    }

    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO del." + table + " (a, b, c, d, e) VALUES (1, 2, 3, 4, 5);")
                ->is_success());
    auto cur = test_helpers::exec(dispatcher, "SELECT e FROM del." + table + ";");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
}

// The DROP's half of the same backfill (column 11 on the tombstone), which now also feeds
// manager_disk_t::max_persisted_commit_id_sync, the reopen frontier.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::a_dropped_columns_tombstone_carries_its_commit_id") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/dropped_at_backfill");
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "dropped_at_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);

    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());
    const auto added = pg_attribute_rows_for(space, table_oid, "c");
    REQUIRE(added.live == 1);

    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " DROP COLUMN c;")->is_success());
    const auto rows = pg_attribute_rows_for(space, table_oid, "c");
    CHECK(rows.live == 0);
    REQUIRE(rows.tombstones == 1);
    CHECK(rows.dropped_at_commit_id != 0);
    CHECK(rows.dropped_at_commit_id > added.added_at_commit_id);
}

// The stamp must survive a RESTART, not only the commit. ~base_otterbrix_t CHECKPOINTs on clean
// shutdown, so this pins the checkpointed leg, not the WAL-replay leg (that needs an UNCLEAN
// restart, a crash-injection case of its own).
TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_added_columns_commit_id_survives_a_restart") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/added_at_restart");
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    const std::string table = "added_at_restart_t";
    catalog::oid_t table_oid = catalog::INVALID_OID;
    std::int64_t added_at_before = 0;

    INFO("phase 1: ALTER ... ADD COLUMN; the scope exit checkpoints");
    {
        delete_refusal_spaces_t space(config);
        auto* dispatcher = space.dispatcher();
        seed_plain_table(dispatcher, table);

        table_oid = table_oid_named(space, table);
        REQUIRE(table_oid != catalog::INVALID_OID);

        REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());
        const auto rows = pg_attribute_rows_for(space, table_oid, "c");
        REQUIRE(rows.live == 1);
        REQUIRE(rows.added_at_commit_id != 0);
        added_at_before = rows.added_at_commit_id;
    }

    INFO("phase 2: reopen the same directory — the stamp has to come back off the disk");
    {
        delete_refusal_spaces_t space(config);
        auto* dispatcher = space.dispatcher();

        const auto rows = pg_attribute_rows_for(space, table_oid, "c");
        INFO("added_at_commit_id was " << added_at_before << " before the restart and " << rows.added_at_commit_id
                                       << " after");
        CHECK(rows.live == 1);
        CHECK(rows.added_at_commit_id == added_at_before);

        REQUIRE(test_helpers::exec(dispatcher, "SELECT c FROM del." + table + ";")->is_success());
        REQUIRE(
            test_helpers::exec(dispatcher, "INSERT INTO del." + table + " (a, b, c) VALUES (1, 2, 3);")->is_success());
        auto cur = test_helpers::exec(dispatcher, "SELECT c FROM del." + table + ";");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1);

        REQUIRE(test_helpers::exec(dispatcher, "CHECKPOINT;")->is_success());
    }

    INFO("phase 3: a second restart, so the row written after the first one is covered too");
    {
        delete_refusal_spaces_t space(config);
        auto* dispatcher = space.dispatcher();

        const auto rows = pg_attribute_rows_for(space, table_oid, "c");
        INFO("added_at_commit_id after the second restart = " << rows.added_at_commit_id);
        CHECK(rows.live == 1);
        CHECK(rows.added_at_commit_id == added_at_before);

        auto cur = test_helpers::exec(dispatcher, "SELECT c FROM del." + table + ";");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1);
    }
}

// The two callers of the singular delete_pg_catalog_rows that delete-then-append: their delete
// must carry the transaction like their read does, or the replacement lands on top of the row
// it should have replaced -- unnoticed, since that route (unique_future<void>) has no error channel.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_in_transaction_rename_leaves_one_attribute_row") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/rename_in_txn");
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "rename_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());
    auto renamed = dispatcher->execute_sql(txn, "ALTER TABLE del." + table + " RENAME COLUMN c TO d;");
    INFO("RENAME of a column ADDed in the same transaction: "
         << (renamed->is_error() ? std::string(renamed->get_error().what.c_str()) : std::string("success")));
    CHECK(renamed->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "COMMIT;")->is_success());

    const auto old_rows = pg_attribute_rows_for(space, table_oid, "c");
    const auto new_rows = pg_attribute_rows_for(space, table_oid, "d");
    INFO("pg_attribute after the in-transaction RENAME: live 'c'=" << old_rows.live << " live 'd'=" << new_rows.live);
    CHECK(old_rows.live == 0);
    CHECK(new_rows.live == 1);
}

TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_in_transaction_create_index_leaves_one_pg_index_row") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/create_index_in_txn");
    auto config = test_helpers::make_test_config(dir);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "indexed_t";
    seed_plain_table(dispatcher, table);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "CREATE INDEX one_row_idx ON del." + table + " (a);")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "COMMIT;")->is_success());

    const auto index_oid = table_oid_named(space, "one_row_idx");
    REQUIRE(index_oid != catalog::INVALID_OID);

    const auto rows =
        catalog_rows_with(space, catalog::well_known_oid::pg_index_table, catalog::pg_index_col::indexrelid, index_oid);
    INFO("pg_index rows for indexrelid " << static_cast<unsigned>(index_oid)
                                         << " after the in-transaction CREATE INDEX: " << rows);
    CHECK(rows == 1);
}
