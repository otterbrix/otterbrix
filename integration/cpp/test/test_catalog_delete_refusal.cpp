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

// A DDL statement must not report success over a catalog row it did not delete: refuse the WAL
// write behind the DROP's catalog scrub (via the DEV_MODE seam in services/wal/wal_page.hpp,
// same technique as test_wal_write_refusal.cpp) and check the row survives. Runs inside an
// explicit transaction so the statement's answer is decided by the refused PHYSICAL_DELETE
// itself, not by autocommit's own trailing commit record failing for an unrelated reason.
// Asserted on CONTENT after COMMIT (not ROLLBACK), so a scrub that silently no-oped couldn't pass.

using namespace components;

namespace {

    const std::string kTableName = "wide_refusal_t";
    // Eight bytes of row id per column, against a 4064-byte WAL page: the pg_attribute scrub of
    // this table cannot fit in one page, which is what makes its refusal land inside the
    // statement instead of in some later flush.
    constexpr int kColumns = 700;

    // Process-wide seam, so it is scoped by this object and narrowed to WAL segment files by
    // path. Same shape as the scope in integration/cpp/test/test_wal_write_refusal.cpp.
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

    // The engine plus the one thing test_spaces does not expose: the disk manager, so catalog
    // CONTENT can be read back directly rather than inferred from a status code.
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

    // Committed rows of `table_oid` whose column `key_col` equals `key`, read back through the
    // disk manager's own funnel. The manager pumps its own inbox on an internal loop thread, so
    // the send only has to be waited on. `snapshot_horizon = max` is "see every COMMITTED row":
    // a bare transaction_data{0, 0} carries horizon 0 and would hide every row whose insert id
    // is a commit id (i.e. everything written inside an explicit transaction).
    template<typename Key>
    core::result_wrapper_t<std::pmr::vector<components::vector::data_chunk_t>>
    catalog_chunks_with(delete_refusal_spaces_t& space, catalog::oid_t table_oid, std::uint64_t key_col, Key key) {
        auto* resource = space.disk()->resource();
        table::transaction_data td{0, 0};
        td.snapshot_horizon = std::numeric_limits<uint64_t>::max();
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

    // The table's pg_class row, addressed by the name the statement names.
    std::size_t pg_class_rows_named(delete_refusal_spaces_t& space, const std::string& name) {
        return catalog_rows_with(space,
                                 catalog::well_known_oid::pg_class_table,
                                 catalog::pg_class_col::relname,
                                 std::string_view{name});
    }

    // The relation's own oid, from the pg_class row that names it. INVALID_OID when the name
    // has no row — asserted by the callers, which all seed the table first.
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

    // What pg_attribute says about ONE column of ONE relation. `live` counts the rows that
    // still describe an existing column (attisdropped false); `tombstones` the ones that say
    // the column was dropped; `added_at_commit_id` is column 10 of the LIVE row, 0 when there
    // is none.
    struct column_rows_t {
        std::size_t live = 0;
        std::size_t tombstones = 0;
        std::int64_t added_at_commit_id = 0;
        // Column 11 of the TOMBSTONE row (0 when there is none). The DROP's half of the same
        // backfill the added_at cases below pin.
        std::int64_t dropped_at_commit_id = 0;
    };

    column_rows_t
    pg_attribute_rows_for(delete_refusal_spaces_t& space, catalog::oid_t table_oid, std::string_view attname) {
        // Keyed on attrelid (column 1): every attribute row of the relation, then matched by
        // name here — the same (attrelid, attname) identity operator_alter_column_drop_t uses.
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
                // Bind the cell before comparing: get_value<string_view> points into the
                // chunk's own buffer, which outlives the comparison.
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

    // A plain two-column table, seeded outside any explicit transaction so its own catalog
    // rows are direct writes and every case below starts from the same committed state.
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
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    wal_fault_scope_t fault;
    fault.faulty_marker = "wal_"; // WAL segment files only; the .otbx files stay untouched

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    seed_wide_table(dispatcher);

    // The row this case is about is there before it starts.
    REQUIRE(pg_class_rows_named(space, kTableName) == 1);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());

    // Arm only now: the DDL above had to reach the journal, so the refusal below can only be
    // about the DROP's own catalog scrub.
    fault.plan.fail_writes_from = fault.plan.writes_seen + 1;
    const auto writes_before = fault.plan.writes_seen;

    auto cur = dispatcher->execute_sql(txn, "DROP TABLE del." + kTableName + ";");

    INFO("a DROP whose catalog scrub the journal refused must FAIL, not report success");
    CHECK(cur->is_error());
    // The refusal really travelled through a refused write, i.e. the record did reach the device
    // inside this statement rather than sitting in the page buffer.
    CHECK(fault.plan.writes_seen > writes_before);

    // The commit itself must be honest, so the fault is gone before it runs. Its own answer is
    // not the subject here (a statement that refused may or may not leave the transaction
    // committable); the catalog CONTENT after it is.
    fault.plan.fail_writes_from = 0;
    auto committed = dispatcher->execute_sql(txn, "COMMIT;");
    INFO("COMMIT after the refused DROP TABLE: "
         << (committed->is_error() ? std::string(committed->get_error().what.c_str()) : std::string("success")));

    // THE POINT OF THE CASE, asserted on CONTENT. The scrub was refused, so the row it would
    // have removed on the way to succeeding is exactly where it was — and a COMMIT rather than a
    // ROLLBACK is what makes that a real assertion: a rollback would restore the row even in the
    // world where the delete silently went through.
    const auto rows = pg_class_rows_named(space, kTableName);
    INFO("pg_class rows named '" << kTableName << "' after the refused DROP TABLE + COMMIT: " << rows);
    CHECK(rows == 1);
}

// The collapse guard: with nothing injected the SAME statements must SUCCEED and the table's
// pg_class row must disappear exactly when the DROP does. Without it the case above could go
// green by collapse — any change that made every DROP TABLE fail would satisfy every assertion
// it makes.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::a_healthy_drop_table_scrubs_the_catalog") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/healthy");
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
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

// A column ADDed then DROPped in the same transaction must actually drop: the delete's scan
// (agent_disk_t::delete_pg_catalog_rows_inner) used to run via detail::inline_scan with NO
// transaction, so it could not see a pg_attribute row still carrying insert_id==transaction_id
// (unpublished until commit), unlike the read path which does carry ctx->txn.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::a_column_added_and_dropped_in_one_transaction_is_dropped") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/add_drop_in_txn");
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "add_drop_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);
    // The column is not there before the transaction opens — neither live nor as a tombstone.
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

    // CONTENT. The live row the ADD wrote has to be gone; the DROP's tombstone is what
    // legitimately stays behind (attnum is never reused, so the slot keeps describing itself).
    const auto rows = pg_attribute_rows_for(space, table_oid, "c");
    INFO("pg_attribute rows for column 'c' after COMMIT: live=" << rows.live << " tombstones=" << rows.tombstones);
    CHECK(rows.live == 0);
}

// The collapse guard for the case above: the SAME add-then-drop, split across two autocommit
// statements, must behave identically. Without it, a change that made DROP COLUMN delete
// nothing at all — or one that made ADD COLUMN write nothing — would satisfy every assertion
// the transactional case makes.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::a_column_added_and_dropped_in_autocommit_is_dropped") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/add_drop_autocommit");
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "add_drop_ac_t";
    seed_plain_table(dispatcher, table);

    const auto table_oid = table_oid_named(space, table);
    REQUIRE(table_oid != catalog::INVALID_OID);

    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " ADD COLUMN c bigint;")->is_success());
    // The ADD really wrote the row this case is about, so the DROP below has something to do.
    REQUIRE(pg_attribute_rows_for(space, table_oid, "c").live == 1);

    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " DROP COLUMN c;")->is_success());
    CHECK(pg_attribute_rows_for(space, table_oid, "c").live == 0);
}

// added_at_commit_id backfill (agent_disk_t::update_pg_attribute_commit_id_field_inner) used to
// scan with a default transaction_data{} (transaction_id 0), rejected by use_inserted_version for
// any insert_id >= TRANSACTION_ID_START — it silently skipped in-transaction ADD COLUMN rows,
// leaving the placeholder 0 ("visible before every snapshot"). Fixed by scanning with ctx.txn;
// that change reaches update_segment_t::merge_update_loop_internal, whose tail loop used to never
// terminate and overran a stack array (floor tests: components/table/test/test_update_merge.cpp).
TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_in_transaction_add_column_row_carries_its_commit_id") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/added_at_backfill");
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
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
    // The whole point of the column: 0 means "visible to every snapshot ever taken", which is
    // the one thing a column added by a transaction that has only just committed is not.
    CHECK(rows.added_at_commit_id != 0);
}

// The autocommit shape of the same thing. An ALTER outside BEGIN still runs the commit
// operator, still emits the marker, and still has to end with a real id on the row — this is
// the path the merge crash was first reproduced on, so it is pinned separately from the
// in-transaction one above.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_autocommit_add_column_row_carries_its_commit_id") {
    const std::filesystem::path dir =
        integration_fixture_path("test_catalog_delete_refusal/added_at_backfill_autocommit");
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
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

    // The columns the table was created with keep the placeholder: CREATE TABLE passes
    // added_at_commit_id=0 on purpose (ddl_metadata_builder's contract, "always visible"), and
    // nothing above backfills them. Stated so the change of meaning is not mistaken for reach.
    CHECK(pg_attribute_rows_for(space, table_oid, "a").added_at_commit_id == 0);
}

// The first patch of a pg_attribute column takes update_segment_t::update's else-leg (no merge);
// the second patch of the same vector at a higher row id takes merge_update_loop_internal — one
// ALTER alone cannot reach that leg, hence two ALTERs here.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::two_added_columns_each_carry_their_own_commit_id") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/added_at_backfill_twice");
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
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
    // Distinct commits, in order: the second ALTER's id is strictly the later one.
    CHECK(d.added_at_commit_id > c.added_at_commit_id);

    // A third ALTER so the merge leg runs again: the merged row used to come back with a
    // DANGLING attname (merge_update_loop_internal's phase 2 stored the update vector's bytes
    // uncopied, and that vector is a temporary), surfacing as "path 'd' was not found" / "path
    // 'a' is ambiguous". Floor proof: test_update_merge.cpp, a_merged_string_update_owns_its_bytes.
    REQUIRE(test_helpers::exec(dispatcher, "ALTER TABLE del." + table + " ADD COLUMN e bigint;")->is_success());
    const auto e = pg_attribute_rows_for(space, table_oid, "e");
    CHECK(e.live == 1);
    CHECK(e.added_at_commit_id > d.added_at_commit_id);

    for (const char* col : {"a", "b", "c", "d", "e"}) {
        INFO("column " << col);
        REQUIRE(
            test_helpers::exec(dispatcher, std::string("SELECT ") + col + " FROM del." + table + ";")->is_success());
    }

    // And the added columns are writable, which is the point of the whole rule.
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO del." + table + " (a, b, c, d, e) VALUES (1, 2, 3, 4, 5);")
                ->is_success());
    auto cur = test_helpers::exec(dispatcher, "SELECT e FROM del." + table + ";");
    REQUIRE(cur->is_success());
    REQUIRE(cur->size() == 1);
}

// The DROP's half of the same backfill: kind_t::dropped_at, column 11, on the tombstone the
// DROP leaves behind. It rode the same blind scan and kept the same permanent 0, which read as
// "never dropped" -- and went unnoticed because attisdropped is checked first by every consumer
// and rejects the tombstone anyway. Now that it carries the real commit id it also feeds
// manager_disk_t::max_persisted_commit_id_sync, the reopen frontier, which until now could only
// ever return 0 from the pg_attribute half.
TEST_CASE("integration::cpp::test_catalog_delete_refusal::a_dropped_columns_tombstone_carries_its_commit_id") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/dropped_at_backfill");
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
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
    // The DROP commits strictly after the ADD, and the tombstone has to say so.
    CHECK(rows.dropped_at_commit_id > added.added_at_commit_id);
}

// ===========================================================================
// THE STAMP HAS TO BE THERE AFTER A RESTART, NOT ONLY AFTER THE COMMIT.
// EVERYTHING past `if (row_ids.empty()) co_return;` in
// agent_disk_t::update_pg_attribute_commit_id_field_inner (services/disk/agent_disk.cpp) is
// unreachable while the scan above that line cannot see the row it was asked to patch --
// including both legs that make the patch outlive the session: the write_physical_update send
// (the WAL record a replay would need) and the direct_update_sync at the bottom (the storage
// write a CHECKPOINT folds into the .otbx). The cases above prove the stamp is readable in the
// SAME session, which is exactly what those two legs are NOT needed for: the checkpointer walks
// data segments and never flushes update_segment_t's updates_, so an unjournalled,
// uncheckpointed patch is session-local and a same-session assertion passes either way.
// So this case restarts the engine and reads pg_attribute back. The id must be the SAME id -- a
// different one would mean the reopen re-derived a stamp rather than restoring one.
// WHICH LEG THIS ACTUALLY PROVES, since the two are not interchangeable: ~base_otterbrix_t
// CHECKPOINTs every disk table on a clean shutdown, so closing the phase-1 scope folds the
// direct_update_sync patch into the .otbx before phase 2 opens it. What is pinned is therefore
// the direct_update_sync leg plus that fold. Measured, not assumed: with the write_physical_update
// send stubbed out this case still passes, and with the ctx.txn scan reverted to
// transaction_data{} it fails at the phase-1 REQUIRE with `0 != 0`. Isolating the journal leg
// needs an UNCLEAN restart -- no destructor, no shutdown checkpoint, replay as the only carrier
// -- which is a crash-injection case of its own.
// It is also the other half of test_persistence.cpp's
// reopen_keeps_committed_deletes_invisible: added_at_commit_id feeds
// manager_disk_t::max_persisted_commit_id_sync, the reopen commit-clock frontier, which while
// the stamp was a permanent 0 could only ever contribute 0.
// ===========================================================================
TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_added_columns_commit_id_survives_a_restart") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/added_at_restart");
    // Cleared once, here: every scope below must open the SAME directory.
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
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

        // The column is still USABLE, which is what the stamp is FOR: the reopened commit clock
        // is seeded from this very value, and a clock that started below it would make
        // resolve_table judge the column "added after my snapshot".
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

// ===========================================================================
// THE TWO CALLERS OF THE SINGULAR delete_pg_catalog_rows THAT DELETE-THEN-APPEND.
// Both of these read a row, delete it, and append a replacement carrying the same identity —
// operator_alter_column_rename_t (pg_attribute, new attname) and
// operator_create_index_backfill_t (pg_index, indisvalid=true). Their read carries the
// transaction, and their delete must carry it too: a delete blind to the transaction cannot see
// the row it is replacing inside a BEGIN, so nothing is removed and the replacement lands ON
// TOP. One identity, two live rows, and a success reported over it — the singular route is
// unique_future<void>, so neither operator can be told otherwise.
// These two cases are why the visibility fix is the fix and not a workaround: the singular route
// still has no error channel (see the debt note in services/disk/manager_disk_ddl.cpp), so the
// delete looking at the same catalog as the read is the only thing keeping it from silently
// missing.
// ===========================================================================
TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_in_transaction_rename_leaves_one_attribute_row") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/rename_in_txn");
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
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

    // The old name must be gone and the new one must be there ONCE. The old-name count is the
    // half that was broken: the delete missed and the append landed anyway.
    const auto old_rows = pg_attribute_rows_for(space, table_oid, "c");
    const auto new_rows = pg_attribute_rows_for(space, table_oid, "d");
    INFO("pg_attribute after the in-transaction RENAME: live 'c'=" << old_rows.live << " live 'd'=" << new_rows.live);
    CHECK(old_rows.live == 0);
    CHECK(new_rows.live == 1);
}

TEST_CASE("integration::cpp::test_catalog_delete_refusal::an_in_transaction_create_index_leaves_one_pg_index_row") {
    const std::filesystem::path dir = integration_fixture_path("test_catalog_delete_refusal/create_index_in_txn");
    auto config = test_helpers::make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;

    delete_refusal_spaces_t space(config);
    auto* dispatcher = space.dispatcher();
    const std::string table = "indexed_t";
    seed_plain_table(dispatcher, table);

    auto txn = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(txn, "BEGIN;")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "CREATE INDEX one_row_idx ON del." + table + " (a);")->is_success());
    REQUIRE(dispatcher->execute_sql(txn, "COMMIT;")->is_success());

    // The index relation's own oid, i.e. the indexrelid every pg_index row for it carries.
    const auto index_oid = table_oid_named(space, "one_row_idx");
    REQUIRE(index_oid != catalog::INVALID_OID);

    // operator_create_index_backfill_t deletes the indisvalid=false row the metadata operator
    // wrote and appends the indisvalid=true one. Exactly ONE must survive.
    const auto rows =
        catalog_rows_with(space, catalog::well_known_oid::pg_index_table, catalog::pg_index_col::indexrelid, index_oid);
    INFO("pg_index rows for indexrelid " << static_cast<unsigned>(index_oid)
                                         << " after the in-transaction CREATE INDEX: " << rows);
    CHECK(rows == 1);
}
