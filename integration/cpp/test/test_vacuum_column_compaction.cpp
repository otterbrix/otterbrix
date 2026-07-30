// ============================================================================
// VACUUM physically compacts a column that ALTER TABLE ... DROP COLUMN removed
// from the catalog of an ordinary (relkind='r') relation.
//
// DROP COLUMN is a CATALOG operation: pg_attribute loses the column immediately
// (tombstone + dropped_at_commit_id) while the storage's PHYSICAL column list only
// ever grows. The scan survives that by projecting on each column's catalog
// identity instead of its position, so values stay put — but a relation whose
// physical layout no longer matches its logical one is DISPLACED, and for a
// displaced relation plan generation refuses column pruning, filter pushdown,
// aggregate pushdown and index probes, because all four address a storage column
// by the logical ordinal the validator resolved.
//
// Nothing ever removed the dead physical column, so that refusal was permanent:
// one dropped column cost the relation those four optimisations forever. Removing
// it is compaction, and compaction is VACUUM's job everywhere else in this engine.
//
// WHAT IS OBSERVED, and why these two observables and not a proxy:
//
//   * services::disk::storage_column_attoids_sync — the PHYSICAL column list, read
//     straight off the agent slice through the test-only disk-manager window. This
//     is the fact under test; everything else is a consequence of it.
//
//   * services::disk::pushdown_reply_rows() — a DEV_MODE process-global counter
//     bumped by the agent-side reduce with the number of rows its FINALIZED partial
//     ships back. It is non-zero if and only if the aggregate was pushed to the
//     owning agent, and aggregate pushdown is gated on exactly
//     scan_identity_projection_t::displaced (create_plan_aggregate.cpp). So the
//     counter reads `displaced()` end-to-end, through the real planner, without the
//     test naming an internal predicate.
//
// THREE columns minimum, MIDDLE one dropped: on two columns "projected correctly"
// and "swapped twice" are the same observation, so a two-column table cannot tell a
// working compaction from one that shifted every value one slot left. Each column
// here carries a value range that names it (a=1x, b=2x, c=3x, d=4x), so a shift is
// visible in the VALUE and not only in the column count.
// ============================================================================

#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/tests/generaty.hpp>
#include <components/tests/temp_dir.hpp>
#include <services/disk/agent_disk.hpp>
#include <services/disk/manager_disk.hpp>

using namespace test_helpers;

namespace {

    // The only user table in these fixtures, so the live-table scan identifies it
    // without the test having to parse pg_class itself.
    components::catalog::oid_t only_user_table(test_spaces& space) {
        auto live = space.disk_manager()->scan_live_table_oids_sync();
        REQUIRE(live.size() == 1);
        return live[0];
    }

    // Physical storage width, i.e. how many columns the storage still holds. Read
    // while the engine is quiescent (no statement in flight), the contract of every
    // *_sync window onto the agent slices.
    std::size_t physical_column_count(test_spaces& space, components::catalog::oid_t table_oid) {
        return space.disk_manager()->storage_column_attoids_sync(table_oid).size();
    }

    // Runs one scalar aggregate and reports how many rows the agent-side reduce shipped
    // back. 0 means the aggregate was NOT pushed down — the coordinator computed it over
    // a full scan instead, which is what a displaced relation forces.
    uint64_t pushed_reply_rows_for(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        services::disk::reset_pushdown_reply_rows();
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, sql);
        INFO("aggregate error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        return services::disk::pushdown_reply_rows();
    }

    void run_sql(otterbrix::wrapper_dispatcher_t* dispatcher, const char* sql) {
        auto session = otterbrix::session_id_t();
        INFO(sql);
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    // One pg_attribute row, as the MVCC-versioning columns describe it.
    // pg_attribute layout: 0=attoid 1=attrelid 2=attname ... 7=attisdropped ...
    // 10=added_at_commit_id 11=dropped_at_commit_id.
    struct attribute_row_t {
        components::catalog::oid_t attoid{components::catalog::INVALID_OID};
        std::string attname;
        bool attisdropped{false};
        std::int64_t added_at{0};
        std::int64_t dropped_at{0};
    };

    // Every pg_attribute row belonging to `relid`, read straight off the agent slice while
    // the engine is quiescent. There is no SQL surface for pg_attribute's commit-id
    // columns, and they are precisely what the compaction's MVCC gate reads.
    std::vector<attribute_row_t> attribute_rows(test_spaces& space, components::catalog::oid_t relid) {
        core::pmr::otterbrix_resource resource;
        auto batches = space.disk_manager()->scan_storage_for_rebuild_sync(
            components::catalog::well_known_oid::pg_attribute_table,
            &resource);
        std::vector<attribute_row_t> rows;
        for (const auto& chunk : batches) {
            if (chunk.column_count() < 12) {
                continue;
            }
            for (std::uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(1, i) ||
                    static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i)) != relid) {
                    continue;
                }
                attribute_row_t row;
                row.attoid = chunk.is_null(0, i)
                                 ? components::catalog::INVALID_OID
                                 : static_cast<components::catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                if (!chunk.is_null(2, i)) {
                    // Bound to a named local: the view points into this chunk's string buffer.
                    const auto name = chunk.get_value<std::string_view>(2, i);
                    row.attname.assign(name.data(), name.size());
                }
                row.attisdropped = !chunk.is_null(7, i) && chunk.get_value<bool>(7, i);
                row.added_at = chunk.is_null(10, i) ? 0 : chunk.get_value<std::int64_t>(10, i);
                row.dropped_at = chunk.is_null(11, i) ? 0 : chunk.get_value<std::int64_t>(11, i);
                rows.push_back(std::move(row));
            }
        }
        return rows;
    }

} // namespace

// ---------------------------------------------------------------------------
// (1) The core claim: after VACUUM the dead physical column is GONE, the relation
//     stops being displaced, and aggregate pushdown comes back on its own.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::vacuum_column_compaction::drops_the_dead_physical_column") {
    auto config = test_create_config(test_temp_path("test_vacuum_column_compaction/drops"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    run_sql(dispatcher, "CREATE DATABASE VacDb;");
    run_sql(dispatcher, "CREATE TABLE VacDb.t (a BIGINT, b BIGINT, c BIGINT, d BIGINT);");
    run_sql(dispatcher,
            "INSERT INTO VacDb.t (a, b, c, d) VALUES (10, 20, 30, 40), (11, 21, 31, 41), (12, 22, 32, 42);");

    const auto table_oid = only_user_table(space);
    REQUIRE(physical_column_count(space, table_oid) == 4);

    INFO("before the drop the relation is not displaced, so the aggregate IS pushed down");
    REQUIRE(pushed_reply_rows_for(dispatcher, "SELECT SUM(c) FROM VacDb.t;") > 0);

    run_sql(dispatcher, "ALTER TABLE VacDb.t DROP COLUMN b;");

    INFO("the drop is a catalog-only event: the storage still carries four columns");
    REQUIRE(physical_column_count(space, table_oid) == 4);

    INFO("and the relation is now displaced, so the aggregate is NOT pushed down");
    REQUIRE(pushed_reply_rows_for(dispatcher, "SELECT SUM(c) FROM VacDb.t;") == 0);

    run_sql(dispatcher, "VACUUM;");

    INFO("VACUUM reclaims the dead column: the storage is three wide");
    REQUIRE(physical_column_count(space, table_oid) == 3);

    INFO("and the four optimisations come back — the aggregate is pushed down again");
    REQUIRE(pushed_reply_rows_for(dispatcher, "SELECT SUM(c) FROM VacDb.t;") > 0);

    INFO("VALUES DID NOT MOVE: every surviving column still answers with its own");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a, c, d FROM VacDb.t ORDER BY a;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 3);
        REQUIRE(cur->column_count() == 3);
        // cursor_t::value is (col_idx, row_idx).
        REQUIRE(cur->value(0, 0).value<int64_t>() == 10);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 30);
        REQUIRE(cur->value(2, 0).value<int64_t>() == 40);
        REQUIRE(cur->value(0, 1).value<int64_t>() == 11);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 31);
        REQUIRE(cur->value(2, 1).value<int64_t>() == 41);
        REQUIRE(cur->value(0, 2).value<int64_t>() == 12);
        REQUIRE(cur->value(1, 2).value<int64_t>() == 32);
        REQUIRE(cur->value(2, 2).value<int64_t>() == 42);
    }
    INFO("the dropped name still does not resolve");
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "SELECT b FROM VacDb.t;")->is_error());
    }
    INFO("SELECT * fans out over the compacted schema");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM VacDb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 3);
        REQUIRE(test_has_column(*cur, "a"));
        REQUIRE(test_has_column(*cur, "c"));
        REQUIRE(test_has_column(*cur, "d"));
        REQUIRE_FALSE(test_has_column(*cur, "b"));
    }
    INFO("aggregate values are the columns' own, whichever path computed them");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT SUM(c), SUM(d), COUNT(*) FROM VacDb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->value(0, 0).value<int64_t>() == 93);  // 30+31+32; b would give 63
        REQUIRE(cur->value(1, 0).value<int64_t>() == 123); // 40+41+42
        REQUIRE(cur->value(2, 0).value<uint64_t>() == 3);
    }
    INFO("a pushed-down filter now addresses the right storage slot");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a, d FROM VacDb.t WHERE c = 31;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 11);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 41);
    }
}

// ---------------------------------------------------------------------------
// (1b) The value the compaction's MVCC gate is made of: pg_attribute's commit-id
//      columns have to actually carry a commit id.
//
//      DROP COLUMN writes its tombstone with dropped_at_commit_id = 0 and leaves a
//      backfill marker, because the commit id does not exist until COMMIT. The
//      backfill then patches the row — and it used to patch it under the txn-less
//      "see all committed" view, which cannot see any row the committing transaction
//      has just written. On an ADD that found nothing; on a DROP it found the DELETED
//      original (still visible to a see-all view, and written first) and stamped that,
//      leaving the tombstone at 0 permanently.
//
//      Nothing noticed, because nothing read the fields: resolve_table hides a
//      tombstone on attisdropped before it ever looks at the timestamp, and an
//      added_at of 0 reads as "added before every snapshot" — which is what a visible
//      column already implied. The compaction is the first reader that needs the value
//      to be RIGHT, since "no snapshot can still resolve this column" is exactly
//      `dropped_at != 0 && dropped_at <= lowest_active_start_time`. A permanently-0
//      field makes that test permanently false and the compaction permanently dead.
//
//      Only the DROP half is fixed. The ADD half is characterized below and handed
//      back rather than fixed: making that patch land is a one-line change and it
//      makes an ALTER-added column unresolvable to the next statement, for a reason
//      that is not the added_at value itself. See the comment on `read_own_writes` in
//      agent_disk_t::update_pg_attribute_commit_id_field_inner.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::vacuum_column_compaction::pg_attribute_commit_ids_are_stamped") {
    auto config = test_create_config(test_temp_path("test_vacuum_column_compaction/commit_ids"));
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;

    INFO("phase 1: one ADD and one DROP, each leaving a commit-id backfill marker");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        run_sql(dispatcher, "CREATE DATABASE VacDb;");
        run_sql(dispatcher, "CREATE TABLE VacDb.t (a BIGINT, b BIGINT, c BIGINT) WITH (storage = 'disk');");
        run_sql(dispatcher, "ALTER TABLE VacDb.t ADD COLUMN d BIGINT;");
        run_sql(dispatcher, "ALTER TABLE VacDb.t DROP COLUMN b;");
    }

    // Read in a freshly reopened engine BEFORE any statement runs: the *_sync windows
    // read the agent slices from this thread, which is only safe while the engine is
    // quiescent. Reopening also proves the stamps are durable and not just in-memory.
    test_spaces space(config);
    const auto table_oid = only_user_table(space);
    const auto rows = attribute_rows(space, table_oid);

    INFO("the DROP tombstone carries the commit id of the transaction that dropped it");
    {
        std::size_t tombstones = 0;
        for (const auto& row : rows) {
            if (!row.attisdropped) {
                continue;
            }
            ++tombstones;
            REQUIRE(row.attname == "b");
            REQUIRE(row.dropped_at > 0); // pre-fix: 0, and the compaction never fires
        }
        REQUIRE(tombstones == 1);
    }
    INFO("KNOWN GAP, characterized: the ADDed column's added_at_commit_id stays 0");
    {
        // The ADD half of the same backfill is deliberately still reading the see-all
        // view, so it still finds no row and patches nothing. This assertion is written
        // the way it is so that CLOSING the gap fails HERE — loudly, in the test that
        // explains it — instead of somewhere downstream. Flip it to `> 0` together with
        // the fix; the reason it is not flipped today is on `read_own_writes` in
        // agent_disk_t::update_pg_attribute_commit_id_field_inner.
        bool found = false;
        for (const auto& row : rows) {
            if (row.attisdropped || row.attname != "d") {
                continue;
            }
            found = true;
            CHECK(row.added_at == 0); // flips to `> 0` when the ADD half is fixed
            REQUIRE(row.dropped_at == 0);
        }
        REQUIRE(found);
    }
    INFO("a column that was neither added nor dropped by ALTER keeps dropped_at at 0");
    {
        for (const auto& row : rows) {
            if (row.attname == "a" || row.attname == "c") {
                REQUIRE_FALSE(row.attisdropped);
                REQUIRE(row.dropped_at == 0);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// (2) Writes keep landing in the right columns after the physical list narrowed.
//     A compaction that renumbered the storage without telling the append matcher
//     would show up here and nowhere else.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::vacuum_column_compaction::writes_after_compaction_land_correctly") {
    auto config = test_create_config(test_temp_path("test_vacuum_column_compaction/writes"));
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    run_sql(dispatcher, "CREATE DATABASE VacDb;");
    run_sql(dispatcher, "CREATE TABLE VacDb.t (a BIGINT, b BIGINT, c BIGINT);");
    run_sql(dispatcher, "INSERT INTO VacDb.t (a, b, c) VALUES (10, 20, 30), (11, 21, 31);");
    run_sql(dispatcher, "ALTER TABLE VacDb.t DROP COLUMN b;");
    run_sql(dispatcher, "VACUUM;");

    const auto table_oid = only_user_table(space);
    REQUIRE(physical_column_count(space, table_oid) == 2);

    run_sql(dispatcher, "INSERT INTO VacDb.t (a, c) VALUES (12, 32);");
    run_sql(dispatcher, "UPDATE VacDb.t SET c = 99 WHERE a = 10;");
    run_sql(dispatcher, "DELETE FROM VacDb.t WHERE a = 11;");

    INFO("the storage did not re-widen behind the writes");
    REQUIRE(physical_column_count(space, table_oid) == 2);
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a, c FROM VacDb.t ORDER BY a;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 10);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 99);
        REQUIRE(cur->value(0, 1).value<int64_t>() == 12);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 32);
    }
    INFO("a second VACUUM over an already-compacted relation is a no-op, not a second drop");
    run_sql(dispatcher, "VACUUM;");
    REQUIRE(physical_column_count(space, table_oid) == 2);
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a, c FROM VacDb.t ORDER BY a;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 99);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 32);
    }
}

// ---------------------------------------------------------------------------
// (3) It survives a restart. A checkpointed .otbx holds its OWN physical column
//     list (TABLE_META_MAGIC / TABLE_META_VERSION, one attoid per column), so a
//     compaction that does not reach the file is undone by the next reopen. This
//     pins which step persists it: VACUUM narrows the live table, CHECKPOINT writes
//     that width to the file — the same division of labour the row-level compaction
//     in vacuum_inner already lives under.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::vacuum_column_compaction::survives_restart_after_checkpoint") {
    auto config = test_create_config(test_temp_path("test_vacuum_column_compaction/restart"));
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;

    components::catalog::oid_t table_oid = components::catalog::INVALID_OID;

    INFO("phase 1: three columns, drop the middle one, VACUUM, then CHECKPOINT");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        run_sql(dispatcher, "CREATE DATABASE VacDb;");
        run_sql(dispatcher, "CREATE TABLE VacDb.t (a BIGINT, b BIGINT, c BIGINT) WITH (storage = 'disk');");
        run_sql(dispatcher, "INSERT INTO VacDb.t (a, b, c) VALUES (10, 20, 30), (11, 21, 31);");
        run_sql(dispatcher, "ALTER TABLE VacDb.t DROP COLUMN b;");

        table_oid = only_user_table(space);
        REQUIRE(physical_column_count(space, table_oid) == 3);

        run_sql(dispatcher, "VACUUM;");
        REQUIRE(physical_column_count(space, table_oid) == 2);
        run_sql(dispatcher, "CHECKPOINT;");
    }

    INFO("phase 2: reopen — the .otbx agrees, and so does everything above it");
    {
        test_spaces space(config);
        // Read before issuing any statement, while the engine is quiescent.
        REQUIRE(space.disk_manager()->has_storage(table_oid));
        REQUIRE(physical_column_count(space, table_oid) == 2);

        auto* dispatcher = space.dispatcher();
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT a, c FROM VacDb.t ORDER BY a;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 2);
            REQUIRE(cur->column_count() == 2);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 10);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 30);
            REQUIRE(cur->value(0, 1).value<int64_t>() == 11);
            REQUIRE(cur->value(1, 1).value<int64_t>() == 31);
        }
        INFO("the relation reopens UNdisplaced, so the aggregate is pushed down");
        REQUIRE(pushed_reply_rows_for(dispatcher, "SELECT SUM(c) FROM VacDb.t;") > 0);
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "SELECT b FROM VacDb.t;")->is_error());
        }
    }
}

// ---------------------------------------------------------------------------
// (4) The reconciliation the .otbx version record exists for: a VACUUM that is
//     NOT followed by a checkpoint leaves the file at the OLD width, and the
//     reopen must come back to the displaced-but-correct state rather than to a
//     storage and a catalog that disagree about what the relation is. Losing an
//     optimisation across a crash is the acceptable half of that trade; reading a
//     neighbour's values is not.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::vacuum_column_compaction::uncheckpointed_compaction_reopens_correct") {
    auto config = test_create_config(test_temp_path("test_vacuum_column_compaction/no_checkpoint"));
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;

    components::catalog::oid_t table_oid = components::catalog::INVALID_OID;

    INFO("phase 1: drop + VACUUM, and deliberately no CHECKPOINT");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        run_sql(dispatcher, "CREATE DATABASE VacDb;");
        run_sql(dispatcher, "CREATE TABLE VacDb.t (a BIGINT, b BIGINT, c BIGINT) WITH (storage = 'disk');");
        run_sql(dispatcher, "INSERT INTO VacDb.t (a, b, c) VALUES (10, 20, 30), (11, 21, 31);");
        run_sql(dispatcher, "ALTER TABLE VacDb.t DROP COLUMN b;");
        run_sql(dispatcher, "VACUUM;");
        table_oid = only_user_table(space);
        REQUIRE(physical_column_count(space, table_oid) == 2);
        run_sql(dispatcher, "INSERT INTO VacDb.t (a, c) VALUES (12, 32);");
    }

    INFO("phase 2: reopen — whatever width the file came back at, the VALUES are the columns' own");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT a, c FROM VacDb.t ORDER BY a;");
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 3);
            REQUIRE(cur->column_count() == 2);
            REQUIRE(cur->value(0, 0).value<int64_t>() == 10);
            REQUIRE(cur->value(1, 0).value<int64_t>() == 30);
            REQUIRE(cur->value(0, 1).value<int64_t>() == 11);
            REQUIRE(cur->value(1, 1).value<int64_t>() == 31);
            REQUIRE(cur->value(0, 2).value<int64_t>() == 12);
            REQUIRE(cur->value(1, 2).value<int64_t>() == 32);
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "SELECT b FROM VacDb.t;")->is_error());
        }
        INFO("and a VACUUM in the reopened process compacts whatever the file brought back");
        run_sql(dispatcher, "VACUUM;");
        REQUIRE(physical_column_count(space, table_oid) == 2);
    }
}

// ---------------------------------------------------------------------------
// (5) TWO dropped columns, dropped in two separate statements, with a survivor on
//     each side of each hole. This is the shape that pins two things at once:
//
//     * every COMMIT of a DROP patches one pg_attribute row in place, so the second
//       DROP is the first write with an existing update chain to merge into — the
//       drain loop that used to raise its own bound and run off the end of the id
//       array (pinned at the layer that owns it in
//       components::table::column_compaction::two_in_place_updates_merge_without_running_off);
//
//     * one VACUUM has to reclaim BOTH dead columns, not just the first, and the two
//       survivors sit at storage slots 0 and 3 — so a compaction that removed one hole
//       and renumbered around the other would move d's values onto c's slot.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::vacuum_column_compaction::two_separate_drops_are_both_reclaimed") {
    auto config = test_create_config(test_temp_path("test_vacuum_column_compaction/two_drops"));
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    run_sql(dispatcher, "CREATE DATABASE VacDb;");
    run_sql(dispatcher, "CREATE TABLE VacDb.t (a BIGINT, b BIGINT, c BIGINT, d BIGINT) WITH (storage = 'disk');");
    run_sql(dispatcher, "INSERT INTO VacDb.t (a, b, c, d) VALUES (10, 20, 30, 40), (11, 21, 31, 41);");
    run_sql(dispatcher, "ALTER TABLE VacDb.t DROP COLUMN b;");
    run_sql(dispatcher, "ALTER TABLE VacDb.t DROP COLUMN c;");

    const auto table_oid = only_user_table(space);
    REQUIRE(physical_column_count(space, table_oid) == 4);

    run_sql(dispatcher, "VACUUM;");
    INFO("one VACUUM reclaims BOTH dead columns");
    REQUIRE(physical_column_count(space, table_oid) == 2);

    INFO("and the two survivors, which were slots 0 and 3, still answer with their own");
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT a, d FROM VacDb.t ORDER BY a;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
        REQUIRE(cur->column_count() == 2);
        REQUIRE(cur->value(0, 0).value<int64_t>() == 10);
        REQUIRE(cur->value(1, 0).value<int64_t>() == 40); // b would give 20, c would give 30
        REQUIRE(cur->value(0, 1).value<int64_t>() == 11);
        REQUIRE(cur->value(1, 1).value<int64_t>() == 41);
    }
    INFO("the relation is undisplaced again, so the aggregate is pushed down");
    REQUIRE(pushed_reply_rows_for(dispatcher, "SELECT SUM(d) FROM VacDb.t;") > 0);
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "SELECT b FROM VacDb.t;")->is_error());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "SELECT c FROM VacDb.t;")->is_error());
    }
}
