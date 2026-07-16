// ===========================================================================
// RESTART CONSISTENCY -- the differential battery.
//
// Each group asserts one clause of the RC invariant: what the database did
// before the restart, it must still do after the restart. See restart_probe.hpp
// for the machinery and for why the probes are shaped the way they are.
//
// Two rules every group here obeys, and why:
//
//   * A probe that WRITES must CLEAN UP. The post-restart phase runs on a data
//     directory that already contains the pre-restart phase's writes. Without a
//     cleanup, every unkeyed read (a COUNT, a full scan) sees one extra row and
//     "diverges" for a reason the harness itself created.
//
//   * A probe may freely SELECT its own key. The phase key is folded back out of
//     the observation before the two phases are compared, so a row keyed $K+1
//     renders identically in both.
//
// Groups are independently selectable, so a group that bricks the database
// cannot hide a divergence elsewhere:
//
//   RC_ONLY=defaults,index_content  ./test_otterbrix "[restart]"
//   RC_DATA_ROOT=/var/tmp/rc        (default: /tmp/otterbrix/restart_consistency)
// ===========================================================================

#include "restart_probe.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/log/log.hpp>
#include <services/wal/record.hpp>
#include <services/wal/wal_reader.hpp>

using namespace restart_rc;

namespace {

    bool group_enabled(const std::string& name) {
        const char* only = std::getenv("RC_ONLY");
        if (!only) {
            return true;
        }
        const std::string all = std::string(",") + only + ",";
        return all.find("," + name + ",") != std::string::npos;
    }

    // Run one group under every storage mode and every restart flavor. Each
    // (mode, flavor) is CHECKed rather than REQUIREd, so one broken mode still
    // reports the others.
    void check_restart_consistency(const group_t& group) {
        if (!group_enabled(group.name)) {
            SUCCEED("group " << group.name << " disabled by RC_ONLY");
            return;
        }
        const storage_mode modes[] = {storage_mode::in_memory, storage_mode::disk, storage_mode::disk_checkpoint};
        const restart_flavor flavors[] = {restart_flavor::clean, restart_flavor::crash};

        for (auto mode : modes) {
            for (auto flavor : flavors) {
                const auto report = check_group(group, mode, flavor);
                INFO("group=" << group.name << " mode=" << to_string(mode) << " restart=" << to_string(flavor) << "\n"
                              << report.describe());
                CHECK(report.consistent());
            }
        }
    }

    const std::string create_db = "CREATE DATABASE rcdb;";

} // namespace

// ---------------------------------------------------------------------------
// Column DEFAULTs. The archetypal write-only divergence: a row written BEFORE
// the restart keeps its defaults (the value is baked into the WAL payload), so
// only an INSERT issued AFTER the restart can see that the schema lost them.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::restart_consistency::defaults", "[restart]") {
    group_t g;
    g.name = "defaults";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (a bigint, b int DEFAULT 5, c bigint DEFAULT 7, s string DEFAULT 'dflt')$S;",
               "INSERT INTO rcdb.t (a) VALUES (1), (2);"};
    g.probes = {
        {"existing_rows", {}, {{"all", "SELECT * FROM rcdb.t;"}}, {}},
        // The whole point: a NEW row, inserted in THIS phase, must get the same
        // defaults it would have got before the restart.
        //
        // The `values` oracle also pins a bug the differential cannot see: `b int
        // DEFAULT 5` is filled with an indeterminate non-NULL value, because the DDL
        // never casts the literal (a BIGINT 5) to the column type and vector_t::set_value
        // silently early-returns on the type mismatch -- before setting validity. That is
        // wrong BEFORE any restart, so `after == before` would happily certify it.
        {"new_row",
         {"INSERT INTO rcdb.t (a) VALUES ($K);"},
         {{"values", "SELECT b, c, s FROM rcdb.t WHERE a = $K;", "OK|rows=1|cols=3|[I32:5,I64:7,STR:\"dflt\"]"},
          {"b_not_null", "SELECT a FROM rcdb.t WHERE a = $K AND b IS NULL;", "OK|rows=0|cols=1"},
          {"c_is_7", "SELECT a FROM rcdb.t WHERE a = $K AND c = 7;", "OK|rows=1|cols=1|[I64:$K+0]"},
          {"s_is_dflt", "SELECT a FROM rcdb.t WHERE a = $K AND s = 'dflt';", "OK|rows=1|cols=1|[I64:$K+0]"}},
         {"DELETE FROM rcdb.t WHERE a = $K;"}},
        // An explicit value must still override the default after a restart.
        // Keyed $K+1 so it can never be confused with the new_row probe's row.
        {"explicit_overrides_default",
         {"INSERT INTO rcdb.t (a, c) VALUES ($K + 1, 42);"},
         {{"value", "SELECT c FROM rcdb.t WHERE a = $K + 1;"}},
         {"DELETE FROM rcdb.t WHERE a = $K + 1;"}},
    };
    check_restart_consistency(g);
}

// A DEFAULT on a TIMESTAMP column: the catalog's default codec cannot encode
// temporal types at all, so this loses the default even where a bigint survives.
TEST_CASE("integration::cpp::restart_consistency::timestamp_default", "[restart]") {
    group_t g;
    g.name = "timestamp_default";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (id bigint, created timestamp DEFAULT '2020-01-01 00:00:00')$S;",
               "INSERT INTO rcdb.t (id) VALUES (1);"};
    g.probes = {
        {"new_row",
         {"INSERT INTO rcdb.t (id) VALUES ($K);"},
         {{"value", "SELECT created FROM rcdb.t WHERE id = $K;"},
          // Not an epoch literal: the point is only that the default was APPLIED.
          {"not_null", "SELECT id FROM rcdb.t WHERE id = $K AND created IS NULL;", "OK|rows=0|cols=1"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
    };
    check_restart_consistency(g);
}

// NOT NULL + DEFAULT on the same column. The plan layer deliberately delegates
// NOT NULL enforcement for a defaulted column to storage ("the disk agent fills
// them non-NULL"). If the restart drops either the default or the not-null flag,
// this stores a NULL in a NOT NULL column -- or silently writes zero rows.
TEST_CASE("integration::cpp::restart_consistency::not_null_with_default", "[restart]") {
    group_t g;
    g.name = "not_null_with_default";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (a bigint, b bigint NOT NULL DEFAULT 7)$S;",
               "INSERT INTO rcdb.t (a) VALUES (1);"};
    g.probes = {
        {"insert_omitting_b",
         {"INSERT INTO rcdb.t (a) VALUES ($K);"},
         {{"row", "SELECT a, b FROM rcdb.t WHERE a = $K;"},
          {"no_null_stored", "SELECT a FROM rcdb.t WHERE b IS NULL;"}},
         {"DELETE FROM rcdb.t WHERE a = $K;"}},
    };
    check_restart_consistency(g);
}

// NOT NULL enforcement itself must survive, with byte-identical error text.
TEST_CASE("integration::cpp::restart_consistency::not_null_enforcement", "[restart]") {
    group_t g;
    g.name = "not_null_enforcement";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (id bigint, tag string NOT NULL)$S;",
               "INSERT INTO rcdb.t (id, tag) VALUES (1, 'red');"};
    g.probes = {
        {"violation_rejected",
         {"INSERT INTO rcdb.t (id, tag) VALUES ($K, NULL);"},
         {{"not_stored", "SELECT * FROM rcdb.t WHERE id = $K;"}, {"table_intact", "SELECT * FROM rcdb.t;"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
        {"valid_row_accepted",
         {"INSERT INTO rcdb.t (id, tag) VALUES ($K, 'ok');"},
         {{"row", "SELECT tag FROM rcdb.t WHERE id = $K;"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
    };
    check_restart_consistency(g);
}

// CHECK / UNIQUE / FOREIGN KEY must still reject after the restart.
TEST_CASE("integration::cpp::restart_consistency::check_constraint", "[restart]") {
    group_t g;
    g.name = "check_constraint";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (id bigint, amount bigint)$S;",
               "ALTER TABLE rcdb.t ADD CONSTRAINT ck_amount CHECK (amount > 0);",
               "INSERT INTO rcdb.t (id, amount) VALUES (1, 10);"};
    g.probes = {
        {"violation_rejected",
         {"INSERT INTO rcdb.t (id, amount) VALUES ($K, -1);"},
         {{"not_stored", "SELECT * FROM rcdb.t WHERE id = $K;"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
        {"valid_accepted",
         {"INSERT INTO rcdb.t (id, amount) VALUES ($K, 5);"},
         {{"row", "SELECT amount FROM rcdb.t WHERE id = $K;"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
    };
    check_restart_consistency(g);
}

TEST_CASE("integration::cpp::restart_consistency::unique_constraint", "[restart]") {
    group_t g;
    g.name = "unique_constraint";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (id bigint, name string)$S;",
               "ALTER TABLE rcdb.t ADD CONSTRAINT uq_id UNIQUE (id);",
               "INSERT INTO rcdb.t (id, name) VALUES (1, 'a');"};
    g.probes = {
        {"duplicate_rejected",
         {"INSERT INTO rcdb.t (id, name) VALUES (1, 'dup');"},
         {{"still_one_row", "SELECT * FROM rcdb.t WHERE id = 1;", "OK|rows=1|cols=2|[I64:1,STR:\"a\"]"}},
         {}},
        // The oracle matters here: the value read back is identical before and after the
        // restart, so a differential test alone certifies it -- even when it is wrong.
        {"fresh_key_accepted",
         {"INSERT INTO rcdb.t (id, name) VALUES ($K, 'ok');"},
         {{"row", "SELECT name FROM rcdb.t WHERE id = $K;", "OK|rows=1|cols=1|[STR:\"ok\"]"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
    };
    check_restart_consistency(g);
}

TEST_CASE("integration::cpp::restart_consistency::foreign_key", "[restart]") {
    group_t g;
    g.name = "foreign_key";
    g.setup = {create_db,
               "CREATE TABLE rcdb.parent (id bigint, v string)$S;",
               "CREATE TABLE rcdb.child (id bigint, parent_id bigint)$S;",
               "INSERT INTO rcdb.parent (id, v) VALUES (1, 'p1');",
               "ALTER TABLE rcdb.child ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES rcdb.parent (id);",
               "INSERT INTO rcdb.child (id, parent_id) VALUES (10, 1);"};
    g.probes = {
        {"orphan_rejected",
         {"INSERT INTO rcdb.child (id, parent_id) VALUES ($K, 999);"},
         {{"not_stored", "SELECT * FROM rcdb.child WHERE id = $K;"}},
         {"DELETE FROM rcdb.child WHERE id = $K;"}},
        {"valid_child_accepted",
         {"INSERT INTO rcdb.child (id, parent_id) VALUES ($K, 1);"},
         {{"row", "SELECT parent_id FROM rcdb.child WHERE id = $K;"}},
         {"DELETE FROM rcdb.child WHERE id = $K;"}},
    };
    check_restart_consistency(g);
}

// ---------------------------------------------------------------------------
// Physical row_id stability. A live UPDATE is an MVCC delete+append (the row
// moves to the end of the table); replay applies the same record in place. The
// two disagree about where the row is, and every later record keyed by physical
// row_id then lands on the wrong slot -- or on no slot at all.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::restart_consistency::update_then_delete", "[restart]") {
    group_t g;
    g.name = "update_then_delete";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (k bigint, v bigint)$S;",
               "INSERT INTO rcdb.t (k, v) VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8);",
               "UPDATE rcdb.t SET v = 999 WHERE k = 5;",
               "DELETE FROM rcdb.t WHERE k = 6;"};
    g.probes = {
        {"state",
         {},
         {{"all_rows", "SELECT * FROM rcdb.t;"},
          {"updated_row", "SELECT v FROM rcdb.t WHERE k = 5;"},
          {"deleted_row_gone", "SELECT * FROM rcdb.t WHERE k = 6;"}},
         {}},
    };
    check_restart_consistency(g);
}

TEST_CASE("integration::cpp::restart_consistency::two_updates", "[restart]") {
    group_t g;
    g.name = "two_updates";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (k bigint, v bigint)$S;",
               "INSERT INTO rcdb.t (k, v) VALUES (1,1),(2,2),(3,3),(4,4),(5,5);",
               "UPDATE rcdb.t SET v = 100 WHERE k = 2;",
               "UPDATE rcdb.t SET v = 200 WHERE k = 4;"};
    g.probes = {
        {"state",
         {},
         {{"all_rows", "SELECT * FROM rcdb.t;"},
          {"first_update", "SELECT v FROM rcdb.t WHERE k = 2;"},
          {"second_update", "SELECT v FROM rcdb.t WHERE k = 4;"}},
         {}},
    };
    check_restart_consistency(g);
}

// The same row updated twice: the second UPDATE is keyed by a row_id the first
// UPDATE already moved.
TEST_CASE("integration::cpp::restart_consistency::update_same_row_twice", "[restart]") {
    group_t g;
    g.name = "update_same_row_twice";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (k bigint, v bigint)$S;",
               "INSERT INTO rcdb.t (k, v) VALUES (1,1),(2,2),(3,3);",
               "UPDATE rcdb.t SET v = 20 WHERE k = 2;",
               "UPDATE rcdb.t SET v = 30 WHERE k = 2;"};
    g.probes = {
        {"state",
         {},
         {{"all_rows", "SELECT * FROM rcdb.t;"}, {"final_value", "SELECT v FROM rcdb.t WHERE k = 2;"}},
         {}},
    };
    check_restart_consistency(g);
}

// An UPDATE issued AFTER the restart must behave like one issued before it.
TEST_CASE("integration::cpp::restart_consistency::update_after_restart", "[restart]") {
    group_t g;
    g.name = "update_after_restart";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (k bigint, v bigint)$S;",
               "INSERT INTO rcdb.t (k, v) VALUES (1,10),(2,20),(3,30);"};
    g.probes = {
        // Idempotent: sets a constant, then puts it back, so both phases end in
        // the same state.
        {"update_sets_constant",
         {"UPDATE rcdb.t SET v = 77 WHERE k = 2;"},
         {{"value", "SELECT v FROM rcdb.t WHERE k = 2;"}, {"others_untouched", "SELECT * FROM rcdb.t WHERE k <> 2;"}},
         {"UPDATE rcdb.t SET v = 20 WHERE k = 2;"}},
    };
    check_restart_consistency(g);
}

// Compaction renumbers every surviving row from zero, with no WAL barrier. Any
// DELETE or UPDATE recorded after that point replays against the OLD numbering.
TEST_CASE("integration::cpp::restart_consistency::delete_then_compact", "[restart]") {
    group_t g;
    g.name = "delete_then_compact";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (a bigint, v bigint)$S;",
               "INSERT INTO rcdb.t (a, v) VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10);",
               // >30% dead rows: crosses the automatic cleanup threshold, which
               // compacts and renumbers with no WAL barrier.
               "DELETE FROM rcdb.t WHERE a IN (1, 2, 3, 4);",
               "DELETE FROM rcdb.t WHERE a = 6;",
               "UPDATE rcdb.t SET v = 99 WHERE a = 8;"};
    g.probes = {
        {"state",
         {},
         {{"survivors", "SELECT * FROM rcdb.t;"},
          {"post_compact_delete_stuck", "SELECT * FROM rcdb.t WHERE a = 6;"},
          {"post_compact_update_stuck", "SELECT v FROM rcdb.t WHERE a = 8;"},
          {"no_resurrection", "SELECT * FROM rcdb.t WHERE a <= 4;"}},
         {}},
    };
    check_restart_consistency(g);
}

// The OTHER compaction site: VACUUM (cleanup_versions + compact), not the commit-time
// maybe_cleanup. Kept distinct because the deletes stay below the 30% auto-cleanup threshold, so
// ONLY the explicit VACUUM renumbers. Same failure mode as delete_then_compact if VACUUM omits its
// PHYSICAL_COMPACT epoch marker: the post-VACUUM DELETE/UPDATE replay against the pre-VACUUM numbering.
TEST_CASE("integration::cpp::restart_consistency::vacuum_then_restart", "[restart]") {
    group_t g;
    g.name = "vacuum_then_restart";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (a bigint, v bigint)$S;",
               "INSERT INTO rcdb.t (a, v) VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10);",
               // 20% dead: BELOW the 30% commit-compaction threshold, so maybe_cleanup does NOT
               // pre-empt — only the explicit VACUUM renumbers the survivors.
               "DELETE FROM rcdb.t WHERE a IN (1, 2);",
               "VACUUM;",
               // Captured against the POST-VACUUM numbering; replay must re-run the VACUUM compaction
               // (via its epoch marker) before applying these or they land on the wrong rows.
               "DELETE FROM rcdb.t WHERE a = 5;",
               "UPDATE rcdb.t SET v = 99 WHERE a = 8;"};
    g.probes = {
        {"state",
         {},
         {{"survivors", "SELECT * FROM rcdb.t;"},
          {"post_vacuum_delete_stuck", "SELECT * FROM rcdb.t WHERE a = 5;"},
          {"post_vacuum_update_stuck", "SELECT v FROM rcdb.t WHERE a = 8;"},
          {"no_resurrection", "SELECT * FROM rcdb.t WHERE a <= 2;"}},
         {}},
    };
    check_restart_consistency(g);
}

// An aborted transaction's INSERTs occupy physical row-ids in the live run --
// ROLLBACK reverts marks, not placement -- but replay filters uncommitted
// records, so every committed append after the abort lands lower on replay and
// every positional DELETE/UPDATE recorded after the abort resolves against the
// wrong rows (until the next compact marker closes the numbering epoch).
TEST_CASE("integration::cpp::restart_consistency::aborted_insert_shifts_replay_numbering", "[restart]") {
    group_t g;
    g.name = "aborted_insert_numbering";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (a bigint, v bigint)$S;",
               "@txn BEGIN;",
               "@txn INSERT INTO rcdb.t (a, v) VALUES (100,100),(101,101),(102,102),(103,103),(104,104),"
               "(105,105),(106,106),(107,107),(108,108),(109,109);",
               "@txn ROLLBACK;",
               // Committed rows land at physical ids ABOVE the ten aborted-but-placed rows.
               "INSERT INTO rcdb.t (a, v) VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10);",
               // Positional records captured against the shifted (post-abort) numbering.
               "DELETE FROM rcdb.t WHERE a = 5;",
               "UPDATE rcdb.t SET v = 99 WHERE a = 8;"};
    g.probes = {
        {"state",
         {},
         {{"survivors", "SELECT * FROM rcdb.t;"},
          {"deleted_stays_deleted", "SELECT * FROM rcdb.t WHERE a = 5;"},
          {"update_sticks", "SELECT v FROM rcdb.t WHERE a = 8;"},
          {"aborted_rows_stay_gone", "SELECT * FROM rcdb.t WHERE a >= 100;"}},
         {}},
    };
    check_restart_consistency(g);
}

// Multi-row-group flavor of the aborted-insert cell: everything crosses the
// 1024-row vector boundary. The aborted placement spans several row groups
// (version slots past the first group are the ones the slot-convention bug
// mis-addressed), and the positional records after the abort resolve against
// a numbering that includes thousands of dead rows.
TEST_CASE("integration::cpp::restart_consistency::aborted_insert_multi_row_group", "[restart]") {
    group_t g;
    g.name = "aborted_insert_multi_rg";
    g.setup = {create_db,
               "CREATE TABLE rcdb.big (a bigint, v bigint)$S;",
               "INSERT INTO rcdb.big (a, v) VALUES (0,0),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),"
               "(8,8),(9,9),(10,10),(11,11),(12,12),(13,13),(14,14),(15,15);",
               // Double 7 times: 16 -> 2048 committed rows across 2+ row groups,
               // with `a` kept unique per generation.
               "INSERT INTO rcdb.big SELECT a + 16, v FROM rcdb.big;",
               "INSERT INTO rcdb.big SELECT a + 32, v FROM rcdb.big;",
               "INSERT INTO rcdb.big SELECT a + 64, v FROM rcdb.big;",
               "INSERT INTO rcdb.big SELECT a + 128, v FROM rcdb.big;",
               "INSERT INTO rcdb.big SELECT a + 256, v FROM rcdb.big;",
               "INSERT INTO rcdb.big SELECT a + 512, v FROM rcdb.big;",
               "INSERT INTO rcdb.big SELECT a + 1024, v FROM rcdb.big;",
               // 2048 pending rows placed across row groups, then rolled back.
               "@txn BEGIN;",
               "@txn INSERT INTO rcdb.big SELECT a + 10000, v FROM rcdb.big;",
               "@txn ROLLBACK;",
               // Positional records captured against the post-abort numbering,
               // touching rows well past the first row group.
               "DELETE FROM rcdb.big WHERE a < 8;",
               "UPDATE rcdb.big SET v = 77 WHERE a = 1500;"};
    g.probes = {
        {"state",
         {},
         {{"count", "SELECT COUNT(*) FROM rcdb.big;"},
          {"deleted_stay_deleted", "SELECT * FROM rcdb.big WHERE a < 8;"},
          {"update_sticks", "SELECT v FROM rcdb.big WHERE a = 1500;"},
          {"aborted_rows_stay_gone", "SELECT COUNT(*) FROM rcdb.big WHERE a >= 10000;"},
          {"late_sample", "SELECT * FROM rcdb.big WHERE a IN (1024, 1999, 2047);"}},
         {}},
    };
    check_restart_consistency(g);
}

// Whole-table DELETE, VACUUM to zero, then reuse of the emptied row-id space.
// The VACUUM epoch marker must replay before the re-inserts or they land above
// 20 ghost tombstones and every later positional record misresolves.
TEST_CASE("integration::cpp::restart_consistency::whole_table_delete_vacuum_reuse", "[restart]") {
    group_t g;
    g.name = "whole_table_delete_vacuum";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (a bigint, v bigint)$S;",
               "INSERT INTO rcdb.t (a, v) VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9),(10,10),"
               "(11,11),(12,12),(13,13),(14,14),(15,15),(16,16),(17,17),(18,18),(19,19),(20,20);",
               "DELETE FROM rcdb.t;",
               "VACUUM;",
               // Fresh rows at the recycled physical ids 0..4.
               "INSERT INTO rcdb.t (a, v) VALUES (101,1),(102,2),(103,3),(104,4),(105,5);",
               "DELETE FROM rcdb.t WHERE a = 103;",
               "UPDATE rcdb.t SET v = 42 WHERE a = 105;"};
    g.probes = {
        {"state",
         {},
         {{"survivors", "SELECT * FROM rcdb.t;"},
          {"old_rows_gone", "SELECT COUNT(*) FROM rcdb.t WHERE a <= 20;"},
          {"post_reuse_delete_stuck", "SELECT * FROM rcdb.t WHERE a = 103;"},
          {"post_reuse_update_stuck", "SELECT v FROM rcdb.t WHERE a = 105;"}},
         {}},
    };
    check_restart_consistency(g);
}

// ---------------------------------------------------------------------------
// Type fidelity. The .otbx column record stores a bare uint8 logical_type, so
// everything a type extension carries -- decimal width/scale, array size, list
// and struct children -- is destroyed on reload.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::restart_consistency::decimal", "[restart]") {
    group_t g;
    g.name = "decimal";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (id bigint, price decimal(10,2))$S;",
               "INSERT INTO rcdb.t (id, price) VALUES (1, 12.34), (2, 99.99);"};
    g.probes = {
        {"existing",
         {},
         {{"all_rows", "SELECT * FROM rcdb.t;"}, {"value_predicate", "SELECT id FROM rcdb.t WHERE price = 12.34;"}},
         {}},
        {"new_row",
         {"INSERT INTO rcdb.t (id, price) VALUES ($K, 7.25);"},
         {{"value", "SELECT price FROM rcdb.t WHERE id = $K;"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
    };
    check_restart_consistency(g);
}

TEST_CASE("integration::cpp::restart_consistency::array", "[restart]") {
    group_t g;
    g.name = "array";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (id bigint, xs int[3])$S;",
               "INSERT INTO rcdb.t (id, xs) VALUES (1, ARRAY[1,2,3]);"};
    g.probes = {
        {"existing", {}, {{"ids", "SELECT id FROM rcdb.t;"}}, {}},
        {"new_row",
         {"INSERT INTO rcdb.t (id, xs) VALUES ($K, ARRAY[7,8,9]);"},
         {{"id", "SELECT id FROM rcdb.t WHERE id = $K;"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
    };
    check_restart_consistency(g);
}

// The scalar types the catalog type codec cannot name: they decode back to
// logical_type::UNKNOWN. The column TYPE still round-trips through the probes even
// with no rows, because a NULL cell is tagged with the cursor's declared type.
TEST_CASE("integration::cpp::restart_consistency::unsigned_types", "[restart]") {
    group_t g;
    g.name = "unsigned_types";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (id bigint, u8 utinyint, u16 usmallint, u32 uinteger, u64 ubigint)$S;",
               "INSERT INTO rcdb.t (id) VALUES (1);"};
    g.probes = {
        // Reads the declared type of every unsigned column: a column that decays to
        // UNKNOWN across the restart changes this rendering even though every value
        // in it is NULL.
        {"schema",
         {},
         {{"declared_types",
           "SELECT * FROM rcdb.t WHERE id = 1;",
           "OK|rows=1|cols=5|[I64:1,U8:NULL,U16:NULL,U32:NULL,U64:NULL]"}},
         {}},
        {"new_row",
         {"INSERT INTO rcdb.t (id) VALUES ($K);"},
         {{"types", "SELECT u8, u16, u32, u64 FROM rcdb.t WHERE id = $K;"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
    };
    check_restart_consistency(g);
}

// ---------------------------------------------------------------------------
// NULL-ness. Validity is not persisted by the checkpoint at all: the bitmap is
// rebuilt implicitly all-valid, so a NULL comes back as whatever the raw buffer
// happened to hold.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::restart_consistency::null_validity", "[restart]") {
    group_t g;
    g.name = "null_validity";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (id bigint, n bigint, s string)$S;",
               "INSERT INTO rcdb.t (id, n, s) VALUES (1, 10, 'a'), (2, NULL, NULL), (3, 30, 'c');"};
    g.probes = {
        {"existing",
         {},
         {{"all_rows", "SELECT * FROM rcdb.t;"},
          {"is_null", "SELECT id FROM rcdb.t WHERE n IS NULL;"},
          {"is_not_null", "SELECT id FROM rcdb.t WHERE n IS NOT NULL;"},
          {"string_is_null", "SELECT id FROM rcdb.t WHERE s IS NULL;"},
          {"count_ignores_null", "SELECT COUNT(n) FROM rcdb.t;"},
          {"sum_ignores_null", "SELECT SUM(n) FROM rcdb.t;"}},
         {}},
        {"new_null_row",
         {"INSERT INTO rcdb.t (id, n) VALUES ($K, NULL);"},
         {{"reads_back_null", "SELECT id FROM rcdb.t WHERE id = $K AND n IS NULL;"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
    };
    check_restart_consistency(g);
}

// ---------------------------------------------------------------------------
// Indexes. A live INSERT mirrors the SUBMITTED chunk to the index; bootstrap
// rebuilds the index from a full storage scan. A row whose indexed column was
// DB-filled from a DEFAULT is therefore in one and not the other.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::restart_consistency::index_content", "[restart]") {
    group_t g;
    g.name = "index_content";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (id bigint, k bigint DEFAULT 55)$S;",
               "INSERT INTO rcdb.t (id, k) VALUES (1, 10), (2, 20), (3, 30);",
               "CREATE INDEX idx_k ON rcdb.t (k);"};
    g.probes = {
        {"existing", {}, {{"indexed_lookup", "SELECT id FROM rcdb.t WHERE k = 20;", "OK|rows=1|cols=1|[I64:2]"}}, {}},
        // This row takes its k from the DEFAULT -- the value the live index path never
        // sees (it indexes the SUBMITTED chunk, which has no k) and the rebuilt-from-
        // storage index does. The oracles are what make this group mean anything: the
        // divergence is real, but so is the fact that the pre-restart side is the WRONG
        // one, and a purely differential probe cannot say which side to fix.
        {"defaulted_key",
         {"INSERT INTO rcdb.t (id) VALUES ($K);"},
         {{"via_index", "SELECT id FROM rcdb.t WHERE k = 55;", "OK|rows=1|cols=1|[I64:$K+0]"},
          {"via_scan", "SELECT k FROM rcdb.t WHERE id = $K;", "OK|rows=1|cols=1|[I64:55]"},
          {"index_count", "SELECT COUNT(*) FROM rcdb.t WHERE k >= 0;", "OK|rows=1|cols=1|[U64:4]"},
          {"scan_count", "SELECT COUNT(*) FROM rcdb.t;", "OK|rows=1|cols=1|[U64:4]"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
    };
    check_restart_consistency(g);
}

// An index must still be maintained by writes issued after the restart.
TEST_CASE("integration::cpp::restart_consistency::index_maintained_after_restart", "[restart]") {
    group_t g;
    g.name = "index_maintained_after_restart";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (id bigint, k bigint)$S;",
               "INSERT INTO rcdb.t (id, k) VALUES (1, 10), (2, 20), (3, 30);",
               "CREATE INDEX idx_k ON rcdb.t (k);"};
    g.probes = {
        {"insert_then_lookup",
         {"INSERT INTO rcdb.t (id, k) VALUES ($K, 4242);"},
         {{"via_index", "SELECT id FROM rcdb.t WHERE k = 4242;"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
        {"delete_removes_from_index",
         {"INSERT INTO rcdb.t (id, k) VALUES ($K, 5150);", "DELETE FROM rcdb.t WHERE id = $K;"},
         {{"gone_from_index", "SELECT id FROM rcdb.t WHERE k = 5150;"}},
         {}},
    };
    check_restart_consistency(g);
}

// ---------------------------------------------------------------------------
// DDL that must survive a restart, and DDL issued after one.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::restart_consistency::alter_add_column", "[restart]") {
    group_t g;
    g.name = "alter_add_column";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (a bigint)$S;",
               "INSERT INTO rcdb.t (a) VALUES (1), (2);",
               "ALTER TABLE rcdb.t ADD COLUMN b bigint;"};
    g.probes = {
        {"schema", {}, {{"width", "SELECT * FROM rcdb.t WHERE a < 100;"}}, {}},
        {"write_new_column",
         {"INSERT INTO rcdb.t (a, b) VALUES ($K, 5);"},
         {{"row", "SELECT a, b FROM rcdb.t WHERE a = $K;"}},
         {"DELETE FROM rcdb.t WHERE a = $K;"}},
    };
    check_restart_consistency(g);
}

// ADD COLUMN followed by DROP COLUMN exercises the one update in the engine that is
// genuinely in place: the pg_attribute commit-id backfill, which patches a catalog row
// that must NOT move. Replaying it as an MVCC delete+append would tombstone the row and
// re-append it, leaving two live pg_attribute rows for the same attribute -- so the
// dropped column would come back, or appear twice.
TEST_CASE("integration::cpp::restart_consistency::catalog_column_lifecycle", "[restart]") {
    group_t g;
    g.name = "catalog_column_lifecycle";
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (a bigint)$S;",
               "INSERT INTO rcdb.t (a) VALUES (1), (2);",
               "ALTER TABLE rcdb.t ADD COLUMN keep bigint;",
               "ALTER TABLE rcdb.t ADD COLUMN gone bigint;",
               "ALTER TABLE rcdb.t DROP COLUMN gone;"};
    g.probes = {
        // The dropped column must stay dropped, and the kept one must appear exactly once.
        {"schema", {}, {{"shape", "SELECT * FROM rcdb.t WHERE a < 100;"}}, {}},
        {"dropped_column_unusable", {}, {{"select_it", "SELECT gone FROM rcdb.t;"}}, {}},
        {"kept_column_writable",
         {"INSERT INTO rcdb.t (a, keep) VALUES ($K, 9);"},
         {{"row", "SELECT a, keep FROM rcdb.t WHERE a = $K;"}},
         {"DELETE FROM rcdb.t WHERE a = $K;"}},
    };
    check_restart_consistency(g);
}

TEST_CASE("integration::cpp::restart_consistency::create_table_after_restart", "[restart]") {
    group_t g;
    g.name = "create_table_after_restart";
    // A dropped table's OID can be handed out again after a restart, and the
    // dropped table's INSERT records are still in the WAL. A brand-new table can
    // therefore come up pre-populated with a dead table's rows.
    g.setup = {create_db,
               "CREATE TABLE rcdb.keeper (x bigint)$S;",
               "CREATE TABLE rcdb.doomed (x bigint)$S;",
               "INSERT INTO rcdb.doomed (x) VALUES (111), (222), (333);",
               "DROP TABLE rcdb.doomed;"};
    g.probes = {
        {"fresh_table",
         {"CREATE TABLE rcdb.fresh$K (x bigint)$S;"},
         {{"is_empty", "SELECT * FROM rcdb.fresh$K;"}},
         {}},
        {"fresh_table_writable",
         {"INSERT INTO rcdb.fresh$K (x) VALUES (7);"},
         {{"row", "SELECT x FROM rcdb.fresh$K;"}},
         {"DROP TABLE rcdb.fresh$K;"}},
        {"keeper", {}, {{"untouched", "SELECT * FROM rcdb.keeper;"}}, {}},
    };
    check_restart_consistency(g);
}

// ---------------------------------------------------------------------------
// Session settings that change stored values and comparisons.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::restart_consistency::timezone", "[restart]") {
    group_t g;
    g.name = "timezone";
    // SET TIMEZONE is SESSION state: it does not survive a restart, and no client would
    // expect it to. What must survive is its effect on what got STORED, and the value
    // the setting persisted into pg_settings. So the SET is re-issued in every phase --
    // the probe then asks whether the same literal, under the same session setting,
    // still lands on the same instant.
    g.session_setup = {"SET TIMEZONE = 'Asia/Tokyo';"};
    g.setup = {create_db,
               "CREATE TABLE rcdb.t (id bigint, ts timestamptz)$S;",
               "INSERT INTO rcdb.t (id, ts) VALUES (1, TIMESTAMPTZ '2020-01-01 12:00:00');"};
    g.probes = {
        {"existing", {}, {{"value", "SELECT ts FROM rcdb.t WHERE id = 1;"}}, {}},
        {"new_literal",
         {"INSERT INTO rcdb.t (id, ts) VALUES ($K, TIMESTAMPTZ '2020-01-01 12:00:00');"},
         {{"same_instant", "SELECT ts FROM rcdb.t WHERE id = $K;"}},
         {"DELETE FROM rcdb.t WHERE id = $K;"}},
    };
    check_restart_consistency(g);
}

// ---------------------------------------------------------------------------
// I-2 zero-apply release. A DELETE/UPDATE under a non-pushable predicate reads
// through a bare mutating full_scan whose retained cursor is normally released
// by the storage apply (storage_delete_rows / storage_update). When ZERO rows
// match, no apply is ever sent — the operator must release the pin itself at
// its final drive. Sessions are minted per statement, so no later statement's
// sweep-on-open can match this (oid, session): an unreleased pin defers
// compaction of the table forever. Observable end-to-end: VACUUM after the
// zero-match DMLs must still compact and emit its PHYSICAL_COMPACT epoch
// marker into the WAL.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::restart_consistency::zero_match_dml_releases_compaction_pin", "[restart]") {
    const auto dir = data_root() / "zero_match_pin";
    auto config = test_create_config(dir);
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;

    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        auto run = [&](const std::string& sql) {
            auto cur = test_helpers::exec(dispatcher, sql);
            INFO(sql);
            REQUIRE(cur);
            REQUIRE_FALSE(cur->is_error());
            return cur;
        };
        run("CREATE DATABASE rcdb;");
        run("CREATE TABLE rcdb.t (id BIGINT);");
        run("INSERT INTO rcdb.t (id) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),"
            "(10),(11),(12),(13),(14),(15),(16),(17),(18),(19);");
        // 20% dead: below the 30% commit-time threshold, so only VACUUM compacts.
        run("DELETE FROM rcdb.t WHERE id < 4;");
        // Zero-match DMLs under a non-pushable (column-vs-column) predicate: each
        // drains a bare mutating scan that captures all 16 live ids, then applies
        // nothing.
        run("DELETE FROM rcdb.t WHERE id < id;");
        run("UPDATE rcdb.t SET id = 0 WHERE id < id;");
        run("VACUUM;");
        // A committed write pair after the VACUUM: the FULL-sync COMMIT flushes
        // every preceding WAL record (including the compact marker) durably, and
        // the pair leaves the row set unchanged.
        run("INSERT INTO rcdb.t (id) VALUES (999);");
        run("DELETE FROM rcdb.t WHERE id = 999;");
        auto cur = run("SELECT * FROM rcdb.t;");
        REQUIRE(cur->size() == 16);
    }

    auto log = initialization_logger("zero_match_pin", dir.string() + "/");
    services::wal::wal_reader_t reader(config.wal, log);
    const auto records = reader.read_committed_records(services::wal::id_t{0});
    bool user_table_compacted = false;
    for (const auto& r : records) {
        if (r.record_type == services::wal::wal_record_type::PHYSICAL_COMPACT &&
            r.table_oid >= components::catalog::FIRST_USER_OID) {
            user_table_compacted = true;
        }
    }
    REQUIRE(user_table_compacted);
}

// The other zero-apply-at-final shape: the DML DID match rows, but the matched
// count is an exact multiple of dml_flush_row_threshold, so the LAST mid-pump
// flush consumed the whole buffer and the final drive flushes nothing. The
// mid-pump apply cannot release the pin (the scan cursor is still open when it
// lands — releasing an active cursor would break the capture window), and the
// empty final drive sends no apply, so only an explicit final-drive release
// keeps the pin from leaking.
TEST_CASE("integration::cpp::restart_consistency::exact_threshold_dml_releases_compaction_pin", "[restart]") {
    const auto dir = data_root() / "exact_threshold_pin";
    auto config = test_create_config(dir);
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;
    config.execution.dml_flush_row_threshold = 4;

    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        auto run = [&](const std::string& sql) {
            auto cur = test_helpers::exec(dispatcher, sql);
            INFO(sql);
            REQUIRE(cur);
            REQUIRE_FALSE(cur->is_error());
            return cur;
        };
        run("CREATE DATABASE rcdb;");
        run("CREATE TABLE rcdb.t (id BIGINT);");
        run("INSERT INTO rcdb.t (id) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),"
            "(10),(11),(12),(13),(14),(15),(16),(17),(18),(19);");
        // Matches EXACTLY the flush threshold (4): one mid-pump flush drains the
        // buffer, the final drive has nothing to flush. 4/20 = 20% dead stays
        // below the 30% commit-time threshold, so only VACUUM compacts.
        run("DELETE FROM rcdb.t WHERE id < 4;");
        run("VACUUM;");
        run("INSERT INTO rcdb.t (id) VALUES (999);");
        run("DELETE FROM rcdb.t WHERE id = 999;");
        auto cur = run("SELECT * FROM rcdb.t;");
        REQUIRE(cur->size() == 16);
    }

    auto log = initialization_logger("exact_threshold_pin", dir.string() + "/");
    services::wal::wal_reader_t reader(config.wal, log);
    const auto records = reader.read_committed_records(services::wal::id_t{0});
    bool user_table_compacted = false;
    for (const auto& r : records) {
        if (r.record_type == services::wal::wal_record_type::PHYSICAL_COMPACT &&
            r.table_oid >= components::catalog::FIRST_USER_OID) {
            user_table_compacted = true;
        }
    }
    REQUIRE(user_table_compacted);
}

// A ROLLBACK'd INSERT must not wedge compaction. Abort reverts marks, not
// placement, so the aborted rows keep sitting in the table — but if their
// insert stamps stay PENDING (>= 2^62), has_version_above() trips forever and
// every compaction site (commit-time, VACUUM, checkpoint) refuses the table
// from then on. The aborted rows must be re-stamped committed-dead at abort:
// invisible to every snapshot, reclaimable, and counted as dead. Observable:
// VACUUM after the rollback still compacts and emits its PHYSICAL_COMPACT
// marker.
TEST_CASE("integration::cpp::restart_consistency::rollback_does_not_wedge_compaction", "[restart]") {
    const auto dir = data_root() / "rollback_compaction";
    auto config = test_create_config(dir);
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = true;

    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        auto run = [&](const std::string& sql, const otterbrix::session_id_t* s = nullptr) {
            otterbrix::session_id_t own;
            auto cur = dispatcher->execute_sql(s != nullptr ? *s : own, sql);
            INFO(sql);
            REQUIRE(cur);
            REQUIRE_FALSE(cur->is_error());
            return cur;
        };
        run("CREATE DATABASE rcdb;");
        run("CREATE TABLE rcdb.t (id BIGINT);");
        run("INSERT INTO rcdb.t (id) VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9),"
            "(10),(11),(12),(13),(14),(15),(16),(17),(18),(19);");
        {
            const otterbrix::session_id_t txn;
            run("BEGIN;", &txn);
            run("INSERT INTO rcdb.t (id) VALUES (100),(101),(102),(103),(104);", &txn);
            run("ROLLBACK;", &txn);
        }
        // 4/20 committed-dead (20%): below the commit-time threshold, so only the
        // explicit VACUUM compacts — if the aborted rows' stamps let it.
        run("DELETE FROM rcdb.t WHERE id < 4;");
        run("VACUUM;");
        run("INSERT INTO rcdb.t (id) VALUES (999);");
        run("DELETE FROM rcdb.t WHERE id = 999;");
        auto cur = run("SELECT * FROM rcdb.t;");
        REQUIRE(cur->size() == 16);
    }

    auto log = initialization_logger("rollback_compaction", dir.string() + "/");
    services::wal::wal_reader_t reader(config.wal, log);
    const auto records = reader.read_committed_records(services::wal::id_t{0});
    bool user_table_compacted = false;
    for (const auto& r : records) {
        if (r.record_type == services::wal::wal_record_type::PHYSICAL_COMPACT &&
            r.table_oid >= components::catalog::FIRST_USER_OID) {
            user_table_compacted = true;
        }
    }
    REQUIRE(user_table_compacted);
}

