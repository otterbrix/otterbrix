#include "test_config.hpp"

#include <catch2/catch.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <core/operations_helper.hpp>
#include <services/collection/executor.hpp>

namespace {

    int find_column(const components::cursor::cursor_t& cur, std::string_view name) {
        const auto& chunk = cur.chunk_data();
        for (uint64_t i = 0; i < chunk.column_count(); ++i) {
            if (chunk.data[i].type().alias() == name) {
                return static_cast<int>(i);
            }
        }
        return -1;
    }

    template<typename Int>
    void check_int_array_1_2_3(const components::cursor::cursor_t& cur) {
        REQUIRE(cur.is_success());
        REQUIRE(cur.size() == 1);
        REQUIRE(cur.chunk_data().column_count() == 1);
        auto v = cur.chunk_data().value(0, 0);
        const auto& children = v.children();
        REQUIRE(children.size() == 3);
        REQUIRE(children[0].value<Int>() == static_cast<Int>(1));
        REQUIRE(children[1].value<Int>() == static_cast<Int>(2));
        REQUIRE(children[2].value<Int>() == static_cast<Int>(3));
    }

} // namespace

TEST_CASE("integration::cpp::correctness_bugs::array_int_slot_width") {
    auto config = test_create_config("/tmp/test_correctness_bugs/array_int_slot_width");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.intarr  (xs INT[3]);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.smlarr  (xs SMALLINT[3]);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.bigarr  (xs BIGINT[3]);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.intarr (xs) VALUES (ARRAY[1,2,3]);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.smlarr (xs) VALUES (ARRAY[1,2,3]);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.bigarr (xs) VALUES (ARRAY[1,2,3]);")->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT xs FROM t.intarr;");
        check_int_array_1_2_3<int32_t>(*cur);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT xs FROM t.smlarr;");
        check_int_array_1_2_3<int16_t>(*cur);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT xs FROM t.bigarr;");
        check_int_array_1_2_3<int64_t>(*cur);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::alias_collision") {
    auto config = test_create_config("/tmp/test_correctness_bugs/alias_collision");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.a (name STRING, val INT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.b (name STRING, val INT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.a (name, val) VALUES ('A1', 1);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.b (name, val) VALUES ('B1', 1);")->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session,
                                           "SELECT a.name AS aname, b.name AS bname\n"
                                           "FROM   t.a a INNER JOIN t.b b ON a.val = b.val;");
        INFO("alias_collision error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().column_count() == 2);

        int ai = find_column(*cur, "aname");
        int bi = find_column(*cur, "bname");
        REQUIRE(ai >= 0);
        REQUIRE(bi >= 0);
        REQUIRE(ai != bi);
        REQUIRE(cur->chunk_data().value(static_cast<uint64_t>(ai), 0).value<std::string_view>() == "A1");
        REQUIRE(cur->chunk_data().value(static_cast<uint64_t>(bi), 0).value<std::string_view>() == "B1");
    }
}

TEST_CASE("integration::cpp::correctness_bugs::star_prefix") {
    SECTION("table-qualified star") {
        auto config = test_create_config("/tmp/test_correctness_bugs/star_prefix_table");
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.x (id INT, a STRING, b STRING);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.y (id INT, c STRING);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.x (id, a, b) VALUES (1,'a','b');")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.y (id, c) VALUES (1,'c');")->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT t.x.* FROM t.x INNER JOIN t.y ON t.x.id=t.y.id;");
            INFO("table-qualified star error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_success());
            REQUIRE(cur->size() == 1);
            REQUIRE(cur->chunk_data().column_count() == 3);

            int id_i = find_column(*cur, "id");
            int a_i = find_column(*cur, "a");
            int b_i = find_column(*cur, "b");
            REQUIRE(id_i >= 0);
            REQUIRE(a_i >= 0);
            REQUIRE(b_i >= 0);
            REQUIRE(cur->chunk_data().value(static_cast<uint64_t>(id_i), 0).value<int32_t>() == 1);
            REQUIRE(cur->chunk_data().value(static_cast<uint64_t>(a_i), 0).value<std::string_view>() == "a");
            REQUIRE(cur->chunk_data().value(static_cast<uint64_t>(b_i), 0).value<std::string_view>() == "b");
        }
    }

    SECTION("struct field wildcard (out of scope, must error)") {
        auto config = test_create_config("/tmp/test_correctness_bugs/star_prefix_struct");
        test_clear_directory(config);
        config.disk.on = false;
        config.wal.on = false;
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();

        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TYPE p_t AS (px INT, py INT);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.s (id INT, p p_t);")->is_success());
        }
        {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.s (id, p) VALUES (1, ROW(10,20));")->is_success());
        }

        {
            auto session = otterbrix::session_id_t();
            auto cur = dispatcher->execute_sql(session, "SELECT (s.p).* FROM t.s s;");
            INFO("struct.* error: " << (cur->is_error() ? cur->get_error().what : "none"));
            REQUIRE(cur->is_error());
            REQUIRE(cur->get_error().type == core::error_code_t::unimplemented_yet);
        }
    }
}

TEST_CASE("integration::cpp::correctness_bugs::count_case_no_else") {
    auto config = test_create_config("/tmp/test_correctness_bugs/count_case_no_else");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.x (status STRING);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(
            dispatcher
                ->execute_sql(session,
                              "INSERT INTO t.x (status) VALUES ('paid'),('paid'),('paid'),('cancelled'),('cancelled');")
                ->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(CASE WHEN status='paid' THEN 1 END) AS n FROM t.x;");
        INFO("COUNT(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().column_count() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 3);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT SUM(CASE WHEN status='paid' THEN 1 ELSE 0 END) AS n FROM t.x;");
        INFO("SUM(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int64_t>() == 3);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::min_max_avg_case_no_else") {
    auto config = test_create_config("/tmp/test_correctness_bugs/min_max_avg_case_no_else");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.y (score INT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.y (score) VALUES (50),(60),(72),(85);")->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT MIN(CASE WHEN score >= 70 THEN score END) FROM t.y;");
        INFO("MIN(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int32_t>() == 72);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT MAX(CASE WHEN score >= 70 THEN score END) FROM t.y;");
        INFO("MAX(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int32_t>() == 85);
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT AVG(CASE WHEN score >= 70 THEN score END) FROM t.y;");
        INFO("AVG(CASE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        auto v = cur->chunk_data().value(0, 0);
        const auto& t = v.type();
        if (t.type() == components::types::logical_type::DOUBLE) {
            REQUIRE(core::is_equals(v.value<double>(), 78.5));
        } else if (t.type() == components::types::logical_type::FLOAT) {
            REQUIRE(core::is_equals(v.value<float>(), 78.5f));
        } else {
            REQUIRE(v.value<int64_t>() == 78);
        }
    }

    {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT MIN(CASE WHEN score >= 70 THEN score ELSE 999999 END) FROM t.y;");
        INFO("baseline MIN(CASE ELSE) error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<int32_t>() == 72);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::enum_scan_predicate") {
    auto config = test_create_config("/tmp/test_correctness_bugs/enum_scan_predicate");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TYPE oddness_t AS ENUM('even','odd');")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.e (n INT, kind oddness_t);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(
            dispatcher
                ->execute_sql(session, "INSERT INTO t.e (n, kind) VALUES (1,'odd'),(2,'even'),(3,'odd'),(4,'even');")
                ->is_success());
    }

    SECTION("6a scan-pushed STRING compare to ENUM") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM t.e WHERE kind='even';");
        INFO("6a error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    SECTION("6b ordinal baseline") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM t.e WHERE kind=0;");
        INFO("6b error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    SECTION("6c JOIN baseline") {
        auto session = otterbrix::session_id_t();
        auto cur =
            dispatcher->execute_sql(session, "SELECT a.* FROM t.e a INNER JOIN t.e b ON a.n=b.n WHERE a.kind='even';");
        INFO("6c error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 2);
    }

    SECTION("6d invalid ENUM string must error") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT * FROM t.e WHERE kind='invalid_xyz';");
        INFO("6d error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }
}

// A constraint (CHECK / FK) that errors AFTER the DML operator already appended its
// rows, in AUTOCOMMIT, must leave NO physical trace: the appended (uncommitted) rows
// must be REVERTED, not lingered. Mechanism of the bug being guarded against: the
// insert's await_async_and_resume does the WAL-first storage_append and records the
// append range on the pipeline context; the constraint operator above it (driven
// bottom-up, AFTER the insert) then errors. If the executor's error path skips lifting
// the recorded append range into the result, the autocommit abort tail has nothing to
// revert and the bad row physically lingers (txn_abort alone does not scrub it). These
// tests assert the row is ABSENT after the violation — including the deterministic
// "re-insert the same id succeeds" probe, which is RED if a uniqueness-free physical
// row still sits in the table.
TEST_CASE("integration::cpp::correctness_bugs::check_violation_autocommit_no_linger") {
    auto config = test_create_config("/tmp/test_correctness_bugs/check_violation_autocommit_no_linger");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        // age is bigint so the CHECK constant compares same-type (mirrors the
        // existing streaming_dml::check_constraint test).
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.acc (id bigint, age bigint);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(
            dispatcher->execute_sql(session, "ALTER TABLE t.acc ADD CONSTRAINT chk_age CHECK (age > 0);")->is_success());
    }

    // AUTOCOMMIT INSERT that violates the CHECK (age = -5 fails age > 0). The
    // statement MUST error.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.acc (id, age) VALUES (1, -5);");
        INFO("CHECK-violating INSERT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }

    // The bad row must be ABSENT: it was physically appended before the CHECK ran,
    // and the autocommit abort must have reverted that append.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS c FROM t.acc;");
        INFO("post-violation COUNT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 0);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM t.acc WHERE id = 1;");
        INFO("post-violation SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    // Deterministic physical-leak probe: a VALID re-insert of the SAME id must
    // succeed and the table must then hold EXACTLY ONE row. If the reverted append
    // had lingered, a full scan / COUNT here would observe the stale row too.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.acc (id, age) VALUES (1, 42);");
        INFO("valid re-insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS c FROM t.acc;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 1);
    }
}

TEST_CASE("integration::cpp::correctness_bugs::fk_violation_autocommit_no_linger") {
    auto config = test_create_config("/tmp/test_correctness_bugs/fk_violation_autocommit_no_linger");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.parent (id bigint, name text);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.child (id bigint, parent_id bigint);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(session,
                                  "ALTER TABLE t.child ADD CONSTRAINT fk_p "
                                  "FOREIGN KEY (parent_id) REFERENCES t.parent (id);")
                    ->is_success());
    }
    // One parent row (id == 1) exists; a child referencing id == 99 has no parent.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.parent (id, name) VALUES (1, 'p1');")->is_success());
    }

    // AUTOCOMMIT INSERT into the child referencing a missing parent: MUST error.
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.child (id, parent_id) VALUES (7, 99);");
        INFO("FK-violating INSERT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }

    // The child row must be ABSENT (the append must have been reverted on abort).
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS c FROM t.child;");
        INFO("post-FK-violation COUNT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 0);
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT id FROM t.child WHERE id = 7;");
        INFO("post-FK-violation SELECT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 0);
    }

    // Deterministic probe: a VALID insert referencing the existing parent succeeds
    // and the child table then holds exactly one row (the stale FK-violating row,
    // had it lingered, would push the count to 2).
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.child (id, parent_id) VALUES (8, 1);");
        INFO("valid child insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS c FROM t.child;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 1);
    }
}

// Deterministic RED/GREEN probe of the PHYSICAL revert. The black-box tests above
// assert the externally-visible "row absent" contract, but MVCC permanently masks the
// leaked row from every SQL read (its insert_id stays >= TRANSACTION_ID_START — a
// pending-txn id that is never lowered to a commit_id and is never reused), so they
// pass even with the leak present. This test observes the FIX MECHANISM directly via
// the DEV_MODE executor counter dml_appends_reverted(): a CHECK/FK violation in
// autocommit appends a row BEFORE the constraint fails, and the executor's failed-
// statement abort path MUST lift that recorded append range and physically revert it
// (storage_revert_appends → row_group_t::revert_append truncates the slot back). Before
// the fix the error path breaks BEFORE the dml_appends lift, so the counter does not
// move and the physical slot lingers — this assertion is RED. After the fix it bumps by
// exactly one per leaked range.
TEST_CASE("integration::cpp::correctness_bugs::check_violation_autocommit_reverts_physical_append") {
    auto config = test_create_config("/tmp/test_correctness_bugs/check_violation_reverts_physical_append");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.acc (id bigint, age bigint);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(
            dispatcher->execute_sql(session, "ALTER TABLE t.acc ADD CONSTRAINT chk_age CHECK (age > 0);")->is_success());
    }

    const auto reverts_before = services::collection::executor::dml_appends_reverted();
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.acc (id, age) VALUES (1, -5);");
        INFO("CHECK-violating INSERT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }
    const auto reverts_after = services::collection::executor::dml_appends_reverted();

    // The physically-appended (then constraint-rejected) row's base append range
    // must have been reverted on the abort path. RED before the fix (counter unchanged
    // because the error path skipped the dml_appends lift).
    INFO("dml_appends_reverted before=" << reverts_before << " after=" << reverts_after);
    REQUIRE(reverts_after == reverts_before + 1);
}

TEST_CASE("integration::cpp::correctness_bugs::fk_violation_autocommit_reverts_physical_append") {
    auto config = test_create_config("/tmp/test_correctness_bugs/fk_violation_reverts_physical_append");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.parent (id bigint, name text);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.child (id bigint, parent_id bigint);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher
                    ->execute_sql(session,
                                  "ALTER TABLE t.child ADD CONSTRAINT fk_p "
                                  "FOREIGN KEY (parent_id) REFERENCES t.parent (id);")
                    ->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO t.parent (id, name) VALUES (1, 'p1');")->is_success());
    }

    const auto reverts_before = services::collection::executor::dml_appends_reverted();
    {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "INSERT INTO t.child (id, parent_id) VALUES (7, 99);");
        INFO("FK-violating INSERT error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_error());
    }
    const auto reverts_after = services::collection::executor::dml_appends_reverted();

    INFO("dml_appends_reverted before=" << reverts_before << " after=" << reverts_after);
    REQUIRE(reverts_after == reverts_before + 1);
}

// A scalar aggregate over a COLUMN argument (not count(*)) over an EMPTY table must
// emit COUNT=0 (SUM/MIN/MAX/AVG=NULL), not crash. The global-aggregate empty path
// (operator_group_t::empty_aggregate_result) drives the aggregator over a batch with
// no chunks; operator_func_t::aggregate_batch_impl must not assert resolving the
// column key against a 0-column chunk. This is the deterministic, single-threaded
// reproduction of the integration::cpp::production::concurrent_read_write abort.
TEST_CASE("integration::cpp::correctness_bugs::aggregate_column_arg_empty_table") {
    auto config = test_create_config("/tmp/test_correctness_bugs/aggregate_column_arg_empty_table");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE t;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t.empty_tbl (id bigint, value bigint);")->is_success());
    }

    SECTION("COUNT(column) over empty table is 0") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT COUNT(id) AS cnt FROM t.empty_tbl;");
        INFO("COUNT(id) empty error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).value<uint64_t>() == 0);
    }

    SECTION("SUM(column) over empty table is NULL") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT SUM(value) AS s FROM t.empty_tbl;");
        INFO("SUM(value) empty error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).is_null());
    }

    SECTION("MIN(column) over empty table is NULL") {
        auto session = otterbrix::session_id_t();
        auto cur = dispatcher->execute_sql(session, "SELECT MIN(value) AS m FROM t.empty_tbl;");
        INFO("MIN(value) empty error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->chunk_data().value(0, 0).is_null());
    }
}
