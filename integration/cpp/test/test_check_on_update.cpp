#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// UPDATE does not enforce CHECK constraints. This is the second half of C1, and it is a DEFECT:
// the INSERT of the very same value is rejected, so the constraint is not simply "not implemented
// for this statement" — it is enforced on one write path and silently skipped on the other.
//
// Measured, not read off the code: INSERT (2, -1) is rejected; UPDATE SET age = -1 succeeds and
// leaves the row sitting outside its CHECK.
//
// WHY IT IS NOT FIXED HERE, and what was tried. node_update_t carries no check expressions at all
// (set_check_exprs exists only on node_insert_t), and planner.cpp's rewrite_update builds
// node_check_constraint_t with NOT NULL + unique groups only. Wiring CHECK through the same three
// places the INSERT path uses — carrier on the node, enrich populating it, planner passing it — DOES
// make this test green, and then breaks the UPDATE write path: the table ends up with THREE rows
// (1,5) (7,5) (7,42) where it should hold one, i.e. new versions are appended and the old ones are
// never tombstoned. Proven by A/B: with the check node attached the table has 3 rows, without it 1.
//
// The cause is structural, not a missing argument. operator_check_constraint_t is built for the
// INSERT shape — it reads the source chunks and is documented as the plan ROOT ("check_constraint is
// the plan ROOT, so output_ becomes the result cursor") — whereas an UPDATE write set is the
// gathered storage row carrying the absolute row_ids the tombstone needs. Making CHECK work on
// UPDATE means settling that contract, which is design work, not plumbing. The attempt was reverted
// rather than left half-done.
//
// Hidden ([.]) because it fails. Run it with [checkupd].

TEST_CASE("integration::cpp::test_check_on_update::update_violating_a_check_is_rejected", "[.][checkupd]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_check_on_update/probe");
    test_clear_directory(config);
    config.disk.on = true;
    config.wal.on = false;
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE c;")->is_success());
    REQUIRE(exec("CREATE TABLE c.t (id bigint, age bigint);")->is_success());
    REQUIRE(exec("ALTER TABLE c.t ADD CONSTRAINT age_pos CHECK (age > 0);")->is_success());
    REQUIRE(exec("INSERT INTO c.t (id, age) VALUES (1, 5);")->is_success());

    // Control: INSERT of the same offending value IS rejected, which is what makes an accepted
    // UPDATE a defect rather than a decision about this constraint.
    {
        auto cur = exec("INSERT INTO c.t (id, age) VALUES (2, -1);");
        INFO("INSERT violating the CHECK must fail");
        REQUIRE(cur->is_error());
    }

    {
        auto cur = exec("UPDATE c.t SET age = -1 WHERE id = 1;");
        INFO("UPDATE moving a row outside the CHECK must fail the same way");
        CHECK(cur->is_error());
    }
    {
        auto cur = exec("SELECT id FROM c.t WHERE age < 0;");
        REQUIRE(cur->is_success());
        INFO("no row may sit outside its CHECK");
        CHECK(cur->size() == 0);
    }

    // The other half of the guard: turning the check on must not start rejecting valid updates, and
    // it must not reject an update that never touches the checked column either.
    {
        auto cur = exec("UPDATE c.t SET age = 42 WHERE id = 1;");
        INFO("an UPDATE that keeps the row inside its CHECK must still succeed");
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("UPDATE c.t SET id = 7 WHERE id = 1;");
        INFO("an UPDATE that does not touch the checked column must still succeed");
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("SELECT age FROM c.t WHERE id = 7;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        CHECK(cur->value(0, 0).value<int64_t>() == 42);
    }
}
