#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

// A CHECK constraint must hold on UPDATE, not only on INSERT.
//
// This test was written against a defect: an UPDATE could move a row outside its CHECK while the
// same value was rejected on INSERT, so one constraint was enforced on one write path and silently
// skipped on the other. The defect was fixed upstream (#618) while this branch was in flight and the
// test went green unchanged — which is what makes it worth keeping rather than deleting: it was
// written blind to that fix and still pins the behaviour.
//
// The control INSERT matters as much as the UPDATE. Without it a green result could not tell
// "the constraint is enforced" apart from "nothing is checked anywhere". The last two cases guard
// the other direction: enforcing CHECK must not start rejecting valid updates, including one that
// never touches the checked column.

TEST_CASE("integration::cpp::test_check_on_update::update_violating_a_check_is_rejected", "[checkupd]") {
    auto config = test_create_config("/tmp/otterbrix/integration/test_check_on_update/probe");
    test_clear_directory(config);
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

    // Control: INSERT of the same offending value IS rejected.
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
