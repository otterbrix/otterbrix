#include "test_config.hpp"

#include <catch2/catch.hpp>
#include <string>

// End-to-end regression tests for UNIQUE / PRIMARY KEY constraint enforcement.
//
// These exercise the FULL path a SQL UNIQUE/PK travels: the DDL persists a
// pg_constraint row (contype 'u'/'p'); operator_resolve_constraint reads it back
// on INSERT/UPDATE and stamps the resolve node's unique_constraints(); the
// dispatcher enrich pass copies those onto the DML node's unique_groups(); the
// planner wraps the DML in a node_check_constraint_t carrying the groups + the
// table_oid; and create_plan_check_constraint splices an
// operator_unique_constraint_t below the check sink. That operator dedups the
// just-written batch (within-batch) and scans existing rows via the DML's
// write-set snapshot (the left_-spine walk), so a duplicate key aborts the DML.
//
// DDL note: UNIQUE / PRIMARY KEY are added via ALTER TABLE ADD CONSTRAINT (the
// same form the FK tests use — see test_stacked_constraints.cpp). The base table
// is created with plain columns first, then the constraint is attached.

using namespace test_helpers;

// ---------------------------------------------------------------------------
// (A) UNIQUE column: duplicate INSERT against an existing row is rejected;
//     distinct keys are accepted.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_unique_constraint_e2e::unique_existing_row") {
    auto config = make_test_config("/tmp/test_unique_constraint_e2e/unique_existing_row", /*disk_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: users(id) with a UNIQUE constraint on id") {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.users (id bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.users ADD CONSTRAINT uq_users_id UNIQUE (id);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.users (id, name) VALUES (1, 'Alice');")->is_success());
    }

    INFO("distinct key accepted") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.users (id, name) VALUES (2, 'Bob');");
        INFO("distinct insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }

    INFO("duplicate key against existing row is rejected") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.users (id, name) VALUES (1, 'Eve');");
        REQUIRE(cur->is_error());
    }
}

// ---------------------------------------------------------------------------
// (B) PRIMARY KEY column: same enforcement via contype 'p'.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_unique_constraint_e2e::primary_key_existing_row") {
    auto config = make_test_config("/tmp/test_unique_constraint_e2e/primary_key_existing_row", /*disk_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: parts(id) with a PRIMARY KEY on id") {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.parts (id bigint, label text);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.parts ADD CONSTRAINT pk_parts_id PRIMARY KEY (id);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.parts (id, label) VALUES (100, 'gear');")->is_success());
    }

    INFO("distinct primary key accepted") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.parts (id, label) VALUES (200, 'bolt');");
        INFO("distinct insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }

    INFO("duplicate primary key is rejected") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.parts (id, label) VALUES (100, 'nut');");
        REQUIRE(cur->is_error());
    }
}

// ---------------------------------------------------------------------------
// (C) Within-ONE-batch duplicate: a multi-row VALUES insert whose rows collide
//     with EACH OTHER (no pre-existing row) is rejected by the within-batch dedup.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_unique_constraint_e2e::within_batch_duplicate") {
    auto config = make_test_config("/tmp/test_unique_constraint_e2e/within_batch_duplicate", /*disk_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: accounts(id) UNIQUE, no rows yet") {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.accounts (id bigint, owner text);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.accounts ADD CONSTRAINT uq_accounts_id UNIQUE (id);")
                    ->is_success());
    }

    INFO("multi-row VALUES with an internal duplicate is rejected") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.accounts (id, owner) VALUES (5, 'a'), (5, 'b');");
        REQUIRE(cur->is_error());
    }

    INFO("multi-row VALUES with all-distinct keys is accepted") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.accounts (id, owner) VALUES (6, 'a'), (7, 'b');");
        INFO("distinct multi-row insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }
}

// ---------------------------------------------------------------------------
// (D) UPDATE that creates a duplicate key is rejected (unique enforcement on
//     the UPDATE write-set).
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_unique_constraint_e2e::update_creates_duplicate") {
    auto config = make_test_config("/tmp/test_unique_constraint_e2e/update_creates_duplicate", /*disk_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: seats(id) UNIQUE with rows 1 and 2") {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.seats (id bigint, row_no bigint);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.seats ADD CONSTRAINT uq_seats_id UNIQUE (id);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.seats (id, row_no) VALUES (1, 10), (2, 20);")->is_success());
    }

    INFO("UPDATE that collides id=2 into the existing id=1 is rejected") {
        auto cur = exec(dispatcher, "UPDATE TestDatabase.seats SET id = 1 WHERE id = 2;");
        REQUIRE(cur->is_error());
    }

    INFO("UPDATE to a fresh distinct id is accepted") {
        auto cur = exec(dispatcher, "UPDATE TestDatabase.seats SET id = 3 WHERE id = 2;");
        INFO("distinct update error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }
}

// ---------------------------------------------------------------------------
// (E) DEFAULT-backed UNIQUE column: an INSERT that OMITS the column stores the
//     table DEFAULT, so omitted-column rows still participate in uniqueness —
//     both against an existing defaulted row and within one batch.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_unique_constraint_e2e::default_column_duplicate") {
    auto config = make_test_config("/tmp/test_unique_constraint_e2e/default_column_duplicate", /*disk_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: tickets(code) UNIQUE with DEFAULT 5") {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.tickets (id bigint, code bigint DEFAULT 5);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.tickets ADD CONSTRAINT uq_tickets_code UNIQUE (code);")
                    ->is_success());
    }

    INFO("first defaulted insert is accepted and stores the default") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.tickets (id) VALUES (1);");
        INFO("insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
        auto sel = exec(dispatcher, "SELECT code FROM TestDatabase.tickets WHERE id = 1;");
        REQUIRE(sel->is_success());
        REQUIRE(sel->size() == 1);
        REQUIRE(sel->value(0, 0).value<int64_t>() == 5);
    }

    INFO("second insert omitting the column collides with the stored default") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.tickets (id) VALUES (2);");
        REQUIRE(cur->is_error());
    }

    INFO("an explicit value equal to the default collides too") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.tickets (id, code) VALUES (3, 5);");
        REQUIRE(cur->is_error());
    }

    INFO("an explicit distinct value is still accepted") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.tickets (id, code) VALUES (4, 6);");
        INFO("distinct insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::test_unique_constraint_e2e::default_column_within_batch_duplicate") {
    auto config =
        make_test_config("/tmp/test_unique_constraint_e2e/default_column_within_batch", /*disk_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: badges(code) UNIQUE with DEFAULT 5, no rows yet") {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.badges (id bigint, code bigint DEFAULT 5);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.badges ADD CONSTRAINT uq_badges_code UNIQUE (code);")
                    ->is_success());
    }

    INFO("one batch omitting the column twice collides within the batch (both store DEFAULT 5)") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.badges (id) VALUES (1), (2);");
        REQUIRE(cur->is_error());
    }
}

// ---------------------------------------------------------------------------
// (F) PRIMARY KEY implies NOT NULL: a PK added via ALTER TABLE ADD CONSTRAINT
//     must reject NULL keys — explicit NULL and an omitted (no-DEFAULT) column.
// ---------------------------------------------------------------------------
TEST_CASE("integration::cpp::test_unique_constraint_e2e::primary_key_rejects_null") {
    auto config = make_test_config("/tmp/test_unique_constraint_e2e/primary_key_rejects_null", /*disk_on=*/true);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: parts(id) with a PRIMARY KEY on id") {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.parts (id bigint, label text);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.parts ADD CONSTRAINT pk_parts_id PRIMARY KEY (id);")
                    ->is_success());
    }

    INFO("explicit NULL primary key is rejected") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.parts (id, label) VALUES (NULL, 'ghost');");
        REQUIRE(cur->is_error());
    }

    INFO("INSERT omitting the PK column is rejected (would store NULL)") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.parts (label) VALUES ('phantom');");
        REQUIRE(cur->is_error());
    }

    INFO("a non-NULL primary key is still accepted") {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.parts (id, label) VALUES (1, 'gear');");
        INFO("insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }

    INFO("no NULL-keyed rows leaked into the table") {
        auto cur = exec(dispatcher, "SELECT COUNT(label) AS c FROM TestDatabase.parts;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 1);
    }
}
