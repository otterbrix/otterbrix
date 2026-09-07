#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/physical_plan/operators/operator_unique_constraint.hpp>
#include <string>

// End-to-end path for UNIQUE/PK: the DDL persists a pg_constraint row; operator_resolve_constraint reads it
// on INSERT/UPDATE and stamps unique_constraints(), which operator_unique_constraint_t dedups and scans for.

using namespace test_helpers;

TEST_CASE("integration::cpp::test_unique_constraint_e2e::unique_existing_row") {
    auto config = make_test_config(integration_fixture_path("test_unique_constraint_e2e/unique_existing_row"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: users(id) with a UNIQUE constraint on id");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.users (id bigint, name text);")->is_success());
        REQUIRE(
            exec(dispatcher, "ALTER TABLE TestDatabase.users ADD CONSTRAINT uq_users_id UNIQUE (id);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.users (id, name) VALUES (1, 'Alice');")->is_success());
    }

    INFO("distinct key accepted");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.users (id, name) VALUES (2, 'Bob');");
        INFO("distinct insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }

    INFO("duplicate key against existing row is rejected");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.users (id, name) VALUES (1, 'Eve');");
        REQUIRE(cur->is_error());
    }
}

// PRIMARY KEY enforces the same way, via contype 'p'.
TEST_CASE("integration::cpp::test_unique_constraint_e2e::primary_key_existing_row") {
    auto config = make_test_config(integration_fixture_path("test_unique_constraint_e2e/primary_key_existing_row"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: parts(id) with a PRIMARY KEY on id");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.parts (id bigint, label text);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.parts ADD CONSTRAINT pk_parts_id PRIMARY KEY (id);")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.parts (id, label) VALUES (100, 'gear');")->is_success());
    }

    INFO("distinct primary key accepted");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.parts (id, label) VALUES (200, 'bolt');");
        INFO("distinct insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }

    INFO("duplicate primary key is rejected");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.parts (id, label) VALUES (100, 'nut');");
        REQUIRE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::test_unique_constraint_e2e::within_batch_duplicate") {
    auto config = make_test_config(integration_fixture_path("test_unique_constraint_e2e/within_batch_duplicate"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: accounts(id) UNIQUE, no rows yet");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.accounts (id bigint, owner text);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.accounts ADD CONSTRAINT uq_accounts_id UNIQUE (id);")
                    ->is_success());
    }

    INFO("multi-row VALUES with an internal duplicate is rejected");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.accounts (id, owner) VALUES (5, 'a'), (5, 'b');");
        REQUIRE(cur->is_error());
    }

    INFO("multi-row VALUES with all-distinct keys is accepted");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.accounts (id, owner) VALUES (6, 'a'), (7, 'b');");
        INFO("distinct multi-row insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::test_unique_constraint_e2e::update_creates_duplicate") {
    auto config = make_test_config(integration_fixture_path("test_unique_constraint_e2e/update_creates_duplicate"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: seats(id) UNIQUE with rows 1 and 2");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.seats (id bigint, row_no bigint);")->is_success());
        REQUIRE(
            exec(dispatcher, "ALTER TABLE TestDatabase.seats ADD CONSTRAINT uq_seats_id UNIQUE (id);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.seats (id, row_no) VALUES (1, 10), (2, 20);")->is_success());
    }

    INFO("UPDATE that collides id=2 into the existing id=1 is rejected");
    {
        auto cur = exec(dispatcher, "UPDATE TestDatabase.seats SET id = 1 WHERE id = 2;");
        REQUIRE(cur->is_error());
    }

    INFO("UPDATE to a fresh distinct id is accepted");
    {
        auto cur = exec(dispatcher, "UPDATE TestDatabase.seats SET id = 3 WHERE id = 2;");
        INFO("distinct update error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }
}

// An omitted column stores the table DEFAULT, so defaulted rows still participate in uniqueness.
TEST_CASE("integration::cpp::test_unique_constraint_e2e::default_column_duplicate") {
    auto config = make_test_config(integration_fixture_path("test_unique_constraint_e2e/default_column_duplicate"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: tickets(code) UNIQUE with DEFAULT 5");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(
            exec(dispatcher, "CREATE TABLE TestDatabase.tickets (id bigint, code bigint DEFAULT 5);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.tickets ADD CONSTRAINT uq_tickets_code UNIQUE (code);")
                    ->is_success());
    }

    INFO("first defaulted insert is accepted and stores the default");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.tickets (id) VALUES (1);");
        INFO("insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
        auto sel = exec(dispatcher, "SELECT code FROM TestDatabase.tickets WHERE id = 1;");
        REQUIRE(sel->is_success());
        REQUIRE(sel->size() == 1);
        REQUIRE(sel->value(0, 0).value<int64_t>() == 5);
    }

    INFO("second insert omitting the column collides with the stored default");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.tickets (id) VALUES (2);");
        REQUIRE(cur->is_error());
    }

    INFO("an explicit value equal to the default collides too");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.tickets (id, code) VALUES (3, 5);");
        REQUIRE(cur->is_error());
    }

    INFO("an explicit distinct value is still accepted");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.tickets (id, code) VALUES (4, 6);");
        INFO("distinct insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }
}

TEST_CASE("integration::cpp::test_unique_constraint_e2e::default_column_within_batch_duplicate") {
    auto config = make_test_config(integration_fixture_path("test_unique_constraint_e2e/default_column_within_batch"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: badges(code) UNIQUE with DEFAULT 5, no rows yet");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.badges (id bigint, code bigint DEFAULT 5);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.badges ADD CONSTRAINT uq_badges_code UNIQUE (code);")
                    ->is_success());
    }

    INFO("one batch omitting the column twice collides within the batch (both store DEFAULT 5)");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.badges (id) VALUES (1), (2);");
        REQUIRE(cur->is_error());
    }
}

// PRIMARY KEY implies NOT NULL, for both an explicit NULL and an omitted (no-DEFAULT) column.
TEST_CASE("integration::cpp::test_unique_constraint_e2e::primary_key_rejects_null") {
    auto config = make_test_config(integration_fixture_path("test_unique_constraint_e2e/primary_key_rejects_null"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    INFO("setup: parts(id) with a PRIMARY KEY on id");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.parts (id bigint, label text);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.parts ADD CONSTRAINT pk_parts_id PRIMARY KEY (id);")
                    ->is_success());
    }

    INFO("explicit NULL primary key is rejected");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.parts (id, label) VALUES (NULL, 'ghost');");
        REQUIRE(cur->is_error());
    }

    INFO("INSERT omitting the PK column is rejected (would store NULL)");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.parts (label) VALUES ('phantom');");
        REQUIRE(cur->is_error());
    }

    INFO("a non-NULL primary key is still accepted");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.parts (id, label) VALUES (1, 'gear');");
        INFO("insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE_FALSE(cur->is_error());
    }

    INFO("no NULL-keyed rows leaked into the table");
    {
        auto cur = exec(dispatcher, "SELECT COUNT(label) AS c FROM TestDatabase.parts;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->value(0, 0).value<uint64_t>() == 1);
    }
}

// The existing-row scan layer packs qualifying rows from several >1024-row input chunks into
// DEFAULT_VECTOR_CAPACITY-sized keys chunks, mixing rows across chunks since every other row is
// NULL-keyed; the mid-flush path is covered by bounded_dml_flush::error_after_mid_flush_reverts_all.
TEST_CASE("integration::cpp::test_unique_constraint_e2e::multi_chunk_straddle_accepted") {
    auto config = make_test_config(integration_fixture_path("test_unique_constraint_e2e/multi_chunk_straddle"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    constexpr unsigned kRows = 3000;
    auto id_is_null = [](unsigned i) { return i % 2 == 0; };

    unsigned expected_non_null = 0;
    for (unsigned i = 0; i < kRows; ++i) {
        if (!id_is_null(i)) {
            ++expected_non_null;
        }
    }

    INFO("setup: big(id) UNIQUE, empty");
    {
        REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.big (id bigint, name text);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.big ADD CONSTRAINT uq_big_id UNIQUE (id);")->is_success());
    }

    INFO("one >1024-row insert (NULLs interspersed, non-NULLs all distinct) is accepted");
    {
        auto cur = seed_rows(dispatcher, "TestDatabase.big", "id, name", kRows, [&](unsigned i) {
            std::stringstream s;
            if (id_is_null(i)) {
                s << "(NULL, 'n')";
            } else {
                s << "(" << i << ", 'n')";
            }
            return s.str();
        });
        INFO("bulk insert error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == kRows);

        auto all = exec(dispatcher, "SELECT COUNT(name) AS c FROM TestDatabase.big;");
        REQUIRE(all->is_success());
        REQUIRE(all->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(kRows));
        auto keyed = exec(dispatcher, "SELECT COUNT(id) AS c FROM TestDatabase.big;");
        REQUIRE(keyed->is_success());
        REQUIRE(keyed->value(0, 0).value<uint64_t>() == static_cast<uint64_t>(expected_non_null));
    }

    INFO("a duplicate of a HIGH existing key (beyond the first packed chunk) is rejected");
    {
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.big (id, name) VALUES (2001, 'dup');");
        REQUIRE(cur->is_error());
    }
}

// The existing-row layer costs a full table pass per 1024 written rows (manager_disk_t::scan_by_keys)
// even when no key column changed; on a 200k-row table that measured 519 ms with a PK against 57 ms
// without one. The planner drops such groups before the operator is ever spliced in.
TEST_CASE("integration::cpp::test_unique_constraint_e2e::update_off_key_skips_existing_row_scan") {
    auto config = make_test_config(integration_fixture_path("test_unique_constraint_e2e/update_off_key"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.t (id bigint, payload bigint);")->is_success());
    REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.t ADD CONSTRAINT t_pk PRIMARY KEY (id);")->is_success());

    constexpr int kRows = 3000;
    {
        std::string sql = "INSERT INTO TestDatabase.t (id, payload) VALUES ";
        for (int i = 0; i < kRows; ++i) {
            if (i != 0) {
                sql += ", ";
            }
            sql += "(" + std::to_string(i) + ", " + std::to_string(i) + ")";
        }
        sql += ";";
        REQUIRE(exec(dispatcher, sql)->is_success());
    }

    INFO("UPDATE of a non-key column must not scan the table for existing keys");
    const auto scans_before = components::operators::unique_constraint_scan_sends();
    REQUIRE(exec(dispatcher, "UPDATE TestDatabase.t SET payload = payload + 1;")->is_success());
    const auto scans_after = components::operators::unique_constraint_scan_sends();
    CHECK(scans_after == scans_before);

    INFO("the key column is still protected: updating it into a duplicate fails");
    REQUIRE(exec(dispatcher, "UPDATE TestDatabase.t SET id = 0 WHERE id = 1;")->is_error());

    INFO("and the rows really were updated");
    {
        auto cur = exec(dispatcher, "SELECT payload FROM TestDatabase.t WHERE id = 5;");
        REQUIRE(cur->is_success());
        CHECK(cur->value(0, 0).value<int64_t>() == 6);
    }
}

// A write-set missing a key column must refuse, not skip the group -- the row is already written
// by the time this operator runs (guard in operator_unique_constraint.cpp).
TEST_CASE("integration::cpp::test_unique_constraint_e2e::declared_key_never_admits_a_duplicate_row") {
    auto config = make_test_config(
        integration_fixture_path("test_unique_constraint_e2e/declared_key_never_admits_a_duplicate_row"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());
    REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.badges (id bigint, code bigint, kind bigint);")->is_success());
    REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.badges ADD CONSTRAINT uq_badges_code UNIQUE (code);")
                ->is_success());
    REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.badges ADD CONSTRAINT pk_badges PRIMARY KEY (id, kind);")
                ->is_success());
    REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.badges (id, code, kind) VALUES (1, 7, 100);")->is_success());

    INFO("single-column UNIQUE: the duplicate must be refused AND must not be in the table");
    {
        auto dup = exec(dispatcher, "INSERT INTO TestDatabase.badges (id, code, kind) VALUES (2, 7, 200);");
        INFO("duplicate insert: " << (dup->is_error() ? dup->get_error().what : "accepted"));
        CHECK(dup->is_error());
        auto rows = exec(dispatcher, "SELECT id FROM TestDatabase.badges WHERE code = 7;");
        REQUIRE(rows->is_success());
        CHECK(rows->size() == 1);
    }

    INFO("composite PRIMARY KEY: same, on the pair");
    {
        auto dup = exec(dispatcher, "INSERT INTO TestDatabase.badges (id, code, kind) VALUES (1, 8, 100);");
        INFO("duplicate insert: " << (dup->is_error() ? dup->get_error().what : "accepted"));
        CHECK(dup->is_error());
        auto rows = exec(dispatcher, "SELECT code FROM TestDatabase.badges WHERE id = 1 AND kind = 100;");
        REQUIRE(rows->is_success());
        CHECK(rows->size() == 1);
    }

    INFO("and a row that violates neither key still goes in");
    {
        auto ok = exec(dispatcher, "INSERT INTO TestDatabase.badges (id, code, kind) VALUES (3, 9, 300);");
        INFO("distinct insert: " << (ok->is_error() ? ok->get_error().what : "accepted"));
        CHECK_FALSE(ok->is_error());
        auto rows = exec(dispatcher, "SELECT id FROM TestDatabase.badges WHERE code = 9;");
        REQUIRE(rows->is_success());
        CHECK(rows->size() == 1);
    }
}

// The write-side guard above only holds if no DML shape can omit a key column from the write-set --
// composite-key UPDATE, ALTER-added key, INSERT ... SELECT, quoted identifier, UPDATE ... RETURNING.
TEST_CASE("integration::cpp::test_unique_constraint_e2e::every_dml_shape_exposes_the_key_columns") {
    auto config = make_test_config(
        integration_fixture_path("test_unique_constraint_e2e/every_dml_shape_exposes_the_key_columns"));
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    REQUIRE(exec(dispatcher, "CREATE DATABASE TestDatabase;")->is_success());

    INFO("composite key, UPDATE that SETs only its first column");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.pair (a bigint, b bigint);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.pair ADD CONSTRAINT uq_pair UNIQUE (a, b);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.pair (a, b) VALUES (1, 1), (2, 1);")->is_success());
        auto cur = exec(dispatcher, "UPDATE TestDatabase.pair SET a = 1 WHERE a = 2;");
        INFO("update: " << (cur->is_error() ? cur->get_error().what : "accepted"));
        CHECK(cur->is_error());
        auto rows = exec(dispatcher, "SELECT a FROM TestDatabase.pair WHERE a = 1 AND b = 1;");
        REQUIRE(rows->is_success());
        CHECK(rows->size() == 1);
    }

    INFO("key on a column ALTER TABLE added after creation");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.late (id bigint);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.late ADD COLUMN code bigint;")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.late ADD CONSTRAINT uq_late UNIQUE (code);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.late (id, code) VALUES (1, 7);")->is_success());
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.late (id, code) VALUES (2, 7);");
        INFO("duplicate insert: " << (cur->is_error() ? cur->get_error().what : "accepted"));
        CHECK(cur->is_error());
        auto rows = exec(dispatcher, "SELECT id FROM TestDatabase.late WHERE code = 7;");
        REQUIRE(rows->is_success());
        CHECK(rows->size() == 1);
    }

    INFO("INSERT ... SELECT: the write-set comes from a scan, not a VALUES list");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.src (id bigint, code bigint);")->is_success());
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.dst (id bigint, code bigint);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.dst ADD CONSTRAINT uq_dst UNIQUE (code);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.src (id, code) VALUES (1, 7), (2, 7);")->is_success());
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.dst SELECT id, code FROM TestDatabase.src;");
        INFO("insert-select: " << (cur->is_error() ? cur->get_error().what : "accepted"));
        CHECK(cur->is_error());
        auto rows = exec(dispatcher, "SELECT id FROM TestDatabase.dst WHERE code = 7;");
        REQUIRE(rows->is_success());
        CHECK(rows->size() <= 1);
    }

    INFO("quoted mixed-case identifier: pg_attribute.attname and the write-set alias must agree");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.cased (\"Code\" bigint);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.cased ADD CONSTRAINT uq_cased UNIQUE (\"Code\");")
                    ->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.cased (\"Code\") VALUES (7);")->is_success());
        auto cur = exec(dispatcher, "INSERT INTO TestDatabase.cased (\"Code\") VALUES (7);");
        INFO("duplicate insert: " << (cur->is_error() ? cur->get_error().what : "accepted"));
        CHECK(cur->is_error());
        auto rows = exec(dispatcher, "SELECT \"Code\" FROM TestDatabase.cased WHERE \"Code\" = 7;");
        REQUIRE(rows->is_success());
        CHECK(rows->size() == 1);
    }

    INFO("UPDATE with a RETURNING projection over the key column");
    {
        REQUIRE(exec(dispatcher, "CREATE TABLE TestDatabase.ret (a bigint, b bigint);")->is_success());
        REQUIRE(exec(dispatcher, "ALTER TABLE TestDatabase.ret ADD CONSTRAINT uq_ret UNIQUE (a);")->is_success());
        REQUIRE(exec(dispatcher, "INSERT INTO TestDatabase.ret (a, b) VALUES (1, 1), (2, 1);")->is_success());
        auto cur = exec(dispatcher, "UPDATE TestDatabase.ret SET a = 1 WHERE a = 2 RETURNING b;");
        INFO("update-returning: " << (cur->is_error() ? cur->get_error().what : "accepted"));
        CHECK(cur->is_error());
        auto rows = exec(dispatcher, "SELECT b FROM TestDatabase.ret WHERE a = 1;");
        REQUIRE(rows->is_success());
        CHECK(rows->size() == 1);
    }
}
