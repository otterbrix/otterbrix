// Catalog name uniqueness must hold across SESSIONS, not only within one statement's
// resolve snapshot.
//
// The serial case is already refused: the executor's create arms read the statement's
// resolve snapshot (check_collection_exists / check_namespace_exists / check_type_exists)
// and answer *_already_exists. But that check reads a SNAPSHOT, while the pg_* row lands
// many awaits later as a blind append (append_pg_catalog_row). Two sessions in explicit
// transactions cannot see each other's uncommitted catalog rows, so both CREATEs of the
// same name pass the snapshot check and BOTH rows are written: pg_class ends up with two
// rows for one name, resolve binds whichever row its scan reaches first, and the loser's
// storage is orphaned forever. Not a race — the snapshots make it deterministic.
//
// The refusal these tests pin lives at the WRITE point: the catalog agent (the single
// writer of every pg_* table) checks name occupancy right before the append, where its
// mailbox makes the check-and-append atomic. The second session's CREATE statement
// itself must fail — not its COMMIT.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>

namespace {

    using namespace test_helpers;

    std::string status_of(const components::cursor::cursor_t_ptr& c) {
        if (c == nullptr) {
            return "nullptr cursor";
        }
        if (c->is_error()) {
            return std::string{"ERROR: "} + c->get_error().what.c_str();
        }
        return "success, size=" + std::to_string(c->size());
    }

    std::size_t rows_named(otterbrix::wrapper_dispatcher_t* d, const std::string& table, const std::string& col,
                           const std::string& name) {
        auto cur = exec(d, "SELECT " + col + " FROM pg_catalog." + table + " WHERE " + col + " = '" + name + "';");
        INFO("catalog probe " << table << ": " << status_of(cur));
        REQUIRE(cur->is_success());
        return cur->size();
    }

} // namespace

// Control: the serial autocommit case is refused by the snapshot check and stays refused.
TEST_CASE("catalog_name_uniqueness::control_serial_double_create_table") {
    const auto dir = integration_fixture_path("test_catalog_name_uniqueness/serial");
    auto config = make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE db;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE db.twin (id bigint);")->is_success());
    auto second = exec(d, "CREATE TABLE db.twin (id bigint);");
    INFO("serial second CREATE: " << status_of(second));
    REQUIRE(second->is_error());
    REQUIRE(rows_named(d, "pg_class", "relname", "twin") == 1);
}

// Two sessions, both in explicit transactions, CREATE TABLE under one name. The second
// CREATE statement must refuse — at the statement, not at COMMIT — and exactly one
// pg_class row may exist afterwards.
TEST_CASE("catalog_name_uniqueness::create_table_from_two_txn_sessions") {
    const auto dir = integration_fixture_path("test_catalog_name_uniqueness/table");
    auto config = make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE db;")->is_success());

    auto a = otterbrix::session_id_t();
    auto b = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(a, "BEGIN;")->is_success());
    REQUIRE(d->execute_sql(b, "BEGIN;")->is_success());

    auto ca = d->execute_sql(a, "CREATE TABLE db.twin (id bigint, from_a bigint);");
    auto cb = d->execute_sql(b, "CREATE TABLE db.twin (id bigint, from_b bigint);");
    INFO("A CREATE (in txn): " << status_of(ca));
    INFO("B CREATE (in txn): " << status_of(cb));
    REQUIRE(ca->is_success());
    // THE defect: B's snapshot cannot see A's uncommitted pg_class row, so this passed
    // and a second 'twin' row was appended.
    REQUIRE(cb->is_error());

    auto commit_a = d->execute_sql(a, "COMMIT;");
    INFO("A COMMIT: " << status_of(commit_a));
    REQUIRE(commit_a->is_success());
    // B's failed CREATE already ended B's transaction (failed-DDL statements abort their
    // txn); its COMMIT closes nothing but must not crash. Only record the outcome.
    auto commit_b = d->execute_sql(b, "COMMIT;");
    INFO("B COMMIT after refused CREATE: " << status_of(commit_b));

    REQUIRE(rows_named(d, "pg_class", "relname", "twin") == 1);

    // The survivor is A's schema, whole and writable.
    REQUIRE(exec(d, "INSERT INTO db.twin (id, from_a) VALUES (1, 10);")->is_success());
    auto ins_b = exec(d, "INSERT INTO db.twin (id, from_b) VALUES (2, 20);");
    INFO("INSERT via from_b (loser's schema): " << status_of(ins_b));
    REQUIRE(ins_b->is_error());
    auto sel = exec(d, "SELECT id FROM db.twin;");
    REQUIRE(sel->is_success());
    REQUIRE(sel->size() == 1);
}

// CREATE DATABASE has the same shape: pg_namespace is appended blind. A holds its row
// uncommitted; B (autocommit) must be refused at the write point.
TEST_CASE("catalog_name_uniqueness::create_database_against_uncommitted_namespace") {
    const auto dir = integration_fixture_path("test_catalog_name_uniqueness/database");
    auto config = make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    auto a = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(a, "BEGIN;")->is_success());
    auto ca = d->execute_sql(a, "CREATE DATABASE dupdb;");
    INFO("A CREATE DATABASE (in txn): " << status_of(ca));
    REQUIRE(ca->is_success());

    auto cb = exec(d, "CREATE DATABASE dupdb;");
    INFO("B CREATE DATABASE (autocommit, A uncommitted): " << status_of(cb));
    REQUIRE(cb->is_error());

    REQUIRE(d->execute_sql(a, "COMMIT;")->is_success());
    REQUIRE(rows_named(d, "pg_namespace", "nspname", "dupdb") == 1);
}

// CREATE TYPE: pg_type is appended blind the same way.
TEST_CASE("catalog_name_uniqueness::create_type_against_uncommitted_type") {
    const auto dir = integration_fixture_path("test_catalog_name_uniqueness/type");
    auto config = make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    auto a = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(a, "BEGIN;")->is_success());
    auto ca = d->execute_sql(a, "CREATE TYPE dup_t AS (px bigint, py bigint);");
    INFO("A CREATE TYPE (in txn): " << status_of(ca));
    REQUIRE(ca->is_success());

    auto cb = exec(d, "CREATE TYPE dup_t AS (qx bigint);");
    INFO("B CREATE TYPE (autocommit, A uncommitted): " << status_of(cb));
    REQUIRE(cb->is_error());

    REQUIRE(d->execute_sql(a, "COMMIT;")->is_success());
    REQUIRE(rows_named(d, "pg_type", "typname", "dup_t") == 1);
}

// CREATE INDEX: the index's name lives in its own pg_class row (relkind='i'), appended
// blind like the others.
TEST_CASE("catalog_name_uniqueness::create_index_against_uncommitted_index") {
    const auto dir = integration_fixture_path("test_catalog_name_uniqueness/index");
    auto config = make_test_config(dir, /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    REQUIRE(exec(d, "CREATE DATABASE db;")->is_success());
    REQUIRE(exec(d, "CREATE TABLE db.t1 (x bigint);")->is_success());
    REQUIRE(exec(d, "CREATE TABLE db.t2 (x bigint);")->is_success());

    auto a = otterbrix::session_id_t();
    REQUIRE(d->execute_sql(a, "BEGIN;")->is_success());
    auto ca = d->execute_sql(a, "CREATE INDEX twin_idx ON db.t1 (x);");
    INFO("A CREATE INDEX (in txn): " << status_of(ca));
    REQUIRE(ca->is_success());

    auto cb = exec(d, "CREATE INDEX twin_idx ON db.t2 (x);");
    INFO("B CREATE INDEX (autocommit, A uncommitted): " << status_of(cb));
    REQUIRE(cb->is_error());

    REQUIRE(d->execute_sql(a, "COMMIT;")->is_success());
    REQUIRE(rows_named(d, "pg_class", "relname", "twin_idx") == 1);
}
