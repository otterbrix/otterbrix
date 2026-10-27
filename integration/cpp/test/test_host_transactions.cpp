// The transaction of a statement that reaches a backend: the backend is a participant of otterbrix's transaction,
// it prepares at COMMIT before the WAL marker and is released after it, it rolls back with ROLLBACK and ROLLBACK TO,
// and one transaction writes to one connection only (docs/embedding-host-api.md, "Transactions").

#include "host_fixture.hpp"

#include <components/table/test/fault_injection_file.hpp>
#include <services/wal/wal_page.hpp>

#include <memory>

#include <string>
#include <vector>

using host_fixture::host_t;
using host_fixture::strings;
using rows_t = host_actor::rows_t;
using code = core::error_code_t;

namespace {

    otterbrix::session_id_t any() { return otterbrix::session_id_t(); }

    const std::string orders = "m2.shop.public.orders";

    // m2.shop.public.orders (1, 100), (2, 200), (3, 300) and the local table loc.t.
    void shop(host_t& host) {
        host.register_orders();
        host.backend().set_rows(orders, {{1, 100}, {2, 200}, {3, 300}});
        host.db.ok(any(), "CREATE SCHEMA loc;");
        host.db.ok(any(), "CREATE TABLE loc.t (id BIGINT);");
        host.backend().clear_journal();
    }

    std::int64_t local_rows(host_t& host) { return host.db.ints(any(), "SELECT count(*) FROM loc.t;").front(); }

    // connection.db.public.<table> (id BIGINT) for each of `connections`.
    void tables_of(host_t& host, const strings& connections) {
        for (const auto& connection : connections) {
            host.register_database(connection, "db");
            host.db.ok(any(), "CREATE TABLE " + connection + ".db.public.t (id BIGINT);");
            host.backend(connection).clear_journal();
        }
    }

    std::string second_place(const std::string& first, const std::string& second) {
        return "transaction already wrote to connection \"" + first + "\"; writing to connection \"" + second +
               "\" in the same transaction is not supported";
    }

} // namespace

// A backend that refuses its COMMIT refuses the transaction's: otterbrix's rows go, and the backend rolls back.
TEST_CASE("integration::cpp::host_transactions::a_backend_refusing_its_commit_refuses_the_transaction") {
    host_t host("test_host_transactions/prepare_refused");
    shop(host);
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO loc.t (id) VALUES (1);");
    host.db.ok(session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);");
    host.backend().set_prepare_refusal("server m2: serialization failure");
    host.db.check_refused(session, "COMMIT;", code::connection_closed, "server m2: serialization failure");
    CHECK(local_rows(host) == 0);
    CHECK(host.backend().journal("prepare") == strings{"prepare 1"});
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().journal("release").empty());
    CHECK(host.backend().rows(orders) == rows_t{{1, 100}, {2, 200}, {3, 300}});
    CHECK(host.backend().open_transactions() == 0);
}

// COMMIT of a transaction that wrote to otterbrix and to a backend: both parts are there after it.
TEST_CASE("integration::cpp::host_transactions::a_commit_of_both_parts") {
    host_t host("test_host_transactions/both");
    shop(host);
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO loc.t (id) VALUES (1);");
    host.db.ok(session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);");
    host.db.ok(session, "COMMIT;");
    CHECK(local_rows(host) == 1);
    CHECK(host.backend().journal() == strings{"insert " + orders + " id:bigint,amount:bigint", "prepare 1", "release"});
    CHECK(host.backend().rows(orders).size() == 4);
}

// ROLLBACK reaches every backend the transaction read or wrote.
TEST_CASE("integration::cpp::host_transactions::rollback_reaches_every_participant") {
    host_t host("test_host_transactions/rollback");
    shop(host);
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "SELECT id FROM m2.shop.public.orders;");
    host.db.ok(session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);");
    host.db.ok(session, "ROLLBACK;");
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().journal("prepare").empty());
    CHECK(host.backend().rows(orders).size() == 3);
    CHECK(host.backend().open_transactions() == 0);
}

// A statement that fails inside BEGIN leaves its backend writes to the transaction's ROLLBACK, as PostgreSQL 18
// leaves an aborted transaction's work to ROLLBACK.
TEST_CASE("integration::cpp::host_transactions::a_failed_statement_in_a_transaction_rolls_back_with_it") {
    host_t host("test_host_transactions/failed_statement", nullptr, {"m2"}, {{"m2", 1}});
    shop(host);
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);");
    host.backend().set_failing_round(3);
    host.db.check_refused(session,
                          "INSERT INTO m2.shop.public.orders (id, amount) VALUES (5, 500), (6, 600);",
                          code::connection_closed,
                          "server m2: round 3 failed");
    CHECK(host.backend().journal("abort").empty());
    CHECK(host.backend().rows(orders).size() == 5);
    host.db.ok(session, "ROLLBACK;");
    CHECK(host.backend().journal("abort") == strings{"abort 0"});
    CHECK(host.backend().rows(orders).size() == 3);
}

// A transaction that only read a backend has nothing to commit there: no prepare, only the release.
TEST_CASE("integration::cpp::host_transactions::a_reading_transaction_is_released") {
    host_t host("test_host_transactions/reader");
    shop(host);
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "SELECT id FROM m2.shop.public.orders;");
    CHECK(host.backend().open_transactions() == 1);
    host.db.ok(session, "COMMIT;");
    CHECK(host.backend().journal("prepare").empty());
    CHECK(host.backend().journal("release") == strings{"release"});
    CHECK(host.backend().open_transactions() == 0);
}

// One transaction writes to one connection: a write to a second one is refused before its backend gets anything.
TEST_CASE("integration::cpp::host_transactions::a_second_connection_written_in_a_transaction_is_refused") {
    host_t host("test_host_transactions/two_backends", nullptr, {"pg", "pg2"});
    tables_of(host, {"pg", "pg2"});
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO pg.db.public.t (id) VALUES (1);");
    host.db.check_refused(session,
                          "INSERT INTO pg2.db.public.t (id) VALUES (2);",
                          code::unimplemented_yet,
                          second_place("pg", "pg2"));
    CHECK(host.backend("pg2").journal().empty());
    host.db.ok(session, "ROLLBACK;");
    CHECK(host.backend("pg").rows("pg.db.public.t").empty());
}

// The place of the writes is the transaction's to its end: reads of another connection are free, otterbrix's own
// tables do not count, and ROLLBACK TO does not free it.
TEST_CASE("integration::cpp::host_transactions::the_write_place_is_kept_to_the_end") {
    host_t host("test_host_transactions/place", nullptr, {"pg", "pg2"});
    tables_of(host, {"pg", "pg2"});
    host.db.ok(any(), "CREATE SCHEMA loc;");
    host.db.ok(any(), "CREATE TABLE loc.t (id BIGINT);");
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO loc.t (id) VALUES (1);");
    host.db.ok(session, "SAVEPOINT s;");
    host.db.ok(session, "INSERT INTO pg.db.public.t (id) VALUES (1);");
    host.db.ok(session, "SELECT id FROM pg2.db.public.t;");
    host.db.ok(session, "INSERT INTO loc.t (id) VALUES (2);");
    host.db.ok(session, "ROLLBACK TO SAVEPOINT s;");
    host.db.check_refused(session,
                          "UPDATE pg2.db.public.t SET id = 3;",
                          code::unimplemented_yet,
                          second_place("pg", "pg2"));
    host.db.ok(session, "ROLLBACK;");

    host.db.ok(any(), "INSERT INTO pg.db.public.t (id) VALUES (1);");
    host.db.ok(any(), "INSERT INTO pg2.db.public.t (id) VALUES (2);");
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO pg2.db.public.t (id) VALUES (3);");
    host.db.ok(session, "COMMIT;");
    CHECK(host.backend("pg2").rows("pg2.db.public.t") == rows_t{{2}, {3}});
}

// A DROP whose closure reaches the tables of two connections writes to both: refused alike.
TEST_CASE("integration::cpp::host_transactions::a_drop_reaching_two_connections_is_refused") {
    host_t host("test_host_transactions/drop_two", nullptr, {"pg", "pg2"});
    host.db.ok(any(), "CREATE TYPE mood AS ENUM ('x', 'y');");
    for (const std::string connection : {"pg", "pg2"}) {
        host.register_database(connection, "db");
        host.db.ok(any(), "CREATE TABLE " + connection + ".db.public.t (id BIGINT, m mood);");
        host.backend(connection).clear_journal();
    }
    host.db.check_refused(any(), "DROP TYPE mood CASCADE;", code::unimplemented_yet, second_place("pg", "pg2"));
    CHECK(host.backend("pg").changes().empty());
    CHECK(host.backend("pg2").changes().empty());
}

// A connection the host's map lost: its objects go from the catalog alone by DROP; anything else is refused.
TEST_CASE("integration::cpp::host_transactions::a_vanished_connection") {
    host_t host("test_host_transactions/vanished");
    host.register_orders();
    host.db.ok(any(), "CREATE TABLE m2.shop.public.items (id BIGINT);");
    host.restart_with({});
    host.backend().clear_journal();
    for (const std::string& sql : {"SELECT id FROM m2.shop.public.orders;",
                                   "INSERT INTO m2.shop.public.orders (id) VALUES (1);",
                                   "CREATE TABLE m2.shop.public.more (id BIGINT);"}) {
        host.db.check_refused(any(), sql, code::do_not_exists, "connection \"m2\" does not exist");
    }
    host.db.ok(any(), "DROP TABLE m2.shop.public.orders;");
    CHECK(host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'orders';") ==
          std::vector<std::int64_t>{0});
    host.ok(host.drop_remote_database("m2", "shop"));
    CHECK(host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'items';") ==
          std::vector<std::int64_t>{0});
    CHECK(host.backend().journal().empty());
}

// A connection's database holds tables only: a materialized view there is refused like a view.
TEST_CASE("integration::cpp::host_transactions::a_matview_in_a_connections_database_is_refused") {
    host_t host("test_host_transactions/matview");
    host.register_orders();
    host.db.check_refused(
        any(),
        "CREATE MATERIALIZED VIEW m2.shop.public.mv AS SELECT id FROM m2.shop.public.orders WITH NO DATA;",
        code::invalid_parameter,
        "only tables can be created in a schema of connection \"m2\"");
    CHECK(host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = 'mv';") ==
          std::vector<std::int64_t>{0});
    CHECK(host.backend().changes().empty());
}

// Two connections of one actor are two participants: each gets the end of the transaction with its own tables, and
// only the one written to prepares.
TEST_CASE("integration::cpp::host_transactions::two_connections_of_one_actor_are_two_participants") {
    host_t host("test_host_transactions/one_actor", nullptr, {"pg", "pg2=pg"});
    tables_of(host, {"pg", "pg2"});
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO pg.db.public.t (id) VALUES (1);");
    host.db.ok(session, "SELECT * FROM pg2.db.public.t;");
    host.db.ok(session, "COMMIT;");
    CHECK(host.backend("pg").journal("prepare") == strings{"prepare 1"});
    CHECK(host.backend("pg").journal("release") == strings{"release", "release"});

    host.backend("pg").clear_journal();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO pg.db.public.t (id) VALUES (2);");
    host.db.ok(session, "SELECT * FROM pg2.db.public.t;");
    host.db.ok(session, "ROLLBACK;");
    CHECK(host.backend("pg").journal("abort") == strings{"abort 0", "abort 0"});
}

// A backend that refused the statement's first write message has seen the transaction all the same: the end of the
// transaction reaches it, in autocommit and on ROLLBACK.
TEST_CASE("integration::cpp::host_transactions::a_refused_first_write_still_ends_at_the_backend") {
    host_t host("test_host_transactions/refused_first_write");
    shop(host);
    const char* writes[] = {"INSERT INTO m2.shop.public.orders (id, amount) VALUES (4, 400);",
                            "UPDATE m2.shop.public.orders SET amount = 1 WHERE id = 1;",
                            "DELETE FROM m2.shop.public.orders WHERE id = 1;"};
    for (const char* sql : writes) {
        INFO(sql);
        host.backend().clear_journal();
        host.backend().set_failing_round(1);
        host.db.check_refused(any(), sql, code::connection_closed, "server m2: round 1 failed");
        CHECK(host.backend().journal("abort") == strings{"abort 0"});

        host.backend().clear_journal();
        host.backend().set_failing_round(1);
        const auto session = otterbrix::session_id_t();
        host.db.ok(session, "BEGIN;");
        host.db.check_refused(session, sql, code::connection_closed, "server m2: round 1 failed");
        host.db.ok(session, "ROLLBACK;");
        CHECK(host.backend().journal("abort") == strings{"abort 0"});
    }
    CHECK(host.backend().open_transactions() == 0);
}

namespace {

    // The WAL's segment files fail their writes once armed: the COMMIT's marker is refused.
    class wal_fault_scope_t final : public services::wal::wal_file_interposer_t {
    public:
        wal_fault_scope_t() { services::wal::dev_set_wal_file_interposer(this); }
        ~wal_fault_scope_t() override { services::wal::dev_set_wal_file_interposer(nullptr); }
        wal_fault_scope_t(const wal_fault_scope_t&) = delete;
        wal_fault_scope_t& operator=(const wal_fault_scope_t&) = delete;

        otterbrix_test::fault_plan_t plan;

        std::unique_ptr<core::filesystem::file_handle_t>
        wrap(const std::filesystem::path& path, std::unique_ptr<core::filesystem::file_handle_t> inner) override {
            if (inner != nullptr && path.filename().string().rfind("wal_", 0) == 0) {
                return std::make_unique<otterbrix_test::faulty_file_handle_t>(std::move(inner), plan);
            }
            return inner;
        }
    };

} // namespace

// The WAL refuses the COMMIT's marker after the writing connection prepared: that connection only gets its release,
// and a connection of the same actor that only read rolls back.
TEST_CASE("integration::cpp::host_transactions::a_refused_marker_rolls_back_a_reader_of_the_writers_actor") {
    wal_fault_scope_t fault;
    host_t host("test_host_transactions/refused_marker", nullptr, {"pg", "pg2=pg"});
    tables_of(host, {"pg", "pg2"});
    host.db.ok(any(), "CREATE SCHEMA loc;");
    host.db.ok(any(), "CREATE TABLE loc.t (id BIGINT);");
    host.backend("pg").clear_journal();
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "INSERT INTO loc.t (id) VALUES (1);");
    host.db.ok(session, "INSERT INTO pg.db.public.t (id) VALUES (1);");
    host.db.ok(session, "SELECT * FROM pg2.db.public.t;");
    fault.plan.fail_writes_from = fault.plan.writes_seen + 1;
    auto refused = host.db.dispatcher()->execute_sql(session, "COMMIT;");
    fault.plan.fail_writes_from = 0;
    REQUIRE(refused->is_error());
    CHECK(host.backend("pg").journal("prepare") == strings{"prepare 1"});
    CHECK(host.backend("pg").journal("release") == strings{"release"});
    CHECK(host.backend("pg").journal("abort") == strings{"abort 0"});
}
