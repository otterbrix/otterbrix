#include "session_sql_fixture.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>

namespace {

    using strings = std::vector<std::string>;
    using code = core::error_code_t;

    const std::string read_only = "SHOW transaction_read_only;";
    const std::string insert = "INSERT INTO s.t (a) VALUES (1);";
    const std::string isolation = "SHOW transaction_isolation;";

} // namespace

TEST_CASE("integration::cpp::transaction_access::begin") {
    session_sql::fixture_t db("test_transaction_access/begin");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "CREATE TABLE s.t (a bigint);");

    SECTION("read only") {
        db.ok(session, "START TRANSACTION READ ONLY;");
        CHECK(db.strings(session, read_only) == strings{"on"});
        CHECK(db.ints(session, "SELECT count(*) FROM s.t;") == std::vector<std::int64_t>{0});
        CHECK(db.refused(session, insert).type == code::read_only_sql_transaction);
        db.ok(session, "ROLLBACK;");
        CHECK(db.strings(session, read_only) == strings{"off"});
        db.ok(session, insert);
    }

    SECTION("read write") {
        db.ok(session, "SET default_transaction_read_only = on;");
        db.ok(session, "BEGIN READ WRITE;");
        CHECK(db.strings(session, read_only) == strings{"off"});
        db.ok(session, insert);
        db.ok(session, "COMMIT;");
        CHECK(db.ints(session, "SELECT count(*) FROM s.t;") == std::vector<std::int64_t>{1});
    }

    SECTION("deferrable") {
        db.ok(session, "BEGIN READ ONLY, DEFERRABLE;");
        CHECK(db.strings(session, read_only) == strings{"on"});
        CHECK(db.strings(session, "SHOW transaction_deferrable;") == strings{"off"});
        db.ok(session, "SET TRANSACTION DEFERRABLE;");
        db.ok(session, "SET transaction_deferrable = on;");
        CHECK(db.strings(session, "SHOW transaction_deferrable;") == strings{"off"});
        CHECK(db.refused(session, "SET transaction_deferrable = maybe;").type == code::invalid_parameter);
        db.ok(session, "COMMIT;");
        db.ok(session, "SET SESSION CHARACTERISTICS AS TRANSACTION DEFERRABLE;");
        db.ok(session, "SET default_transaction_deferrable = on;");
        CHECK(db.strings(session, "SHOW default_transaction_deferrable;") == strings{"off"});
        CHECK(db.refused(session, "ALTER SYSTEM SET transaction_deferrable = on;").type == code::invalid_parameter);
    }

    SECTION("a weaker isolation level") {
        db.ok(session, "BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY;");
        CHECK(db.strings(session, isolation) == strings{"repeatable read"});
        CHECK(db.strings(session, read_only) == strings{"on"});
        db.ok(session, "COMMIT;");
        db.ok(session, "BEGIN ISOLATION LEVEL READ UNCOMMITTED;");
        db.ok(session, "COMMIT;");
        db.ok(session, "BEGIN ISOLATION LEVEL REPEATABLE READ;");
        db.ok(session, insert);
        db.ok(session, "COMMIT;");
    }

    SECTION("serializable") {
        CHECK(db.refused(session, "BEGIN ISOLATION LEVEL SERIALIZABLE;").type == code::unimplemented_yet);
    }
}

TEST_CASE("integration::cpp::transaction_access::set_transaction") {
    session_sql::fixture_t db("test_transaction_access/set_transaction");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "CREATE TABLE s.t (a bigint);");

    SECTION("in a block") {
        db.ok(session, "BEGIN;");
        db.ok(session, "SET TRANSACTION READ ONLY;");
        CHECK(db.strings(session, read_only) == strings{"on"});
        CHECK(db.refused(session, "CREATE TABLE s.u (a bigint);").type == code::read_only_sql_transaction);
        db.ok(session, "ROLLBACK;");
        CHECK(db.strings(session, read_only) == strings{"off"});
    }

    SECTION("lasts its transaction") {
        db.ok(session, "BEGIN;");
        db.ok(session, "SET SESSION TRANSACTION READ ONLY;");
        db.ok(session, "COMMIT;");
        CHECK(db.strings(session, read_only) == strings{"off"});
        db.ok(session, insert);
    }

    SECTION("an isolation level") {
        db.ok(session, "BEGIN;");
        db.ok(session, "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;");
        CHECK(db.strings(session, isolation) == strings{"repeatable read"});
        db.ok(session, "SET transaction_isolation = 'read uncommitted';");
        CHECK(db.refused(session, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;").type == code::unimplemented_yet);
        db.ok(session, "ROLLBACK;");
        CHECK(db.refused(session, "SET transaction_isolation = 'snapshot';").type == code::invalid_parameter);
        CHECK(db.refused(session, "ALTER SYSTEM SET transaction_isolation = 'read committed';").type ==
              code::invalid_parameter);
    }
}

TEST_CASE("integration::cpp::transaction_access::session_characteristics") {
    session_sql::fixture_t db("test_transaction_access/session_characteristics");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");
    db.ok(session, "CREATE TABLE s.t (a bigint);");

    SECTION("read only") {
        db.ok(session, "SET SESSION CHARACTERISTICS AS TRANSACTION READ ONLY;");
        CHECK(db.strings(session, "SHOW default_transaction_read_only;") == strings{"on"});
        CHECK(db.refused(session, insert).type == code::read_only_sql_transaction);
        db.ok(session, "BEGIN;");
        CHECK(db.strings(session, read_only) == strings{"on"});
        CHECK(db.refused(session, "UPDATE s.t SET a = 2;").type == code::read_only_sql_transaction);
        db.ok(session, "ROLLBACK;");
        db.ok(session, "SET SESSION CHARACTERISTICS AS TRANSACTION READ WRITE;");
        db.ok(session, insert);
    }

    // The access is fixed as the transaction starts: a later default is the next transaction's.
    SECTION("does not change a running transaction") {
        db.ok(session, "BEGIN;");
        db.ok(session, "SET default_transaction_read_only = on;");
        CHECK(db.strings(session, read_only) == strings{"off"});
        db.ok(session, insert);
        db.ok(session, "COMMIT;");
        CHECK(db.strings(session, read_only) == strings{"on"});
    }

    SECTION("an isolation level") {
        db.ok(session, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;");
        db.ok(session, "SET default_transaction_isolation = 'read committed';");
        CHECK(db.strings(session, "SHOW default_transaction_isolation;") == strings{"repeatable read"});
        CHECK(db.refused(session, "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE;").type ==
              code::unimplemented_yet);
        CHECK(db.refused(session, "SET default_transaction_isolation = 'serializable';").type ==
              code::unimplemented_yet);
    }
}
