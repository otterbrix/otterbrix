// DDL of a table of a connection's database: the backend's DDL message is a step of the ordinary DDL plan, after
// otterbrix's checks and before the catalog's rows; only in autocommit, only CREATE / DROP / ALTER ADD|DROP COLUMN,
// and a backend refusal writes nothing.

#include "host_fixture.hpp"

#include <string>
#include <vector>

using host_fixture::host_t;
using code = core::error_code_t;
using strings = std::vector<std::string>;
using ints = std::vector<std::int64_t>;

namespace {

    otterbrix::session_id_t any() { return otterbrix::session_id_t(); }

    void registered(host_t& host) {
        host.register_shop();
        host.backend().clear_journal();
    }

    std::int64_t relations_named(host_t& host, const std::string& name) {
        return host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_class WHERE relname = '" + name + "';").front();
    }

    strings columns_of(host_t& host, const std::string& name) {
        return host.db.strings(any(),
                               "SELECT a.attname FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class c "
                               "ON a.attrelid = c.oid WHERE c.relname = '" +
                                   name + "' ORDER BY a.attnum;");
    }

} // namespace

TEST_CASE("integration::cpp::remote_ddl::create_table_reaches_the_backend_and_the_catalog") {
    host_t host("test_remote_ddl/create");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t2 (id BIGINT NOT NULL, note TEXT);");
    CHECK(host.backend().changes() == strings{"create m2.shop.public.t2 id:not_null note"});
    CHECK(host.db.strings(any(), "SELECT relkind FROM pg_catalog.pg_class WHERE relname = 't2';") == strings{"r"});
    CHECK(columns_of(host, "t2") == strings{"id", "note"});
}

TEST_CASE("integration::cpp::remote_ddl::only_in_autocommit") {
    host_t host("test_remote_ddl/autocommit");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t1 (id BIGINT);");
    host.backend().clear_journal();
    const std::string refusal = "connection \"m2\" only supports DDL in autocommit";
    for (const std::string begin : {"BEGIN;", "SET autocommit = off;"}) {
        for (const std::string sql : {"CREATE TABLE m2.shop.public.t3 (id BIGINT);",
                                      "ALTER TABLE m2.shop.public.t1 ADD COLUMN x BIGINT;",
                                      "DROP TABLE m2.shop.public.t1;"}) {
            INFO(begin << " " << sql);
            const auto session = otterbrix::session_id_t();
            host.db.ok(session, begin);
            host.db.check_refused(session, sql, code::unimplemented_yet, refusal);
            host.db.ok(session, "ROLLBACK;");
        }
    }
    CHECK(host.backend().changes().empty());
    CHECK(relations_named(host, "t3") == 0);
    CHECK(relations_named(host, "t1") == 1);
}

TEST_CASE("integration::cpp::remote_ddl::a_backend_refusal_writes_nothing") {
    host_t host("test_remote_ddl/refusal");
    registered(host);
    host.db.check_refused(any(),
                          "CREATE TABLE m2.shop.public.bad1 (id BIGINT);",
                          code::connection_closed,
                          "backend refused \"bad1\"");
    CHECK(relations_named(host, "bad1") == 0);
}

TEST_CASE("integration::cpp::remote_ddl::alter_add_and_drop_column") {
    host_t host("test_remote_ddl/alter");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t2 (id BIGINT NOT NULL, note TEXT);");
    host.backend().clear_journal();
    host.db.ok(any(), "ALTER TABLE m2.shop.public.t2 ADD COLUMN x BIGINT;");
    host.db.ok(any(), "ALTER TABLE m2.shop.public.t2 DROP COLUMN note;");
    CHECK(host.backend().changes() == strings{"alter add m2.shop.public.t2 x", "alter drop m2.shop.public.t2 note"});
    CHECK(columns_of(host, "t2") == strings{"id", "x"});
}

TEST_CASE("integration::cpp::remote_ddl::other_alters_are_refused") {
    host_t host("test_remote_ddl/other_alters");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t2 (id BIGINT);");
    host.backend().clear_journal();
    for (const std::string sql : {"ALTER TABLE m2.shop.public.t2 RENAME COLUMN id TO id2;",
                                  "ALTER TABLE m2.shop.public.t2 ALTER COLUMN id TYPE TEXT;"}) {
        const auto error = host.db.refused(any(), sql);
        INFO(sql);
        CHECK(error.type == code::unimplemented_yet);
    }
    CHECK(host.backend().changes().empty());
}

TEST_CASE("integration::cpp::remote_ddl::create_table_as_is_refused") {
    host_t host("test_remote_ddl/ctas");
    registered(host);
    // Refused for every target before any name is looked up (#675), a connection's database included.
    const auto error = host.db.refused(any(), "CREATE TABLE m2.shop.public.t4 AS SELECT 1 AS a;");
    CHECK(error.type == code::sql_parse_error);
    CHECK(host.backend().changes().empty());
    CHECK(relations_named(host, "t4") == 0);
}

TEST_CASE("integration::cpp::remote_ddl::constraints_but_not_null_are_refused") {
    host_t host("test_remote_ddl/constraints");
    registered(host);
    for (const std::string sql : {"CREATE TABLE m2.shop.public.t5 (id BIGINT PRIMARY KEY);",
                                  "CREATE TABLE m2.shop.public.t5 (id BIGINT UNIQUE);",
                                  "CREATE TABLE m2.shop.public.t5 (id BIGINT CHECK (id > 0));",
                                  "CREATE TABLE m2.shop.public.t5 (id BIGINT DEFAULT 1);"}) {
        const auto error = host.db.refused(any(), sql);
        INFO(sql);
        CHECK(error.type == code::unimplemented_yet);
    }
    CHECK(host.backend().changes().empty());
    CHECK(relations_named(host, "t5") == 0);
}

// A table gone from the backend is taken away by an ordinary DROP: a view over it keeps it (RESTRICT) and the backend
// is not asked; without the view it goes.
TEST_CASE("integration::cpp::remote_ddl::drop_restrict_before_the_backend") {
    host_t host("test_remote_ddl/drop");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t2 (id BIGINT);");
    host.db.ok(any(), "CREATE SCHEMA loc;");
    host.db.ok(any(), "CREATE VIEW loc.v AS SELECT id FROM m2.shop.public.t2;");
    host.backend().clear_journal();
    const auto error = host.db.refused(any(), "DROP TABLE m2.shop.public.t2;");
    CHECK(error.type == code::dependent_objects_still_exist);
    CHECK(host.backend().changes().empty());
    CHECK(relations_named(host, "t2") == 1);

    host.db.ok(any(), "DROP VIEW loc.v;");
    host.db.ok(any(), "DROP TABLE m2.shop.public.t2;");
    CHECK(host.backend().changes() == strings{"drop m2.shop.public.t2"});
    CHECK(relations_named(host, "t2") == 0);
}

TEST_CASE("integration::cpp::remote_ddl::a_backend_refusal_of_drop_keeps_the_table") {
    host_t host("test_remote_ddl/drop_refused");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.nodrop1 (id BIGINT);");
    host.backend().clear_journal();
    host.db.check_refused(any(),
                          "DROP TABLE m2.shop.public.nodrop1;",
                          code::connection_closed,
                          "backend refused \"nodrop1\"");
    CHECK(relations_named(host, "nodrop1") == 1);
}

TEST_CASE("integration::cpp::remote_ddl::through_the_place") {
    host_t host("test_remote_ddl/place");
    registered(host);
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "SET search_path = 'm2.shop.public';");
    host.db.ok(session, "CREATE TABLE t6 (a BIGINT);");
    CHECK(host.backend().changes() == strings{"create m2.shop.public.t6 a"});
}

TEST_CASE("integration::cpp::remote_ddl::an_index_on_a_table_of_a_connection_is_refused") {
    host_t host("test_remote_ddl/index");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t6 (a BIGINT);");
    host.backend().clear_journal();
    const auto error = host.db.refused(any(), "CREATE INDEX i ON m2.shop.public.t6 (a);");
    CHECK(error.type == code::unimplemented_yet);
    CHECK(host.backend().changes().empty());
}

TEST_CASE("integration::cpp::remote_ddl::a_created_table_is_read_through_its_backend") {
    host_t host("test_remote_ddl/read");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t7 (a BIGINT);");
    host.backend().clear_journal();
    CHECK(host.db.ints(any(), "SELECT count(*) FROM m2.shop.public.t7;") == ints{0});
    CHECK(host.backend().journal("scan") == strings{"scan m2.shop.public.t7"});
}

// DROP SCHEMA ... CASCADE in a connection's database: the backend's DROP of each table it reaches, then the catalog's
// rows; the schema itself is a row of otterbrix's catalog only.
TEST_CASE("integration::cpp::remote_ddl::drop_schema_cascade_drops_each_table_in_the_backend") {
    host_t host("test_remote_ddl/drop_schema");
    registered(host);
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "SET search_path = 'm2.shop.public';");
    host.db.ok(session, "CREATE SCHEMA s;");
    host.db.ok(any(), "CREATE TABLE m2.shop.s.t8 (a BIGINT);");
    host.backend().clear_journal();
    host.db.ok(session, "DROP SCHEMA s CASCADE;");
    CHECK(host.backend().changes() == strings{"drop m2.shop.s.t8"});
    CHECK(relations_named(host, "t8") == 0);
}

// DROP TYPE that reaches a column of a connection's table: RESTRICT refuses it before the backend is asked; CASCADE
// drops the column in the backend, as ALTER TABLE ... DROP COLUMN does; only in autocommit.
TEST_CASE("integration::cpp::remote_ddl::drop_type_cascade_reaches_a_column_of_a_connections_table") {
    host_t host("test_remote_ddl/drop_type");
    registered(host);
    host.db.ok(any(), "CREATE TYPE mood AS ENUM ('x', 'y');");
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t9 (id BIGINT, m mood);");
    host.backend().clear_journal();
    CHECK(host.db.refused(any(), "DROP TYPE mood;").type == code::dependent_objects_still_exist);
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.check_refused(session,
                          "DROP TYPE mood CASCADE;",
                          code::unimplemented_yet,
                          "connection \"m2\" only supports DDL in autocommit");
    host.db.ok(session, "ROLLBACK;");
    CHECK(host.backend().changes().empty());
    host.db.ok(any(), "DROP TYPE mood CASCADE;");
    CHECK(host.backend().changes() == strings{"alter drop m2.shop.public.t9 m"});
    CHECK(columns_of(host, "t9") == strings{"id"});
}

// What the host keeps of a table lives in its own ordinary otterbrix table: the backend's DDL reply brings the rows and
// otterbrix writes them in the statement's transaction: a row after CREATE, none after DROP, none after a backend
// refusal or a refused statement. A schema is otterbrix's catalog only: it has no row.
TEST_CASE("integration::cpp::remote_ddl::the_hosts_parameters_are_rows_of_its_own_table") {
    host_t host("test_remote_ddl/host_rows");
    host.db.ok(any(), "CREATE SCHEMA otterstax;");
    auto created = host.db.ok(any(), "CREATE TABLE otterstax.params (oid BIGINT, name TEXT);");
    REQUIRE(created->created_oid() != components::catalog::INVALID_OID);
    host.backend().set_parameters_table(created->created_oid());
    const auto rows = [&host] { return host.db.strings(any(), "SELECT name FROM otterstax.params ORDER BY name;"); };
    host.register_shop();
    CHECK(rows() == strings{});
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t1 (id BIGINT);");
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t2 (id BIGINT);");
    CHECK(rows() == strings{"m2.shop.public.t1", "m2.shop.public.t2"});
    host.db.ok(any(), "DROP TABLE m2.shop.public.t1;");
    CHECK(rows() == strings{"m2.shop.public.t2"});
    host.db.check_refused(any(),
                          "CREATE TABLE m2.shop.public.bad1 (id BIGINT);",
                          code::connection_closed,
                          "backend refused \"bad1\"");
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    CHECK(host.db.refused(session, "CREATE TABLE m2.shop.public.t3 (id BIGINT);").type == code::unimplemented_yet);
    host.db.ok(session, "ROLLBACK;");
    CHECK(rows() == strings{"m2.shop.public.t2"});
    host.db.ok(session, "SET search_path = 'm2.shop.public';");
    host.db.ok(session, "CREATE SCHEMA s;");
    host.db.ok(any(), "CREATE TABLE m2.shop.s.t4 (id BIGINT);");
    CHECK(rows() == strings{"m2.shop.public.t2", "m2.shop.s.t4"});
    host.db.ok(session, "DROP SCHEMA s CASCADE;");
    CHECK(rows() == strings{"m2.shop.public.t2"});

    // A DROP the backend refuses: no table goes and no row.
    host.db.check_refused(any(),
                          "CREATE TABLE m2.shop.public.bad2 (id BIGINT);",
                          code::connection_closed,
                          "backend refused \"bad2\"");
    host.db.ok(session, "CREATE SCHEMA s5;");
    host.db.ok(any(), "CREATE TABLE m2.shop.s5.t5 (id BIGINT);");
    host.db.ok(any(), "CREATE TABLE m2.shop.s5.nodrop5 (id BIGINT);");
    const auto before = rows();
    host.db.check_refused(session, "DROP SCHEMA s5 CASCADE;", code::connection_closed, "backend refused \"nodrop5\"");
    CHECK(rows() == before);
    CHECK(relations_named(host, "t5") == 1);
}

#ifdef NDEBUG
// The host's rows come in chunks of at most DEFAULT_VECTOR_CAPACITY rows (in Debug a wider chunk cannot be built); a
// wider one is refused, and nothing of it is written.
TEST_CASE("integration::cpp::remote_ddl::host_rows_wider_than_a_chunk_are_refused") {
    host_t host("test_remote_ddl/wide_host_rows");
    host.db.ok(any(), "CREATE SCHEMA otterstax;");
    auto created = host.db.ok(any(), "CREATE TABLE otterstax.params (oid BIGINT, name TEXT);");
    host.backend().set_parameters_table(created->created_oid());
    host.backend().set_parameters_rows(2500);
    host.register_shop();
    host.db.check_refused(any(),
                          "CREATE TABLE m2.shop.public.t1 (id BIGINT);",
                          code::protocol_violation,
                          "a storage answered a chunk of 2500 rows, more than the 1024 rows of a chunk");
    CHECK(host.db.strings(any(), "SELECT name FROM otterstax.params;") == strings{});
}
#endif

// A column the view does not read comes and goes without making the view stale.
TEST_CASE("integration::cpp::remote_ddl::a_column_the_view_does_not_read_keeps_it_fresh") {
    host_t host("test_remote_ddl/view_fresh");
    host.register_orders();
    host.backend().set_rows("m2.shop.public.orders", {{1, 100}, {2, 200}});
    host.db.ok(any(), "CREATE VIEW public.ov AS SELECT id FROM m2.shop.public.orders;");
    host.db.ok(any(), "ALTER TABLE m2.shop.public.orders DROP COLUMN amount;");
    host.db.ok(any(), "ALTER TABLE m2.shop.public.orders ADD COLUMN label BIGINT;");
    host.backend().set_rows("m2.shop.public.orders", {{1, 7}, {2, 8}});
    CHECK(host.db.ints(any(), "SELECT id FROM public.ov ORDER BY id;") == ints{1, 2});
}

// A column the view reads depends on it like a local one: RESTRICT keeps it, before the backend is asked.
TEST_CASE("integration::cpp::remote_ddl::a_column_the_view_reads_is_kept") {
    host_t host("test_remote_ddl/view_column");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.orders (id BIGINT, amount INT);");
    host.db.ok(any(), "CREATE VIEW public.ov AS SELECT id, amount FROM m2.shop.public.orders;");
    host.backend().clear_journal();
    const auto error = host.db.refused(any(), "ALTER TABLE m2.shop.public.orders DROP COLUMN amount;");
    CHECK(error.type == code::dependent_objects_still_exist);
    CHECK(host.backend().changes().empty());
    CHECK(columns_of(host, "orders") == strings{"id", "amount"});
}

// The backend keeps its own defaults: ADD COLUMN ... DEFAULT of a connection's table is refused, as at CREATE.
TEST_CASE("integration::cpp::remote_ddl::add_column_with_a_default_is_refused") {
    host_t host("test_remote_ddl/add_default");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t2 (id BIGINT);");
    host.backend().clear_journal();
    const auto error = host.db.refused(any(), "ALTER TABLE m2.shop.public.t2 ADD COLUMN x BIGINT DEFAULT 5;");
    CHECK(error.type == code::unimplemented_yet);
    CHECK(columns_of(host, "t2") == strings{"id"});
    CHECK(host.backend().changes().empty());
}

// ADD CONSTRAINT names the table of the connection, never a local table of the same database.schema.table name.
TEST_CASE("integration::cpp::remote_ddl::add_constraint_names_the_table_of_the_connection") {
    host_t host("test_remote_ddl/add_constraint");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t2 (id BIGINT);");
    host.db.ok(any(), "CREATE DATABASE shop;");
    host.db.ok(any(), "CREATE TABLE shop.public.t2 (id BIGINT);");
    host.backend().clear_journal();
    const auto error = host.db.refused(any(), "ALTER TABLE m2.shop.public.t2 ADD CONSTRAINT u UNIQUE (id);");
    CHECK(error.type == code::unimplemented_yet);
    CHECK(host.db.ints(any(), "SELECT count(*) FROM pg_catalog.pg_constraint WHERE conname = 'u';") == ints{0});
    CHECK(host.backend().changes().empty());
}

// A local DROP SCHEMA / DROP DATABASE ... CASCADE that reaches a column of a connection's table through a type drops it
// in the backend, and only in autocommit.
TEST_CASE("integration::cpp::remote_ddl::a_local_drop_cascade_reaches_a_column_of_a_connections_table") {
    host_t host("test_remote_ddl/local_cascade");
    registered(host);
    const std::string refusal = "connection \"m2\" only supports DDL in autocommit";
    SECTION("DROP SCHEMA") {
        host.db.ok(any(), "CREATE SCHEMA loc;");
        host.db.ok(any(), "CREATE TYPE loc.mood AS ENUM ('x');");
        host.db.ok(any(), "CREATE TABLE m2.shop.public.t (id BIGINT, m loc.mood);");
        host.backend().clear_journal();
        const auto session = otterbrix::session_id_t();
        host.db.ok(session, "BEGIN;");
        host.db.check_refused(session, "DROP SCHEMA loc CASCADE;", code::unimplemented_yet, refusal);
        host.db.ok(session, "ROLLBACK;");
        CHECK(host.backend().changes().empty());
        host.db.ok(any(), "DROP SCHEMA loc CASCADE;");
        CHECK(host.backend().changes() == strings{"alter drop m2.shop.public.t m"});
        CHECK(columns_of(host, "t") == strings{"id"});
    }
    SECTION("DROP DATABASE") {
        host.db.ok(any(), "CREATE DATABASE locdb;");
        host.db.ok(any(), "CREATE TYPE locdb.public.mood AS ENUM ('x');");
        host.db.ok(any(), "CREATE TABLE m2.shop.public.t (id BIGINT, m locdb.public.mood);");
        host.backend().clear_journal();
        const auto session = otterbrix::session_id_t();
        host.db.ok(session, "BEGIN;");
        host.db.check_refused(session, "DROP DATABASE locdb;", code::unimplemented_yet, refusal);
        host.db.ok(session, "ROLLBACK;");
        CHECK(host.backend().changes().empty());
        host.db.ok(any(), "DROP DATABASE locdb;");
        CHECK(host.backend().changes() == strings{"alter drop m2.shop.public.t m"});
        CHECK(columns_of(host, "t") == strings{"id"});
    }
}

// One ALTER of a connection's table: its subcommands are checked one after another before any reaches the backend.
TEST_CASE("integration::cpp::remote_ddl::subcommands_of_one_alter_are_checked_in_order") {
    host_t host("test_remote_ddl/alter_sequence");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t2 (id BIGINT, a BIGINT);");
    host.backend().clear_journal();
    host.db.check_refused(any(),
                          "ALTER TABLE m2.shop.public.t2 ADD COLUMN x BIGINT, ADD COLUMN x BIGINT;",
                          code::duplicate_column,
                          "column \"x\" already exists");
    host.db.check_refused(any(),
                          "ALTER TABLE m2.shop.public.t2 DROP COLUMN a, DROP COLUMN a;",
                          code::undefined_column,
                          "column \"a\" of relation \"t2\" does not exist; use DROP COLUMN IF EXISTS to ignore it");
    CHECK(host.backend().changes().empty());
    CHECK(columns_of(host, "t2") == strings{"id", "a"});
    host.db.ok(any(), "ALTER TABLE m2.shop.public.t2 DROP COLUMN a, ADD COLUMN a BIGINT;");
    CHECK(host.backend().changes() == strings{"alter drop m2.shop.public.t2 a", "alter add m2.shop.public.t2 a"});
}

// CREATE TABLE IF NOT EXISTS of a table that is there does nothing, before what a connection's database refuses.
TEST_CASE("integration::cpp::remote_ddl::create_if_not_exists_of_a_table_that_is_there") {
    host_t host("test_remote_ddl/if_not_exists");
    registered(host);
    host.db.ok(any(), "CREATE TABLE m2.shop.public.t2 (id BIGINT);");
    host.backend().clear_journal();
    const auto session = otterbrix::session_id_t();
    host.db.ok(session, "BEGIN;");
    host.db.ok(session, "CREATE TABLE IF NOT EXISTS m2.shop.public.t2 (id BIGINT PRIMARY KEY);");
    host.db.ok(session, "ROLLBACK;");
    CHECK(host.backend().changes().empty());
}
