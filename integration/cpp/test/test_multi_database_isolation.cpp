#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/compute/function.hpp>

#include <string>
#include <tuple>

// Regression tests for issue #557: two tables with the same name in different databases must be fully
// independent. Before the fix, name→OID resolution scanned pg_class by relname alone (the
// relnamespace filter never fired because the namespace OID was read at plan-generation time, before
// the sibling resolve_namespace operator stamped it), so `db2.t1` resolved to whichever same-named
// table was created first — a cross-database data leak.
// The fix: operator_resolve_table_t translates the user-typed dbname to a namespace oid itself at
// execution time (mirroring operator_resolve_type_t) and always scans pg_class by (relname,
// relnamespace) for qualified names. The relname-only scan survives only for unqualified names.

TEST_CASE("integration::cpp::multi_database_isolation::same_name_select") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/same_name_select"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db1;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db2;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db1.t1 (id BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE db2.t1 (id BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db1.t1 (id) VALUES (1);")->is_success());
    }

    // The core of issue #557: db2.t1 is empty and must stay empty.
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 0);
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 1);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::same_name_dml_routing") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/same_name_dml_routing"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    for (const auto* sql : {"CREATE DATABASE db1;",
                            "CREATE DATABASE db2;",
                            "CREATE TABLE db1.t1 (id BIGINT);",
                            "CREATE TABLE db2.t1 (id BIGINT);",
                            "INSERT INTO db1.t1 (id) VALUES (10);",
                            "INSERT INTO db2.t1 (id) VALUES (20);"}) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 10);
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 20);
    }

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "UPDATE db2.t1 SET id = 21;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 10);
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 21);
    }

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "DELETE FROM db1.t1;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 0);
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 21);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::same_name_drop") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/same_name_drop"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    for (const auto* sql : {"CREATE DATABASE db1;",
                            "CREATE DATABASE db2;",
                            "CREATE TABLE db1.t1 (id BIGINT);",
                            "CREATE TABLE db2.t1 (id BIGINT);",
                            "INSERT INTO db1.t1 (id) VALUES (1);"}) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "DROP TABLE db2.t1;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 1);
    }
    // db2.t1 is gone: selecting it must not silently read db1.t1.
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1;");
        REQUIRE(c->is_error());
        REQUIRE(c->get_error().type == core::error_code_t::table_not_exists);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::missing_table_not_aliased") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/missing_table_not_aliased"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    for (const auto* sql : {"CREATE DATABASE db1;",
                            "CREATE DATABASE db2;",
                            "CREATE TABLE db1.t1 (id BIGINT);",
                            "INSERT INTO db1.t1 (id) VALUES (1);"}) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    // db2 exists but has no t1: the select must fail, not read db1.t1.
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1;");
        REQUIRE(c->is_error());
        REQUIRE(c->get_error().type == core::error_code_t::table_not_exists);
    }
    // INSERT must not create-or-route into db1's table either.
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "INSERT INTO db2.t1 (id) VALUES (99);");
        REQUIRE(c->is_error());
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 1);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::nonexistent_database_errors") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/nonexistent_database_errors"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    for (const auto* sql :
         {"CREATE DATABASE db1;", "CREATE TABLE db1.t1 (id BIGINT);", "INSERT INTO db1.t1 (id) VALUES (1);"}) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    // A table named t1 exists (in db1), but nosuchdb does not. Before the
    // fix the mis-resolved table node poisoned ns_by_dbname["nosuchdb"] and
    // this returned db1's rows instead of an error.
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM nosuchdb.t1;");
        REQUIRE(c->is_error());
        REQUIRE(c->get_error().type == core::error_code_t::database_not_exists);
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "INSERT INTO nosuchdb.t1 (id) VALUES (2);");
        REQUIRE(c->is_error());
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::view_resolves_in_own_database") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/view_resolves_in_own_database"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    for (const auto* sql : {"CREATE DATABASE db1;",
                            "CREATE DATABASE db2;",
                            "CREATE TABLE db1.t1 (id BIGINT);",
                            "CREATE TABLE db2.t1 (id BIGINT);",
                            "INSERT INTO db1.t1 (id) VALUES (10);",
                            "INSERT INTO db2.t1 (id) VALUES (20);",
                            "CREATE VIEW db2.v AS SELECT id FROM db2.t1;"}) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    // The view body must resolve t1 against db2, not db1 — this covers the
    // view-expansion fresh-resolve path, where the namespace sibling is
    // filtered out of the re-resolve sub-plan and the table operator must
    // resolve the dbname on its own.
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.v;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 20);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::unique_constraint_binds_to_own_table") {
    auto config =
        test_create_config(integration_fixture_path("test_multi_db_isolation/unique_constraint_binds_to_own_table"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    for (const auto* sql : {"CREATE DATABASE db1;",
                            "CREATE DATABASE db2;",
                            "CREATE TABLE db1.t1 (id BIGINT);",
                            "CREATE TABLE db2.t1 (id BIGINT);",
                            "ALTER TABLE db2.t1 ADD CONSTRAINT uq_t1_id UNIQUE (id);",
                            "INSERT INTO db2.t1 (id) VALUES (1);"}) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    // db1.t1 has no unique constraint: duplicate values are fine — and the
    // constraint attached to db2.t1 must not bleed over.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db1.t1 (id) VALUES (1);")->is_success());
        auto session2 = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session2, "INSERT INTO db1.t1 (id) VALUES (1);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "INSERT INTO db2.t1 (id) VALUES (1);");
        REQUIRE(c->is_error());
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 2);
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::index_isolation") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/index_isolation"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    for (const auto* sql : {"CREATE DATABASE db1;",
                            "CREATE DATABASE db2;",
                            "CREATE TABLE db1.t1 (id BIGINT);",
                            "CREATE TABLE db2.t1 (id BIGINT);",
                            "CREATE INDEX idx_id ON db1.t1 (id);",
                            "CREATE INDEX idx_id ON db2.t1 (id);",
                            "INSERT INTO db1.t1 (id) VALUES (10);",
                            "INSERT INTO db2.t1 (id) VALUES (20);"}) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1 WHERE id = 10;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 10);
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1 WHERE id = 10;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 0);
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1 WHERE id = 20;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 20);
    }

    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "DROP INDEX db2.t1.idx_id;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1 WHERE id = 10;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1 WHERE id = 20;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::cross_database_join") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/cross_database_join"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    for (const auto* sql : {"CREATE DATABASE db1;",
                            "CREATE DATABASE db2;",
                            "CREATE TABLE db1.t1 (id BIGINT, a BIGINT);",
                            "CREATE TABLE db2.t1 (id BIGINT, b BIGINT);",
                            "INSERT INTO db1.t1 (id, a) VALUES (1, 100);",
                            "INSERT INTO db2.t1 (id, b) VALUES (1, 200);"}) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    // One statement touching both same-named tables: each side must bind to
    // its own database's store (covers the executor's per-key resolve dedup).
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session,
                                         "SELECT * FROM db1.t1 INNER JOIN db2.t1 "
                                         "ON db1.t1.id >= db2.t1.id AND db1.t1.id <= db2.t1.id;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->column_count() == 4);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::alter_column_isolated") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/alter_column_isolated"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    for (const auto* sql : {"CREATE DATABASE db1;",
                            "CREATE DATABASE db2;",
                            "CREATE TABLE db1.t1 (id BIGINT);",
                            "CREATE TABLE db2.t1 (id BIGINT);",
                            "INSERT INTO db1.t1 (id) VALUES (10);",
                            "INSERT INTO db2.t1 (id) VALUES (20);"}) {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
    }

    // ALTER on db2.t1 must not touch db1.t1 (ALTER rides the AnyName grammar
    // path where the qualifier arrives via the schema position).
    //
    // NOTE: this case asserts isolation only — that the ALTER binds to db2's
    // table and db1 stays untouched. It deliberately does not assert that the
    // rename itself took effect; that is now gated by
    // integration/cpp/test/test_alter_rename_column.cpp.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "ALTER TABLE db2.t1 RENAME COLUMN id TO id2;")->is_success());
    }
    // db1.t1 keeps its original column and its single row (before the fix,
    // the INSERT mis-route alone already gave db1.t1 two rows here).
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT id FROM db1.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 10);
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 20);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::unqualified_names_preserved") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/unqualified_names_preserved"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // Unqualified CREATE TABLE keeps working: naming no database, it lands in
    // public (test_unqualified_name_ambiguity.cpp covers unqualified access).
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t1 (id BIGINT);")->is_success());
    }

    // A table created unqualified lives in public, so another database's
    // qualifier does not reach it.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE DATABASE db1;")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1;");
        REQUIRE(c->is_error());
        REQUIRE(c->get_error().type == core::error_code_t::table_not_exists);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::restart_persistence_isolation") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/restart_persistence_isolation"));
    test_clear_directory(config);
    // disk + WAL ON: isolation must survive checkpoint/recovery.

    INFO("phase 1: create same-named tables in two databases, insert distinct rows");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        for (const auto* sql : {"CREATE DATABASE db1;",
                                "CREATE DATABASE db2;",
                                "CREATE TABLE db1.t1 (id BIGINT);",
                                "CREATE TABLE db2.t1 (id BIGINT);",
                                "INSERT INTO db1.t1 (id) VALUES (10);",
                                "INSERT INTO db2.t1 (id) VALUES (20);"}) {
            auto session = otterbrix::session_id_t();
            REQUIRE(dispatcher->execute_sql(session, sql)->is_success());
        }
    }

    INFO("phase 2: restart — both tables keep exactly their own rows");
    {
        test_spaces space(config);
        auto* dispatcher = space.dispatcher();
        {
            auto session = otterbrix::session_id_t();
            auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1;");
            REQUIRE(c->is_success());
            REQUIRE(c->size() == 1);
            REQUIRE(c->value(0, 0).value<int64_t>() == 10);
        }
        {
            auto session = otterbrix::session_id_t();
            auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1;");
            REQUIRE(c->is_success());
            REQUIRE(c->size() == 1);
            REQUIRE(c->value(0, 0).value<int64_t>() == 20);
        }
    }
}

// uid and schema are federation slots: this catalog stores a relation under a database and nothing
// else, a statement that WRITES through one of them is refused rather than quietly landing in the
// database slot with the segment dropped.
TEST_CASE("integration::cpp::multi_database_isolation::write_through_a_schema_segment_is_refused") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/schema_segment_write"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE d;")->is_success());
    REQUIRE(exec("CREATE TABLE d.t (id BIGINT);")->is_success());
    REQUIRE(exec("INSERT INTO d.t (id) VALUES (1), (2);")->is_success());

    for (const auto* sql : {"CREATE TABLE d.s.t2 (id BIGINT);",
                            "CREATE TABLE u.d.s.t3 (id BIGINT);",
                            "INSERT INTO d.s.t (id) VALUES (3);",
                            "UPDATE d.s.t SET id = 9;",
                            "DELETE FROM d.s.t;",
                            "DROP TABLE d.s.t;"}) {
        auto cursor = exec(sql);
        INFO("[" << sql << "] " << (cursor->is_error() ? cursor->get_error().what.c_str() : "<accepted>"));
        REQUIRE(cursor->is_error());
        REQUIRE(cursor->get_error().type == core::error_code_t::invalid_parameter);
    }

    INFO("nothing above landed: the table keeps its rows and no relation was created");
    auto rows = exec("SELECT id FROM d.t;");
    REQUIRE(rows->is_success());
    REQUIRE(rows->size() == 2);
    auto created = exec("SELECT relname FROM pg_catalog.pg_class WHERE relname = 't2' OR relname = 't3';");
    REQUIRE(created->is_success());
    REQUIRE(created->size() == 0);
}

// The READ path keeps the federation slots
TEST_CASE("integration::cpp::multi_database_isolation::read_through_a_schema_segment_resolves") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/schema_segment_read"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE d;")->is_success());
    REQUIRE(exec("CREATE TABLE d.t (id BIGINT);")->is_success());
    REQUIRE(exec("INSERT INTO d.t (id) VALUES (1), (2);")->is_success());

    auto through_schema = exec("SELECT id FROM d.s.t;");
    INFO("[SELECT id FROM d.s.t;] " << (through_schema->is_error() ? through_schema->get_error().what.c_str()
                                                                   : "<no error>"));
    REQUIRE(through_schema->is_success());
    REQUIRE(through_schema->size() == 2);
}

TEST_CASE("integration::cpp::multi_database_isolation::create_index_on_a_bare_table_name") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/index_bare_name"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    };
    auto relnamespace_of = [&](const std::string& relname) {
        auto cursor = exec("SELECT relnamespace FROM pg_catalog.pg_class WHERE relname = '" + relname + "';");
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 1);
        return cursor->value(0, 0).value<uint32_t>();
    };

    REQUIRE(exec("CREATE DATABASE dix;")->is_success());
    REQUIRE(exec("CREATE TABLE dix.t (id BIGINT);")->is_success());
    REQUIRE(exec("INSERT INTO dix.t (id) VALUES (1), (2);")->is_success());

    auto created = exec("CREATE INDEX idx_bare ON t (id);");
    INFO("[CREATE INDEX idx_bare ON t (id);] " << (created->is_error() ? created->get_error().what.c_str() : "<ok>"));
    REQUIRE(created->is_success());
    CHECK(relnamespace_of("idx_bare") == relnamespace_of("t"));

    auto rows = exec("SELECT id FROM dix.t WHERE id = 1;");
    REQUIRE(rows->is_success());
    CHECK(rows->size() == 1);
}

namespace {
    core::error_t call_probe_exec(components::compute::kernel_context&,
                                  const components::vector::data_chunk_t& in,
                                  components::vector::vector_t& out) {
        const auto* source = in.data[0].data<int64_t>();
        auto* destination = out.data<int64_t>();
        for (uint64_t row = 0; row < in.size(); ++row) {
            destination[row] = source[row] + 1;
        }
        return core::error_t::no_error();
    }

    components::compute::function_ptr make_call_probe(std::pmr::memory_resource* resource) {
        using namespace components::compute;
        function_doc doc{"short_doc", "full_doc", {"arg"}, false};
        auto fn = std::make_unique<vector_function>("call_probe", arity::unary(), doc, 1);
        kernel_signature_t sig(function_type_t::vector,
                               {parameter_type::exact(components::types::logical_type::BIGINT)},
                               {output_type::fixed(components::types::logical_type::BIGINT)});
        std::ignore = fn->add_kernel(resource, vector_kernel{std::move(sig), call_probe_exec});
        return fn;
    }

    void seed_named_rows(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE d;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE d.t (id BIGINT, name TEXT);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO d.t (id, name) VALUES (1, 'a'), (2, 'bb'), (3, 'bb');")
                    ->is_success());
    }

    void require_answered(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql, std::size_t rows) {
        auto cursor = test_helpers::exec(dispatcher, sql);
        INFO("[" << sql << "] " << (cursor->is_error() ? cursor->get_error().what.c_str() : "<no error>"));
        REQUIRE(cursor->is_success());
        CHECK(cursor->size() == rows);
    }

    // The refusal spells the call out as written, so it says which spelling was turned down.
    void require_call_refused(otterbrix::wrapper_dispatcher_t* dispatcher,
                              const std::string& sql,
                              core::error_code_t code,
                              const std::string& written) {
        auto cursor = test_helpers::exec(dispatcher, sql);
        INFO("[" << sql << "] " << (cursor->is_error() ? cursor->get_error().what.c_str() : "<accepted>"));
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == code);
        CHECK(std::string{cursor->get_error().what.c_str()}.find(written) != std::string::npos);
    }
} // namespace

TEST_CASE("integration::cpp::multi_database_isolation::a_call_reaches_only_what_its_namespace_holds") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/call_namespace"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_named_rows(dispatcher);
    REQUIRE_FALSE(
        dispatcher->register_udf(otterbrix::session_id_t(), make_call_probe(dispatcher->resource())).contains_error());

    for (const auto* sql : {"SELECT length(name) FROM d.t;",
                            "SELECT pg_catalog.length(name) FROM d.t;",
                            "SELECT call_probe(id) FROM d.t;",
                            "SELECT public.call_probe(id) FROM d.t;"}) {
        require_answered(dispatcher, sql, 3);
    }
    require_call_refused(dispatcher,
                         "SELECT public.length(name) FROM d.t;",
                         core::error_code_t::unrecognized_function,
                         "public.length");
    require_call_refused(dispatcher,
                         "SELECT pg_catalog.call_probe(id) FROM d.t;",
                         core::error_code_t::unrecognized_function,
                         "pg_catalog.call_probe");
}

TEST_CASE("integration::cpp::multi_database_isolation::a_call_naming_a_database_is_refused") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/call_database"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_named_rows(dispatcher);

    for (const auto& [sql, written] : std::initializer_list<std::pair<const char*, const char*>>{
             {"SELECT d.length(name) FROM d.t;", "d.length"},
             {"SELECT d.count(*) FROM d.t;", "d.count"},
             {"SELECT count(id) FROM d.t GROUP BY name HAVING d.count(id) > 1;", "d.count"},
             {"SELECT * FROM d.generate_series(1, 3);", "d.generate_series"}}) {
        require_call_refused(dispatcher, sql, core::error_code_t::unimplemented_yet, written);
    }
    require_answered(dispatcher, "SELECT * FROM pg_catalog.generate_series(1, 3);", 3);
}

TEST_CASE("integration::cpp::multi_database_isolation::a_call_through_a_schema_segment_is_refused") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/call_schema_segment"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_named_rows(dispatcher);

    require_call_refused(dispatcher,
                         "SELECT d.s.length(name) FROM d.t;",
                         core::error_code_t::invalid_parameter,
                         "d.s.length");
    require_call_refused(dispatcher,
                         "SELECT u.d.s.length(name) FROM d.t;",
                         core::error_code_t::invalid_parameter,
                         "u.d.s.length");
}

TEST_CASE("integration::cpp::multi_database_isolation::a_qualified_call_matches_its_bare_grouping_key") {
    auto config = test_create_config(integration_fixture_path("test_multi_db_isolation/call_grouping_key"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_named_rows(dispatcher);

    require_answered(dispatcher, "SELECT pg_catalog.upper(name) FROM d.t GROUP BY upper(name);", 2);
}
