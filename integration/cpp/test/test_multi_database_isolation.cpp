#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

// Regression tests for issue #557: two tables with the same name in different
// databases must be fully independent. Before the fix, name→OID resolution
// scanned pg_class by relname alone (the relnamespace filter never fired
// because the namespace OID was read at plan-generation time, before the
// sibling resolve_namespace operator stamped it), so `db2.t1` resolved to
// whichever same-named table was created first — a cross-database data leak.
//
// The fix: operator_resolve_table_t translates the user-typed dbname to a
// namespace oid itself at execution time (mirroring operator_resolve_type_t)
// and then always scans pg_class by (relname, relnamespace) for qualified
// names. The relname-only scan survives ONLY for unqualified names.

TEST_CASE("integration::cpp::multi_database_isolation::same_name_select") {
    auto config = test_create_config("/tmp/test_multi_db_isolation/same_name_select");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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
    // db1.t1 still owns its row.
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 1);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::same_name_dml_routing") {
    auto config = test_create_config("/tmp/test_multi_db_isolation/same_name_dml_routing");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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

    // Each table sees exactly its own row.
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

    // UPDATE routed to db2.t1 must not touch db1.t1.
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

    // DELETE routed to db1.t1 must not touch db2.t1.
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
    auto config = test_create_config("/tmp/test_multi_db_isolation/same_name_drop");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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

    // Dropping db2.t1 must not take db1.t1 (or its rows) with it.
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
    // db2.t1 is gone: selecting it must NOT silently read db1.t1.
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1;");
        REQUIRE(c->is_error());
        REQUIRE(c->get_error().type == core::error_code_t::table_not_exists);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::missing_table_not_aliased") {
    auto config = test_create_config("/tmp/test_multi_db_isolation/missing_table_not_aliased");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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
    auto config = test_create_config("/tmp/test_multi_db_isolation/nonexistent_database_errors");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    for (const auto* sql : {"CREATE DATABASE db1;",
                            "CREATE TABLE db1.t1 (id BIGINT);",
                            "INSERT INTO db1.t1 (id) VALUES (1);"}) {
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
    // db1.t1 is untouched.
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db1.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::view_resolves_in_own_database") {
    auto config = test_create_config("/tmp/test_multi_db_isolation/view_resolves_in_own_database");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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
    auto config = test_create_config("/tmp/test_multi_db_isolation/unique_constraint_binds_to_own_table");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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

    // db1.t1 has NO unique constraint: duplicate values are fine — and the
    // constraint attached to db2.t1 must not bleed over.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "INSERT INTO db1.t1 (id) VALUES (1);")->is_success());
        auto session2 = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session2, "INSERT INTO db1.t1 (id) VALUES (1);")->is_success());
    }
    // db2.t1's own constraint still enforces.
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
    auto config = test_create_config("/tmp/test_multi_db_isolation/index_isolation");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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

    // Keyed lookups hit each table's own index and own rows.
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

    // Dropping db2's same-named index must not break db1's.
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
    auto config = test_create_config("/tmp/test_multi_db_isolation/cross_database_join");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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

    // One statement touching BOTH same-named tables: each side must bind to
    // its own database's store (covers the executor's per-key resolve dedup).
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session,
                                         "SELECT * FROM db1.t1 INNER JOIN db2.t1 "
                                         "ON db1.t1.id >= db2.t1.id AND db1.t1.id <= db2.t1.id;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        // 4 columns: db1.t1(id, a) + db2.t1(id, b); values 1, 100, 1, 200.
        REQUIRE(c->column_count() == 4);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::alter_column_isolated") {
    auto config = test_create_config("/tmp/test_multi_db_isolation/alter_column_isolated");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
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
    // NOTE: this case asserts ISOLATION only — that the ALTER binds to db2's
    // table and db1 stays untouched. It deliberately does NOT assert that the
    // rename itself took effect: node_alter_column_t::set_attoid has no
    // callers anywhere in the pipeline, so operator_alter_column_rename_t
    // no-ops with attoid_==INVALID_OID and reports success — a pre-existing
    // defect independent of cross-database isolation, tracked separately.
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
    // db2.t1's data is intact and its own.
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM db2.t1;");
        REQUIRE(c->is_success());
        REQUIRE(c->size() == 1);
        REQUIRE(c->value(0, 0).value<int64_t>() == 20);
    }
}

TEST_CASE("integration::cpp::multi_database_isolation::unqualified_names_preserved") {
    auto config = test_create_config("/tmp/test_multi_db_isolation/unqualified_names_preserved");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // Unqualified handling must be UNCHANGED by the isolation fix (the
    // relname-only scan is preserved for empty dbnames; no session
    // default-database substitution exists). What "unchanged" means today:
    // unqualified CREATE TABLE succeeds (pg_class row with
    // relnamespace=INVALID), but unqualified DML against it fails validation
    // ("collection does not exist") because gather_plan_resolve_index drops
    // metadata whose namespace is INVALID — a pre-existing gap on main
    // (verified on unpatched b3eb02ba), tracked separately as part of the
    // default-database/search-path design task.
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TABLE t1 (id BIGINT);")->is_success());
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "INSERT INTO t1 (id) VALUES (7);");
        REQUIRE(c->is_error()); // pre-existing: identical failure on unpatched main
    }
    {
        auto session = otterbrix::session_id_t();
        auto c = dispatcher->execute_sql(session, "SELECT * FROM t1;");
        REQUIRE(c->is_error()); // pre-existing: same tbl_md gate as INSERT
    }

    // Intended behavior change pinned: a table created UNQUALIFIED
    // (relnamespace = INVALID) is NOT reachable through a database-qualified
    // name — previously the relname-only scan leak-matched it.
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
    auto config = test_create_config("/tmp/test_multi_db_isolation/restart_persistence_isolation");
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
