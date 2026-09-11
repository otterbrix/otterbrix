// Root cause: confkey (the parent side of an FK) got no per-column pg_depend edge, so
// dropping a referenced parent column passed the dependency check and bricked child inserts.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

namespace {

    // The PK on parent.id adds a same-table 'i' edge alongside the FK's cross-table edge, so
    // the refusal below must be selective, not "any dependent blocks".
    void seed(otterbrix::wrapper_dispatcher_t* d) {
        auto exec = [&](const std::string& sql) {
            auto session = otterbrix::session_id_t();
            return d->execute_sql(session, sql);
        };
        REQUIRE(exec("CREATE DATABASE fkdrop;")->is_success());
        REQUIRE(exec("CREATE TABLE fkdrop.parent (id bigint, name text);")->is_success());
        REQUIRE(exec("CREATE TABLE fkdrop.child (id bigint, pid bigint);")->is_success());
        REQUIRE(exec("ALTER TABLE fkdrop.parent ADD CONSTRAINT parent_pk PRIMARY KEY (id);")->is_success());
        REQUIRE(exec("ALTER TABLE fkdrop.child ADD CONSTRAINT fk_pid FOREIGN KEY (pid) "
                     "REFERENCES fkdrop.parent (id);")
                    ->is_success());
        REQUIRE(exec("INSERT INTO fkdrop.parent (id, name) VALUES (1, 'one'), (2, 'two');")->is_success());
        REQUIRE(exec("INSERT INTO fkdrop.child (id, pid) VALUES (10, 1);")->is_success());
    }

} // namespace

TEST_CASE("integration::cpp::test_fk_parent_column_drop::referenced_parent_column_cannot_be_dropped", "[fkdropcol]") {
    auto config = test_create_config(integration_fixture_path("test_fk_parent_column_drop/referenced"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    seed(d);

    {
        auto cur = exec("ALTER TABLE fkdrop.parent DROP COLUMN id;");
        // get_error() throws on a successful cursor, so it's read only behind is_error().
        const std::string what = cur->is_error()
                                     ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                     : std::string{"none"};
        INFO("error: " << what);
        INFO("a column a live FOREIGN KEY references may not be dropped out from under it");
        REQUIRE(cur->is_error());
        CHECK(what.find("fk_pid") != std::string::npos);
        CHECK(what.find("child") != std::string::npos);
        CHECK(what.find("id") != std::string::npos);
    }

    {
        auto cur = exec("SELECT id, name FROM fkdrop.parent WHERE id = 1;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 1);
    }
    {
        auto cur = exec("INSERT INTO fkdrop.child (id, pid) VALUES (11, 2);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        INFO("a valid child insert must still be accepted after the refused DROP");
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("SELECT id FROM fkdrop.child;");
        REQUIRE(cur->is_success());
        CHECK(cur->size() == 2);
    }
    {
        auto cur = exec("INSERT INTO fkdrop.child (id, pid) VALUES (12, 999);");
        INFO("a child row pointing at a missing parent must still be rejected");
        CHECK(cur->is_error());
    }
}

TEST_CASE("integration::cpp::test_fk_parent_column_drop::unreferenced_parent_column_still_drops", "[fkdropcol]") {
    auto config = test_create_config(integration_fixture_path("test_fk_parent_column_drop/unreferenced"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    seed(d);

    // Complement of the guard above: parent.name has no dependent edge, so its drop must
    // still succeed.
    {
        auto cur = exec("ALTER TABLE fkdrop.parent DROP COLUMN name;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("INSERT INTO fkdrop.child (id, pid) VALUES (11, 2);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
}

TEST_CASE("integration::cpp::test_fk_parent_column_drop::child_key_column_drop_takes_the_constraint_with_it",
          "[fkdropcol]") {
    auto config = test_create_config(integration_fixture_path("test_fk_parent_column_drop/child_side"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    seed(d);

    // Child-side baseline the parent fix was measured against: dropping an FK's own key
    // column cascades the constraint away.
    {
        auto cur = exec("ALTER TABLE fkdrop.child DROP COLUMN pid;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("INSERT INTO fkdrop.child (id) VALUES (13);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("ALTER TABLE fkdrop.parent DROP COLUMN id;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
}

TEST_CASE("integration::cpp::test_fk_parent_column_drop::dropping_the_parent_table_clears_the_constraint",
          "[fkdropcol]") {
    auto config = test_create_config(integration_fixture_path("test_fk_parent_column_drop/drop_parent_table"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    seed(d);

    // CASCADE is required since #638 (the bare form is RESTRICT and refuses this edge; see
    // test_drop_default_restrict.cpp).
    {
        auto cur = exec("DROP TABLE fkdrop.parent CASCADE;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("INSERT INTO fkdrop.child (id, pid) VALUES (14, 999);");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        INFO("the constraint went with the parent table; the child must not be left bricked");
        REQUIRE(cur->is_success());
    }
}

TEST_CASE("integration::cpp::test_fk_parent_column_drop::drop_constraint_does_not_claim_success", "[fkdropcol]") {
    auto config = test_create_config(integration_fixture_path("test_fk_parent_column_drop/drop_constraint"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };
    seed(d);

    // "Does not claim success" means the DROP must both succeed and actually remove
    // enforcement, not merely report success while the constraint still blocks inserts.
    {
        auto cur = exec("ALTER TABLE fkdrop.child DROP CONSTRAINT fk_pid;");
        INFO("error: " << (cur->is_error() ? cur->get_error().what : "none"));
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("INSERT INTO fkdrop.child (id, pid) VALUES (15, 999);");
        CHECK(cur->is_success());
    }
}
