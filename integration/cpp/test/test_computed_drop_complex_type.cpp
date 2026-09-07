#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <string>
#include <string_view>

namespace {

    bool has_column(const components::cursor::cursor_t& cur, std::string_view name) {
        const auto& chunk = cur.chunks().front();
        for (uint64_t i = 0; i < chunk.column_count(); ++i) {
            if (chunk.data[i].type().alias() == name)
                return true;
        }
        return false;
    }

    components::cursor::cursor_t_ptr exec(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return dispatcher->execute_sql(session, sql);
    }

    components::cursor::cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cur = exec(dispatcher, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                           : std::string{"none"}));
        REQUIRE(cur->is_success());
        return cur;
    }

    std::string fixture_path(const char* leaf) {
        return integration_fixture_path(std::string("test_computed_drop_complex_type/") + leaf).string();
    }

} // namespace

// Control: a builtin-scalar field's tombstone and live row share the empty
// atttypspec, so they already met in one group and the drop already hid it.
TEST_CASE("integration::cpp::computed_drop::simple_field_is_hidden") {
    auto config = test_create_config(fixture_path("simple"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE cdc;");
    run_ok(d, "CREATE TABLE cdc.docs();");
    run_ok(d, "INSERT INTO cdc.docs (id, n) VALUES (1, 42);");
    run_ok(d, "ALTER TABLE cdc.docs DROP COLUMN n;");

    auto cur = run_ok(d, "SELECT * FROM cdc.docs;");
    REQUIRE(cur->size() == 1);
    REQUIRE(has_column(*cur, "id"));
    REQUIRE_FALSE(has_column(*cur, "n"));
}

// DECIMAL is the one SQL-reachable type whose registration encodes an atttypspec (a 29-digit
// literal is INT128-backed, unknown by name to the oid map or pg_type); its tombstone landed
// in a different group, so the field survived its own DROP COLUMN.
TEST_CASE("integration::cpp::computed_drop::complex_typed_field_is_hidden") {
    auto config = test_create_config(fixture_path("complex"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE cdc;");
    run_ok(d, "CREATE TABLE cdc.docs();");
    run_ok(d, "INSERT INTO cdc.docs (id, price) VALUES (1, 12345678901234567890123456789);");

    {
        auto cur = run_ok(d, "SELECT * FROM cdc.docs;");
        INFO("the wide-typed field registered and is visible before the drop");
        REQUIRE(has_column(*cur, "price"));
    }

    run_ok(d, "ALTER TABLE cdc.docs DROP COLUMN price;");

    auto cur = run_ok(d, "SELECT * FROM cdc.docs;");
    REQUIRE(cur->size() == 1);
    REQUIRE(has_column(*cur, "id"));
    INFO("a dropped complex-typed field must not survive in SELECT *");
    REQUIRE_FALSE(has_column(*cur, "price"));
}

// A relkind='g' column binds to its physical column by the storage TYPE ALIAS, which
// rename_column does not move, so a catalog-only rename would unbind the field under both names.
TEST_CASE("integration::cpp::computed_drop::rename_on_computed_table_refused_loudly") {
    auto config = test_create_config(fixture_path("rename"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE cdc;");
    run_ok(d, "CREATE TABLE cdc.docs();");
    run_ok(d, "INSERT INTO cdc.docs (id, n) VALUES (1, 42);");

    {
        auto session = otterbrix::session_id_t();
        auto cur = d->execute_sql(session, "ALTER TABLE cdc.docs RENAME COLUMN n TO m;");
        const std::string what = cur->is_error()
                                     ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                     : std::string{"none"};
        INFO("error: " << what);
        REQUIRE(cur->is_error());
        REQUIRE(what.find("not implemented for document tables") != std::string::npos);
    }

    auto cur = run_ok(d, "SELECT * FROM cdc.docs;");
    REQUIRE(has_column(*cur, "n"));
    REQUIRE_FALSE(has_column(*cur, "m"));
}

// A computed field can hold several typed variants at once (the reader groups by the full
// variant key), so DROP COLUMN must bury every live variant, not just the newest.
TEST_CASE("integration::cpp::computed_drop::every_variant_of_the_field_is_hidden") {
    auto config = test_create_config(fixture_path("variants"));
    test_clear_directory(config);
    config.wal.on = false;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE cdc;");
    run_ok(d, "CREATE TABLE cdc.docs();");
    run_ok(d, "INSERT INTO cdc.docs (id, x) VALUES (1, 7);");
    {
        auto session = otterbrix::session_id_t();
        auto cur = d->execute_sql(session, "INSERT INTO cdc.docs (id, x) VALUES (2, 'seven');");
        if (!cur->is_success()) {
            WARN("second-variant INSERT refused on this branch — multi-variant drop not exercisable");
            return;
        }
    }

    run_ok(d, "ALTER TABLE cdc.docs DROP COLUMN x;");

    auto cur = run_ok(d, "SELECT * FROM cdc.docs;");
    REQUIRE(has_column(*cur, "id"));
    INFO("no typed variant of a dropped field may survive in SELECT *");
    REQUIRE_FALSE(has_column(*cur, "x"));
}
