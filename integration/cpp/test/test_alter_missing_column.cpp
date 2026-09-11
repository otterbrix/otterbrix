// Both operators used to take an early return on a column-name miss and still call
// mark_executed(), reporting success on a no-op. A relkind='g' (document) table has no
// pg_attribute row at all, so the fix routes document DROP/RENAME through pg_computed_column
// rather than making every document ALTER an error.
//
// Reading a DDL cursor's error twice in one process intermittently corrupts it (doubled text
// or std::length_error, reproduced but not fixed here); each error is read exactly once below,
// and the content checks stay since they are what caught the corruption.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>

namespace {

    bool column_present(const components::cursor::cursor_t& cur, std::string_view name) {
        if (cur.chunks().empty()) {
            return false;
        }
        const auto& chunk = cur.chunks().front();
        for (uint64_t i = 0; i < chunk.column_count(); ++i) {
            if (chunk.data[i].type().alias() == name) {
                return true;
            }
        }
        return false;
    }

    std::string error_text(const components::cursor::cursor_t& cur) {
        // get_error() on a successful cursor throws, so read it only when there is one.
        return cur.is_error() ? std::string{cur.get_error().what.begin(), cur.get_error().what.end()}
                              : std::string{"<no error: statement reported success>"};
    }

    bool mentions(const std::string& text, std::string_view needle) { return text.find(needle) != std::string::npos; }

} // namespace

TEST_CASE("integration::cpp::test_alter_missing_column::drop_missing_column_is_refused", "[altermissing]") {
    auto config = test_create_config(integration_fixture_path("test_alter_missing_column/drop_missing"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE amdb;")->is_success());
    REQUIRE(exec("CREATE TABLE amdb.t (a bigint, b text);")->is_success());
    REQUIRE(exec("INSERT INTO amdb.t (a, b) VALUES (1, 'x');")->is_success());

    INFO("the statement itself must be refused, not silently accepted");
    {
        auto cur = exec("ALTER TABLE amdb.t DROP COLUMN nosuchcol;");
        const std::string what = error_text(*cur);
        INFO("error: " << what);
        REQUIRE_FALSE(cur->is_success());
        CHECK(mentions(what, "nosuchcol"));
        CHECK(mentions(what, "\"t\""));
        CHECK(mentions(what, "does not exist"));
    }

    INFO("and the catalog must be exactly as it was: still two columns, both readable");
    {
        auto cur = exec("SELECT * FROM amdb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(cur->column_count() == 2);
        REQUIRE(column_present(*cur, "a"));
        REQUIRE(column_present(*cur, "b"));
    }
}

TEST_CASE("integration::cpp::test_alter_missing_column::drop_missing_column_if_exists_is_accepted", "[altermissing]") {
    auto config = test_create_config(integration_fixture_path("test_alter_missing_column/drop_if_exists"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE amdb;")->is_success());
    REQUIRE(exec("CREATE TABLE amdb.t (a bigint, b text);")->is_success());
    REQUIRE(exec("INSERT INTO amdb.t (a, b) VALUES (1, 'x');")->is_success());

    INFO("IF EXISTS is the ONE form PostgreSQL lets pass on a missing column");
    {
        auto cur = exec("ALTER TABLE amdb.t DROP COLUMN IF EXISTS nosuchcol;");
        INFO("error: " << error_text(*cur));
        REQUIRE(cur->is_success());
    }

    INFO("IF EXISTS on a column that IS there still drops it");
    {
        auto cur = exec("ALTER TABLE amdb.t DROP COLUMN IF EXISTS b;");
        INFO("error: " << error_text(*cur));
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("SELECT * FROM amdb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 1);
        REQUIRE(column_present(*cur, "a"));
        REQUIRE_FALSE(column_present(*cur, "b"));
    }
}

TEST_CASE("integration::cpp::test_alter_missing_column::rename_missing_column_is_refused", "[altermissing]") {
    auto config = test_create_config(integration_fixture_path("test_alter_missing_column/rename_missing"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE amdb;")->is_success());
    REQUIRE(exec("CREATE TABLE amdb.t (a bigint, b text);")->is_success());
    REQUIRE(exec("INSERT INTO amdb.t (a, b) VALUES (1, 'x');")->is_success());

    INFO("renaming a column that is not there is an error, not a no-op success");
    {
        auto cur = exec("ALTER TABLE amdb.t RENAME COLUMN nosuchcol TO renamed;");
        const std::string what = error_text(*cur);
        INFO("error: " << what);
        REQUIRE_FALSE(cur->is_success());
        CHECK(mentions(what, "nosuchcol"));
        CHECK(mentions(what, "\"t\""));
        CHECK(mentions(what, "does not exist"));
    }

    INFO("nothing was invented: no 'renamed' column appeared, both originals stand");
    {
        auto cur = exec("SELECT * FROM amdb.t;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->column_count() == 2);
        REQUIRE(column_present(*cur, "a"));
        REQUIRE(column_present(*cur, "b"));
        REQUIRE_FALSE(column_present(*cur, "renamed"));
    }
}

TEST_CASE("integration::cpp::test_alter_missing_column::document_table_drop_existing_field_still_works",
          "[altermissing]") {
    auto config = test_create_config(integration_fixture_path("test_alter_missing_column/doc_drop"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE amdb;")->is_success());
    INFO("an empty CREATE TABLE makes a relkind='g' table");
    REQUIRE(exec("CREATE TABLE amdb.docs ();")->is_success());
    REQUIRE(exec("INSERT INTO amdb.docs (a, b) VALUES (1, 'x');")->is_success());

    INFO("dropping a field that IS there must keep working after the miss is made loud");
    {
        auto cur = exec("ALTER TABLE amdb.docs DROP COLUMN b;");
        INFO("error: " << error_text(*cur));
        REQUIRE(cur->is_success());
    }
    {
        auto cur = exec("SELECT * FROM amdb.docs;");
        REQUIRE(cur->is_success());
        REQUIRE(column_present(*cur, "a"));
        REQUIRE_FALSE(column_present(*cur, "b"));
    }
}

TEST_CASE("integration::cpp::test_alter_missing_column::document_table_drop_missing_field_is_refused",
          "[altermissing]") {
    auto config = test_create_config(integration_fixture_path("test_alter_missing_column/doc_drop_missing"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE amdb;")->is_success());
    REQUIRE(exec("CREATE TABLE amdb.docs ();")->is_success());
    REQUIRE(exec("INSERT INTO amdb.docs (a, b) VALUES (1, 'x');")->is_success());

    INFO("a document table gets the SAME answer as a regular one — no per-relkind fallback");
    {
        auto cur = exec("ALTER TABLE amdb.docs DROP COLUMN nosuchcol;");
        const std::string what = error_text(*cur);
        INFO("error: " << what);
        REQUIRE_FALSE(cur->is_success());
        CHECK(mentions(what, "nosuchcol"));
        CHECK(mentions(what, "\"docs\""));
        CHECK(mentions(what, "does not exist"));
    }
    {
        auto cur = exec("SELECT * FROM amdb.docs;");
        REQUIRE(cur->is_success());
        REQUIRE(column_present(*cur, "a"));
        REQUIRE(column_present(*cur, "b"));
    }
}

TEST_CASE("integration::cpp::test_alter_missing_column::document_table_rename_is_refused_not_lied_about",
          "[altermissing]") {
    auto config = test_create_config(integration_fixture_path("test_alter_missing_column/doc_rename"));
    test_clear_directory(config);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();
    auto exec = [&](const std::string& sql) {
        auto session = otterbrix::session_id_t();
        return d->execute_sql(session, sql);
    };

    REQUIRE(exec("CREATE DATABASE amdb;")->is_success());
    REQUIRE(exec("CREATE TABLE amdb.docs ();")->is_success());
    REQUIRE(exec("INSERT INTO amdb.docs (a, b) VALUES (1, 'x');")->is_success());

    // The storage half can't be completed: a relkind='g' column binds to its physical column
    // by the storage column's type alias, and rename_column updates column_definition_t::name_
    // but leaves that alias alone, so the field would vanish from SELECT under both names.
    INFO("renaming a field that IS there is refused with a reason, never reported as renamed");
    {
        auto cur = exec("ALTER TABLE amdb.docs RENAME COLUMN b TO c;");
        const std::string what = error_text(*cur);
        INFO("error: " << what);
        REQUIRE_FALSE(cur->is_success());
        CHECK(mentions(what, "not implemented"));
        CHECK(mentions(what, "\"b\""));
    }

    INFO("and the refusal changed nothing: b is still there, under its own name, readable");
    {
        auto cur = exec("SELECT * FROM amdb.docs;");
        REQUIRE(cur->is_success());
        REQUIRE(cur->size() == 1);
        REQUIRE(column_present(*cur, "a"));
        REQUIRE(column_present(*cur, "b"));
        REQUIRE_FALSE(column_present(*cur, "c"));
    }

    INFO("the refusal is about the FORM, so a field that is not there gets it too");
    {
        auto cur = exec("ALTER TABLE amdb.docs RENAME COLUMN nosuchcol TO z;");
        const std::string what = error_text(*cur);
        INFO("error: " << what);
        REQUIRE_FALSE(cur->is_success());
    }
}
