// ALTER TABLE ... DROP / RENAME COLUMN naming a column that is not there.
//
// Both operators resolved the column by (attrelid, attname) and, on a miss, took
// an early return ending in mark_executed() -- reporting SUCCESS having written
// nothing. PostgreSQL refuses both ("column ... does not exist"), and only
// DROP COLUMN IF EXISTS passes; RENAME has no IF EXISTS form for the column.
//
// A relkind='g' (document) table keeps its columns in pg_computed_column, not
// pg_attribute, so EVERY name misses the pg_attribute lookup these operators do.
// Turning the miss into an error without routing relkind='g' elsewhere would
// refuse legal statements on every document table, so the document cases below
// hold ONE rule for both table shapes: DROP of an existing field still routes to
// the pg_computed_column operator and stays green; DROP of a missing field is
// refused like on a regular table; RENAME on a document table is refused with
// "not implemented" (never reported as done), since the storage half can't be
// completed on this branch (see that case for why).
//
// ctest gives every case its own process. Running the whole tag in ONE process is
// flaky: a refusal message built from an ALTER operator's resource can arrive
// with a size spanning later copies of itself, so re-reading a cursor's error can
// come back doubled/tripled or throw std::length_error. Reproduced but not fixed
// here -- so every message is kept short and each cursor's error is read exactly
// ONCE. The message-content checks stay; they are what caught it.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>
#include <string_view>

namespace {

    // The column names a SELECT actually produced, read off the first chunk's
    // vector aliases (same probe test_sql_features::has_column uses).
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

    // Checked for the words that make the refusal actionable, not just that it refused.
    // Read ONCE per cursor and keep the copy -- same convention as
    // test_fk_parent_column_drop: a second read of a DDL cursor's `what` on this branch
    // intermittently comes back doubled or throws std::length_error, a separate engine
    // defect these cases are not here to pin.
    std::string error_text(const components::cursor::cursor_t& cur) {
        // get_error() on a successful cursor throws, so read it only when there is one.
        return cur.is_error() ? std::string{cur.get_error().what.begin(), cur.get_error().what.end()}
                              : std::string{"<no error: statement reported success>"};
    }

    bool mentions(const std::string& text, std::string_view needle) { return text.find(needle) != std::string::npos; }

} // namespace

// ---------------------------------------------------------------------------
// Regular (relkind='r') tables — the columns live in pg_attribute.
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::test_alter_missing_column::drop_missing_column_is_refused", "[altermissing]") {
    auto config = test_create_config(integration_fixture_path("test_alter_missing_column/drop_missing"));
    test_clear_directory(config);
    config.wal.on = false;
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
    config.wal.on = false;
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
    config.wal.on = false;
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

// ---------------------------------------------------------------------------
// Document (relkind='g') tables — the columns live in pg_computed_column and
// have NO pg_attribute row. These are the legal paths a loud pg_attribute miss
// would brick.
// ---------------------------------------------------------------------------

TEST_CASE("integration::cpp::test_alter_missing_column::document_table_drop_existing_field_still_works",
          "[altermissing]") {
    auto config = test_create_config(integration_fixture_path("test_alter_missing_column/doc_drop"));
    test_clear_directory(config);
    config.wal.on = false;
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
    config.wal.on = false;
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
    config.wal.on = false;
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

    // The catalog half of a document rename is easy: pg_computed_column is versioned, so
    // it's a tombstone under the old name plus a live row under the new one. The storage
    // half can't be completed: a relkind='g' column binds to its physical column by the
    // storage column's TYPE ALIAS, and data_table_t::rename_column updates
    // column_definition_t::name_ while set_name leaves type_ alone -- so the alias keeps
    // the old name and the field would vanish from SELECT under BOTH names. Refused
    // instead of silently "succeeding" into that state.
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
