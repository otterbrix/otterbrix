// ============================================================================
// ALTER TABLE ... DROP / RENAME COLUMN NAMING A COLUMN THAT IS NOT THERE.
//
// Both operators resolved the column by (attrelid, attname) and, on a miss,
// took an early return that ended in mark_executed() — the statement reported
// SUCCESS having written nothing. PostgreSQL refuses both:
//
//   ERROR:  column "nosuchcol" of relation "t" does not exist
//
// and only `DROP COLUMN IF EXISTS` passes (as a notice); RENAME COLUMN has no
// IF EXISTS form for the column at all. The cost of the silent success is a
// script that renames a column and then reads it under the new name: green
// ALTER, failing SELECT, nothing between them to say which of the two lied.
//
// THE MINE UNDER THE FIX. A relkind='g' (document / dynamic-schema) table keeps
// its columns in pg_computed_column, NOT in pg_attribute — so for those tables
// EVERY column name misses the pg_attribute lookup these operators do. Turning
// the miss into an error without routing relkind='g' elsewhere would refuse
// legal statements on every document table. The document cases below gate that
// and hold ONE rule for both table shapes ("loud for regular tables, silent for
// document ones" would be a fallback keyed on the table kind):
//   * DROP of a field that IS there stays green — the planner routes it to the
//     operator that owns pg_computed_column;
//   * DROP of a field that is NOT there is refused, as on a regular table;
//   * RENAME on a document table is refused with the sentence that is TRUE for
//     it — "not implemented", not "does not exist" — never reported as done. It
//     never worked: it reported success and renamed nothing, and the storage
//     half cannot be completed on this branch (the case says why).
//
// HOW TO RUN THESE, AND A DEFECT THEY UNCOVERED BUT DO NOT FIX. ctest gives
// every case its own process, which is how they are green. Running the whole tag
// in ONE process is flaky, for a reason none of them assert: a refusal message
// built from an ALTER operator's resource can arrive at the caller with a size
// that spans the NEXT copies of itself — read back it comes out doubled or
// tripled, or big enough that constructing a std::string from
// what.begin()/what.end() throws std::length_error("basic_string"). It takes a
// handful of refusals in one process, and a longer message reaches it sooner; a
// single refusal in a fresh process is reliable, which is why it stayed hidden.
// Reproduced but NOT fixed here, so: every message below is kept short, and each
// cursor's error is read exactly ONCE and kept. The message-content checks stay
// — they are what caught it.
// ============================================================================

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

    // Reading the message is itself part of the assertion, not decoration. A refusal
    // whose text arrives unreadable is a refusal that tells the user nothing, and no
    // `REQUIRE_FALSE(is_success())` on its own would notice: the messages below are
    // therefore checked for the words that make them actionable.
    //
    // Read ONCE per cursor and keep the copy — the same convention
    // test_fk_parent_column_drop uses. Reading a cursor's error a second time is not
    // reliable on this branch: the DDL paths hand back a cursor whose `what` can be
    // recycled underneath it, and a second read of the same cursor intermittently
    // returns the message doubled or throws std::length_error. That is a defect in the
    // engine, not in these cases, and it is not what they are here to pin.
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

    // RENAME COLUMN on a document table is refused, and this case pins that rather
    // than a weakened assertion. The catalog half of a
    // document rename is straightforward — pg_computed_column is versioned, so it is a
    // tombstone under the old name plus a live row under the new one, and the resolver
    // picks the new name up (measured). The storage half cannot be completed: a
    // relkind='g' column binds to its physical column by the storage column's TYPE
    // ALIAS, and data_table_t::rename_column updates column_definition_t::name_ while
    // set_name leaves type_ alone — so the alias keeps the old name and the storage
    // reports success. Performing the rename therefore makes the field vanish from
    // SELECT under BOTH names with its data unreadable, which is worse than the silent
    // success this file exists to remove. The refusal says so; this case keeps it from
    // quietly going back to reporting success.
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
