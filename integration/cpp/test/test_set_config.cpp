// current_setting() and set_config() (PostgreSQL 18 guc_funcs.c): the settings SET and SET LOCAL keep, read and
// written from a SELECT.

#include "session_sql_fixture.hpp"

#include <catch2/catch_test_macros.hpp>
#include <components/vector/vector_builders.hpp>

#include <string>
#include <utility>
#include <vector>

namespace {

    using strings = std::vector<std::string>;
    using code = core::error_code_t;

    const std::string current_search_path = "SELECT current_setting('search_path');";

} // namespace

TEST_CASE("integration::cpp::set_config::current_setting") {
    session_sql::fixture_t db("test_set_config/current_setting");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");

    SECTION("reads what SET wrote") {
        db.ok(session, "SET search_path = 'main.s';");
        CHECK(db.strings(session, current_search_path) == strings{"main.s"});
        CHECK(db.strings(session, "SELECT current_setting('SEARCH_PATH');") == strings{"main.s"});
    }

    SECTION("an unknown name") {
        CHECK(db.refused(session, "SELECT current_setting('no_such_setting');").type == code::undefined_object);
        CHECK(db.strings(session, "SELECT current_setting('no_such_setting', true);") == strings{"<null>"});
        CHECK(db.refused(session, "SELECT current_setting('no_such_setting', false);").type == code::undefined_object);
    }

    SECTION("of a column") {
        db.ok(session, "CREATE TABLE s.names (n text);");
        db.ok(session, "INSERT INTO s.names (n) VALUES ('search_path');");
        CHECK(db.refused(session, "SELECT current_setting(n) FROM s.names;").type == code::unimplemented_yet);
    }
}

TEST_CASE("integration::cpp::set_config::set_config") {
    session_sql::fixture_t db("test_set_config/set_config");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");

    SECTION("for the session") {
        CHECK(db.strings(session, "SELECT set_config('search_path', 'main.s', false);") == strings{"main.s"});
        CHECK(db.strings(session, current_search_path) == strings{"main.s"});
    }

    SECTION("in a statement's order") {
        auto row = db.ok(session,
                         "SELECT current_setting('search_path') AS before, set_config('search_path', 'main.s', false) "
                         "AS set, current_setting('search_path') AS after;");
        const auto& chunk = row->chunks().front();
        CHECK(chunk.get_value<std::string_view>(0, 0) == "main.public");
        CHECK(chunk.get_value<std::string_view>(1, 0) == "main.s");
        CHECK(chunk.get_value<std::string_view>(2, 0) == "main.s");
    }

    SECTION("for a transaction") {
        db.ok(session, "BEGIN;");
        CHECK(db.strings(session, "SELECT set_config('search_path', 'main.s', true);") == strings{"main.s"});
        CHECK(db.strings(session, current_search_path) == strings{"main.s"});
        db.ok(session, "COMMIT;");
        CHECK(db.strings(session, current_search_path) == strings{"main.public"});
    }

    SECTION("for the session in a transaction rolled back") {
        db.ok(session, "BEGIN;");
        db.ok(session, "SELECT set_config('search_path', 'main.s', false);");
        db.ok(session, "ROLLBACK;");
        CHECK(db.strings(session, current_search_path) == strings{"main.public"});
    }

    SECTION("as a parameter") {
        std::vector<std::pair<size_t, components::vector::vector_t>> value;
        value.emplace_back(1, components::vector::make_scalar(db.dispatcher()->resource(), std::string_view{"main.s"}));
        auto set =
            db.dispatcher()->execute_sql_with_params(session, "SELECT set_config('search_path', $1, false);", value);
        INFO(session_sql::error_of(set));
        REQUIRE(set->is_success());
        CHECK(db.strings(session, current_search_path) == strings{"main.s"});
    }

    SECTION("a value the setting refuses") {
        CHECK(db.refused(session,
                         "SELECT set_config('search_path', 'main.s', false), set_config('TimeZone', 'nowhere', false);")
                  .type == code::invalid_parameter);
        CHECK(db.strings(session, current_search_path) == strings{"main.public"});
    }

    SECTION("where it would not run once") {
        db.ok(session, "CREATE TABLE s.t (a bigint);");
        CHECK(db.refused(session, "SELECT set_config('search_path', 'main.s', false) FROM s.t;").type ==
              code::unimplemented_yet);
        CHECK(db.refused(session, "SELECT set_config('search_path', 'main.s', false) WHERE 1 = 0;").type ==
              code::unimplemented_yet);
        CHECK(db.refused(session,
                         "SELECT set_config('search_path', 'main.s', false), set_config('TimeZone', 'utc', true);")
                  .type == code::unimplemented_yet);
        CHECK(db.strings(session, current_search_path) == strings{"main.public"});
    }
}

TEST_CASE("integration::cpp::set_config::show") {
    session_sql::fixture_t db("test_set_config/show");
    const otterbrix::session_id_t session;
    db.ok(session, "CREATE SCHEMA s;");

    SECTION("a setting") {
        db.ok(session, "SET search_path = 'main.s';");
        auto shown = db.ok(session, "SHOW search_path;");
        REQUIRE(shown->size() == 1);
        CHECK(shown->chunks().front().data[0].type().alias() == "search_path");
        CHECK(shown->chunks().front().get_value<std::string_view>(0, 0) == "main.s");
        CHECK(db.strings(session, "SHOW timezone;") == db.strings(session, "SELECT current_setting('TimeZone');"));
        CHECK(db.ok(session, "SHOW TIME ZONE;")->chunks().front().data[0].type().alias() == "TimeZone");
    }

    SECTION("an unknown name") { CHECK(db.refused(session, "SHOW no_such_setting;").type == code::undefined_object); }

    SECTION("all") {
        db.ok(session, "SET search_path = 'main.s';");
        CHECK(
            db.strings(session, "SHOW ALL;") ==
            strings{"autocommit", "default_transaction_read_only", "search_path", "TimeZone", "transaction_read_only"});
        auto all = db.ok(session, "SHOW ALL;");
        REQUIRE(all->size() == 5);
        std::vector<std::string> settings;
        for (const auto& chunk : all->chunks()) {
            for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                settings.emplace_back(chunk.get_value<std::string_view>(1, row));
                CHECK_FALSE(chunk.get_value<std::string_view>(2, row).empty());
            }
        }
        CHECK(settings[2] == "main.s");
    }
}
