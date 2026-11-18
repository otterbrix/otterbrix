#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <integration/cpp/connection.hpp>
#include <unistd.h>

#include <string>
#include <vector>

// COPY ... FROM STDIN with its data: one statement of the session that inserts the rows as INSERT inserts VALUES.

namespace {

    using otterbrix::connection_t;

    void require_ok(connection_t& session, const std::string& sql) {
        auto cursor = session.execute(sql);
        INFO(sql << " error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
    }

    uint64_t copied(connection_t& session, const std::string& copy, const std::string& data) {
        auto cursor = session.copy_from(copy, data);
        INFO(copy << " error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
        return cursor->affected_rows();
    }

    core::error_code_t refused(connection_t& session, const std::string& copy, const std::string& data) {
        auto cursor = session.copy_from(copy, data);
        INFO(copy);
        REQUIRE(cursor->is_error());
        return cursor->get_error().type;
    }

    int64_t number(connection_t& session, const std::string& sql) {
        auto cursor = session.execute(sql);
        INFO(sql << " error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 1);
        auto value = cursor->get<int64_t>(0, 0);
        REQUIRE_FALSE(value.has_error());
        return value.value();
    }

    std::vector<std::string> texts(connection_t& session, const std::string& sql) {
        auto cursor = session.execute(sql);
        INFO(sql << " error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
        std::vector<std::string> out;
        for (uint64_t row = 0; row < cursor->size(); ++row) {
            auto null = cursor->is_null(0, row);
            REQUIRE_FALSE(null.has_error());
            if (null.value()) {
                out.emplace_back("<null>");
                continue;
            }
            auto value = cursor->get<std::string_view>(0, row);
            REQUIRE_FALSE(value.has_error());
            out.emplace_back(value.value());
        }
        return out;
    }

    configuration::config fresh_config(const std::string& name) {
        auto config = test_create_config(integration_fixture_path("copy_from") / name / std::to_string(::getpid()));
        test_clear_directory(config);
        config.log.level = log_t::level::off;
        return config;
    }

    void create_table(connection_t& session) {
        require_ok(session, "CREATE SCHEMA m;");
        require_ok(session,
                   "CREATE TABLE m.t (id BIGINT NOT NULL, name TEXT, d DATE, note TEXT DEFAULT 'n/a', ok BOOLEAN);");
    }

    // `count` text rows "<n>\tname<n>\t2026-01-01\tnote\tt", the row `bad` (0: none) with an id that is no number.
    std::string text_rows(int count, int bad = 0) {
        std::string data;
        for (int row = 1; row <= count; ++row) {
            data += (row == bad ? std::string{"x"} : std::to_string(row)) + "\tname" + std::to_string(row) +
                    "\t2026-01-01\tnote\tt\n";
        }
        return data;
    }

} // namespace

TEST_CASE("integration::cpp::copy_from::formats") {
    auto instance = test_make_otterbrix(fresh_config("formats"));
    connection_t session(instance);
    create_table(session);

    SECTION("text") {
        CHECK(copied(session, "COPY m.t FROM STDIN;", "1\ta\\tb\t2026-01-01\t\\N\tt\n2\t\\N\t\\N\tx\tf\n") == 2);
        CHECK(texts(session, "SELECT name FROM m.t ORDER BY id;") == std::vector<std::string>{"a\tb", "<null>"});
        CHECK(texts(session, "SELECT note FROM m.t ORDER BY id;") == std::vector<std::string>{"<null>", "x"});
        CHECK(number(session, "SELECT count(*) FROM m.t WHERE d = '2026-01-01' AND ok;") == 1);
    }
    SECTION("csv with a header, quotes and NULL") {
        CHECK(copied(session,
                     "COPY m.t (id, name, note) FROM STDIN WITH (FORMAT csv, HEADER true);",
                     "id,name,note\n1,\"a, \"\"b\"\"\",\n2,,\"\"\n") == 2);
        CHECK(texts(session, "SELECT name FROM m.t ORDER BY id;") == std::vector<std::string>{"a, \"b\"", "<null>"});
        CHECK(texts(session, "SELECT note FROM m.t ORDER BY id;") == std::vector<std::string>{"<null>", ""});
    }
    SECTION("a column list leaves the other columns their defaults") {
        CHECK(copied(session, "COPY m.t (name, id) FROM STDIN;", "a\t1\n") == 1);
        CHECK(texts(session, "SELECT note FROM m.t;") == std::vector<std::string>{"n/a"});
    }
    SECTION("DEFAULT") {
        CHECK(copied(session, "COPY m.t (id, note) FROM STDIN WITH (DEFAULT '@');", "1\t@\n2\tx\n") == 2);
        CHECK(texts(session, "SELECT note FROM m.t ORDER BY id;") == std::vector<std::string>{"n/a", "x"});
    }
    SECTION("the old option syntax") {
        CHECK(copied(session, "COPY m.t (id, name) FROM STDIN CSV DELIMITER '|' NULL 'NA';", "1|NA\n2|b\n") == 2);
        CHECK(texts(session, "SELECT name FROM m.t ORDER BY id;") == std::vector<std::string>{"<null>", "b"});
    }
    SECTION("no rows") {
        CHECK(copied(session, "COPY m.t FROM STDIN;", "") == 0);
        CHECK(copied(session, "COPY m.t (id) FROM STDIN;", "") == 0);
        CHECK(number(session, "SELECT count(*) FROM m.t;") == 0);
    }
}

// More rows than one batch, a number of them off any batch boundary.
TEST_CASE("integration::cpp::copy_from::many_rows") {
    auto instance = test_make_otterbrix(fresh_config("many_rows"));
    connection_t session(instance);
    create_table(session);
    CHECK(copied(session, "COPY m.t FROM STDIN;", text_rows(2500)) == 2500);
    CHECK(number(session, "SELECT count(*) FROM m.t;") == 2500);
    CHECK(number(session, "SELECT sum(id) FROM m.t;") == 2500 * 2501 / 2);
}

// A refused row refuses the whole statement: rows of a batch before it are not kept.
TEST_CASE("integration::cpp::copy_from::refusals") {
    auto instance = test_make_otterbrix(fresh_config("refusals"));
    connection_t session(instance);
    create_table(session);

    SECTION("a value of the wrong type in a later batch") {
        CHECK(session.copy_from("COPY m.t FROM STDIN;", text_rows(1500, 1200))->is_error());
    }
    SECTION("NOT NULL") { CHECK(session.copy_from("COPY m.t (id, name) FROM STDIN;", "1\ta\n\\N\tb\n")->is_error()); }
    SECTION("a row of the wrong width") {
        CHECK(refused(session, "COPY m.t FROM STDIN;", "1\ta\t2026-01-01\tn\tt\n2\tb\n") ==
              core::error_code_t::bad_copy_file_format);
        CHECK(refused(session, "COPY m.t FROM STDIN;", "1\ta\n") == core::error_code_t::bad_copy_file_format);
        CHECK(refused(session, "COPY m.t FROM STDIN;", "1\ta\t2026-01-01\tn\tt\textra\n") ==
              core::error_code_t::bad_copy_file_format);
        CHECK(refused(session, "COPY m.t (id, name) FROM STDIN;", "1\n") == core::error_code_t::bad_copy_file_format);
    }
    SECTION("an unterminated CSV quote") {
        CHECK(refused(session, "COPY m.t (id, name) FROM STDIN CSV;", "1,\"a\n") ==
              core::error_code_t::bad_copy_file_format);
    }
    SECTION("what COPY FROM STDIN does not do") {
        CHECK(refused(session, "COPY m.t FROM STDIN WITH (FORMAT binary);", "") ==
              core::error_code_t::unimplemented_yet);
        CHECK(refused(session, "COPY m.t TO STDOUT;", "") == core::error_code_t::unimplemented_yet);
        CHECK(refused(session, "COPY m.t FROM '/tmp/x';", "") == core::error_code_t::unimplemented_yet);
        CHECK(refused(session, "COPY m.t FROM STDIN WITH (FORMAT xml);", "") == core::error_code_t::invalid_parameter);
        CHECK(refused(session, "COPY m.t FROM STDIN WITH (QUOTE '\"');", "") == core::error_code_t::unimplemented_yet);
        CHECK(refused(session, "COPY m.t FROM STDIN WITH (BOGUS 1);", "") == core::error_code_t::sql_parse_error);
        CHECK(refused(session, "SELECT 1;", "") == core::error_code_t::invalid_parameter);
        CHECK(session.execute("COPY m.t FROM STDIN;")->is_error());
    }
    SECTION("a table that does not exist") {
        CHECK(session.copy_from("COPY m.missing FROM STDIN;", "1\n")->is_error());
    }
    CHECK(number(session, "SELECT count(*) FROM m.t;") == 0);
}

// Inside a block a refused COPY aborts it as any refused statement does; a COPY that succeeded is undone by ROLLBACK.
TEST_CASE("integration::cpp::copy_from::in_a_block") {
    auto instance = test_make_otterbrix(fresh_config("in_a_block"));
    connection_t session(instance);
    create_table(session);
    require_ok(session, "BEGIN;");
    CHECK(copied(session, "COPY m.t (id) FROM STDIN;", "1\n2\n") == 2);
    CHECK(number(session, "SELECT count(*) FROM m.t;") == 2);
    CHECK(session.copy_from("COPY m.t (id) FROM STDIN;", "3\nx\n")->is_error());
    CHECK(session.execute("SELECT count(*) FROM m.t;")->is_error());
    require_ok(session, "ROLLBACK;");
    CHECK(number(session, "SELECT count(*) FROM m.t;") == 0);
}

// Two sessions copy into one table, their blocks open at once: each commits its own rows.
TEST_CASE("integration::cpp::copy_from::two_sessions") {
    auto instance = test_make_otterbrix(fresh_config("two_sessions"));
    connection_t a(instance);
    connection_t b(instance);
    create_table(a);
    require_ok(a, "BEGIN;");
    require_ok(b, "BEGIN;");
    CHECK(copied(a, "COPY m.t (id) FROM STDIN;", "1\n2\n3\n") == 3);
    CHECK(copied(b, "COPY m.t (id) FROM STDIN;", "4\n5\n") == 2);
    CHECK(number(a, "SELECT count(*) FROM m.t;") == 3);
    CHECK(number(b, "SELECT count(*) FROM m.t;") == 2);
    require_ok(a, "COMMIT;");
    require_ok(b, "COMMIT;");
    CHECK(number(a, "SELECT count(*) FROM m.t;") == 5);
}

// A timestamptz field without an offset reads in the session's time zone, as an INSERT's literal does.
TEST_CASE("integration::cpp::copy_from::session_zone") {
    auto instance = test_make_otterbrix(fresh_config("session_zone"));
    connection_t session(instance);
    require_ok(session, "CREATE SCHEMA m;");
    require_ok(session, "CREATE TABLE m.z (id BIGINT, tstz TIMESTAMPTZ);");
    require_ok(session, "SET TIME ZONE 'Europe/Berlin';");
    CHECK(copied(session, "COPY m.z FROM STDIN;", "1\t2026-07-01 12:00:00\n2\t2026-07-01 12:00:00+00\n") == 2);
    require_ok(session, "SET TIME ZONE 'UTC';");
    CHECK(number(session, "SELECT id FROM m.z WHERE tstz = '2026-07-01 10:00:00';") == 1);
    CHECK(number(session, "SELECT id FROM m.z WHERE tstz = '2026-07-01 12:00:00';") == 2);
}
