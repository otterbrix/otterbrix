#include "integration_fixture_path.hpp"
#include "test_config.hpp"
#include <catch2/catch_test_macros.hpp>
#include <integration/cpp/connection.hpp>
#include <unistd.h>

#include <set>
#include <string>
#include <vector>

namespace {

    using otterbrix::connection_t;

    void require_ok(connection_t& session, const std::string& sql) {
        auto cursor = session.execute(sql);
        INFO("statement: " << sql << " error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
    }

    // 10:00 written without a zone is 07:00 UTC (row 1) at +03:00 and 10:00 UTC (row 2) at UTC: the ids tell which
    // zone the statement ran in.
    std::set<int64_t> zone_probe(connection_t& session) {
        auto cursor = session.execute("SELECT id FROM m.t WHERE tstz = '2024-01-01 10:00:00'::timestamp;");
        INFO("probe error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
        std::set<int64_t> ids;
        for (uint64_t row = 0; row < cursor->size(); ++row) {
            auto id = cursor->get<int64_t>(0, row);
            REQUIRE_FALSE(id.has_error());
            ids.insert(id.value());
        }
        return ids;
    }

    const std::set<int64_t> at_plus_three{1};
    const std::set<int64_t> at_utc{2};

    std::size_t row_count(connection_t& session) {
        auto cursor = session.execute("SELECT id FROM m.t;");
        INFO("count error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
        return cursor->size();
    }

    std::vector<std::string> stored_timezones(connection_t& session) {
        auto cursor = session.execute("SELECT setting FROM pg_catalog.pg_settings WHERE name = 'TimeZone';");
        INFO("pg_settings error: " << (cursor->is_error() ? cursor->get_error().what : ""));
        REQUIRE(cursor->is_success());
        std::vector<std::string> values;
        for (uint64_t row = 0; row < cursor->size(); ++row) {
            auto value = cursor->get<std::string_view>(0, row);
            REQUIRE_FALSE(value.has_error());
            values.emplace_back(value.value());
        }
        return values;
    }

    configuration::config fresh_config(const std::string& name) {
        auto config =
            test_create_config(integration_fixture_path("session_settings") / name / std::to_string(::getpid()));
        test_clear_directory(config);
        config.log.level = log_t::level::off;
        return config;
    }

    void seed(connection_t& session) {
        require_ok(session, "CREATE DATABASE m;");
        require_ok(session, "CREATE TABLE m.t (id BIGINT, tstz TIMESTAMPTZ);");
        require_ok(session,
                   "INSERT INTO m.t (id, tstz) VALUES (1, '2024-01-01 07:00:00+00'), (2, '2024-01-01 10:00:00+00');");
    }

} // namespace

TEST_CASE("integration::cpp::session_settings::rollback_undoes_set") {
    auto instance = test_make_otterbrix(fresh_config("rollback_undoes_set"));
    connection_t session(instance);
    seed(session);

    require_ok(session, "BEGIN;");
    require_ok(session, "SET TIME ZONE '+03:00';");
    REQUIRE(zone_probe(session) == at_plus_three);
    require_ok(session, "ROLLBACK;");
    REQUIRE(zone_probe(session) == at_utc);
}

TEST_CASE("integration::cpp::session_settings::set_local_ends_with_its_transaction") {
    auto config = fresh_config("set_local_ends_with_its_transaction");
    {
        auto instance = test_make_otterbrix(config);
        connection_t session(instance);
        seed(session);

        require_ok(session, "BEGIN;");
        require_ok(session, "SET LOCAL TIME ZONE '+03:00';");
        REQUIRE(zone_probe(session) == at_plus_three);
        require_ok(session, "COMMIT;");
        REQUIRE(zone_probe(session) == at_utc);

        require_ok(session, "BEGIN;");
        require_ok(session, "SET LOCAL TIME ZONE '+03:00';");
        require_ok(session, "ROLLBACK;");
        REQUIRE(zone_probe(session) == at_utc);
    }
    auto instance = test_make_otterbrix(config);
    connection_t session(instance);
    REQUIRE(zone_probe(session) == at_utc);
}

TEST_CASE("integration::cpp::session_settings::rollback_to_savepoint_undoes_set") {
    auto instance = test_make_otterbrix(fresh_config("rollback_to_savepoint_undoes_set"));
    connection_t session(instance);
    seed(session);

    SECTION("a SET after the savepoint") {
        require_ok(session, "BEGIN;");
        require_ok(session, "SET TIME ZONE '+03:00';");
        require_ok(session, "SAVEPOINT p;");
        require_ok(session, "SET TIME ZONE '+05:00';");
        require_ok(session, "ROLLBACK TO p;");
        REQUIRE(zone_probe(session) == at_plus_three);
        require_ok(session, "COMMIT;");
        REQUIRE(zone_probe(session) == at_plus_three);
    }
    SECTION("a SET LOCAL after the savepoint") {
        require_ok(session, "BEGIN;");
        require_ok(session, "SAVEPOINT p;");
        require_ok(session, "SET LOCAL TIME ZONE '+03:00';");
        require_ok(session, "ROLLBACK TO p;");
        REQUIRE(zone_probe(session) == at_utc);
        require_ok(session, "COMMIT;");
        REQUIRE(zone_probe(session) == at_utc);
    }
    SECTION("the savepoint stays after ROLLBACK TO") {
        require_ok(session, "BEGIN;");
        require_ok(session, "SAVEPOINT p;");
        require_ok(session, "SET TIME ZONE '+03:00';");
        require_ok(session, "ROLLBACK TO p;");
        require_ok(session, "SET TIME ZONE '+03:00';");
        require_ok(session, "ROLLBACK TO p;");
        REQUIRE(zone_probe(session) == at_utc);
        require_ok(session, "COMMIT;");
    }
    SECTION("a released savepoint leaves the SET to the transaction") {
        require_ok(session, "BEGIN;");
        require_ok(session, "SAVEPOINT p;");
        require_ok(session, "SET TIME ZONE '+03:00';");
        require_ok(session, "RELEASE p;");
        REQUIRE(zone_probe(session) == at_plus_three);
        require_ok(session, "ROLLBACK;");
        REQUIRE(zone_probe(session) == at_utc);
    }
    SECTION("ROLLBACK TO ends the failure and undoes the SET") {
        require_ok(session, "BEGIN;");
        require_ok(session, "SAVEPOINT p;");
        require_ok(session, "SET TIME ZONE '+03:00';");
        REQUIRE(session.execute("SELECT nosuch FROM m.nosuch;")->is_error());
        require_ok(session, "ROLLBACK TO p;");
        REQUIRE(zone_probe(session) == at_utc);
        require_ok(session, "COMMIT;");
    }
}

TEST_CASE("integration::cpp::session_settings::set_and_set_local_in_one_transaction") {
    auto instance = test_make_otterbrix(fresh_config("set_and_set_local_in_one_transaction"));
    connection_t session(instance);
    seed(session);

    SECTION("a SET after a SET LOCAL outlives the transaction") {
        require_ok(session, "BEGIN;");
        require_ok(session, "SET LOCAL TIME ZONE '+05:00';");
        require_ok(session, "SET TIME ZONE '+03:00';");
        REQUIRE(zone_probe(session) == at_plus_three);
        require_ok(session, "COMMIT;");
        REQUIRE(zone_probe(session) == at_plus_three);
    }
    SECTION("a SET LOCAL after a SET masks it until COMMIT") {
        require_ok(session, "BEGIN;");
        require_ok(session, "SET TIME ZONE '+03:00';");
        require_ok(session, "SET LOCAL TIME ZONE 'UTC';");
        REQUIRE(zone_probe(session) == at_utc);
        require_ok(session, "COMMIT;");
        REQUIRE(zone_probe(session) == at_plus_three);
    }
}

TEST_CASE("integration::cpp::session_settings::set_local_outside_a_transaction_does_nothing") {
    auto instance = test_make_otterbrix(fresh_config("set_local_outside_a_transaction"));
    connection_t session(instance);
    seed(session);

    require_ok(session, "SET LOCAL TIME ZONE '+03:00';");
    REQUIRE(zone_probe(session) == at_utc);
}

TEST_CASE("integration::cpp::session_settings::set_belongs_to_its_session") {
    auto instance = test_make_otterbrix(fresh_config("set_belongs_to_its_session"));
    connection_t a(instance);
    connection_t b(instance);
    seed(a);

    require_ok(a, "SET TIME ZONE '+03:00';");
    REQUIRE(zone_probe(a) == at_plus_three);
    REQUIRE(zone_probe(b) == at_utc);
}

TEST_CASE("integration::cpp::session_settings::autocommit_belongs_to_its_session") {
    auto instance = test_make_otterbrix(fresh_config("autocommit_belongs_to_its_session"));
    connection_t a(instance);
    connection_t b(instance);
    seed(a);

    require_ok(a, "SET autocommit = off;");
    require_ok(a, "INSERT INTO m.t (id, tstz) VALUES (3, '2024-01-02 00:00:00+00');");
    REQUIRE(row_count(b) == 2);
    require_ok(a, "COMMIT;");
    REQUIRE(row_count(b) == 3);
}

TEST_CASE("integration::cpp::session_settings::set_is_not_stored") {
    auto config = fresh_config("set_is_not_stored");
    {
        auto instance = test_make_otterbrix(config);
        connection_t session(instance);
        seed(session);
        require_ok(session, "SET TIME ZONE '+03:00';");
        REQUIRE(zone_probe(session) == at_plus_three);
    }
    auto instance = test_make_otterbrix(config);
    connection_t session(instance);
    REQUIRE(zone_probe(session) == at_utc);
    REQUIRE(stored_timezones(session) == std::vector<std::string>{"utc"});
}

TEST_CASE("integration::cpp::session_settings::alter_system_stores_the_default") {
    auto config = fresh_config("alter_system_stores_the_default");
    {
        auto instance = test_make_otterbrix(config);
        connection_t writer(instance);
        connection_t idle(instance);
        connection_t own(instance);
        seed(writer);
        require_ok(own, "SET TIME ZONE 'UTC';");
        REQUIRE(zone_probe(idle) == at_utc);

        require_ok(writer, "ALTER SYSTEM SET timezone = '+03:00';");
        REQUIRE(zone_probe(writer) == at_plus_three);
        REQUIRE(zone_probe(idle) == at_plus_three);
        REQUIRE(zone_probe(own) == at_utc);
        connection_t fresh(instance);
        REQUIRE(zone_probe(fresh) == at_plus_three);
    }
    auto instance = test_make_otterbrix(config);
    connection_t session(instance);
    REQUIRE(zone_probe(session) == at_plus_three);
    REQUIRE(stored_timezones(session) == std::vector<std::string>{"utc", "+03:00"});
}

TEST_CASE("integration::cpp::session_settings::alter_system_reset_restores_the_built_in_default") {
    auto config = fresh_config("alter_system_reset");
    {
        auto instance = test_make_otterbrix(config);
        connection_t session(instance);
        seed(session);

        require_ok(session, "ALTER SYSTEM SET timezone = '+03:00';");
        require_ok(session, "ALTER SYSTEM RESET timezone;");
        REQUIRE(zone_probe(session) == at_utc);

        require_ok(session, "ALTER SYSTEM SET timezone = '+03:00';");
        require_ok(session, "ALTER SYSTEM SET timezone TO DEFAULT;");
        REQUIRE(zone_probe(session) == at_utc);

        require_ok(session, "ALTER SYSTEM SET timezone = '+03:00';");
        require_ok(session, "ALTER SYSTEM SET autocommit = off;");
        require_ok(session, "ALTER SYSTEM RESET ALL;");
        REQUIRE(zone_probe(session) == at_utc);
        connection_t other(instance);
        require_ok(other, "INSERT INTO m.t (id, tstz) VALUES (3, '2024-01-02 00:00:00+00');");
        REQUIRE(row_count(session) == 3);
    }
    auto instance = test_make_otterbrix(config);
    connection_t session(instance);
    REQUIRE(zone_probe(session) == at_utc);
}

TEST_CASE("integration::cpp::session_settings::alter_system_refused_inside_a_transaction_block") {
    auto instance = test_make_otterbrix(fresh_config("alter_system_in_a_block"));
    connection_t session(instance);
    seed(session);

    SECTION("BEGIN") { require_ok(session, "BEGIN;"); }
    SECTION("the implicit transaction of autocommit off") {
        require_ok(session, "SET autocommit = off;");
        REQUIRE(zone_probe(session) == at_utc);
    }
    auto refused = session.execute("ALTER SYSTEM SET timezone = '+03:00';");
    REQUIRE(refused->is_error());
    CHECK(refused->get_error().type == core::error_code_t::active_sql_transaction);
    CHECK(std::string(refused->get_error().what) == "ALTER SYSTEM cannot run inside a transaction block");
    require_ok(session, "ROLLBACK;");
    REQUIRE(stored_timezones(session) == std::vector<std::string>{"utc"});
    REQUIRE(zone_probe(session) == at_utc);
}

TEST_CASE("integration::cpp::session_settings::implicit_transaction_of_autocommit_off") {
    auto instance = test_make_otterbrix(fresh_config("implicit_transaction"));
    connection_t session(instance);
    seed(session);

    require_ok(session, "SET autocommit = off;");
    require_ok(session, "SET TIME ZONE '+03:00';");
    REQUIRE(zone_probe(session) == at_plus_three);
    require_ok(session, "ROLLBACK;");
    REQUIRE(zone_probe(session) == at_utc);

    require_ok(session, "SET LOCAL TIME ZONE '+03:00';");
    REQUIRE(zone_probe(session) == at_plus_three);
    require_ok(session, "COMMIT;");
    REQUIRE(zone_probe(session) == at_utc);
}

TEST_CASE("integration::cpp::session_settings::reset_returns_to_the_default") {
    auto instance = test_make_otterbrix(fresh_config("reset_returns_to_the_default"));
    connection_t session(instance);
    seed(session);

    SECTION("RESET name") {
        require_ok(session, "SET TIME ZONE '+03:00';");
        require_ok(session, "RESET timezone;");
        REQUIRE(zone_probe(session) == at_utc);
    }
    SECTION("SET name TO DEFAULT") {
        require_ok(session, "SET TIME ZONE '+03:00';");
        require_ok(session, "SET timezone TO DEFAULT;");
        REQUIRE(zone_probe(session) == at_utc);
    }
    SECTION("RESET ALL") {
        require_ok(session, "SET TIME ZONE '+03:00';");
        require_ok(session, "SET autocommit = off;");
        require_ok(session, "COMMIT;");
        require_ok(session, "RESET ALL;");
        require_ok(session, "COMMIT;");
        REQUIRE(zone_probe(session) == at_utc);
        connection_t other(instance);
        require_ok(other, "INSERT INTO m.t (id, tstz) VALUES (3, '2024-01-02 00:00:00+00');");
        auto rows = session.execute("SELECT id FROM m.t;");
        REQUIRE(rows->is_success());
        CHECK(rows->size() == 3);
    }
    SECTION("the default is the stored one") {
        connection_t admin(instance);
        require_ok(admin, "ALTER SYSTEM SET timezone = '+03:00';");
        require_ok(session, "SET TIME ZONE 'UTC';");
        REQUIRE(zone_probe(session) == at_utc);
        require_ok(session, "RESET timezone;");
        REQUIRE(zone_probe(session) == at_plus_three);
    }
}

TEST_CASE("integration::cpp::session_settings::reset_rolls_back_like_set") {
    auto instance = test_make_otterbrix(fresh_config("reset_rolls_back_like_set"));
    connection_t session(instance);
    seed(session);
    require_ok(session, "SET TIME ZONE '+03:00';");

    SECTION("ROLLBACK") {
        require_ok(session, "BEGIN;");
        require_ok(session, "RESET timezone;");
        REQUIRE(zone_probe(session) == at_utc);
        require_ok(session, "ROLLBACK;");
        REQUIRE(zone_probe(session) == at_plus_three);
    }
    SECTION("ROLLBACK TO") {
        require_ok(session, "BEGIN;");
        require_ok(session, "SAVEPOINT p;");
        require_ok(session, "SET timezone TO DEFAULT;");
        require_ok(session, "ROLLBACK TO p;");
        REQUIRE(zone_probe(session) == at_plus_three);
        require_ok(session, "COMMIT;");
        REQUIRE(zone_probe(session) == at_plus_three);
    }
    SECTION("SET LOCAL name TO DEFAULT lasts until COMMIT") {
        require_ok(session, "BEGIN;");
        require_ok(session, "SET LOCAL timezone TO DEFAULT;");
        REQUIRE(zone_probe(session) == at_utc);
        require_ok(session, "COMMIT;");
        REQUIRE(zone_probe(session) == at_plus_three);
    }
}

TEST_CASE("integration::cpp::session_settings::decimal_settings_do_not_exist") {
    auto instance = test_make_otterbrix(fresh_config("decimal_settings_do_not_exist"));
    connection_t session(instance);

    const auto unknown = session.execute("SET nosuch = '10';");
    REQUIRE(unknown->is_error());
    for (const std::string name : {"decimal_width", "decimal_scale"}) {
        for (const std::string statement : {"SET", "ALTER SYSTEM SET"}) {
            const auto sql = statement + " " + name + " = '10';";
            auto refused = session.execute(sql);
            INFO("statement: " << sql);
            REQUIRE(refused->is_error());
            CHECK(refused->get_error().type == unknown->get_error().type);
            CHECK(std::string(refused->get_error().what) == statement + " " + name + " is not supported");
        }
    }
}
