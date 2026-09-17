// DROP CONSTRAINT finds the pg_constraint row and cascades its pg_depend edges through the same
// dynamic-cascade machinery DROP TABLE uses (seed = (pg_constraint, conoid)).

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>
#include <string>

namespace {

    using namespace test_helpers;

    std::string fixture_path(const char* leaf) {
        return integration_fixture_path(std::string("test_alter_drop_constraint/") + leaf).string();
    }

    components::cursor::cursor_t_ptr run_ok(otterbrix::wrapper_dispatcher_t* d, const std::string& sql) {
        auto cur = exec(d, sql);
        INFO("statement: " << sql);
        INFO("error: " << (cur->is_error() ? std::string{cur->get_error().what.begin(), cur->get_error().what.end()}
                                           : std::string{"none"}));
        REQUIRE(cur->is_success());
        return cur;
    }

    std::string error_text(const components::cursor::cursor_t_ptr& cur) {
        if (!cur->is_error()) {
            return {};
        }
        return std::string{cur->get_error().what.begin(), cur->get_error().what.end()};
    }

} // namespace

TEST_CASE("integration::cpp::alter_drop_constraint::double_pk_repair", "[dropconstraint]") {
    auto config = make_test_config(fixture_path("double_pk"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE adc;");
    run_ok(d, "CREATE TABLE adc.t (a bigint, b bigint);");
    run_ok(d, "ALTER TABLE adc.t ADD CONSTRAINT pk_a PRIMARY KEY (a);");
    run_ok(d, "ALTER TABLE adc.t ADD CONSTRAINT pk_b PRIMARY KEY (b);");
    {
        auto cur = exec(d, "INSERT INTO adc.t (a, b) VALUES (1, 2);");
        INFO("two 'p' rows must refuse the DML that would enforce them");
        REQUIRE(cur->is_error());
    }

    run_ok(d, "ALTER TABLE adc.t DROP CONSTRAINT pk_b;");

    run_ok(d, "INSERT INTO adc.t (a, b) VALUES (1, 2);");
    {
        auto cur = exec(d, "INSERT INTO adc.t (a, b) VALUES (1, 3);");
        INFO("error: " << error_text(cur));
        REQUIRE(cur->is_error());
    }
}

// The parent column, un-droppable while the FK lived, becomes droppable once the drop scrubs its pg_depend edges.
TEST_CASE("integration::cpp::alter_drop_constraint::fk_drop_releases_parent", "[dropconstraint]") {
    auto config = make_test_config(fixture_path("fk"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE adc;");
    run_ok(d, "CREATE TABLE adc.parent (id bigint, name text);");
    run_ok(d, "CREATE TABLE adc.child (id bigint, pid bigint);");
    run_ok(d, "ALTER TABLE adc.parent ADD CONSTRAINT parent_pk PRIMARY KEY (id);");
    run_ok(d,
           "ALTER TABLE adc.child ADD CONSTRAINT fk_pid FOREIGN KEY (pid) "
           "REFERENCES adc.parent (id);");
    run_ok(d, "INSERT INTO adc.parent (id, name) VALUES (1, 'one');");
    {
        auto cur = exec(d, "INSERT INTO adc.child (id, pid) VALUES (10, 999);");
        INFO("control: the FK enforces before the drop");
        REQUIRE(cur->is_error());
    }

    run_ok(d, "ALTER TABLE adc.child DROP CONSTRAINT fk_pid;");

    run_ok(d, "INSERT INTO adc.child (id, pid) VALUES (10, 999);");
    run_ok(d, "ALTER TABLE adc.parent DROP COLUMN id;");
}

TEST_CASE("integration::cpp::alter_drop_constraint::check_drop_stops_enforcement", "[dropconstraint]") {
    auto config = make_test_config(fixture_path("check"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE adc;");
    run_ok(d, "CREATE TABLE adc.t (a bigint);");
    run_ok(d, "ALTER TABLE adc.t ADD CONSTRAINT a_positive CHECK (a > 0);");
    {
        auto cur = exec(d, "INSERT INTO adc.t (a) VALUES (-5);");
        INFO("control: the CHECK enforces before the drop");
        REQUIRE(cur->is_error());
    }

    run_ok(d, "ALTER TABLE adc.t DROP CONSTRAINT a_positive;");

    run_ok(d, "INSERT INTO adc.t (a) VALUES (-5);");
}

// A missing name refuses loudly and names it; IF EXISTS on that name is the sanctioned no-op success.
TEST_CASE("integration::cpp::alter_drop_constraint::missing_name_refuses_if_exists_passes", "[dropconstraint]") {
    auto config = make_test_config(fixture_path("missing"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE adc;");
    run_ok(d, "CREATE TABLE adc.t (a bigint);");
    run_ok(d, "ALTER TABLE adc.t ADD CONSTRAINT a_positive CHECK (a > 0);");

    {
        auto cur = exec(d, "ALTER TABLE adc.t DROP CONSTRAINT nope;");
        const auto what = error_text(cur);
        INFO("error: " << what);
        REQUIRE(cur->is_error());
        CHECK(what.find("nope") != std::string::npos);
    }
    {
        auto cur = exec(d, "INSERT INTO adc.t (a) VALUES (-5);");
        REQUIRE(cur->is_error());
    }

    run_ok(d, "ALTER TABLE adc.t DROP CONSTRAINT IF EXISTS nope;");
    {
        auto cur = exec(d, "INSERT INTO adc.t (a) VALUES (-5);");
        REQUIRE(cur->is_error());
    }
}
