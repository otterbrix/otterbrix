// components/sql/transformer/impl/transform_table.cpp's option loop compared each DefElem's name against
// only "storage" and silently `continue`d past every other name, so an unimplemented or misspelled option
// was accepted instead of refused.

#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <string>

namespace {

    using namespace test_helpers;

    std::string fixture_path(const char* leaf) {
        return integration_fixture_path(std::string("test_create_table_with_options/") + leaf).string();
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

// `storage` keeps its own message naming why it's gone, not just that it's unsupported.
TEST_CASE("integration::cpp::create_table_with_options::storage_keeps_its_own_message") {
    auto config = make_test_config(fixture_path("storage"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE opt;");

    auto cur = exec(d, "CREATE TABLE opt.t (id bigint) WITH (storage = 'memory');");
    REQUIRE_FALSE(cur->is_success());
    CHECK(error_text(cur).find("always disk-backed") != std::string::npos);
}

TEST_CASE("integration::cpp::create_table_with_options::unknown_option_is_refused_by_name") {
    auto config = make_test_config(fixture_path("unknown"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE opt;");

    auto cur = exec(d, "CREATE TABLE opt.t (id bigint) WITH (fillfactor = 70);");
    REQUIRE_FALSE(cur->is_success());
    CHECK(error_text(cur).find("fillfactor") != std::string::npos);

    auto gone = exec(d, "SELECT id FROM opt.t;");
    CHECK_FALSE(gone->is_success());
}

// `storag` carries the same false belief the `storage` refusal corrects, yet is invisible to a
// single-name comparison.
TEST_CASE("integration::cpp::create_table_with_options::a_typo_of_storage_is_refused") {
    auto config = make_test_config(fixture_path("typo"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE opt;");

    auto cur = exec(d, "CREATE TABLE opt.t (id bigint) WITH (storag = 'memory');");
    REQUIRE_FALSE(cur->is_success());
    CHECK(error_text(cur).find("storag") != std::string::npos);

    auto gone = exec(d, "SELECT id FROM opt.t;");
    CHECK_FALSE(gone->is_success());
}

// The `storage` message must not depend on where in the WITH list it appears.
TEST_CASE("integration::cpp::create_table_with_options::storage_wins_from_any_position") {
    auto config = make_test_config(fixture_path("storage_second"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE opt;");

    auto cur = exec(d, "CREATE TABLE opt.t (id bigint) WITH (fillfactor = 70, storage = 'memory');");
    REQUIRE_FALSE(cur->is_success());
    CHECK(error_text(cur).find("always disk-backed") != std::string::npos);
}

TEST_CASE("integration::cpp::create_table_with_options::no_options_still_creates") {
    auto config = make_test_config(fixture_path("none"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE opt;");
    run_ok(d, "CREATE TABLE opt.t (id bigint);");
    run_ok(d, "INSERT INTO opt.t (id) VALUES (1);");
    CHECK(run_ok(d, "SELECT id FROM opt.t;")->size() == 1);
}
