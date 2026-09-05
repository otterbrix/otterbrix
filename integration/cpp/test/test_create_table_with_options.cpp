// ============================================================================
// `CREATE TABLE ... WITH (...)` MUST NOT SWALLOW AN OPTION IT DOES NOT IMPLEMENT.
//
// The option loop in
// components/sql/transformer/impl/transform_table.cpp compared each DefElem's
// name against exactly one string, "storage", and `continue`d past everything
// else. `storage` was refused because every table is disk-backed and a user
// writing it believes they are still choosing a storage mode — but that
// reasoning covers every other option in the list just as well, since NONE of
// them is implemented either. Any other name fell out of the bottom of the loop
// and the CREATE TABLE proceeded exactly as if the WITH clause had not been
// written.
//
// The sharpest shape is a typo of the one name that WAS handled: `storag =
// 'memory'` reached the same "you have selected a storage mode" belief the
// `storage` refusal exists to correct, and got no refusal at all.
// ============================================================================

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

// The pre-existing refusal, kept pinned: `storage` keeps its own sentence,
// which names the reason the option is gone rather than just its absence.
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

// The defect: every other option name. The refusal must NAME the option, so a
// user can tell which of several they wrote is the unsupported one.
TEST_CASE("integration::cpp::create_table_with_options::unknown_option_is_refused_by_name") {
    auto config = make_test_config(fixture_path("unknown"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    run_ok(d, "CREATE DATABASE opt;");

    auto cur = exec(d, "CREATE TABLE opt.t (id bigint) WITH (fillfactor = 70);");
    REQUIRE_FALSE(cur->is_success());
    CHECK(error_text(cur).find("fillfactor") != std::string::npos);

    // and nothing was created under that name.
    auto gone = exec(d, "SELECT id FROM opt.t;");
    CHECK_FALSE(gone->is_success());
}

// A typo of `storage` is the worst case: it carries the very belief the `storage`
// refusal exists to correct, and it is the one shape guaranteed to slip past a
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

// The specific `storage` sentence must win wherever in the list it is written,
// not only when it comes first — otherwise the message a user gets for the same
// clause depends on the order they typed it in.
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

// No WITH clause at all is the overwhelmingly common case and must stay free of
// the new refusal.
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
