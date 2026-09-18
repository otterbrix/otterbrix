// Regression pin: an unresolved oid must never reach mark_executed() through an empty spec
// list — that combination is what let a failed DROP INDEX report success while deleting nothing.

#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <unistd.h>

#include <string>

namespace {

    using namespace test_helpers;

    std::string fixture_path(const char* leaf) {
        return integration_fixture_path(std::string("test_drop_index_repeat/") + leaf).string();
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

    void seed(otterbrix::wrapper_dispatcher_t* d) {
        run_ok(d, "CREATE DATABASE dix;");
        run_ok(d, "CREATE TABLE dix.t (id bigint, v bigint);");
        run_ok(d, "INSERT INTO dix.t (id, v) VALUES (1, 10), (2, 20);");
        run_ok(d, "CREATE INDEX idx_id ON dix.t (id);");
    }

} // namespace

TEST_CASE("integration::cpp::drop_index_repeat::dropping_twice_refuses_the_second_time") {
    auto config = make_test_config(fixture_path("twice"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);

    run_ok(d, "DROP INDEX dix.t.idx_id;");

    auto second = exec(d, "DROP INDEX dix.t.idx_id;");
    REQUIRE_FALSE(second->is_success());
    CHECK(error_text(second).find("does not exist") != std::string::npos);

    // The third attempt must fail the same way: this is a catalog function, not a call counter.
    auto third = exec(d, "DROP INDEX dix.t.idx_id;");
    CHECK_FALSE(third->is_success());

    CHECK(run_ok(d, "SELECT id FROM dix.t;")->size() == 2);
}

// A never-existed index must fail identically on repeat, not succeed via a surviving
// "already handled" stamp.
TEST_CASE("integration::cpp::drop_index_repeat::a_failed_drop_does_not_license_the_next_one") {
    auto config = make_test_config(fixture_path("failed"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);

    auto first = exec(d, "DROP INDEX dix.t.no_such_index;");
    REQUIRE_FALSE(first->is_success());
    const auto first_text = error_text(first);
    CHECK(first_text.find("does not exist") != std::string::npos);

    auto second = exec(d, "DROP INDEX dix.t.no_such_index;");
    REQUIRE_FALSE(second->is_success());
    CHECK(error_text(second) == first_text);

    run_ok(d, "DROP INDEX dix.t.idx_id;");
}

// IF EXISTS is the one carve-out where a missing index must still succeed, so the refusal
// above cannot be unconditional.
TEST_CASE("integration::cpp::drop_index_repeat::if_exists_is_a_noop_success") {
    auto config = make_test_config(fixture_path("if_exists"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* d = space.dispatcher();

    seed(d);

    run_ok(d, "DROP INDEX dix.t.idx_id;");
    run_ok(d, "DROP INDEX IF EXISTS dix.t.idx_id;");
    run_ok(d, "DROP INDEX IF EXISTS dix.t.never_existed;");

    CHECK(run_ok(d, "SELECT id FROM dix.t;")->size() == 2);
}
