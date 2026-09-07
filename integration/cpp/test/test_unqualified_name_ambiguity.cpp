#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>

// Otterbrix has no search_path, so a relname match in more than one USER namespace is refused
// (ambiguous_name) instead of resolved by storage order; pg_catalog alone keeps PostgreSQL's
// search-path precedence, so it still wins over a same-named user table.

using namespace components;

namespace {

    // dbone.alpha has 2 rows and dbtwo.alpha has 5, so an unqualified answer betrays which
    // table replied.
    void seed_two_alphas(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE dbone;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE dbtwo;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE dbone.alpha (id BIGINT);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE dbtwo.alpha (id BIGINT);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO dbone.alpha (id) VALUES (1), (2);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO dbtwo.alpha (id) VALUES (10), (20), (30), (40), (50);")
                    ->is_success());
    }

    void require_ambiguous(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cursor = test_helpers::exec(dispatcher, sql);
        REQUIRE(cursor);
        if (cursor->is_success()) {
            const auto n = cursor->size();
            INFO("[" << sql << "] silently answered from ONE of two same-named tables: " << n << " row(s) — that is "
                     << (n == 2 ? "dbone.alpha" : n == 5 ? "dbtwo.alpha" : "neither table whole"));
            REQUIRE(cursor->is_error());
        }
        INFO("[" << sql << "] error: " << cursor->get_error().what.c_str());
        REQUIRE(cursor->get_error().type == core::error_code_t::ambiguous_name);
    }

    void require_count(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql, std::size_t expected) {
        auto cursor = test_helpers::exec(dispatcher, sql);
        REQUIRE(cursor);
        INFO("[" << sql << "] " << (cursor->is_error() ? cursor->get_error().what.c_str() : "<no error>"));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == expected);
    }

} // namespace

TEST_CASE("integration::cpp::unqualified_name_ambiguity::select_is_refused_not_answered_from_either") {
    auto config = test_helpers::make_test_config(integration_fixture_path("unqualified_ambiguity/select"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_two_alphas(dispatcher);

    require_ambiguous(dispatcher, "SELECT * FROM alpha;");

    require_count(dispatcher, "SELECT * FROM dbone.alpha;", 2);
    require_count(dispatcher, "SELECT * FROM dbtwo.alpha;", 5);

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE dbtwo.beta (id BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO dbtwo.beta (id) VALUES (7), (8), (9);")->is_success());
    require_count(dispatcher, "SELECT * FROM beta;", 3);
}

// A silent pick on DML would destroy the losing table's rows, not just misreport them, so DML
// needs the same refusal proven separately from SELECT.
TEST_CASE("integration::cpp::unqualified_name_ambiguity::dml_is_refused_and_both_tables_keep_their_rows") {
    auto config = test_helpers::make_test_config(integration_fixture_path("unqualified_ambiguity/dml"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_two_alphas(dispatcher);

    {
        auto cursor = test_helpers::exec(dispatcher, "DELETE FROM alpha;");
        REQUIRE(cursor);
        INFO("[DELETE FROM alpha;] must refuse: two tables carry that name"
             << (cursor->is_error() ? std::string(" — got: ") + cursor->get_error().what.c_str() : ""));
        REQUIRE(cursor->is_error());
        REQUIRE(cursor->get_error().type == core::error_code_t::ambiguous_name);
    }

    require_count(dispatcher, "SELECT * FROM dbone.alpha;", 2);
    require_count(dispatcher, "SELECT * FROM dbtwo.alpha;", 5);
}

// pg_catalog is searched before user namespaces, so a same-named user table must not turn this
// into another ambiguous_name case.
TEST_CASE("integration::cpp::unqualified_name_ambiguity::pg_catalog_wins_over_a_user_shadow") {
    auto config = test_helpers::make_test_config(integration_fixture_path("unqualified_ambiguity/pg_catalog_shadow"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE dbone;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE dbone.pg_class (id BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO dbone.pg_class (id) VALUES (1);")->is_success());

    {
        auto cursor = test_helpers::exec(dispatcher, "DELETE FROM pg_class;");
        REQUIRE(cursor);
        REQUIRE(cursor->is_error());
        INFO("[DELETE FROM pg_class;] error: " << cursor->get_error().what.c_str());
        REQUIRE(std::string(cursor->get_error().what.c_str()).find("system catalog") != std::string::npos);
    }

    require_count(dispatcher, "SELECT * FROM dbone.pg_class;", 1);
}
