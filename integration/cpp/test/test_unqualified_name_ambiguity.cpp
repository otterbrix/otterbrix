#include "test_config.hpp"
#include "integration_fixture_path.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>

// An unqualified table name resolves through a relname-only pg_class scan across EVERY
// namespace. When the same relname lives in two databases, the scan used to answer whichever
// row came first in storage order — silently, with no way to tell WHICH table replied.
//
// PostgreSQL 18 (docs/current, ddl-schemas) resolves an unqualified name via search_path
// ("$user", public by default) with pg_catalog implicitly searched FIRST; a miss across the
// whole path is an error. Otterbrix has no search_path and no session-level current database
// to anchor one, so among USER namespaces there is no order to follow: more than one match is
// refused loudly (ambiguous_name), and the message says how to qualify. The one anchorless
// piece of the PostgreSQL rule is kept: a pg_catalog relation wins over any user table that
// shadows its name, so `DELETE FROM pg_class` keeps hitting the catalog guard, never a user
// table that happens to share the name.

using namespace components;

namespace {

    // Two databases, one relname, DIFFERENT content: dbone.alpha holds 2 rows, dbtwo.alpha
    // holds 5. Any answer to an unqualified `alpha` is measurably one table or the other.
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

// SELECT through an ambiguous unqualified name must refuse, not answer 2 rows (dbone) or
// 5 rows (dbtwo) depending on storage order. Qualified spellings keep answering their own
// content, and an unqualified name that exists exactly once keeps resolving.
TEST_CASE("integration::cpp::unqualified_name_ambiguity::select_is_refused_not_answered_from_either") {
    auto config = test_helpers::make_test_config(integration_fixture_path("unqualified_ambiguity/select"),
                                                 /*wal_on=*/true);
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_two_alphas(dispatcher);

    require_ambiguous(dispatcher, "SELECT * FROM alpha;");

    // Both qualified names still answer their OWN rows.
    require_count(dispatcher, "SELECT * FROM dbone.alpha;", 2);
    require_count(dispatcher, "SELECT * FROM dbtwo.alpha;", 5);

    // A relname that exists in exactly one database still resolves unqualified.
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE dbtwo.beta (id BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO dbtwo.beta (id) VALUES (7), (8), (9);")->is_success());
    require_count(dispatcher, "SELECT * FROM beta;", 3);
}

// DML through an ambiguous unqualified name is where the silent pick costs data: a DELETE
// that lands on "whichever alpha came first" empties a table the user never named. Refuse,
// and BOTH tables keep their rows.
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

// A user table that SHADOWS a catalog name must not make the catalog unreachable, and must
// never become the target of an unqualified catalog spelling: PostgreSQL 18 searches
// pg_catalog before the path, so `DELETE FROM pg_class` resolves to the system table and dies
// on the catalog-DML guard — while the user's own pg_class, reachable only by qualifying it,
// keeps its rows. Pinned so the ambiguity refusal above never regresses this into
// "pg_class is ambiguous".
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
        // The refusal must be the catalog guard's — proof the name resolved to
        // pg_catalog.pg_class, not to the user's shadow and not to an ambiguity.
        INFO("[DELETE FROM pg_class;] error: " << cursor->get_error().what.c_str());
        REQUIRE(std::string(cursor->get_error().what.c_str()).find("system catalog") != std::string::npos);
    }

    require_count(dispatcher, "SELECT * FROM dbone.pg_class;", 1);
}
