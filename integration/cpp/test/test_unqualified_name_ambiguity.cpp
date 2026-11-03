#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <cstdint>
#include <string>
#include <vector>

// A bare name looks in pg_catalog and then in the schema of the session's place (main.public for a fresh session),
// as PostgreSQL 18 does with a one-element search_path: a same-named table in another schema never answers it, and
// pg_catalog still wins over a same-named user table.

using namespace components;

namespace {

    // dbone.alpha has 2 rows and dbtwo.alpha has 5, so an unqualified answer betrays which
    // table replied.
    void seed_two_alphas(otterbrix::wrapper_dispatcher_t* dispatcher) {
        REQUIRE(test_helpers::exec(dispatcher, "CREATE SCHEMA dbone;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE SCHEMA dbtwo;")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE dbone.alpha (id BIGINT);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE dbtwo.alpha (id BIGINT);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO dbone.alpha (id) VALUES (1), (2);")->is_success());
        REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO dbtwo.alpha (id) VALUES (10), (20), (30), (40), (50);")
                    ->is_success());
    }

    void require_missing(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql, const std::string& name) {
        auto cursor = test_helpers::exec(dispatcher, sql);
        REQUIRE(cursor);
        if (cursor->is_success()) {
            const auto n = cursor->size();
            INFO("[" << sql << "] answered from another schema: " << n << " row(s) — that is "
                     << (n == 2   ? "dbone.alpha"
                         : n == 5 ? "dbtwo.alpha"
                                  : "neither table whole"));
            REQUIRE(cursor->is_error());
        }
        INFO("[" << sql << "] error: " << cursor->get_error().what.c_str());
        REQUIRE(cursor->get_error().type == core::error_code_t::table_not_exists);
        REQUIRE(std::string(cursor->get_error().what.c_str()) == "relation \"" + name + "\" does not exist");
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
    auto config = test_helpers::make_test_config(integration_fixture_path("unqualified_ambiguity/select"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_two_alphas(dispatcher);

    require_missing(dispatcher, "SELECT * FROM alpha;", "alpha");

    require_count(dispatcher, "SELECT * FROM dbone.alpha;", 2);
    require_count(dispatcher, "SELECT * FROM dbtwo.alpha;", 5);

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE dbtwo.beta (id BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO dbtwo.beta (id) VALUES (7), (8), (9);")->is_success());
    require_missing(dispatcher, "SELECT * FROM beta;", "beta");

    auto in_dbtwo = otterbrix::session_id_t();
    REQUIRE(dispatcher->execute_sql(in_dbtwo, "SET search_path = 'dbtwo';")->is_success());
    auto from_dbtwo = dispatcher->execute_sql(in_dbtwo, "SELECT * FROM alpha;");
    REQUIRE(from_dbtwo->is_success());
    CHECK(from_dbtwo->size() == 5);
}

// A silent pick on DML would destroy the losing table's rows, not just misreport them, so DML
// needs the same refusal proven separately from SELECT.
TEST_CASE("integration::cpp::unqualified_name_ambiguity::dml_is_refused_and_both_tables_keep_their_rows") {
    auto config = test_helpers::make_test_config(integration_fixture_path("unqualified_ambiguity/dml"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();
    seed_two_alphas(dispatcher);

    {
        auto cursor = test_helpers::exec(dispatcher, "DELETE FROM alpha;");
        REQUIRE(cursor->is_error());
        CHECK(cursor->get_error().type == core::error_code_t::table_not_exists);
    }

    require_count(dispatcher, "SELECT * FROM dbone.alpha;", 2);
    require_count(dispatcher, "SELECT * FROM dbtwo.alpha;", 5);
}

// pg_catalog is searched before the place, so a same-named user table never shadows it.
TEST_CASE("integration::cpp::unqualified_name_ambiguity::pg_catalog_wins_over_a_user_shadow") {
    auto config = test_helpers::make_test_config(integration_fixture_path("unqualified_ambiguity/pg_catalog_shadow"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE SCHEMA dbone;")->is_success());
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

namespace {
    std::uint32_t namespace_oid(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& nspname) {
        auto cursor = test_helpers::exec(dispatcher,
                                         "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname = '" + nspname + "';");
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 1);
        auto cell0 = cursor->get<std::uint32_t>(0, 0);
        REQUIRE_FALSE(cell0.has_error());
        return cell0.value();
    }

    // One entry per pg_class row of that name, so a missing row and a duplicate both show.
    std::vector<std::uint32_t> relnamespaces_of(otterbrix::wrapper_dispatcher_t* dispatcher,
                                                const std::string& relname) {
        auto cursor =
            test_helpers::exec(dispatcher,
                               "SELECT relnamespace FROM pg_catalog.pg_class WHERE relname = '" + relname + "';");
        REQUIRE(cursor->is_success());
        std::vector<std::uint32_t> namespaces;
        for (std::size_t row = 0; row < cursor->size(); ++row) {
            auto cell1 = cursor->get<std::uint32_t>(0, row);
            REQUIRE_FALSE(cell1.has_error());
            namespaces.push_back(cell1.value());
        }
        return namespaces;
    }
} // namespace

TEST_CASE("integration::cpp::unqualified_name_ambiguity::create_lands_in_public") {
    auto config = test_helpers::make_test_config(integration_fixture_path("unqualified_ambiguity/create_public"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE gamma (id BIGINT);")->is_success());
    CHECK(relnamespaces_of(dispatcher, "gamma") == std::vector<std::uint32_t>{namespace_oid(dispatcher, "public")});
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO gamma (id) VALUES (1), (2);")->is_success());
    require_count(dispatcher, "SELECT * FROM public.gamma;", 2);
    require_count(dispatcher, "SELECT * FROM gamma;", 2);
}

TEST_CASE("integration::cpp::unqualified_name_ambiguity::public_table_stays_reachable") {
    auto config = test_helpers::make_test_config(integration_fixture_path("unqualified_ambiguity/reachable"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE alpha (id BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO alpha (id) VALUES (1), (2), (3);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE SCHEMA dbtwo;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE dbtwo.alpha (id BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO dbtwo.alpha (id) VALUES (10), (20), (30), (40), (50);")
                ->is_success());

    require_count(dispatcher, "SELECT * FROM alpha;", 3);
    require_count(dispatcher, "SELECT * FROM public.alpha;", 3);
    REQUIRE(test_helpers::exec(dispatcher, "DROP TABLE public.alpha;")->is_success());
    require_missing(dispatcher, "SELECT * FROM alpha;", "alpha");
    require_count(dispatcher, "SELECT * FROM dbtwo.alpha;", 5);
}

TEST_CASE("integration::cpp::unqualified_name_ambiguity::create_beside_namesake") {
    auto config = test_helpers::make_test_config(integration_fixture_path("unqualified_ambiguity/create_beside"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE SCHEMA dbone;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE dbone.alpha (id BIGINT);")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "INSERT INTO dbone.alpha (id) VALUES (1), (2);")->is_success());

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE alpha (id BIGINT);")->is_success());
    require_count(dispatcher, "SELECT * FROM public.alpha;", 0);
    require_count(dispatcher, "SELECT * FROM dbone.alpha;", 2);
}

TEST_CASE("integration::cpp::unqualified_name_ambiguity::other_creates_land_in_public") {
    auto config = test_helpers::make_test_config(integration_fixture_path("unqualified_ambiguity/create_kinds"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE src (id BIGINT);")->is_success());
    for (const char* sql : {"CREATE VIEW src_view AS SELECT id FROM src;",
                            "CREATE MATERIALIZED VIEW src_matview AS SELECT id FROM src WITH NO DATA;",
                            "CREATE SEQUENCE src_seq;",
                            "CREATE FUNCTION src_fn(x INT) RETURNS INT AS 'x -> x';"}) {
        auto cursor = test_helpers::exec(dispatcher, sql);
        INFO("[" << sql << "] " << (cursor->is_error() ? cursor->get_error().what.c_str() : "<no error>"));
        REQUIRE(cursor->is_success());
    }

    const std::vector<std::uint32_t> in_public{namespace_oid(dispatcher, "public")};
    CHECK(relnamespaces_of(dispatcher, "src_view") == in_public);
    CHECK(relnamespaces_of(dispatcher, "src_matview") == in_public);
    CHECK(relnamespaces_of(dispatcher, "src_seq") == in_public);
    CHECK(relnamespaces_of(dispatcher, "src_fn") == in_public);
}

TEST_CASE("integration::cpp::unqualified_name_ambiguity::index_on_public_table") {
    auto config = test_helpers::make_test_config(integration_fixture_path("unqualified_ambiguity/index_public"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE delta (id BIGINT);")->is_success());
    {
        auto cursor = test_helpers::exec(dispatcher, "CREATE INDEX delta_id ON public.delta (id);");
        INFO("[CREATE INDEX] " << (cursor->is_error() ? cursor->get_error().what.c_str() : "<no error>"));
        REQUIRE(cursor->is_success());
    }
    CHECK(relnamespaces_of(dispatcher, "delta_id") == std::vector<std::uint32_t>{namespace_oid(dispatcher, "public")});
}
