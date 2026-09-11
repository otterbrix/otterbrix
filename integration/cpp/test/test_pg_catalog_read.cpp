#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <string>

// pg_class / pg_attribute carry rows about the catalog itself (seeded at bootstrap, as
// PostgreSQL does), so SQL reads over pg_catalog resolve through the same resolver as any
// user relation. These cases pin the READ side; the write side is pinned by
// test_pg_catalog_dml_guard.cpp.

using namespace components;

TEST_CASE("integration::cpp::pg_catalog_read::pg_class_lists_user_tables") {
    auto config = test_helpers::make_test_config(integration_fixture_path("pg_catalog_read/base"));
    config.log.level = log_t::level::off;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    REQUIRE(test_helpers::exec(dispatcher, "CREATE DATABASE readdb;")->is_success());
    REQUIRE(test_helpers::exec(dispatcher, "CREATE TABLE readdb.alpha (id BIGINT, name STRING);")->is_success());

    {
        auto cursor =
            test_helpers::exec(dispatcher, "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'alpha';");
        INFO("error: " << (cursor->is_error() ? cursor->get_error().what.c_str() : "<none>"));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 1);
    }

    {
        // The catalog's self-rows: pg_class knows pg_class.
        auto cursor =
            test_helpers::exec(dispatcher, "SELECT relname FROM pg_catalog.pg_class WHERE relname = 'pg_class';");
        INFO("error: " << (cursor->is_error() ? cursor->get_error().what.c_str() : "<none>"));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() == 1);
    }

    {
        // pg_attribute knows the columns of the table just created.
        auto cursor = test_helpers::exec(
            dispatcher,
            "SELECT attname FROM pg_catalog.pg_attribute WHERE attname = 'id' AND attisdropped = false;");
        INFO("error: " << (cursor->is_error() ? cursor->get_error().what.c_str() : "<none>"));
        REQUIRE(cursor->is_success());
        REQUIRE(cursor->size() >= 1);
    }
}
