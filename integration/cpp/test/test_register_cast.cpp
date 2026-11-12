#include "integration_fixture_path.hpp"
#include "test_config.hpp"

#include <catch2/catch_test_macros.hpp>

#include <components/casts/cast_registry.hpp>

using namespace components;
using namespace components::casts;
using types::complex_logical_type;
using types::logical_type;

namespace {

    core::error_t
    noop_cast(const vector::vector_t&, vector::vector_t*, const graph_execution_context&, uint64_t) noexcept {
        return core::error_t::no_error();
    }

    cast_entry make_entry() {
        return cast_entry{cast_function_t{noop_cast, nullptr},
                          cast_cost{.precision_loss = 0, .footprint = 8},
                          /*convertable_inplace*/ false};
    }

    // BOOLEAN -> DATE has no default cast, so it is a clean slate to register.
    const complex_logical_type kSource{logical_type::BOOLEAN};
    const complex_logical_type kTarget{logical_type::DATE};

} // namespace

TEST_CASE("integration::cpp::register_cast::duplicate_not_allowed") {
    auto config = test_create_config(integration_fixture_path("test_register_cast_dup"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    // First registration of a novel cast succeeds across every executor registry.
    CHECK_FALSE(dispatcher->register_cast(session, kSource, kTarget, make_entry()).contains_error());
    // Re-registering the same (source, target) pair is rejected.
    CHECK(dispatcher->register_cast(session, kSource, kTarget, make_entry()).contains_error());
}

TEST_CASE("integration::cpp::register_cast::unregistered_type_rejected") {
    auto config = test_create_config(integration_fixture_path("test_register_cast_udt"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // Name-only type: the resolve pipeline has to look it up in the catalog,
    // exactly as a UDT written in SQL would arrive.
    const auto udt = complex_logical_type::create_unknown("cast_udt");
    const complex_logical_type builtin{logical_type::BOOLEAN};

    // Nothing named cast_udt exists yet, so neither direction can be registered.
    {
        auto session = otterbrix::session_id_t();
        CHECK(dispatcher->register_cast(session, udt, builtin, make_entry()).contains_error());
    }
    {
        auto session = otterbrix::session_id_t();
        CHECK(dispatcher->register_cast(session, builtin, udt, make_entry()).contains_error());
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TYPE cast_udt AS ENUM ('odd', 'even');")->is_success());
    }
    // The type now resolves, so both directions register.
    {
        auto session = otterbrix::session_id_t();
        CHECK_FALSE(dispatcher->register_cast(session, udt, builtin, make_entry()).contains_error());
    }
    {
        auto session = otterbrix::session_id_t();
        CHECK_FALSE(dispatcher->register_cast(session, builtin, udt, make_entry()).contains_error());
    }
}

TEST_CASE("integration::cpp::register_cast::unregister_deletes") {
    auto config = test_create_config(integration_fixture_path("test_register_cast_del"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    CHECK_FALSE(dispatcher->register_cast(session, kSource, kTarget, make_entry()).contains_error());
    // Unregister removes it everywhere; a fresh registration then succeeds again —
    // which it could not if the cast still existed (it would be a duplicate).
    CHECK_FALSE(dispatcher->unregister_cast(session, kSource, kTarget).contains_error());
    CHECK_FALSE(dispatcher->register_cast(session, kSource, kTarget, make_entry()).contains_error());
}

namespace {

    std::size_t rows_of(otterbrix::wrapper_dispatcher_t* dispatcher, const std::string& sql) {
        auto cursor = dispatcher->execute_sql(otterbrix::session_id_t(), sql);
        INFO(sql);
        REQUIRE(cursor->is_success());
        return cursor->size();
    }

} // namespace

// pg_cast lists the built-in casts as the registry has them (castcontext from its level), not only registered ones;
// casts to and from text are I/O conversions, which PostgreSQL does not list either.
TEST_CASE("integration::cpp::register_cast::builtin_casts_in_pg_cast") {
    auto config = test_create_config(integration_fixture_path("test_register_cast_builtin_rows"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    CHECK(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_cast WHERE oid < 16384;") > 0);
    CHECK(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_cast WHERE oid >= 16384;") == 0);
    // int4 -> int8 widens implicitly, int8 -> int4 narrows on assignment, int4 -> oid as PostgreSQL has it.
    CHECK(rows_of(dispatcher,
                  "SELECT oid FROM pg_catalog.pg_cast WHERE castsource = 23 AND casttarget = 20 "
                  "AND castcontext = 'i';") == 1);
    CHECK(rows_of(dispatcher,
                  "SELECT oid FROM pg_catalog.pg_cast WHERE castsource = 20 AND casttarget = 23 "
                  "AND castcontext = 'a';") == 1);
    CHECK(rows_of(dispatcher,
                  "SELECT oid FROM pg_catalog.pg_cast WHERE castsource = 23 AND casttarget = 26 "
                  "AND castcontext = 'i';") == 1);
    CHECK(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_cast WHERE castsource = 25 OR casttarget = 25;") == 0);
}

// A built-in cast is required by the database system, as PostgreSQL refuses DROP CAST on a pinned one.
TEST_CASE("integration::cpp::register_cast::builtin_cast_not_unregistered") {
    auto config = test_create_config(integration_fixture_path("test_register_cast_builtin_drop"));
    test_clear_directory(config);
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    const complex_logical_type int4{logical_type::INTEGER};
    const complex_logical_type int8{logical_type::BIGINT};
    CHECK(dispatcher->unregister_cast(otterbrix::session_id_t(), int4, int8).contains_error());
    CHECK(rows_of(dispatcher, "SELECT oid FROM pg_catalog.pg_cast WHERE castsource = 23 AND casttarget = 20;") == 1);
}