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
    auto config = test_create_config("/tmp/test_register_cast_dup");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    // First registration of a novel cast succeeds across every executor registry.
    CHECK(dispatcher->register_cast(session, kSource, kTarget, make_entry()));
    // Re-registering the same (source, target) pair is rejected.
    CHECK_FALSE(dispatcher->register_cast(session, kSource, kTarget, make_entry()));
}

TEST_CASE("integration::cpp::register_cast::unregistered_type_rejected") {
    auto config = test_create_config("/tmp/test_register_cast_udt");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    // Name-only type: the resolve pipeline has to look it up in the catalog,
    // exactly as a UDT written in SQL would arrive.
    const auto udt = complex_logical_type::create_unknown("cast_udt");
    const complex_logical_type builtin{logical_type::BOOLEAN};

    // Nothing named cast_udt exists yet, so neither direction can be registered.
    {
        auto session = otterbrix::session_id_t();
        CHECK_FALSE(dispatcher->register_cast(session, udt, builtin, make_entry()));
    }
    {
        auto session = otterbrix::session_id_t();
        CHECK_FALSE(dispatcher->register_cast(session, builtin, udt, make_entry()));
    }
    {
        auto session = otterbrix::session_id_t();
        REQUIRE(dispatcher->execute_sql(session, "CREATE TYPE cast_udt AS ENUM ('odd', 'even');")->is_success());
    }
    // The type now resolves, so both directions register.
    {
        auto session = otterbrix::session_id_t();
        CHECK(dispatcher->register_cast(session, udt, builtin, make_entry()));
    }
    {
        auto session = otterbrix::session_id_t();
        CHECK(dispatcher->register_cast(session, builtin, udt, make_entry()));
    }
}

TEST_CASE("integration::cpp::register_cast::unregister_deletes") {
    auto config = test_create_config("/tmp/test_register_cast_del");
    test_clear_directory(config);
    config.disk.on = false;
    config.wal.on = false;
    test_spaces space(config);
    auto* dispatcher = space.dispatcher();

    auto session = otterbrix::session_id_t();
    CHECK(dispatcher->register_cast(session, kSource, kTarget, make_entry()));
    // Unregister removes it everywhere; a fresh registration then succeeds again —
    // which it could not if the cast still existed (it would be a duplicate).
    CHECK(dispatcher->unregister_cast(session, kSource, kTarget));
    CHECK(dispatcher->register_cast(session, kSource, kTarget, make_entry()));
}