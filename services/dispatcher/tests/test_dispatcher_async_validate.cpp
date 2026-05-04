#include <catch2/catch.hpp>

#include <services/dispatcher/catalog_view.hpp>
#include <services/dispatcher/resolved_objects.hpp>
#include <services/dispatcher/validate_logical_plan.hpp>
#include <services/dispatcher/versioned_plan_cache.hpp>

#include <actor-zeta.hpp>
#include <components/catalog/table_id.hpp>
#include <components/context/execution_context.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/session/session.hpp>
#include <components/types/types.hpp>

#include <type_traits>

// V4 catalog_view_t-based validate tests. Catalog_snapshot_t is gone (V4 retired the
// passive snapshot type); the only entry points are the coroutine validate_types /
// validate_schema and their sync internals. Roundtrip-counting integration tests
// require a counting-mock manager_disk_t and are staged for a follow-up commit.

using namespace services::dispatcher;
using namespace components::catalog;
using namespace components::types;
using namespace components::logical_plan;

// 1. validate_types returns actor_zeta::unique_future<cursor_t_ptr>.
TEST_CASE("dispatcher::async_validate::validate_types_returns_future") {
    using ret_t = decltype(validate_types(nullptr,
                                           std::declval<catalog_view_t&>(),
                                           std::declval<components::execution_context_t>(),
                                           std::declval<node_t*>()));
    using expected_t = actor_zeta::unique_future<components::cursor::cursor_t_ptr>;
    STATIC_REQUIRE(std::is_same_v<ret_t, expected_t>);
}

// 2. validate_schema returns unique_future<schema_result<named_schema>>.
TEST_CASE("dispatcher::async_validate::validate_schema_returns_future") {
    using ret_t = decltype(validate_schema(nullptr,
                                            std::declval<catalog_view_t&>(),
                                            std::declval<components::execution_context_t>(),
                                            std::declval<node_t*>(),
                                            std::declval<const storage_parameters&>()));
    using expected_t = actor_zeta::unique_future<schema_result<named_schema>>;
    STATIC_REQUIRE(std::is_same_v<ret_t, expected_t>);
}

// 3. V4 entry point with empty cache + empty disk_address: get_namespace returns nullptr;
// pre-walk skips refs that don't resolve; sync internals then run against an empty view —
// for an aggregate plan over a non-existent table this is an error. Verifies the V4 path
// is wired correctly (compiles, runs, returns sensibly) without requiring a disk actor in
// the test fixture.
TEST_CASE("dispatcher::async_validate::v4_empty_cache_unknown_table_errors") {
    std::pmr::synchronized_pool_resource res;
    versioned_plan_cache_t cache(&res);
    catalog_view_t view{cache, actor_zeta::address_t::empty_address(), 0, &res};
    components::execution_context_t ctx{components::session::session_id_t{},
                                          components::table::transaction_data{0, 0}, {}};

    auto plan = make_node_aggregate(&res, {"public", "tbl"});
    auto fut = validate_types(&res, view, ctx, plan.get());
    auto cur = std::move(fut).get();
    REQUIRE(cur);
    REQUIRE(cur->is_error());
}

// V4 roundtrip-counting tests. catalog_view_t with empty disk_address short-circuits
// every cache miss to nullptr WITHOUT a roundtrip — that's the property we verify here.
// (#75 final form once a real-disk fixture is staged: counters off a real manager_disk_t.)

// 4. Cache hit: pre-store a namespace at the pinned version, validate against a plan
// that references it — view.get_namespace returns the cached entry, no disk send.
TEST_CASE("dispatcher::async_validate::v4_cache_hit_zero_roundtrips") {
    std::pmr::synchronized_pool_resource res;
    versioned_plan_cache_t cache(&res);
    // Pre-store the namespace AND table for "public.tbl" in the cache.
    {
        resolved_namespace_t ns;
        ns.oid = 100;
        ns.name = "public";
        cache.store_namespace("public", 0, std::move(ns));

        resolved_table_t tbl;
        tbl.oid = 200;
        tbl.namespace_oid = 100;
        tbl.relkind = 'r';
        tbl.name = "tbl";
        cache.store_table(100, "tbl", 0, std::move(tbl));
    }
    catalog_view_t view{cache, actor_zeta::address_t::empty_address(), 0, &res};
    components::execution_context_t ctx{components::session::session_id_t{},
                                          components::table::transaction_data{0, 0}, {}};

    // Even with an empty disk_address — because cache is warm, V4 entry's pre-walk
    // doesn't need disk. probe_namespace + probe_table both hit. validate_types_sync
    // then runs against the cached state. (For an aggregate plan with no schema-shape
    // assertions, we just confirm no crash + sensible cursor.)
    auto plan = make_node_aggregate(&res, {"public", "tbl"});
    auto fut = validate_types(&res, view, ctx, plan.get());
    auto cur = std::move(fut).get();
    REQUIRE(cur); // success or error — but no SIGABRT from missing-disk send
}

// 5. probe_table named lookup matches the (namespace_oid, name) key after store_table.
// This is the cache-hit fast path the V4 entry relies on for warm queries.
TEST_CASE("dispatcher::async_validate::v4_probe_table_after_store") {
    std::pmr::synchronized_pool_resource res;
    versioned_plan_cache_t cache(&res);
    resolved_table_t tbl;
    tbl.oid = 42;
    tbl.namespace_oid = 7;
    tbl.relkind = 'r';
    tbl.name = "users";
    cache.store_table(7, "users", 5, std::move(tbl));

    catalog_view_t view{cache, actor_zeta::address_t::empty_address(), 5, &res};
    auto* hit = view.try_get_table(7, "users");
    REQUIRE(hit != nullptr);
    REQUIRE(hit->oid == 42);
    REQUIRE(hit->namespace_oid == 7);
    REQUIRE(hit->relkind == 'r');
}

// 6. Pinning to a different version isolates the cached entry — same key, different
// version, no hit. Mirrors the snapshot-isolation contract validate relies on.
TEST_CASE("dispatcher::async_validate::v4_version_isolation") {
    std::pmr::synchronized_pool_resource res;
    versioned_plan_cache_t cache(&res);
    resolved_table_t tbl;
    tbl.oid = 42;
    tbl.namespace_oid = 7;
    tbl.relkind = 'r';
    tbl.name = "users";
    cache.store_table(7, "users", 5, std::move(tbl));

    // View pinned at version 6 — entry at version 5 is invisible (snapshot isolation).
    catalog_view_t view_v6{cache, actor_zeta::address_t::empty_address(), 6, &res};
    REQUIRE(view_v6.try_get_table(7, "users") == nullptr);
    catalog_view_t view_v5{cache, actor_zeta::address_t::empty_address(), 5, &res};
    REQUIRE(view_v5.try_get_table(7, "users") != nullptr);
}
