#pragma once

#include "resolved_objects.hpp"
#include "versioned_plan_cache.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/detail/future.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>

#include <cstdint>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace services::dispatcher {

    // V4 catalog access facade. Wraps the dispatcher's plan cache + the disk actor's
    // address + the transaction's pinned catalog version. Exposes async getters that
    // probe the cache first and co_await the disk on miss (filling the cache before
    // returning).
    //
    // Pointer lifetime: the cache holds entries by value at (key, version). Pinned
    // versions are protected from eviction, so as long as the caller's transaction is
    // pinned at `pinned_version`, returned pointers remain valid for the duration of
    // execute_plan. Do not store across pin boundaries (commit/abort_transaction).
    //
    // Spec ref: catalog-migration-to-postgresql-style.md §181-198 (V4), §1083-1106
    // (cache flow).
    class catalog_view_t {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        catalog_view_t(versioned_plan_cache_t& cache,
                        actor_zeta::address_t disk_address,
                        std::uint64_t pinned_version,
                        std::pmr::memory_resource* resource) noexcept
            : cache_(cache)
            , disk_address_(std::move(disk_address))
            , pinned_version_(pinned_version)
            , resource_(resource) {}

        // actor_zeta's coroutine allocator looks up `this->resource()` to allocate the
        // coroutine state when an actor-style member coroutine yields. catalog_view_t isn't
        // an actor, but get_*() methods use co_await on actor sends — exposing the caller-
        // provided resource via this getter lets the framework allocate without requiring
        // catalog_view_t to be an actor.
        std::pmr::memory_resource* resource() const noexcept { return resource_; }

        // Async getters. Probe cache; on miss send to disk and store result before
        // returning. nullptr indicates "not found in catalog" OR "disk send failed"
        // (caller treats both as error). Caller is expected to convert nullptr to a
        // cursor_t_ptr error at validate time.
        unique_future<const resolved_namespace_t*>
        get_namespace(components::execution_context_t ctx, std::string name);

        unique_future<const resolved_table_t*>
        get_table(components::execution_context_t ctx,
                   components::catalog::oid_t namespace_oid,
                   std::string name);

        unique_future<const resolved_type_t*>
        get_type(components::execution_context_t ctx,
                  components::catalog::oid_t namespace_oid,
                  std::string name);

        unique_future<const resolved_function_t*>
        get_function(components::execution_context_t ctx,
                      components::catalog::oid_t namespace_oid,
                      std::string name,
                      std::vector<components::catalog::oid_t> arg_type_oids);

        // Cross-namespace function lookup by name only — wraps disk's
        // resolve_function_by_name (#45). Used by validate's overload resolution
        // (function_name_exists / function_exists / get_function migration in #57)
        // where the caller knows the function name but not the exact arg-type signature
        // and must iterate to find a matching overload.
        //
        // Each returned pointer is stored in cache keyed by (name, arg_type_oids per
        // signature). Subsequent same-args lookups via probe_function hit those entries.
        unique_future<std::vector<const resolved_function_t*>>
        get_functions_by_name(components::execution_context_t ctx, std::string name);

        // Synchronous probes — return nullptr on miss, no disk roundtrip. Useful for
        // code paths that have already pre-loaded the cache (e.g. inside
        // validate_schema_sync after validate_schema (coroutine) pre-walked the AST).
        const resolved_namespace_t* try_get_namespace(std::string_view name) const noexcept;
        const resolved_table_t* try_get_table(components::catalog::oid_t namespace_oid,
                                                std::string_view name) const noexcept;
        const resolved_type_t* try_get_type(components::catalog::oid_t namespace_oid,
                                              std::string_view name) const noexcept;
        const resolved_function_t*
        try_get_function(std::string_view name,
                          std::span<const components::catalog::oid_t> arg_type_oids) const noexcept;

        // FK snapshot getters.  Both async variants call disk.read_rows_by_key on
        // pg_constraint + pg_attribute to build fully-resolved resolved_fk_t entries
        // and store them in the per-txn fk_outgoing_ / fk_referencing_ maps.
        // The sync probes hit those maps without any disk round-trip.
        //
        // get_fks_outgoing: FKs where table_oid is the CHILD (for INSERT/UPDATE checks).
        // get_fks_referencing: FKs where table_oid is the PARENT (for DELETE cascade).
        unique_future<std::vector<resolved_fk_t>>
        get_fks_outgoing(components::execution_context_t ctx,
                          components::catalog::oid_t table_oid);

        unique_future<std::vector<resolved_fk_t>>
        get_fks_referencing(components::execution_context_t ctx,
                             components::catalog::oid_t table_oid);

        const std::vector<resolved_fk_t>*
        try_get_fks_outgoing(components::catalog::oid_t table_oid) const noexcept;

        const std::vector<resolved_fk_t>*
        try_get_fks_referencing(components::catalog::oid_t table_oid) const noexcept;

        // Inspectors.
        std::uint64_t pinned_version() const noexcept { return pinned_version_; }
        actor_zeta::address_t disk_address() const noexcept { return disk_address_; }
        versioned_plan_cache_t& cache() noexcept { return cache_; }

    private:
        versioned_plan_cache_t& cache_;
        actor_zeta::address_t disk_address_;
        std::uint64_t pinned_version_;
        std::pmr::memory_resource* resource_;

        // Per-txn FK snapshots (not in versioned_plan_cache — keyed by table_oid).
        std::unordered_map<components::catalog::oid_t, std::vector<resolved_fk_t>> fk_outgoing_;
        std::unordered_map<components::catalog::oid_t, std::vector<resolved_fk_t>> fk_referencing_;
    };

} // namespace services::dispatcher