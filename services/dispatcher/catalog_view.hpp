#pragma once

// KEEP per task #75 — see docs/phase7-design-decisions.md.
// catalog_view_t is retained as a thin per-execute_plan facade over the
// disk actor. It is constructed fresh on each execute_plan (no longer-lived
// caching), so it has no stale-data problem; rewriting validate_logical_plan
// (~1865 LOC) to use raw read_rows_by_key would be high-churn / low-gain.

#include "resolved_objects.hpp"

#include <actor-zeta.hpp>
#include <actor-zeta/detail/future.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace services::dispatcher {

    // Thin wrapper over disk_address_. Каждый get_* делает actor send к manager_disk,
    // try_get_* — синхронный пробник по локальному unordered_map per-instance кэшу
    // (заполняется в рамках одного execute_plan, чтобы не делать дублирующих round-trip'ов
    // в течение одной валидации). Между вызовами execute_plan кэш не сохраняется.
    class catalog_view_t {
    public:
        template<typename T>
        using unique_future = actor_zeta::unique_future<T>;

        catalog_view_t(actor_zeta::address_t disk_address,
                        std::pmr::memory_resource* resource) noexcept
            : disk_address_(std::move(disk_address))
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

        // Synchronous probes — return nullptr on miss, no disk roundtrip. Useful for
        // code paths that have already pre-loaded the cache (e.g. inside
        // validate_schema_sync after validate_schema (coroutine) pre-walked the AST).
        const resolved_namespace_t* try_get_namespace(std::string_view name) const noexcept;
        const resolved_table_t* try_get_table(components::catalog::oid_t namespace_oid,
                                                std::string_view name) const noexcept;
        const resolved_type_t* try_get_type(components::catalog::oid_t namespace_oid,
                                              std::string_view name) const noexcept;

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

        // CHECK constraint snapshot getter.
        // Returns vector of (constraint_name, expression_string) for all CHECK constraints
        // on table_oid (contype='c', non-null conexpr).
        unique_future<std::vector<std::pair<std::string, std::string>>>
        get_check_exprs(components::execution_context_t ctx,
                         components::catalog::oid_t table_oid);

    private:
        actor_zeta::address_t disk_address_;
        std::pmr::memory_resource* resource_;

        // Per-instance кэш на время одного execute_plan / size / get_schema. Заполняется
        // get_*() и читается try_get_*(), чтобы избежать дублирующих round-trip'ов внутри
        // одной валидации. После выхода из метода объект уничтожается. Ключи tbl/type/fn —
        // составная строка (oid|name) — чтобы не вводить кастомных hash-functor'ов.
        std::unordered_map<std::string, resolved_namespace_t> ns_cache_;
        std::unordered_map<std::string, resolved_table_t> tbl_cache_;
        std::unordered_map<std::string, resolved_type_t> type_cache_;
        std::unordered_map<components::catalog::oid_t, std::vector<resolved_fk_t>> fk_outgoing_;
        std::unordered_map<components::catalog::oid_t, std::vector<resolved_fk_t>> fk_referencing_;
    };

} // namespace services::dispatcher