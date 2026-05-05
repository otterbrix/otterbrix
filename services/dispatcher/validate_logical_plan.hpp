#pragma once

#include <actor-zeta/detail/future.hpp>

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/dependency_walker.hpp>
#include <components/context/execution_context.hpp>
#include <components/cursor/cursor.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/types.hpp>

#include <functional>
#include <span>
#include <string_view>

namespace components::catalog {
    class table_id;
}

namespace services::dispatcher {

    class catalog_view_t;

    using column_path = std::pmr::vector<size_t>;
    struct type_from_t {
        std::string result_alias;
        components::types::complex_logical_type type;
    };
    struct type_path_t {
        column_path path;
        components::types::complex_logical_type type;
    };

    using named_schema = std::pmr::vector<type_from_t>;
    using type_paths = std::pmr::vector<type_path_t>;

    // Useful for data, that could not be stored in components::cursor::cursor_t_ptr
    template<typename T>
    class schema_result {
    public:
        explicit schema_result(T&& value)
            : schema_(std::forward<T>(value))
            , error_(components::cursor::error_code_t::none) {}
        explicit schema_result(std::pmr::memory_resource* resource, components::cursor::error_t error)
            : schema_(resource)
            , error_(std::move(error)) {}

        bool is_error() const { return error_.type != components::cursor::error_code_t::none; }
        const components::cursor::error_t& error() const { return error_; }
        const T& value() const { return schema_; }
        T& value() { return schema_; }

    private:
        T schema_;
        components::cursor::error_t error_;
    };

    // V4 view-based check_*_exists. Use try_get_* (sync probe of cache); caller must ensure the
    // referenced names are pre-loaded (V4 entry points do this via co_await on miss).
    // nullptr from try_get_* maps to a not-found error cursor.
    components::cursor::cursor_t_ptr check_namespace_exists(std::pmr::memory_resource* resource,
                                                            const catalog_view_t& view,
                                                            const components::catalog::table_id& id);
    components::cursor::cursor_t_ptr check_collection_exists(std::pmr::memory_resource* resource,
                                                             const catalog_view_t& view,
                                                             const components::catalog::table_id& id);
    // Probe `alias` against each namespace in `search_path` (in order). Returns success
    // on first hit. If `search_path` is empty, falls back to {public, pg_catalog}.
    // Caller is responsible for pre-loading via co_await view.get_type for each namespace
    // it intends to search — this function is a sync probe of the cache.
    components::cursor::cursor_t_ptr
    check_type_exists(std::pmr::memory_resource* resource,
                       const catalog_view_t& view,
                       const std::string& alias,
                       std::span<const components::catalog::oid_t> search_path = {});

    // Walk a complex_logical_type tree and invoke `visit(name)` for every nested STRUCT,
    // ENUM, or UNKNOWN node whose type_name() is non-empty. Used by DDL pre-load and by
    // validate's table-type pre-walk to enumerate every UDT reference (recursively, at
    // arbitrary depth). Visitor receives a std::string_view; lifetime is the type tree.
    void walk_user_type_refs(const components::types::complex_logical_type& type,
                              const std::function<void(std::string_view)>& visit);

    // V4 entry points — pre-walk the plan AST collecting referenced collections, co_await
    // catalog_view_t::get_namespace / get_table for each (cache hit = 0 roundtrips, miss =
    // 1 per missing item), then delegate to the sync internals.
    //
    // Spec ref: docs/v4-catalog-refactoring.md §5 Phase E.3.
    actor_zeta::unique_future<components::cursor::cursor_t_ptr>
    validate_types(std::pmr::memory_resource* resource,
                   catalog_view_t& view,
                   components::execution_context_t ctx,
                   components::logical_plan::node_t* node);
    [[nodiscard]] actor_zeta::unique_future<schema_result<named_schema>>
    validate_schema(std::pmr::memory_resource* resource,
                    catalog_view_t& view,
                    components::execution_context_t ctx,
                    components::logical_plan::node_t* node,
                    const components::logical_plan::storage_parameters& parameters);

    // Sync internals — the coroutine versions wrap these and immediately co_return. Not
    // exposed to callers; kept here so dispatcher tests can invoke validate without an
    // actor scheduler when needed.
    components::cursor::cursor_t_ptr
    validate_types_sync(std::pmr::memory_resource* resource,
                        const catalog_view_t& view,
                        components::logical_plan::node_t* node);
    schema_result<named_schema>
    validate_schema_sync(std::pmr::memory_resource* resource,
                         const catalog_view_t& view,
                         components::logical_plan::node_t* node,
                         const components::logical_plan::storage_parameters& parameters);

    // validate_drop_restrict: check that no 'n'-type pg_depend rows block a RESTRICT drop.
    // fetch_deps is a closure over the pg_depend snapshot (provided by caller from
    // catalog_view or disk). Returns nullptr on success, error cursor on RESTRICT violation.
    components::cursor::cursor_t_ptr
    validate_drop_restrict(std::pmr::memory_resource*               resource,
                           components::catalog::oid_t               seed_classid,
                           components::catalog::oid_t               seed_oid,
                           const components::catalog::fetch_deps_fn& fetch_deps);

    // validate_type_recursion: for CREATE TABLE / CREATE TYPE nodes, detect circular user-
    // defined type references (e.g. STRUCT A { b: B } and STRUCT B { a: A }).
    // Returns nullptr on success, error cursor if a cycle is detected.
    // view must have the existing user types pre-loaded (via prior co_await get_type calls).
    components::cursor::cursor_t_ptr
    validate_type_recursion(std::pmr::memory_resource*                      resource,
                            const catalog_view_t&                           view,
                            const components::types::complex_logical_type&  root_type);

} // namespace services::dispatcher