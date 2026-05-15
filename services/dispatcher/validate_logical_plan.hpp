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
#include <components/types/user_type_walk.hpp>

#include <functional>
#include <span>
#include <string_view>

namespace components::catalog {
    class table_id;
}

namespace services::dispatcher {

    namespace impl {
        struct plan_resolve_index_t;
    }

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

    // Existence checks read from the plan-tree idx supplied explicitly by
    // the dispatcher. Returns nullptr on hit, otherwise an error cursor with
    // the standard "database/collection does not exist" diagnostic.
    components::cursor::cursor_t_ptr check_namespace_exists(std::pmr::memory_resource* resource,
                                                            const impl::plan_resolve_index_t* idx,
                                                            const components::catalog::table_id& id);
    components::cursor::cursor_t_ptr check_collection_exists(std::pmr::memory_resource* resource,
                                                             const impl::plan_resolve_index_t* idx,
                                                             const components::catalog::table_id& id);
    // Probe `alias` against the plan-tree idx (impl::type_md_for) for each
    // dbname in `search_dbnames` in order. Returns success on first hit.
    // If `search_dbnames` is empty, falls back to {"public", "pg_catalog"}.
    // The transformer emits resolve_type for every (dbname, alias) tuple
    // this helper queries.
    components::cursor::cursor_t_ptr
    check_type_exists(std::pmr::memory_resource* resource,
                       const impl::plan_resolve_index_t* idx,
                       const std::string& alias,
                       std::span<const std::string> search_dbnames = {});

    // Walk a complex_logical_type tree and visit every nested UDT reference.
    // Implementation lives in components/types/user_type_walk.hpp so transformer,
    // dispatcher and validate share one impl. The using-declaration keeps
    // existing call sites unchanged.
    using ::components::types::walk_user_type_refs;

    // Entry points — read referenced catalog metadata from the plan-tree
    // resolve idx (populated by Pass 1's operator_resolve_*_t), then delegate
    // to the sync internals. All catalog reads go through the resolve idx.
    // `idx` is supplied by the dispatcher (built once after Pass 1); recursive
    // validate_schema_sync calls thread the same pointer through.
    actor_zeta::unique_future<components::cursor::cursor_t_ptr>
    validate_types(std::pmr::memory_resource* resource,
                   components::execution_context_t ctx,
                   const impl::plan_resolve_index_t* idx,
                   components::logical_plan::node_t* node);
    [[nodiscard]] actor_zeta::unique_future<schema_result<named_schema>>
    validate_schema(std::pmr::memory_resource* resource,
                    components::execution_context_t ctx,
                    const impl::plan_resolve_index_t* idx,
                    components::logical_plan::node_t* node,
                    const components::logical_plan::storage_parameters& parameters);

    // Sync internals — the coroutine versions wrap these and immediately co_return. Not
    // exposed to callers; kept here so dispatcher tests can invoke validate without an
    // actor scheduler when needed.
    components::cursor::cursor_t_ptr
    validate_types_sync(std::pmr::memory_resource* resource,
                        const impl::plan_resolve_index_t* idx,
                        components::logical_plan::node_t* node);
    schema_result<named_schema>
    validate_schema_sync(std::pmr::memory_resource* resource,
                         const impl::plan_resolve_index_t* idx,
                         components::logical_plan::node_t* node,
                         const components::logical_plan::storage_parameters& parameters);

    // validate_drop_restrict: check that no 'n'-type pg_depend rows block a RESTRICT drop.
    // fetch_deps is a closure over the pg_depend snapshot (provided by caller from disk).
    // Returns nullptr on success, error cursor on RESTRICT violation.
    components::cursor::cursor_t_ptr
    validate_drop_restrict(std::pmr::memory_resource*               resource,
                           components::catalog::oid_t               seed_classid,
                           components::catalog::oid_t               seed_oid,
                           const components::catalog::fetch_deps_fn& fetch_deps);

    // validate_type_recursion: for CREATE TABLE / CREATE TYPE nodes, detect circular user-
    // defined type references (e.g. STRUCT A { b: B } and STRUCT B { a: A }).
    // Returns nullptr on success, error cursor if a cycle is detected.
    // `idx` supplies the plan-tree type metadata index.
    components::cursor::cursor_t_ptr
    validate_type_recursion(std::pmr::memory_resource*                      resource,
                            const impl::plan_resolve_index_t*               idx,
                            const components::types::complex_logical_type&  root_type);

} // namespace services::dispatcher