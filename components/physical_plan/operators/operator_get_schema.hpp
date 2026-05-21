#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    // Operator implementation of manager_dispatcher_t::get_schema.
    //
    // For each (database, collection) id, resolves the namespace OID via
    // pg_namespace.nspname → oid, the table OID via pg_class.(relname,
    // relnamespace) → oid + relkind, then the column set via pg_attribute.
    // attrelid → rows. Each non-dropped pg_attribute row yields one
    // complex_logical_type field (atttypspec preferred, atttypid → builtin
    // fallback).
    //
    // Per-id output:
    //   * relkind 'r'/non-computed table  → STRUCT alias "schema"
    //   * relkind 'g' computed table      → STRUCT alias "latest_types"
    //   * unresolved (missing ns or table) → logical_type::INVALID
    //
    // Cursor-format invariant: schemas_ has exactly ids_.size() entries on
    // completion, in input order. take_schemas() transfers ownership and
    // matches the cursor produced by the legacy dispatcher path
    // (make_cursor(resource, std::pmr::vector<complex_logical_type>)).
    class operator_get_schema_t final : public read_only_operator_t {
    public:
        operator_get_schema_t(std::pmr::memory_resource* resource,
                              log_t log,
                              std::vector<std::pair<std::string, std::string>> ids);

        // Move out the resolved schemas after execution. Caller owns the
        // resulting vector; the operator's internal state is left empty.
        std::pmr::vector<types::complex_logical_type> take_schemas();

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::vector<std::pair<std::string, std::string>> ids_;
        std::pmr::vector<types::complex_logical_type> schemas_;
    };

} // namespace components::operators
