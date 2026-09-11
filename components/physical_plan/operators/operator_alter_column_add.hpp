#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/column_definition.hpp>

namespace components::operators {

    // resolve_table re-reads pg_attribute on every call; no in-memory schema hook here either.
    class operator_alter_column_add_t final : public read_write_operator_t {
    public:
        operator_alter_column_add_t(std::pmr::memory_resource* resource,
                                    log_t log,
                                    components::catalog::oid_t table_oid,
                                    components::table::column_definition_t column);

        // Sourceless sink leaf driven via the bottom-up needs_async_finalize pass; push()/finalize() default to no-ops.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t table_oid_;
        components::table::column_definition_t column_;
    };

} // namespace components::operators
