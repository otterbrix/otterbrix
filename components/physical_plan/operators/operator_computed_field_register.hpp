#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/table/column_definition.hpp>

#include <vector>

namespace components::operators {

    // Runs after an INSERT into a relkind='g' (computing) table; (re)registers each column into pg_computed_column. A
    // SAME-TYPE row is a no-op -- refcount is alive/dead only, not bumped per INSERT.
    class operator_computed_field_register_t final : public read_write_operator_t {
    public:
        operator_computed_field_register_t(std::pmr::memory_resource* resource,
                                           log_t log,
                                           components::catalog::oid_t table_oid,
                                           std::vector<components::table::column_definition_t> columns);

        // SINK with an async commit: INSERT-into-relkind='g' lowers to sequence_t(insert, register), and
        // needs_async_finalize drives register.await bottom-up AFTER insert.await, so register.push() is never reached;
        // push()/finalize() inherit the no-op defaults.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::catalog::oid_t table_oid_;
        std::vector<components::table::column_definition_t> columns_;
    };

} // namespace components::operators
