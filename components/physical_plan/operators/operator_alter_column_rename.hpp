#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // Matches the live row by (attrelid, attname), not attoid_: node_alter_column_t::set_attoid has
    // no callers, so attoid_ is always INVALID. Re-appends the row carrying over
    // attoid/attnum/atttypid/added_at_commit_id (rename is identity-preserving); the storage-side
    // rename applies AFTER the WAL commit marker, so an ABORT never has anything to undo.
    class operator_alter_column_rename_t final : public read_write_operator_t {
    public:
        operator_alter_column_rename_t(std::pmr::memory_resource* resource,
                                       log_t log,
                                       components::catalog::oid_t table_oid,
                                       components::catalog::oid_t attoid,
                                       std::string old_name,
                                       std::string new_name);

        // Sourceless sink leaf driven via the bottom-up needs_async_finalize pass; push()/finalize() default to no-ops.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t table_oid_;
        components::catalog::oid_t attoid_;
        std::string old_name_;
        std::string new_name_;
    };

} // namespace components::operators
