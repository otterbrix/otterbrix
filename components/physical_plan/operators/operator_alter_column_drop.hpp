#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // Resolves dependents from pg_depend, aborting (RESTRICT) or dropping them (CASCADE) BEFORE
    // soft-deleting the column; no in-memory schema hook — resolve_table re-reads pg_attribute for the tombstone.
    class operator_alter_column_drop_t final : public read_write_operator_t {
    public:
        operator_alter_column_drop_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::catalog::oid_t table_oid,
                                     core::columnname_t column_name,
                                     qualified_name_t relation,
                                     char relkind,
                                     components::catalog::oid_t attoid,
                                     components::catalog::drop_behavior_t behavior);

        // Sourceless sink leaf driven via the bottom-up needs_async_finalize pass; push()/finalize() default to no-ops.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t table_oid_;
        // No namespace_oid_: the column resolves by (attrelid=table_oid_, attname), and table_oid is already
        // unique across namespaces, so a stored namespace oid would be dead state nobody reads.
        core::columnname_t column_name_;
        qualified_name_t relation_;
        char relkind_;
        components::catalog::oid_t attoid_;
        components::catalog::drop_behavior_t behavior_;
    };

} // namespace components::operators
