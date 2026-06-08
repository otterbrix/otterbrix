#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // ALTER TABLE ... DROP COLUMN — single clause. Resolves dependents from
    // pg_depend and aborts (RESTRICT) or drops them (CASCADE) BEFORE soft-deleting
    // the column (delete the live pg_attribute row, append an attisdropped=true
    // tombstone). No in-memory schema hook: later resolve_table runs pick up the
    // tombstone via pg_attribute reads.
    class operator_alter_column_drop_t final : public read_write_operator_t {
    public:
        operator_alter_column_drop_t(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::catalog::oid_t table_oid,
                                     components::catalog::oid_t namespace_oid,
                                     std::string column_name,
                                     components::catalog::oid_t attoid,
                                     components::catalog::drop_behavior_t behavior);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::catalog::oid_t table_oid_;
        components::catalog::oid_t namespace_oid_;
        std::string column_name_;
        components::catalog::oid_t attoid_;
        components::catalog::drop_behavior_t behavior_;
    };

} // namespace components::operators
