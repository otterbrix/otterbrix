#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // Drops one column from a relkind='g' (computing / Mongo-style
    // dynamic-schema) table by appending a new pg_computed_column row
    // carrying attrefcount = 0 (a tombstone). The reader filters refcount<=0
    // so the column disappears on the next resolve.
    //
    // Choosing append-tombstone (over a delete_pg_catalog_rows physical
    // delete) keeps the audit trail of every (column, version) pair, mirroring
    // the row-versioning style the rest of the catalog uses for relkind='g'.
    class operator_computed_field_unregister_t final : public read_write_operator_t {
    public:
        operator_computed_field_unregister_t(std::pmr::memory_resource* resource,
                                             log_t log,
                                             components::catalog::oid_t table_oid,
                                             components::catalog::oid_t attoid,
                                             std::string column_name);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::catalog::oid_t table_oid_;
        components::catalog::oid_t attoid_;
        std::string column_name_;
    };

} // namespace components::operators
