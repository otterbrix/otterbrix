#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace components::operators {

    // DROP INDEX runtime: removes the in-memory index engine entry and scrubs
    // the pg_class/pg_index/pg_depend rows for the index oid.
    //
    // The catalog-row deletes are produced upstream by rewrite_drop_index from
    // primitive_delete leaves. Co-locating them with the index-actor teardown
    // keeps the failure boundary simple — a partial scrub surfaces as a single
    // operator error before the engine entry is removed (so a retry can find
    // and finish the cleanup).
    class operator_drop_index_t final : public read_write_operator_t {
    public:
        struct catalog_delete_t {
            collection_full_name_t      catalog_table;
            std::int64_t                oid_col_idx;
            components::catalog::oid_t  target_oid;
        };

        operator_drop_index_t(std::pmr::memory_resource*       resource,
                               log_t                             log,
                               collection_full_name_t            collection,
                               std::string                       index_name,
                               std::vector<catalog_delete_t>     catalog_deletes);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        collection_full_name_t           collection_;
        std::string                       index_name_;
        std::vector<catalog_delete_t>    catalog_deletes_;
    };

} // namespace components::operators
