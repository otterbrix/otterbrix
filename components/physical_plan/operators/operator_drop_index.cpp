#include "operator_drop_index.hpp"

#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <set>
#include <vector>

namespace components::operators {

    operator_drop_index_t::operator_drop_index_t(std::pmr::memory_resource* resource,
                                                 log_t log,
                                                 components::catalog::oid_t table_oid,
                                                 std::string index_name,
                                                 std::vector<catalog_delete_t> catalog_deletes)
        // Re-using operator_type::create_collection — see the same comment in
        // operator_create_index_metadata_t. The type tag is internally
        // informational; the executor's generic-DDL path treats this operator
        // as a write-only no-output step.
        : read_write_operator_t(resource, std::move(log), operator_type::create_collection)
        , table_oid_(table_oid)
        , index_name_(std::move(index_name))
        , catalog_deletes_(std::move(catalog_deletes)) {}

    void operator_drop_index_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_drop_index_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Scrub catalog rows referencing the dropped index. Dependants are
        // deleted before pg_class (order set by rewrite_drop_index). Each delete
        // is keyed by (oid_col_idx, target_oid) so in a catalog with multiple oid
        // columns (e.g. pg_depend's objid AND refobjid) only the right row is hit.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
            std::pmr::vector<services::disk::pg_catalog_delete_spec_t> specs(resource_);
            specs.reserve(catalog_deletes_.size());
            for (auto& d : catalog_deletes_) {
                specs.push_back({d.catalog_table_oid, d.oid_col_idx, d.target_oid});
                if (ctx->txn.transaction_id != 0)
                    ctx->pg_catalog_delete_tables.insert(d.catalog_table_oid);
            }
            if (!specs.empty()) {
                auto [_, fut] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                 exec_ctx,
                                                 std::move(specs));
                co_await std::move(fut);
            }
        }

        // Drop the in-memory index entry. Tolerant of an unknown name: no error
        // if the engine never saw the index (metadata existed but backfill never ran).
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::drop_index,
                                               ctx->session,
                                               table_oid_,
                                               services::index::index_name_t(index_name_));
            co_await std::move(ixf);
        }

        mark_executed();
    }

} // namespace components::operators
