#include "operator_drop_index.hpp"

#include <components/catalog/helpers.hpp>
#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

#include <vector>

namespace components::operators {

    operator_drop_index_t::operator_drop_index_t(std::pmr::memory_resource* resource,
                                                 log_t log,
                                                 components::catalog::oid_t table_oid,
                                                 components::catalog::oid_t index_oid,
                                                 std::vector<catalog_delete_t> catalog_deletes)
        // Re-using operator_type::create_collection — see the same comment in
        // operator_create_index_metadata_t. The type tag is internally
        // informational; the executor's generic-DDL path treats this operator
        // as a write-only no-output step.
        : read_write_operator_t(resource, std::move(log), operator_type::create_collection)
        , table_oid_(table_oid)
        , index_oid_(index_oid)
        , catalog_deletes_(std::move(catalog_deletes)) {}

    actor_zeta::unique_future<void> operator_drop_index_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Scrub catalog rows referencing the dropped index. Dependants are
        // deleted before pg_class (order set by rewrite_drop_index). Each delete
        // is keyed by (oid_col_idx, target_oid) so in a catalog with multiple oid
        // columns (e.g. pg_depend's objid AND refobjid) only the right row is hit.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
            std::pmr::vector<services::disk::pg_catalog_delete_spec_t> specs(resource_);
            specs.reserve(catalog_deletes_.size());
            // WHICH ZERO IS AN ERROR HERE, and pg_index and pg_class are the SAME question.
            // pg_depend rows are optional bookkeeping — an index with no dependency row deletes
            // none of them and that is healthy. The other two are not optional and there is no
            // ground to separate them: rewrite_drop_index emits these specs only once index_oid
            // resolved, and what it resolved AGAINST is the index's pg_class entry
            // (enrich_logical_plan stamps set_index_oid from rt_index->table_oid(), the index
            // relation's own oid, read out of pg_class by operator_resolve_table_t under this
            // same transaction), while build_create_index_writes writes the pg_index row for
            // every index there is. Calling zero on pg_index an error and zero on pg_class
            // healthy — as this table did — contradicted itself with the argument it gave: a
            // scrub that removed no pg_class row leaves the relation in the catalog under its
            // name for the next statement to resolve. So the two are collected together, and
            // judged together below.
            std::pmr::vector<std::size_t> identity_specs(resource_);
            for (auto& d : catalog_deletes_) {
                const bool is_pg_index_identity =
                    d.catalog_table_oid == components::catalog::well_known_oid::pg_index_table &&
                    d.oid_col_idx == static_cast<std::int64_t>(components::catalog::pg_index_col::indexrelid);
                const bool is_pg_class_identity =
                    d.catalog_table_oid == components::catalog::well_known_oid::pg_class_table &&
                    d.oid_col_idx == static_cast<std::int64_t>(components::catalog::pg_class_col::oid);
                if (index_oid_ != components::catalog::INVALID_OID && d.target_oid == index_oid_ &&
                    (is_pg_index_identity || is_pg_class_identity)) {
                    identity_specs.push_back(specs.size());
                }
                specs.push_back({d.catalog_table_oid, d.oid_col_idx, d.target_oid});
                if (ctx->txn.transaction_id != 0)
                    ctx->pg_catalog_delete_tables.insert(d.catalog_table_oid);
            }
            if (!specs.empty()) {
                auto [_, fut] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                 exec_ctx,
                                                 std::move(specs));
                auto deleted_r = co_await std::move(fut);
                // AHEAD OF THE ENGINE TEARDOWN BELOW, and the order is the point: the teardown
                // is this operator's other mutation, and it must not happen over a catalog that
                // still describes the index.
                //
                // WHAT A RETRY CAN AND CANNOT DO, stated rather than promised — and it is what
                // decides the SHAPE of the verdict below. The manager stops its loop at the
                // FIRST refusal, so what a refused scrub leaves is a prefix-deleted spec list,
                // and rewrite_drop_index puts pg_class last: the usual leftover is a pg_class
                // row whose pg_index row is already gone. A retry re-resolves the index by name
                // against exactly that surviving pg_class row, so it CAN finish the job.
                //
                // Which is why the rule is "at least one identity row went", not "every one
                // did". Demanding both would make that retry refuse forever — pg_index is
                // already gone, so its spec can only answer zero — and the leftover would be
                // permanent. Answering zero on BOTH is the state the verdict actually exists
                // for: nothing that makes this index exist was removed, so the statement did
                // not drop the index it named and must not tear the engine down over it.
                if (deleted_r.has_error()) {
                    set_error(deleted_r.error());
                    mark_failed();
                    co_return;
                }
                const auto& deleted = deleted_r.value();
                bool identity_row_deleted = false;
                for (const auto i : identity_specs) {
                    if (i < deleted.size() && deleted[i] > 0) {
                        identity_row_deleted = true;
                        break;
                    }
                }
                if (!identity_specs.empty() && !identity_row_deleted) {
                    std::string msg = "operator_drop_index: no identity row of index oid ";
                    msg += std::to_string(static_cast<unsigned>(index_oid_));
                    msg += " was deleted — the index is still in the catalog";
                    set_error(
                        core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource_}});
                    mark_failed();
                    co_return;
                }
            }
        }

        // Drop the in-memory index entry. Tolerant of an unknown oid: no error
        // if the engine never saw the index (metadata existed but backfill never ran).
        if (ctx->index_address != actor_zeta::address_t::empty_address()) {
            auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::drop_index,
                                               ctx->session,
                                               table_oid_,
                                               index_oid_);
            co_await std::move(ixf);
        }

        mark_executed();
    }

} // namespace components::operators
