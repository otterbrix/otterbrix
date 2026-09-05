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
        // Arriving here with no specs, or no disk service, is a caller-invariant violation — the planner
        // never produces this shape (rewrite_drop_index refuses an unresolved index up front). Proceeding
        // would tear down the engine entry while the catalog still describes the index.
        if (catalog_deletes_.empty() || ctx->disk_address == actor_zeta::address_t::empty_address()) {
            std::string msg = "operator_drop_index: index oid ";
            msg += std::to_string(static_cast<unsigned>(index_oid_));
            msg += catalog_deletes_.empty() ? " arrived with no catalog delete specs"
                                            : " cannot be scrubbed: no disk service is wired";
            msg += " — nothing this statement could verify as dropped";
            set_error(core::error_t{core::error_code_t::index_not_exists,
                                    std::pmr::string{std::move(msg), resource_}});
            mark_failed();
            co_return;
        }

        // Scrub catalog rows referencing the dropped index. Dependants are
        // deleted before pg_class (order set by rewrite_drop_index). Each delete
        // is keyed by (oid_col_idx, target_oid) so in a catalog with multiple oid
        // columns (e.g. pg_depend's objid AND refobjid) only the right row is hit.
        {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
            std::pmr::vector<services::disk::pg_catalog_delete_spec_t> specs(resource_);
            specs.reserve(catalog_deletes_.size());
            // pg_depend rows are optional (zero deleted is healthy); pg_index and pg_class are not, and
            // can't be judged separately — rewrite_drop_index resolves index_oid against pg_class, while
            // build_create_index_writes always writes a pg_index row for it. So both are collected and
            // judged as one identity group below.
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
            {
                auto [_, fut] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                exec_ctx,
                                                std::move(specs));
                auto deleted_r = co_await std::move(fut);
                // Must happen before the engine teardown below, over a catalog that still describes the
                // index. The manager stops at the FIRST refusal, and rewrite_drop_index puts pg_class last,
                // so the usual leftover is a pg_class row with pg_index already gone — a retry can still
                // finish the job by re-resolving against that row. Hence the verdict is "at least one
                // identity row went", not "both": demanding both would make pg_index's already-gone spec
                // block every retry forever.
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
            auto [_ix, ixf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                          &services::index::manager_index_t::drop_index,
                                                          ctx->session,
                                                          table_oid_,
                                                          index_oid_);
            co_await std::move(ixf);
        }

        mark_executed();
    }

} // namespace components::operators
