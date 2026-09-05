#include "operator_computed_field_unregister.hpp"

#include "alter_validators.hpp"

#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_computed_field_unregister_t::operator_computed_field_unregister_t(std::pmr::memory_resource* resource,
                                                                               log_t log,
                                                                               catalog::oid_t table_oid,
                                                                               catalog::oid_t attoid,
                                                                               std::string column_name,
                                                                               bool missing_ok)
        : read_write_operator_t(resource, std::move(log), operator_type::computed_field_unregister)
        , table_oid_(table_oid)
        , attoid_(attoid)
        , column_name_(std::move(column_name))
        , missing_ok_(missing_ok) {}

    actor_zeta::unique_future<void>
    operator_computed_field_unregister_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Concurrent INSERT registering same field while ALTER DROP
        // is in flight. MVCC isolation: each txn sees its own snapshot of
        // pg_computed_column. Three orderings possible:
        //   1. ALTER commits first, INSERT sees tombstone -> register skips (refcount<=0
        //      tombstone treated as "field exists but dead"; resolver hides it).
        //      INSERT data lands in storage column 'x' but is not exposed by reader.
        //   2. INSERT commits first, ALTER tombstone applied later -> field hidden post-ALTER.
        //   3. Both commit independently — resolver max(attversion) determines visibility.
        //
        // This sometimes produces "ghost data" (storage has values for a column the
        // reader hides). VACUUM physical-compaction would reclaim; until then,
        // ghost data is harmless (invisible to user).
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        constexpr catalog::oid_t pg_computed_column = catalog::well_known_oid::pg_computed_column_table;

        // Routing by attoid (pre-stamped by enrich_logical_plan).
        // INVALID_OID means the resolver couldn't find a live computed column —
        // treat as idempotent no-op (matches the prior attname-scan miss).
        // Group 1: planner creates this operator directly from ALTER's
        // sub-clause without an enrich pass (planner.cpp:585-598), so
        // attoid_ is INVALID by default. Fall through to the
        // pg_computed_column scan below and match by attname instead of
        // attoid (the pre-existing attoid path remains a fast path for
        // callers that do stamp it).

        // Scan by relid and filter by attoid in-callback: a keyed (relid, attoid)
        // read won't do because the same column can have multiple version rows
        // and we need max(attversion).
        // pg_computed_column layout: 0=relid 1=attoid 2=attname
        // 3=atttypid 4=atttypspec 5=attversion 6=attrefcount.
        std::pmr::vector<std::uint64_t> r_keys(resource_);
        r_keys.emplace_back(catalog::pg_computed_column_col::relid);
        auto [_r, rf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::read_chunks_by_key,
                                         exec_ctx,
                                         pg_computed_column,
                                         std::move(r_keys),
                                         components::operators::make_key_chunk(resource_, table_oid_),
                                         std::pmr::vector<std::uint64_t>{resource_});
        auto batches_r = co_await std::move(rf);
        if (batches_r.has_error()) {
            // A failed pg_computed_column read is not a miss; saying "not found" here hides it.
            set_error(batches_r.error());
            co_return;
        }
        auto& batches = batches_r.value();

        // pick the latest live row matching attoid_ (max attversion AND attrefcount > 0).
        std::int64_t max_version = -1;
        catalog::oid_t live_attoid = catalog::INVALID_OID;
        catalog::oid_t live_atttypid = catalog::INVALID_OID;
        bool found_live = false;
        for (auto& chunk : batches) {
            if (chunk.column_count() < 7)
                continue;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(1, i) || chunk.is_null(5, i) || chunk.is_null(6, i))
                    continue;
                const auto row_attoid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));
                // Match by attoid when enrich stamped it; otherwise fall back to
                // matching by attname (column_name_).
                if (attoid_ != catalog::INVALID_OID) {
                    if (row_attoid != attoid_)
                        continue;
                } else {
                    if (chunk.is_null(2, i))
                        continue;
                    if (chunk.get_value<std::string_view>(2, i) != column_name_)
                        continue;
                }
                const auto v = chunk.get_value<std::int64_t>(5, i);
                const auto rc = chunk.get_value<std::int64_t>(6, i);
                if (rc <= 0)
                    continue;
                if (v > max_version) {
                    max_version = v;
                    live_attoid = row_attoid;
                    live_atttypid = chunk.is_null(3, i)
                                        ? catalog::INVALID_OID
                                        : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(3, i));
                    found_live = true;
                }
            }
        }
        if (!found_live) {
            // No live version row for this field. This used to be an "idempotent no-op":
            // mark_executed() and out, so `ALTER TABLE docs DROP COLUMN nosuchcol`
            // reported SUCCESS on a document table exactly as its pg_attribute sibling
            // did on a regular one. Both are now refused, and refused HERE rather than
            // in one operator for both kinds: a document table's columns live in
            // pg_computed_column, so this is the only catalog that can answer whether
            // the field exists. Answering it in the operator that owns the catalog is
            // what keeps the loudness from being a fallback keyed on relkind — the rule
            // is one ("a column that is not there is an error"), the lookup is two.
            //
            // IF EXISTS is honoured on the same terms as the regular path.
            if (missing_ok_) {
                mark_executed();
                co_return;
            }
            std::pmr::vector<std::uint64_t> cl_keys(resource_);
            cl_keys.emplace_back(catalog::pg_class_col::oid);
            auto [_cl, clf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::read_chunks_by_key,
                                               exec_ctx,
                                               catalog::well_known_oid::pg_class_table,
                                               std::move(cl_keys),
                                               components::operators::make_key_chunk(resource_, table_oid_),
                                               std::pmr::vector<std::uint64_t>{resource_});
            auto cls_batches_r = co_await std::move(clf);
            if (cls_batches_r.has_error()) {
                set_error(cls_batches_r.error());
                co_return;
            }
            auto rel_id = alter_validators::relation_identity_of(cls_batches_r.value());
            std::string rel = std::move(rel_id.relname);
            if (rel.empty()) {
                rel = "oid ";
                rel += std::to_string(table_oid_);
            }
            // KEEP THE MESSAGE SHORT — under about 120 bytes. A longer error string
            // built from this operator's resource_ comes back corrupted (doubled, or with
            // a size that makes reading it throw std::length_error) before the executor
            // has even copied it into a cursor. That is a separate, pre-existing defect —
            // reproduced on this branch, NOT fixed here, and unrelated to the cursor-side
            // lifetime bug that test_cursor_error_lifetime.cpp covers. The pre-existing
            // FK-blocking refusal further down already sits at that edge.
            std::string msg = "column \"";
            msg += column_name_;
            msg += "\" of relation \"";
            msg += rel;
            msg += "\" does not exist; use DROP COLUMN IF EXISTS to ignore it";
            set_error(core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }

        // Tombstone row: version = max+1, refcount = 0, same attoid so any
        // pg_depend attrefs stay valid; readers drop it via the refcount<=0 gate.
        auto cc_row = catalog::build_pg_computed_column_row(resource_,
                                                            table_oid_,
                                                            live_attoid,
                                                            column_name_,
                                                            live_atttypid,
                                                            max_version + 1,
                                                            /*attrefcount=*/std::int64_t{0});
        auto [_w, wf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::append_pg_catalog_row,
                                         exec_ctx,
                                         pg_computed_column,
                                         std::move(cc_row));
        auto rng_r = co_await std::move(wf);
        if (rng_r.has_error()) {
            // The tombstone IS the unregistration. Without it the column stays live and
            // reporting success would hide that from the statement.
            set_error(rng_r.error());
            mark_failed();
            co_return;
        }
        if (rng_r.value().count > 0) {
            ctx->pg_catalog_appends.push_back(std::move(rng_r.value()));
        }

        // Note: a previous version of this code added an immediate
        // compact_relkind_g_storage call here (drop physical columns whose
        // tombstones were just written), but the subsequent re-INSERT path
        // (dynamic_schema_re_add_after_drop) crashed row_group::append with
        // column-count mismatch because storage::drop_column doesn't fully
        // reset row_group state when called mid-pipeline. Compaction is
        // therefore deferred to operator_vacuum_t (runs the same logic
        // asynchronously). For now SELECT * on relkind='g' continues to leak
        // dropped columns until VACUUM runs.
        mark_executed();
    }

} // namespace components::operators
