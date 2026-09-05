#include "operator_fk_cascade.hpp"

#include "constraint_util.hpp"

#include <cstring>

#include <components/base/collection_full_name.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>
#include <services/disk/manager_disk.hpp>

#include <limits>

namespace components::operators {

    operator_fk_cascade_t::operator_fk_cascade_t(std::pmr::memory_resource* resource, log_t log, catalog::fk_info_t fk)
        : read_write_operator_t(resource, log, operator_type::fk_cascade)
        , fk_(std::move(fk)) {}

    actor_zeta::unique_future<void> operator_fk_cascade_t::await_async_and_resume(pipeline::context_t* ctx) {
        // fk_cascade is the plan ROOT, so output_ becomes the DELETE result cursor: the matched parent rows,
        // read from the DELETE's constraint_input() snapshot via the left_ spine (single source, R6).
        const auto& source = constraint_detail::resolve_constraint_source(left_);
        output_ = source;
        if (!source || source->size() == 0) {
            mark_executed();
            co_return;
        }
        const auto& in_chunks = output_->chunks();
        execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->execution_context.timezone_offset};

        const auto& par_indices = fk_.parent_col_indices;
        const std::size_t absent = std::numeric_limits<std::size_t>::max();

        // parent_col_indices and child_col_names must be the same length; nothing on the DDL path rejects a
        // lopsided `FOREIGN KEY (a, b) REFERENCES parent (x)`, so this is the only place that checks. Refusing
        // here names the real defect — silently proceeding would delete the parent and orphan its children.
        if (par_indices.size() != fk_.child_col_names.size()) {
            std::pmr::string what{"FK constraint: foreign key column count mismatch — ", resource_};
            what.append(std::to_string(fk_.child_col_names.size()).c_str());
            what.append(" referencing column(s) vs ");
            what.append(std::to_string(par_indices.size()).c_str());
            what.append(" referenced column(s)");
            set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
            mark_failed();
            co_return;
        }

        // A cascade that can't be evaluated is not a cascade with no children — reporting success would let the
        // DELETE stand and orphan the child rows (worse under RESTRICT, whose purpose is to stop that delete).
        // `absent` (enrich_logical_plan's marker for an unresolved parent column name) and an empty list both
        // mean the key never resolved, not that it matched nothing.
        for (std::size_t i = 0; i < par_indices.size(); ++i) {
            if (par_indices[i] != absent) {
                continue;
            }
            std::pmr::string what{"FK constraint: referenced column ", resource_};
            if (i < fk_.parent_col_names.size()) {
                what.append("\"");
                what.append(fk_.parent_col_names[i].c_str());
                what.append("\" ");
            }
            what.append("has no resolved position in the parent row — the ON DELETE action cannot be evaluated");
            set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
            mark_failed();
            co_return;
        }
        if (par_indices.empty()) {
            set_error(core::error_t{
                core::error_code_t::invalid_constraint,
                std::pmr::string{"FK constraint: no referenced columns resolved — the ON DELETE action "
                                 "cannot be evaluated",
                                 resource_}});
            mark_failed();
            co_return;
        }

        // A child_col_default_specs shorter than child_col_schema_indices would silently fall the tail columns
        // into SET NULL instead of erroring (the SET DEFAULT leg only guards `ci < ...size()`). Checked here,
        // before any fetch, since enrich fills both vectors in one loop and a skew is unreachable via SQL today
        // but not via any other producer of fk_info_t.
        if (fk_.del_action == 'd' &&
            fk_.child_col_default_specs.size() < fk_.child_col_schema_indices.size()) {
            std::pmr::string what{"FK constraint: ON DELETE SET DEFAULT has ", resource_};
            what.append(std::to_string(fk_.child_col_default_specs.size()).c_str());
            what.append(" default spec(s) for ");
            what.append(std::to_string(fk_.child_col_schema_indices.size()).c_str());
            what.append(" referencing column(s) — a column with no spec would silently be set to "
                        "NULL instead of its default");
            set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
            mark_failed();
            co_return;
        }

        // Child key column names are the same for every row; hoist them once.
        std::pmr::vector<std::string> key_cols(resource_);
        key_cols.reserve(fk_.child_col_names.size());
        for (const auto& n : fk_.child_col_names) {
            key_cols.emplace_back(n);
        }

        // Windowed per-chunk scan (one combined keys-chunk over all batches would overflow chunk capacity);
        // per_row_child_ids aggregates across chunks. The keys-chunk is an OWNED copy (crosses the mailbox).
        // scan_by_keys guarantees result.size() == keys.size() on success (one bucket per key, in order), but no
        // branch below indexes by position — RESTRICT/NO ACTION only check bucket emptiness, CASCADE/SET NULL/SET
        // DEFAULT flatten every bucket into one id set — so only the ids themselves ever address anything. The
        // scan runs under exec_ctx's transaction, so a child row this txn already deleted is filtered out.

        // chunk.data[] doesn't bound-check, so an out-of-range par_indices entry here would read PAST the
        // chunk's column array rather than refuse. The child side of this operator refuses the same shape
        // ("row batch has N column(s)..."), so the parent side must too.
        auto refuse_narrow_parent = [&](std::size_t width, std::size_t pidx, std::size_t slot) {
            std::pmr::string what{"FK constraint: the matched parent rows have ", resource_};
            what.append(std::to_string(width).c_str());
            what.append(" column(s), too few to hold referenced column ");
            if (slot < fk_.parent_col_names.size()) {
                what.append("\"");
                what.append(fk_.parent_col_names[slot].c_str());
                what.append("\" ");
            }
            what.append("at position ");
            what.append(std::to_string(pidx).c_str());
            what.append(" — the ON DELETE action cannot be evaluated");
            set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
            mark_failed();
        };

        std::pmr::vector<types::complex_logical_type> key_types(resource_);
        key_types.reserve(par_indices.size());
        for (std::size_t j = 0; j < par_indices.size(); ++j) {
            if (par_indices[j] >= in_chunks.front().column_count()) {
                refuse_narrow_parent(in_chunks.front().column_count(), par_indices[j], j);
                co_return;
            }
            key_types.push_back(in_chunks.front().data[par_indices[j]].type());
        }
        std::pmr::vector<std::pmr::vector<std::int64_t>> per_row_child_ids(resource_);
        for (const auto& chunk : in_chunks) {
            if (chunk.size() == 0) {
                continue;
            }
            components::vector::data_chunk_t keys(resource_, key_types, chunk.size());
            for (std::size_t j = 0; j < par_indices.size(); ++j) {
                // Per chunk, not once for the front one: every chunk in the snapshot is
                // read at this index, so every chunk has to be wide enough to hold it.
                if (par_indices[j] >= chunk.column_count()) {
                    refuse_narrow_parent(chunk.column_count(), par_indices[j], j);
                    co_return;
                }
                components::vector::vector_ops::copy(chunk.data[par_indices[j]], keys.data[j], chunk.size(), 0, 0);
            }
            keys.set_cardinality(chunk.size());

            // Child key column names cross the mailbox per scan, so copy them each time.
            std::pmr::vector<std::string> col_names(resource_);
            col_names.reserve(key_cols.size());
            for (const auto& n : key_cols) {
                col_names.emplace_back(n);
            }
            auto [_s, sfut] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                          &services::disk::manager_disk_t::scan_by_keys,
                                                          exec_ctx,
                                                          fk_.child_table_oid,
                                                          std::move(col_names),
                                                          std::move(keys));
            auto chunk_child_ids_r = co_await std::move(sfut);
            if (chunk_child_ids_r.has_error()) {
                // A failed child-key read is not a miss; treating it as one lets the
                // operation proceed on data that was never read.
                set_error(chunk_child_ids_r.error());
                co_return;
            }
            auto& chunk_child_ids = chunk_child_ids_r.value();
            for (auto& ids : chunk_child_ids) {
                per_row_child_ids.push_back(std::move(ids));
            }
        }

        switch (fk_.del_action) {
            case 'a': // NO ACTION
            case 'r': // RESTRICT
                // Blocks the delete if ANY bucket is non-empty; reads emptiness only, no positional pairing.
                for (const auto& child_ids : per_row_child_ids) {
                    if (!child_ids.empty()) {
                        set_error(core::error_t{
                            core::error_code_t::invalid_constraint,
                            std::pmr::string{"FK constraint violated: child rows reference deleted parent row",
                                             resource_}});
                        co_return;
                    }
                }
                break;

            case 'c': { // CASCADE — delete child rows via storage_delete_rows
                // Stamped with the PARENT txn (exec_ctx) so COMMIT publishes / ROLLBACK reverts it
                // (revert_all_deletes(parent_txn_id)). Deletes by id, so flattening buckets only needs to
                // preserve the SET — unlike SET NULL/SET DEFAULT below, CASCADE never reads a row back.
                std::pmr::vector<int64_t> all_child_ids(resource_);
                for (const auto& child_ids : per_row_child_ids) {
                    for (auto id : child_ids) {
                        all_child_ids.push_back(id);
                    }
                }
                if (all_child_ids.empty())
                    break;

                components::vector::vector_t row_ids_vec(resource_, types::logical_type::BIGINT, all_child_ids.size());
                for (std::size_t i = 0; i < all_child_ids.size(); ++i) {
                    row_ids_vec.data<int64_t>()[i] = all_child_ids[i];
                }
                auto [_d, dfut] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::storage_delete_rows,
                                                              exec_ctx,
                                                              fk_.child_table_oid,
                                                              std::move(row_ids_vec),
                                                              static_cast<uint64_t>(all_child_ids.size()));
                // The child delete IS the cascade: if refused, the parent DELETE below must not stand. COUNT
                // is not checked — legitimately lower when a row already carries a delete stamp.
                auto deleted_r = co_await std::move(dfut);
                if (deleted_r.has_error()) {
                    set_error(deleted_r.error());
                    mark_failed();
                    co_return;
                }
                // Track the child delete on the parent txn so COMMIT publishes it
                // and ABORT reverts it. txn_id 0 (direct-API / no active txn) needs
                // no tracking: the delete is already visible-to-all and irreversible.
                if (ctx->txn.transaction_id != 0) {
                    ctx->dml_deletes.push_back(
                        components::table::dml_delete_range_t{fk_.child_table_oid, ctx->txn.transaction_id});
                }
                break;
            }
            case 'n':   // SET NULL
            case 'd': { // SET DEFAULT
                // Mirrors CASCADE: flatten to one id set, one fetch + one update against child_table_oid. Unlike
                // CASCADE, the reply is NOT positionally the request — the fetch drops rows this txn can't see —
                // so each row is paired back to its id via the fetched chunk's OWN row_ids.
                std::pmr::vector<int64_t> all_child_ids(resource_);
                for (const auto& child_ids : per_row_child_ids) {
                    for (auto id : child_ids) {
                        all_child_ids.push_back(id);
                    }
                }
                if (all_child_ids.empty())
                    break;

                // Single fetch for the whole set.
                components::vector::vector_t fetch_ids(resource_, types::logical_type::BIGINT, all_child_ids.size());
                for (std::size_t i = 0; i < all_child_ids.size(); ++i) {
                    fetch_ids.data<int64_t>()[i] = all_child_ids[i];
                }
                auto [_f, ffut] =
                    actor_zeta::otterbrix::send(ctx->disk_address,
                                                &services::disk::manager_disk_t::storage_fetch,
                                                ctx->session,
                                                fk_.child_table_oid,
                                                std::move(fetch_ids),
                                                static_cast<uint64_t>(all_child_ids.size()),
                                                // No projection — narrowing here isn't proven safe and would
                                                // read back stub columns.
                                                std::vector<size_t>{},
                                                // Runs inside the parent txn's own snapshot, so it also sees its
                                                // own earlier writes and skips rows it already deleted.
                                                ctx->txn,
                                                components::table::fetch_visibility_t::SNAPSHOT,
                                                // No cap — every child row must be transformed.
                                                /*limit=*/int64_t{-1});
                auto fetched_r = co_await std::move(ffut); // vector of ≤CAP chunks
                if (fetched_r.has_error()) {
                    // Must abort on a failed read — applying the transform to empty cells would corrupt rows.
                    set_error(fetched_r.error());
                    co_return;
                }
                auto fetched = std::move(fetched_r.value());
                if (fetched.empty())
                    break;

                const bool is_set_null = (fk_.del_action == 'n');
                // Apply the uniform per-column transform to every fetched row in every chunk.
                for (std::size_t ci = 0; ci < fk_.child_col_schema_indices.size(); ++ci) {
                    const auto schema_idx = fk_.child_col_schema_indices[ci];
                    // Skipping this column would leave the FK still pointing at the deleted parent row —
                    // exactly what SET NULL/SET DEFAULT exists to prevent. Marker set by
                    // operator_resolve_constraint_t when the column's position could not be resolved.
                    if (schema_idx == absent) {
                        std::pmr::string what{"FK constraint: referencing column ", resource_};
                        if (ci < fk_.child_col_names.size()) {
                            what.append("\"");
                            what.append(fk_.child_col_names[ci].c_str());
                            what.append("\" ");
                        }
                        what.append(is_set_null ? "has no resolved position in the child table — it cannot be "
                                                  "set to NULL"
                                                : "has no resolved position in the child table — it cannot be "
                                                  "set to its default");
                        set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
                        mark_failed();
                        co_return;
                    }
                    // SET DEFAULT: decode attdefspec once; NULL default → same as SET NULL.
                    // The decode is type-directed, and the column's stored type is right
                    // here in the fetched chunk.
                    std::optional<types::logical_value_t> default_val;
                    if (!is_set_null && ci < fk_.child_col_default_specs.size() &&
                        !fk_.child_col_default_specs[ci].empty() && !fetched.empty() &&
                        schema_idx < fetched.front().column_count()) {
                        if (auto ec = components::catalog::decode_default_spec(resource_,
                                                                               fetched.front().data[schema_idx].type(),
                                                                               fk_.child_col_default_specs[ci],
                                                                               default_val);
                            ec.contains_error()) {
                            // A default that fails to decode is catalog corruption — never fall back to SET NULL.
                            set_error(std::move(ec));
                            mark_failed();
                            co_return;
                        }
                        if (default_val.has_value() && default_val->is_null()) {
                            default_val.reset(); // explicit DEFAULT NULL == SET NULL here
                        }
                    }
                    for (auto& chunk : fetched) {
                        // Fetched without a projection, so every chunk is full-width and schema_idx is a real
                        // position; a too-narrow chunk is an unexpected reply shape, not something to skip.
                        if (schema_idx >= chunk.column_count()) {
                            std::pmr::string what{"FK constraint: the child row batch has ", resource_};
                            what.append(std::to_string(chunk.column_count()).c_str());
                            what.append(" column(s), too few to hold referencing column at position ");
                            what.append(std::to_string(schema_idx).c_str());
                            what.append(" — the ON DELETE action cannot be applied");
                            set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
                            mark_failed();
                            co_return;
                        }
                        for (uint64_t r = 0; r < chunk.size(); ++r) {
                            if (!is_set_null && default_val.has_value()) {
                                chunk.set_value(schema_idx, r, *default_val);
                            } else {
                                chunk.data[schema_idx].validity().set_invalid(r);
                            }
                        }
                    }
                }

                // Addressed by chunk.row_ids (what FETCH actually returned), not by re-slicing all_child_ids:
                // the fetch drops rows this txn can't see, so positional addressing would shift ids and write
                // SET NULL/SET DEFAULT to the wrong child rows.
                std::pmr::vector<components::vector::vector_t> upd_ids_batch(resource_);
                std::pmr::vector<components::vector::data_chunk_t> upd_data_batch(resource_);
                for (auto& chunk : fetched) {
                    const uint64_t n = chunk.size();
                    if (n == 0) {
                        continue;
                    }
                    components::vector::vector_t ids(resource_, types::logical_type::BIGINT, n);
                    std::memcpy(ids.data(), chunk.row_ids.data(), n * sizeof(int64_t));
                    upd_ids_batch.emplace_back(std::move(ids));
                    upd_data_batch.emplace_back(std::move(chunk));
                }
                // Stamped with the PARENT txn on both the append (new versions) and delete (superseded old
                // versions) channels, so COMMIT publishes / ROLLBACK reverts the whole update.
                auto [_u, ufut] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                              &services::disk::manager_disk_t::storage_update,
                                                              exec_ctx,
                                                              fk_.child_table_oid,
                                                              std::move(upd_ids_batch),
                                                              std::move(upd_data_batch));
                // The update reply carries any write_conflict / out_of_memory; surface it as a
                // clean error cursor instead of silently dropping it.
                auto update_result = co_await std::move(ufut);
                if (update_result.has_error()) {
                    set_error(update_result.error());
                    mark_failed();
                    co_return;
                }
                // MVCC update = delete-old + append-new; track both on the parent txn (same shape as
                // operator_update) so COMMIT/ABORT cover both halves.
                if (ctx->txn.transaction_id != 0) {
                    auto [upd_row_start, upd_row_count] = update_result.value();
                    if (upd_row_count > 0) {
                        ctx->dml_appends.push_back(
                            components::table::dml_append_range_t{fk_.child_table_oid, upd_row_start, upd_row_count});
                    }
                    ctx->dml_deletes.push_back(
                        components::table::dml_delete_range_t{fk_.child_table_oid, ctx->txn.transaction_id});
                }
                break;
            }
            default: {
                // Falling through here would perform no cascade and report SUCCESS, orphaning child rows — the
                // exact outcome the operator exists to prevent. confdeltype is normalized to {a,r,c,n,d} by both
                // transformer routes, so anything else is a catalog this engine did not produce.
                std::pmr::string what{"FK constraint: ON DELETE action '", resource_};
                what.append(std::pmr::string(1, fk_.del_action, resource_));
                what.append("' in pg_constraint.confdeltype is not one of the actions this build can apply "
                            "— the cascade cannot be evaluated");
                set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
                mark_failed();
                co_return;
            }
        }
        mark_executed();
    }

} // namespace components::operators
