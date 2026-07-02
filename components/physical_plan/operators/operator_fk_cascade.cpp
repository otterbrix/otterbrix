#include "operator_fk_cascade.hpp"

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

    namespace {
        // Resolve the deleted parent rows fk_cascade looks up referencing children
        // for. The DELETE child snapshots its matched OLD (pre-delete) rows into
        // constraint_input() at the top of its await_async_and_resume, so prefer
        // that — it is populated in BOTH paths (fk_cascade always runs after the
        // delete's await). Fall back to the scan's output_ (the pre-delete row
        // values, materialized path) then the delete's pass-through output_, so the
        // materialized entry point keeps working unchanged.
        const operator_data_ptr& resolve_fk_cascade_source(const operator_ptr& left) {
            if (left->constraint_input() && left->constraint_input()->size() > 0) {
                return left->constraint_input();
            }
            if (left->left() && left->left()->output() && left->left()->output()->size() > 0) {
                return left->left()->output();
            }
            return left->output();
        }
    } // namespace

    actor_zeta::unique_future<void> operator_fk_cascade_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Resolve the source here directly in await_async_and_resume. fk_cascade is the
        // plan ROOT, so output_ becomes the DELETE result cursor — set it to the deleted
        // (matched) rows (the cursor count equals the number of deleted parent rows
        // regardless of cascade outcome).
        const auto& source = resolve_fk_cascade_source(left_);
        output_ = source;
        if (!source || source->size() == 0) {
            mark_executed();
            co_return;
        }
        const auto& in_chunks = output_->chunks();
        execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz};

        const auto& par_indices = fk_.parent_col_indices;
        const std::size_t absent = std::numeric_limits<std::size_t>::max();
        // If indices weren't resolved at plan time, skip cascade.
        for (auto idx : par_indices) {
            if (idx == absent) {
                mark_executed();
                co_return;
            }
        }
        if (par_indices.empty()) {
            mark_executed();
            co_return;
        }

        // Child key column names are the same for every row; hoist them once.
        std::pmr::vector<std::string> key_cols(resource_);
        key_cols.reserve(fk_.child_col_names.size());
        for (const auto& n : fk_.child_col_names) {
            key_cols.emplace_back(n);
        }

        // Stage A: per input chunk (each <= DEFAULT_VECTOR_CAPACITY rows), build an OWNED keys-chunk
        // and scan the child table; accumulate per_row_child_ids across all chunks.
        // per_row_child_ids[row] = referencing child row_ids for that parent row (empty -> nothing
        // references it). Gathering ALL streamed batches into one combined keys-chunk would overflow
        // the chunk capacity (the source can stream many batches), so the scan is windowed per chunk;
        // the cascade actions below aggregate the per-row results across ALL parent rows, so the
        // per-chunk scan is value-equivalent to the old single combined scan. The keys-chunk is an
        // OWNED copy (it crosses the mailbox; actors must not share buffers). The per-chunk scans are
        // sequential co_awaits in this nested operator coroutine (driven by the executor) — no lost-wakeup.
        std::pmr::vector<types::complex_logical_type> key_types(resource_);
        key_types.reserve(par_indices.size());
        for (auto pidx : par_indices) {
            key_types.push_back(in_chunks.front().data[pidx].type());
        }
        std::pmr::vector<std::pmr::vector<std::int64_t>> per_row_child_ids(resource_);
        for (const auto& chunk : in_chunks) {
            if (chunk.size() == 0) {
                continue;
            }
            components::vector::data_chunk_t keys(resource_, key_types, chunk.size());
            for (std::size_t j = 0; j < par_indices.size(); ++j) {
                components::vector::vector_ops::copy(chunk.data[par_indices[j]], keys.data[j], chunk.size(), 0, 0);
            }
            keys.set_cardinality(chunk.size());

            // Child key column names cross the mailbox per scan, so copy them each time.
            std::pmr::vector<std::string> col_names(resource_);
            col_names.reserve(key_cols.size());
            for (const auto& n : key_cols) {
                col_names.emplace_back(n);
            }
            auto [_s, sfut] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::scan_by_keys,
                                               exec_ctx,
                                               fk_.child_table_oid,
                                               std::move(col_names),
                                               std::move(keys));
            auto chunk_child_ids = co_await std::move(sfut);
            for (auto& ids : chunk_child_ids) {
                per_row_child_ids.push_back(std::move(ids));
            }
        }

        switch (fk_.del_action) {
            case 'a': // NO ACTION
            case 'r': // RESTRICT
                // Any referencing child row blocks the parent delete.
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
                // Aggregate every referencing child row_id across all parent rows
                // into one delete. The child delete is stamped with the PARENT txn
                // id (exec_ctx) so it is part of the parent's transaction: the
                // executor records the child table on the txn's delete channel, so
                // COMMIT publishes the cascade delete and ROLLBACK reverts it
                // (revert_all_deletes(parent_txn_id)) — all-or-nothing atomicity.
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
                auto [_d, dfut] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_delete_rows,
                                                   exec_ctx,
                                                   fk_.child_table_oid,
                                                   std::move(row_ids_vec),
                                                   static_cast<uint64_t>(all_child_ids.size()));
                co_await std::move(dfut);
                // Track the child delete on the parent txn so COMMIT publishes it
                // and ABORT reverts it. txn_id 0 (direct-API / no active txn) needs
                // no tracking: the delete is already visible-to-all and irreversible.
                if (ctx->txn.transaction_id != 0) {
                    ctx->cascade_dml_deletes.push_back(
                        components::table::dml_delete_range_t{fk_.child_table_oid, ctx->txn.transaction_id});
                }
                break;
            }
            case 'n':   // SET NULL
            case 'd': { // SET DEFAULT
                // Mirror the CASCADE branch's flattening: aggregate EVERY referencing
                // child row_id across all parent rows into ONE set, then do a single
                // fetch + single update against the SAME child_table_oid (one owning
                // agent). The SET NULL / SET DEFAULT transform is uniform across rows
                // — it keys off per-COLUMN child_col_schema_indices / per-COLUMN
                // child_col_default_specs, never off the parent row — so a single
                // combined update chunk is value-correct. Each child row_id stays
                // paired with its fetched chunk position because storage_fetch returns
                // rows positionally aligned with the requested row_ids, and
                // storage_update applies data[i] to row_ids[i] positionally.
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
                auto [_f, ffut] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_fetch,
                                                   ctx->session,
                                                   fk_.child_table_oid,
                                                   std::move(fetch_ids),
                                                   static_cast<uint64_t>(all_child_ids.size()));
                auto fetched = co_await std::move(ffut); // vector of ≤CAP chunks
                if (fetched.empty())
                    break;

                const bool is_set_null = (fk_.del_action == 'n');
                // Apply the uniform per-column transform to every fetched row in every chunk.
                for (std::size_t ci = 0; ci < fk_.child_col_schema_indices.size(); ++ci) {
                    const auto schema_idx = fk_.child_col_schema_indices[ci];
                    if (schema_idx == absent)
                        continue;
                    // SET DEFAULT: decode attdefspec once; NULL default → same as SET NULL.
                    std::optional<types::logical_value_t> default_val;
                    if (!is_set_null) {
                        const auto& spec =
                            ci < fk_.child_col_default_specs.size() ? fk_.child_col_default_specs[ci] : std::string{};
                        default_val =
                            spec.empty() ? std::nullopt : components::catalog::decode_default_spec(resource_, spec);
                    }
                    for (auto& chunk : fetched) {
                        if (schema_idx >= chunk.column_count())
                            continue;
                        for (uint64_t r = 0; r < chunk.size(); ++r) {
                            if (!is_set_null && default_val.has_value()) {
                                chunk.set_value(schema_idx, r, *default_val);
                            } else {
                                chunk.data[schema_idx].validity().set_invalid(r);
                            }
                        }
                    }
                }

                // Single update for the whole set — one chunk per fetched chunk, with the
                // flat all_child_ids sliced positionally to match each chunk's rows.
                std::pmr::vector<components::vector::vector_t> upd_ids_batch(resource_);
                std::pmr::vector<components::vector::data_chunk_t> upd_data_batch(resource_);
                std::size_t id_base = 0;
                for (auto& chunk : fetched) {
                    const uint64_t n = chunk.size();
                    components::vector::vector_t ids(resource_, types::logical_type::BIGINT, n);
                    for (uint64_t i = 0; i < n; ++i) {
                        ids.data<int64_t>()[i] = all_child_ids[id_base + i];
                    }
                    id_base += n;
                    upd_ids_batch.emplace_back(std::move(ids));
                    upd_data_batch.emplace_back(std::move(chunk));
                }
                // Stamp the child update with the PARENT txn (exec_ctx) so the
                // SET NULL / SET DEFAULT version write rides the parent's
                // transaction: the executor tracks the child table on BOTH the
                // append channel (the new versions) and the delete channel (the
                // superseded old versions, marked deleted at parent_txn_id), so
                // COMMIT publishes the child update and ROLLBACK reverts it.
                auto [_u, ufut] = actor_zeta::send(ctx->disk_address,
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
                // MVCC update = delete-old + append-new. Track BOTH on the parent
                // txn (same shape as operator_update's dml_* swap-info), so COMMIT
                // publishes the appended new versions and the delete tombstones, and
                // ABORT reverts the appends (storage_revert_appends) and un-stamps
                // the delete marks (revert_all_deletes(parent_txn_id)).
                if (ctx->txn.transaction_id != 0) {
                    auto [upd_row_start, upd_row_count] = update_result.value();
                    if (upd_row_count > 0) {
                        ctx->cascade_dml_appends.push_back(
                            components::table::dml_append_range_t{fk_.child_table_oid, upd_row_start, upd_row_count});
                    }
                    ctx->cascade_dml_deletes.push_back(
                        components::table::dml_delete_range_t{fk_.child_table_oid, ctx->txn.transaction_id});
                }
                break;
            }
            default:
                break;
        }
        mark_executed();
    }

} // namespace components::operators
