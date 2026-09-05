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
        // Resolve the source here directly in await_async_and_resume. fk_cascade is the
        // plan ROOT, so output_ becomes the DELETE result cursor — set it to the deleted
        // (matched) rows (the cursor count equals the number of deleted parent rows
        // regardless of cascade outcome). Multiple cascade ops STACK above one DELETE, so
        // walk DOWN the left_ spine to the DELETE's constraint_input() snapshot of its
        // matched OLD rows (single canonical source, R6). Empty => nothing to cascade.
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

        // THE TWO COLUMN LISTS MUST BE THE SAME LENGTH, and this is the only place that
        // says so. The keys-chunk below is built from parent_col_indices (one column per
        // referenced column) while the key column NAMES sent alongside it are
        // child_col_names (one per referencing column); the disk side resolves the names
        // and matches the two counts. Nothing on the DDL path rejects
        // `FOREIGN KEY (a, b) REFERENCES parent (x)` — the transformer copies both lists
        // verbatim and each is resolved to attoids on its own — so a lopsided constraint
        // does reach here. The disk side now refuses it, but it refuses a request it
        // cannot read; the defect is the CONSTRAINT, and naming it here is what makes the
        // error legible. Refusing is not optional: a cascade that cannot be evaluated and
        // reports "no children" deletes the parent and orphans the child rows.
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
        //
        // WHAT THE OUTER INDEX IS AND IS NOT. scan_by_keys states its own invariant: on SUCCESS
        // result.size() == keys.size(), one (possibly empty) bucket per key in input order — a
        // shape it guarantees rather than one this operator infers, which is why the error legs
        // below return instead of reading a short answer as "matched nothing". But NO branch below
        // indexes per_row_child_ids: RESTRICT / NO ACTION only ask whether a bucket is non-empty,
        // and CASCADE / SET NULL / SET DEFAULT flatten every bucket into one id set. So the parent
        // row a bucket came from is never needed, and nothing here pairs a reply with a request by
        // position. The ids themselves are the addressing, all the way down.
        //
        // The ids are also already the reader's OWN view: the semi-join streams the child table
        // under exec_ctx's transaction, so a child row this transaction has itself deleted earlier
        // in the statement is filtered out of the buckets and never reaches any action below.
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
                // Any referencing child row blocks the parent delete. This branch reads
                // EMPTINESS only — never a bucket's index, never a row's position — so it
                // has no pairing to get wrong; the loop is over buckets, not over parents.
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
                //
                // NO REPLY IS PAIRED WITH A REQUEST HERE. This is the whole reason the
                // C4b rework of SET NULL / SET DEFAULT below has no counterpart in this
                // branch: CASCADE never reads a row back. The ids come out of the scan and
                // go straight into storage_delete_rows as the rows to mark deleted, which
                // addresses each row BY ITS ID. Flattening therefore only has to preserve
                // the SET — order, bucket boundaries and any short-vs-long answer are all
                // irrelevant to a by-id delete, so there is no positional assumption to
                // break and nothing for chunk.row_ids to correct. Should this branch ever
                // grow a read-modify-write step, it acquires the pairing problem the SET
                // NULL branch has, and must be addressed by the ids the reply REPORTS.
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
                // READ THE REPLY. The child delete is the whole cascade: if it is refused,
                // the parent DELETE below it must not stand, or the rows this branch was
                // supposed to remove outlive the row they reference. The reply used to be
                // dropped on the floor and storage_delete_rows had no error channel at all,
                // so a refusal was indistinguishable from a delete of already-stamped rows.
                // The COUNT is deliberately not checked: it is legitimately lower than the
                // request when a row already carries a delete stamp.
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
                // Mirror the CASCADE branch's flattening: aggregate EVERY referencing
                // child row_id across all parent rows into ONE set, then do a single
                // fetch + single update against the SAME child_table_oid (one owning
                // agent). The SET NULL / SET DEFAULT transform is uniform across rows
                // — it keys off per-COLUMN child_col_schema_indices / per-COLUMN
                // child_col_default_specs, never off the parent row — so a single
                // combined update chunk is value-correct. Each fetched row is paired
                // back to its id through the chunk's OWN row_ids, which the producer
                // stamps with the rows it carries: the reply is NOT positionally the
                // request, because the fetch drops rows this transaction may not see.
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
                                                   static_cast<uint64_t>(all_child_ids.size()),
                                                   // No projection: which columns the cascade's consumers read is not
                                                   // proven here, and an unproven narrowing reads back stubs silently.
                                                   std::vector<size_t>{},
                                                   // The cascade runs INSIDE the parent transaction and must see
                                                   // exactly what it sees — including its own earlier writes, and
                                                   // NOT a child row it has already deleted in this same statement.
                                                   ctx->txn,
                                                   components::table::fetch_visibility_t::SNAPSHOT);
                auto fetched_r = co_await std::move(ffut); // vector of ≤CAP chunks
                if (fetched_r.has_error()) {
                    // A failed child-row read must abort the cascade: applying the
                    // SET NULL / SET DEFAULT transform to silently-empty cells and
                    // writing them back would corrupt the child rows.
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
                    if (schema_idx == absent)
                        continue;
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
                            // A default that does not decode is catalog corruption. Applying
                            // SET DEFAULT as SET NULL instead would be a silent substitution.
                            set_error(std::move(ec));
                            mark_failed();
                            co_return;
                        }
                        if (default_val.has_value() && default_val->is_null()) {
                            default_val.reset(); // explicit DEFAULT NULL == SET NULL here
                        }
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

                // Single update for the whole set — one chunk per fetched chunk, addressed by
                // the ids the FETCH REPORTS rather than by re-slicing all_child_ids.
                //
                // The old code walked all_child_ids positionally on the assumption that the
                // reply is the request. Since C4b it is not: the point fetch drops rows this
                // transaction may not see (a child row the same statement already deleted is
                // the reachable case), so one dropped row would shift every later id by one
                // and the SET NULL / SET DEFAULT would be written to the WRONG child rows.
                // chunk.row_ids is stamped by the producer with the rows the chunk carries.
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
                        ctx->dml_appends.push_back(
                            components::table::dml_append_range_t{fk_.child_table_oid, upd_row_start, upd_row_count});
                    }
                    ctx->dml_deletes.push_back(
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
