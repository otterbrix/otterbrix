#include "operator_delete.hpp"
#include "predicates/predicate.hpp"
#include <components/vector/vector_operations.hpp>

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

    operator_delete::operator_delete(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::catalog::oid_t table_oid,
                                     std::pmr::vector<select_column_t> returning,
                                     expressions::expression_ptr expr)
        : read_write_operator_t(resource, log, operator_type::remove)
        , table_oid_(table_oid)
        , expression_(std::move(expr))
        , returning_(std::move(returning)) {}

    operator_delete::operator_delete(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::catalog::oid_t catalog_table_oid,
                                     std::int64_t oid_col_idx,
                                     components::catalog::oid_t target_oid)
        : read_write_operator_t(resource, log, operator_type::remove)
        , table_oid_(catalog_table_oid)
        , returning_(resource)
        , oid_col_idx_(oid_col_idx)
        , target_oid_(target_oid) {}

    void operator_delete::accept_resolved_metadata(resolved_table_metadata_t metadata) {
        // See operator_insert for the contract.
        if (table_oid_ == components::catalog::INVALID_OID && metadata.table_oid != components::catalog::INVALID_OID) {
            table_oid_ = metadata.table_oid;
        }
        resolved_metadata_ = std::move(metadata);
    }

    void operator_delete::ensure_simple_init_() {
        if (simple_init_done_) {
            return;
        }
        modified_ = operators::make_operator_write_data(resource_);
        simple_init_done_ = true;
    }

    core::error_t operator_delete::consume_batch_(pipeline::context_t* pipeline_context,
                                                  const vector::data_chunk_t& chunk) {
        using components::vector::data_chunk_t;
        ensure_simple_init_();
        if (chunk.size() == 0) {
            return core::error_t::no_error();
        }
        const bool collect_returning = !returning_.empty();
        auto types = chunk.types();

        // Match each row. expression_ is null for the simple predicate-scan DELETE
        // (the scan already pushed the WHERE), so create_all_true_predicate matches
        // every scan row; a non-null expression_ (legacy callers) is honored too.
        auto predicate = expression_ ? predicates::create_predicate(resource_,
                                                                    pipeline_context->function_registry,
                                                                    expression_,
                                                                    types,
                                                                    types,
                                                                    &pipeline_context->parameters,
                                                                    pipeline_context->session_tz)
                                     : predicates::create_all_true_predicate(resource_);

        // Matched ABSOLUTE row-ids of THIS batch (kept separate so the index mirror
        // pairs each staged old-row with its own id, regardless of batch order).
        vector::vector_t batch_ids(resource_, types::logical_type::BIGINT, chunk.size());
        // Indexing of matched rows into `chunk`, for the gathered old-row / RETURNING copies.
        vector::indexing_vector_t matched_indexing(resource_);
        matched_indexing.reset(chunk.size());

        size_t index = 0;
        for (size_t i = 0; i < chunk.size(); i++) {
            auto check_result = predicate->check(chunk, i);
            if (check_result.has_error()) {
                return check_result.error();
            }
            if (!check_result.value()) {
                continue;
            }
            int64_t abs_id;
            if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                abs_id = static_cast<int64_t>(chunk.data.front().indexing().get_index(i));
            } else {
                abs_id = chunk.row_ids.data<int64_t>()[i];
            }
            batch_ids.data<int64_t>()[index] = abs_id;
            matched_indexing.set_index(index, i);
            index++;
        }
        if (index == 0) {
            return core::error_t::no_error();
        }

        for (size_t i = 0; i < index; i++) {
            modified_->append(static_cast<size_t>(batch_ids.data<int64_t>()[i]));
        }
        for (const auto& type : types) {
            modified_->updated_types_map()[{std::pmr::string(type.alias(), resource_), type}] += index;
        }

        // Stage the matched OLD scan rows + their absolute ids for the index mirror
        // (bounded: only matched rows). The merged staged chunk row k pairs with
        // index_old_row_ids_[k], so manager_index_t::delete_rows reads them aligned.
        {
            data_chunk_t old_matched(resource_, types, index);
            chunk.copy(old_matched, matched_indexing, index);
            old_matched.set_cardinality(index);
            index_old_chunks_.emplace_back(std::move(old_matched));
            for (size_t i = 0; i < index; i++) {
                index_old_row_ids_.push_back(batch_ids.data<int64_t>()[i]);
            }
        }

        // Stage matched RETURNING rows: gather the matched subset, then project the
        // requested columns straight into capacity-bounded chunks.
        if (collect_returning) {
            data_chunk_t affected(resource_, types, index);
            chunk.copy(affected, matched_indexing, index);
            affected.set_cardinality(index);
            auto batches = split_chunk_into_batches(resource_, std::move(affected));
            for (auto& batch : batches) {
                if (batch.size() == 0) {
                    continue;
                }
                auto proj = evaluate_projection(resource_,
                                                returning_,
                                                &batch,
                                                pipeline_context->parameters,
                                                pipeline_context->session_tz);
                if (proj.has_error()) {
                    return proj.error();
                }
                returning_staged_.emplace_back(std::move(proj.value()));
            }
        }
        return core::error_t::no_error();
    }

    core::error_t operator_delete::consume_join_batch_(pipeline::context_t* pipeline_context,
                                                       const vector::data_chunk_t& chunk_left,
                                                       const chunks_vector_t& right_chunks) {
        // DELETE ... USING shared core (R6: one implementation, two entry points).
        // Probes ONE LEFT (target) scan batch against the fully-materialized RIGHT
        // (USING) build chunks: a semi-join (a target row is deleted once regardless
        // of how many USING rows match). Per matched LEFT row it stages the SAME
        // bounded state the simple path does — matched ABSOLUTE row-ids in modified_,
        // the matched OLD left rows + their ids for the index mirror, and (per batch,
        // gathered in lockstep) the projected RETURNING rows from the matched
        // left+right pair. The RIGHT side is taken PER-CHUNK (chunks_vector_t),
        // never merged into one data_chunk_t — a USING/build table > DEFAULT_VECTOR_
        // CAPACITY would overflow a single chunk's capacity assert. push() calls it
        // per LEFT batch. await_async_and_resume drains it all.
        using components::vector::data_chunk_t;
        ensure_simple_init_();
        if (chunk_left.size() == 0) {
            return core::error_t::no_error();
        }
        const bool collect_returning = !returning_.empty();
        auto types_left = chunk_left.types();
        // Right column types come from the first non-empty right chunk (every chunk
        // shares the build-side schema); an all-empty build side yields no matches.
        std::pmr::vector<types::complex_logical_type> types_right(resource_);
        for (const auto& rc : right_chunks) {
            if (rc.size() > 0) {
                types_right = rc.types();
                break;
            }
        }

        auto predicate = expression_ ? predicates::create_predicate(resource_,
                                                                    pipeline_context->function_registry,
                                                                    expression_,
                                                                    types_left,
                                                                    types_right,
                                                                    &pipeline_context->parameters,
                                                                    pipeline_context->session_tz)
                                     : predicates::create_all_true_predicate(resource_);

        // Matched ABSOLUTE row-ids of THIS batch (kept separate so the index mirror
        // pairs each staged old-row with its own id, regardless of batch order).
        vector::vector_t batch_ids(resource_, types::logical_type::BIGINT, chunk_left.size());
        // Index into chunk_left of each matched target row (loop-relative) — for the
        // matched OLD-row / RETURNING left gathers, in lockstep with batch_ids.
        vector::indexing_vector_t matched_indexing(resource_);
        matched_indexing.reset(chunk_left.size());
        // The matched RIGHT (USING) rows gathered PER-ROW in lockstep with the matched
        // target rows, so a joined RETURNING column reads the matched pair. Built
        // row-by-row (NOT via an indexing gather across the small right chunk): a
        // target batch can match far more rows than the right chunk holds (every left
        // row joins the same handful of right rows), so an indexing-copy whose
        // source_count exceeds the right chunk size is invalid — copy the chosen right
        // row into slot `index` directly instead. Bounded by chunk_left.size()
        // (<=DEFAULT_VECTOR_CAPACITY): the semi-join takes at most one right row per
        // left row.
        data_chunk_t affected_right(resource_, types_right, chunk_left.size());

        size_t index = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            bool row_matched = false;
            for (const auto& chunk_right : right_chunks) {
                if (chunk_right.size() == 0) {
                    continue;
                }
                for (size_t j = 0; j < chunk_right.size(); j++) {
                    auto check_result = predicate->check(chunk_left, chunk_right, i, j);
                    if (check_result.has_error()) {
                        return check_result.error();
                    }
                    if (!check_result.value()) {
                        continue;
                    }
                    // Storage / index delete keys on the ABSOLUTE table row id of the
                    // matched left row, NOT the left-chunk loop index — the two diverge
                    // once the table has gaps, multiple row groups, or a non-zero
                    // row-group start. Mirror the simple branch's DICTIONARY fallback.
                    int64_t abs_id;
                    if (chunk_left.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                        abs_id = static_cast<int64_t>(chunk_left.data.front().indexing().get_index(i));
                    } else {
                        abs_id = chunk_left.row_ids.data<int64_t>()[i];
                    }
                    batch_ids.data<int64_t>()[index] = abs_id;
                    matched_indexing.set_index(index, i);
                    if (collect_returning) {
                        for (size_t k = 0; k < chunk_right.column_count(); ++k) {
                            vector::vector_ops::copy(chunk_right.data[k], affected_right.data[k], j + 1, j, index);
                        }
                    }
                    index++;
                    vector::validate_chunk_capacity(affected_right, index);
                    // Semi-join: stop after the first matching USING row.
                    row_matched = true;
                    break;
                }
                if (row_matched) {
                    break;
                }
            }
        }
        if (index == 0) {
            return core::error_t::no_error();
        }
        affected_right.set_cardinality(index);

        for (size_t i = 0; i < index; i++) {
            modified_->append(static_cast<size_t>(batch_ids.data<int64_t>()[i]));
        }
        for (const auto& type : types_left) {
            modified_->updated_types_map()[{std::pmr::string(type.alias(), resource_), type}] += index;
        }

        // Stage the matched OLD left rows + their absolute ids for the index mirror,
        // exactly as the simple (consume_batch_) path does — the merged staged chunk
        // row k pairs with index_old_row_ids_[k], so manager_index_t::delete_rows
        // reads them aligned, even when streaming leaves left_->output() empty.
        {
            data_chunk_t old_matched(resource_, types_left, index);
            chunk_left.copy(old_matched, matched_indexing, index);
            old_matched.set_cardinality(index);
            index_old_chunks_.emplace_back(std::move(old_matched));
            for (size_t i = 0; i < index; i++) {
                index_old_row_ids_.push_back(batch_ids.data<int64_t>()[i]);
            }
        }

        // Stage matched RETURNING rows: gather the matched LEFT subset (valid: index
        // <= chunk_left.size()), pair it with the per-row-built matched RIGHT chunk,
        // split both identically (same row count, same windows), then project each
        // window with the joined right batch. Appended to returning_staged_, which
        // await_async_and_resume drains exactly like the simple path.
        if (collect_returning) {
            data_chunk_t affected_left(resource_, types_left, index);
            chunk_left.copy(affected_left, matched_indexing, index);
            affected_left.set_cardinality(index);

            auto batches_left = split_chunk_into_batches(resource_, std::move(affected_left));
            auto batches_right = split_chunk_into_batches(resource_, std::move(affected_right));
            for (size_t b = 0; b < batches_left.size(); b++) {
                auto& batch = batches_left[b];
                if (batch.size() == 0) {
                    continue;
                }
                vector::data_chunk_t* right_batch = b < batches_right.size() ? &batches_right[b] : nullptr;
                auto proj = evaluate_projection(resource_,
                                                returning_,
                                                &batch,
                                                pipeline_context->parameters,
                                                pipeline_context->session_tz,
                                                right_batch);
                if (proj.has_error()) {
                    return proj.error();
                }
                returning_staged_.emplace_back(std::move(proj.value()));
            }
        }
        return core::error_t::no_error();
    }

    core::error_t
    operator_delete::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& /*out*/) {
        // STREAMING DML SINK: fold one scan batch into the matched-id / index-old /
        // RETURNING staging. Emits nothing (out stays empty); await_async_and_resume
        // drains the staged state into the single WAL->storage->index commit.
        // USING-join shape: probe the LEFT batch against the materialized RIGHT
        // (USING) build chunk; otherwise the simple predicate-scan fold.
        if (right_ && right_->output()) {
            return consume_join_batch_(ctx, input, right_->output()->chunks());
        }
        return consume_batch_(ctx, input);
    }

    actor_zeta::unique_future<void> operator_delete::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;
        using components::vector::vector_t;

        // Catalog-delete mode: delete pg_catalog rows by (oid_col_idx, target_oid)
        // via the WAL-first delete_pg_catalog_rows, then record the catalog table
        // on ctx->pg_catalog_delete_tables so operator_commit_transaction reverts/
        // publishes the MVCC tombstone for it. Bypasses the predicate-scan +
        // storage_delete_rows + WAL physical_delete + index path entirely.
        if (oid_col_idx_ >= 0) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz, table_oid_};
            auto [_c, cf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                             exec_ctx,
                                             table_oid_,
                                             oid_col_idx_,
                                             target_oid_);
            co_await std::move(cf);
            if (ctx->txn.transaction_id != 0) {
                ctx->pg_catalog_delete_tables.insert(table_oid_);
            }
            mark_executed();
            co_return;
        }

        if (!modified_ || modified_->size() == 0) {
            mark_executed();
            co_return;
        }

        // Snapshot the matched OLD (about-to-be-deleted) rows for a parent
        // fk_cascade BEFORE index_old_chunks_ is moved into the index mirror. On the
        // streaming path the scan SOURCE's output_ is empty, so fk_cascade — which
        // looks up child rows referencing the deleted parent keys — reads these
        // staged values instead of left_->left()->output(). The drive runs this
        // DML's await first, then the cascade's, so the snapshot is ready in time.
        // Copy (not move): the same chunks still feed the index mirror below.
        if (!index_old_chunks_.empty()) {
            chunks_vector_t cascade_rows(resource_);
            cascade_rows.reserve(index_old_chunks_.size());
            for (const auto& c : index_old_chunks_) {
                cascade_rows.emplace_back(c.partial_copy(resource_, 0, c.size()));
            }
            constraint_input_ = make_operator_data(resource_, std::move(cascade_rows));
        }

        auto& ids = modified_->ids();
        const size_t modified_size = modified_->size();
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz, table_oid_};

        // When a resolver sibling supplied catalog metadata, build a
        // translation against the scan-output chunk to surface any
        // alias/schema drift. operator_delete only ships row ids to the
        // disk actor (no per-column data), so the translation itself isn't
        // fed downstream — this is purely a diagnostic + wiring hook for
        // future metadata-aware delete paths (e.g. index-only deletes).
        if (resolved_metadata_.has_value() && left_ && left_->output() && !left_->output()->chunks().empty()) {
            // Schema is identical across chunks, so the first chunk drives the
            // alias/translation diagnostic.
            auto& scan_chunk = left_->output()->chunks().front();
            if (scan_chunk.column_count() > 0) {
                auto translation = build_column_key_translation(*resolved_metadata_, scan_chunk);
                for (std::size_t i = 0; i < translation.size(); ++i) {
                    if (translation[i] < 0 && scan_chunk.data[i].type().has_alias()) {
                        trace(log_,
                              "operator_delete: resolved metadata has no column matching scan alias '{}'",
                              std::string(scan_chunk.data[i].type().alias()));
                    }
                }
            }
        }

        // 1. Capture WAL row IDs. The row_ids come from the upstream scan, so they
        //    are fully known before any storage mutation — unlike INSERT (whose final
        //    count depends on dedup), DELETE has no post-op dependency. This lets the
        //    user delete adopt the SAME WAL-first ordering as the catalog delete
        //    (delete_pg_catalog_rows_inner: WAL physical_delete, then the storage mark).
        std::pmr::vector<int64_t> wal_row_ids(resource_);
        wal_row_ids.reserve(modified_size);
        for (size_t i = 0; i < modified_size; i++) {
            wal_row_ids.push_back(static_cast<int64_t>(ids[i]));
        }

        // 2. WAL-FIRST: physical_delete BEFORE the storage mark, so a crash between
        //    the two replays the delete (uncommitted deletes are filtered by replay).
        if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
            auto count = static_cast<uint64_t>(wal_row_ids.size());
            // See operator_insert comment on db_oid temporary hardcode.
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            auto [_w, wf] = actor_zeta::send(ctx->wal_address,
                                             &services::wal::manager_wal_replicate_t::write_physical_delete,
                                             ctx->session,
                                             table_oid_,
                                             std::move(wal_row_ids),
                                             count,
                                             ctx->txn.transaction_id,
                                             db_oid);
            auto wal_id = co_await std::move(wf);
            auto [_df2, dff] =
                actor_zeta::send(ctx->disk_address, &services::disk::manager_disk_t::flush, ctx->session, wal_id);
            ctx->add_pending_disk_future(std::move(dff));
        }

        // 3. storage_delete_rows — mark the rows deleted under this txn (MVCC).
        vector_t row_ids(resource_, types::logical_type::BIGINT, modified_size);
        for (size_t i = 0; i < modified_size; i++) {
            row_ids.data<int64_t>()[i] = static_cast<int64_t>(ids[i]);
        }
        auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_delete_rows,
                                         exec_ctx,
                                         table_oid_,
                                         std::move(row_ids),
                                         static_cast<uint64_t>(modified_size));
        co_await std::move(df);

        // 4. Mirror to index (old data). BOTH paths stage the MATCHED old rows +
        //    their absolute ids into index_old_chunks_/index_old_row_ids_: the
        //    SIMPLE path via consume_batch_ (push()), the USING-join path in its
        //    match loop. So the index delete_rows always receives the matched rows
        //    paired with their own ids — never the first-N scan rows — even when
        //    streaming leaves left_->output() empty. delete_rows takes the staged
        //    chunks as a chunks_vector_t (multi-chunk index I/O); index_old_chunks_
        //    already holds ≤DEFAULT_VECTOR_CAPACITY-sized batches aligned to
        //    index_old_row_ids_.
        if (ctx->index_address != actor_zeta::address_t::empty_address() && !index_old_chunks_.empty()) {
            // Send an OWNED deep copy of the staged old rows across the mailbox. The
            // fk_cascade snapshot above built constraint_input_ from partial_copy()
            // slices of these same chunks — those slices SHARE buffers (shared_ptr)
            // with index_old_chunks_ (vector_t slice = reference(other)). Moving
            // index_old_chunks_ into the manager_index actor would put a buffer on a
            // mailbox while a retained executor object (constraint_input_) still owns
            // it. Deep-copy each (<=DEFAULT_VECTOR_CAPACITY) chunk into fresh FLAT
            // vectors instead, leaving index_old_chunks_/constraint_input_ entirely
            // executor-owned. index_old_row_ids_ carries no shared buffers, so it is
            // moved as before; the copied chunks stay aligned to it row-for-row.
            chunks_vector_t index_old_copy(resource_);
            index_old_copy.reserve(index_old_chunks_.size());
            for (const auto& c : index_old_chunks_) {
                data_chunk_t owned(resource_, c.types(), c.size() == 0 ? 1 : c.size());
                if (c.size() > 0) {
                    c.copy(owned, 0);
                }
                index_old_copy.emplace_back(std::move(owned));
            }
            auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                               &services::index::manager_index_t::delete_rows,
                                               exec_ctx,
                                               table_oid_,
                                               std::move(index_old_copy),
                                               std::move(index_old_row_ids_));
            co_await std::move(ixf);
        }

        // 5. Record swap-info on context.
        ctx->dml_delete_txn_id = ctx->txn.transaction_id;
        ctx->dml_table_oid = table_oid_;

        // 6. Build result chunk. With RETURNING: drain the staged RETURNING here
        // (returning_staged_ is populated as batches are pushed). Without RETURNING,
        // emit a typed chunk whose cardinality carries the affected-row count.
        if (!returning_.empty()) {
            if (!returning_staged_.empty()) {
                set_output(make_operator_data(resource_, std::move(returning_staged_)));
            }
        } else {
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_types,
                                             ctx->session,
                                             table_oid_);
            auto types = co_await std::move(tf);
            // The result carries only the affected-row count as cardinality (no row data),
            // so emit it as a batch of ≤DEFAULT_VECTOR_CAPACITY chunks — a single chunk
            // cannot exceed one vector's worth of rows.
            const uint64_t cap = vector::DEFAULT_VECTOR_CAPACITY;
            chunks_vector_t batches(resource_);
            if (modified_size == 0) {
                data_chunk_t chunk(resource_, types, 1);
                chunk.set_cardinality(0);
                batches.emplace_back(std::move(chunk));
            } else {
                batches.reserve((modified_size + cap - 1) / cap);
                for (uint64_t base = 0; base < modified_size; base += cap) {
                    const uint64_t window = std::min<uint64_t>(cap, modified_size - base);
                    data_chunk_t chunk(resource_, types, window);
                    chunk.set_cardinality(window);
                    batches.emplace_back(std::move(chunk));
                }
            }
            set_output(make_operator_data(resource_, std::move(batches)));
        }
        mark_executed();
    }

} // namespace components::operators
