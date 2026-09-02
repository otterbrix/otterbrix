#include "operator_delete.hpp"

#include "dml_util.hpp"
#include "join_utils.hpp"
#include <atomic>
#include <components/vector/vector_operations.hpp>

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_delete_scanned_columns{0};
    } // namespace
    uint64_t delete_scanned_columns() noexcept { return g_delete_scanned_columns.load(std::memory_order_relaxed); }
#endif

    operator_delete::operator_delete(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::catalog::oid_t table_oid,
                                     std::pmr::vector<projected_column_t> returning,
                                     expressions::expression_ptr expr,
                                     std::int64_t affected_bound)
        : read_write_operator_t(resource, log, operator_type::remove)
        , table_oid_(table_oid)
        , expression_(std::move(expr))
        , condition_(expressions::classify_condition(expression_))
        , returning_(std::move(returning))
        , affected_bound_(affected_bound) {}

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

        // all_true/all_false can skip graph
        if (condition_ == expressions::condition_kind::never) {
            return core::error_t::no_error();
        }
        std::optional<vector::data_chunk_t> produced;
        if (condition_ == expressions::condition_kind::computed) {
            // lazy ininialized graph (if consume() is never called, there is no point in building it)
            if (!graph_) {
                auto built = expressions::build_condition_graph(resource_,
                                                                pipeline_context->parameters.parameters,
                                                                expression_.get(),
                                                                types);
                if (built.has_error()) {
                    return built.error();
                }
                graph_ = std::move(built.value());
            }
            auto decided = expressions::run_graph(graph_.get(),
                                                  pipeline_context->parameters.parameters,
                                                  chunk,
                                                  pipeline_context->execution_context);
            if (decided.has_error()) {
                return decided.error();
            }
            produced = std::move(decided.value());
        }
        const vector::vector_t* decisions = produced.has_value() ? &produced->data.front() : nullptr;

        // Matched ABSOLUTE row-ids of THIS batch (kept separate so the index mirror
        // pairs each staged old-row with its own id, regardless of batch order).
        vector::vector_t batch_ids(resource_, types::logical_type::BIGINT, chunk.size());
        // Indexing of matched rows into `chunk`, for the gathered old-row / RETURNING copies.
        vector::indexing_vector_t matched_indexing(resource_);
        matched_indexing.reset(chunk.size());

        size_t index = 0;
        for (size_t i = 0; i < chunk.size(); i++) {
            if (decisions != nullptr && (decisions->is_null(i) || !decisions->get_value<bool>(i))) {
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
            if (affected.size() != 0) {
                auto proj = evaluate_projection(resource_,
                                                returning_,
                                                &affected,
                                                pipeline_context->parameters,
                                                pipeline_context->execution_context,
                                                &returning_graph_);
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

        if (condition_ == expressions::condition_kind::never) {
            return core::error_t::no_error();
        }
        chunks_vector_t merged(resource_);
        if (condition_ == expressions::condition_kind::computed) {
            if (!graph_) {
                std::pmr::vector<types::complex_logical_type> merged_types(resource_);
                merged_types.reserve(types_left.size() + types_right.size());
                merged_types.insert(merged_types.end(), types_left.begin(), types_left.end());
                merged_types.insert(merged_types.end(), types_right.begin(), types_right.end());
                auto built = expressions::build_condition_graph(resource_,
                                                                pipeline_context->parameters.parameters,
                                                                expression_.get(),
                                                                merged_types,
                                                                types_left.size());
                if (built.has_error()) {
                    return built.error();
                }
                graph_ = std::move(built.value());
            }
            merged.reserve(right_chunks.size());
            for (const auto& chunk_right : right_chunks) {
                merged.push_back(join_detail::merged_chunk(resource_, types_left, chunk_right));
            }
        }

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
            // Affected-row bound (DELETE ... USING ... LIMIT n): stop matching once the
            // running matched total (already-flushed matches in matched_total_ + this
            // batch's index) reaches the bound. -1 = unbounded.
            if (affected_bound_ >= 0 && matched_total_ + index >= static_cast<uint64_t>(affected_bound_)) {
                break;
            }
            bool row_matched = false;
            for (size_t ci = 0; ci < right_chunks.size(); ci++) {
                const auto& chunk_right = right_chunks[ci];
                if (chunk_right.size() == 0) {
                    continue;
                }
                std::optional<vector::data_chunk_t> produced;
                if (graph_) {
                    join_detail::point_at_probe_row(resource_, merged[ci], chunk_left, i);
                    auto decided = expressions::run_graph(graph_.get(),
                                                          pipeline_context->parameters.parameters,
                                                          merged[ci],
                                                          pipeline_context->execution_context);
                    if (decided.has_error()) {
                        return decided.error();
                    }
                    produced = std::move(decided.value());
                }
                const vector::vector_t* decisions = produced.has_value() ? &produced->data.front() : nullptr;
                for (size_t j = 0; j < chunk_right.size(); j++) {
                    if (decisions != nullptr && (decisions->is_null(j) || !decisions->get_value<bool>(j))) {
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
        // Count matched left rows at MATCH time (covers this batch, flushed or not) so the
        // bound survives mid-pump flushes that clear modified_.
        matched_total_ += index;
        if (index == 0) {
            return core::error_t::no_error();
        }
        affected_right.set_cardinality(index);

        for (size_t i = 0; i < index; i++) {
            modified_->append(static_cast<size_t>(batch_ids.data<int64_t>()[i]));
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
        // then project the matched rows with the joined right chunk. Appended to
        // returning_staged_, which await_async_and_resume drains exactly like the
        // simple path.
        if (collect_returning) {
            data_chunk_t affected_left(resource_, types_left, index);
            chunk_left.copy(affected_left, matched_indexing, index);
            affected_left.set_cardinality(index);

            if (affected_left.size() != 0) {
                auto proj = evaluate_projection(resource_,
                                                returning_,
                                                &affected_left,
                                                pipeline_context->parameters,
                                                pipeline_context->execution_context,
                                                &returning_graph_,
                                                &affected_right);
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
#ifdef DEV_MODE
        for (const auto& column : input.data) {
            // A placeholder for an unprojected column carries no buffer at all; a real
            // (even all-NULL) column does. Counting buffers counts what the scan read.
            if (column.data() != nullptr || column.auxiliary() != nullptr) {
                g_delete_scanned_columns.fetch_add(1, std::memory_order_relaxed);
            }
        }
#endif
        if (right_ && right_->output()) {
            return consume_join_batch_(ctx, input, right_->output()->chunks());
        }
        return consume_batch_(ctx, input);
    }

    actor_zeta::unique_future<void> operator_delete::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;
        using components::vector::vector_t;

        // The executor drives this INCREMENTALLY — once per "buffer full" during
        // the pump (dml_flush_is_final==false) and once at finalize (==true). Each call
        // flushes the currently-buffered matched-id slice; only the final call emits the
        // RETURNING / affected-count output and mark_executed. threshold==0 collapses to
        // exactly one final call.
        const bool is_final = ctx->dml_flush_is_final;

        // Catalog-delete mode: delete pg_catalog rows by (oid_col_idx, target_oid)
        // via the WAL-first delete_pg_catalog_rows, then record the catalog table
        // on ctx->pg_catalog_delete_tables so operator_commit_transaction reverts/
        // publishes the MVCC tombstone for it. Bypasses the predicate-scan +
        // storage_delete_rows + WAL physical_delete + index path entirely. It buffers
        // nothing (buffered_rows()==0), so it is a single-shot sink — never mid-flushed.
        if (oid_col_idx_ >= 0) {
            components::execution_context_t exec_ctx{ctx->session,
                                                     ctx->txn,
                                                     ctx->execution_context.timezone_offset,
                                                     table_oid_};
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

        // Flush the buffered matched-id slice, if any. The divergent DELETE storage op
        // (WAL-first physical_delete, then storage_delete_rows, then the index mirror)
        // lives in the NAMED coroutine lambda `op`, which yields a flush_outcome_t;
        // record_flush() then does the COMMON post-storage bookkeeping (constraint
        // accumulation when a parent constraint sits above the DML). DELETE writes its
        // OWN WAL (unlike INSERT, where the disk agent owns it) and appends
        // nothing, so the outcome carries no append range.
        if (modified_ && modified_->size() > 0) {
            // See operator_insert: "an index manager exists" holds for every table, so the real
            // question is whether the TABLE has an index.
            const bool mirror_index = table_has_indexes_ &&
                                      ctx->index_address != actor_zeta::address_t::empty_address() &&
                                      !index_old_chunks_.empty();

            auto op = [this, ctx, mirror_index](
                          std::pmr::memory_resource* res) -> actor_zeta::unique_future<dml_detail::flush_outcome_t> {
                components::execution_context_t exec_ctx{ctx->session,
                                                         ctx->txn,
                                                         ctx->execution_context.timezone_offset,
                                                         table_oid_};
                auto& ids = modified_->ids();
                const size_t modified_size = modified_->size();

                // 1. WAL-FIRST: physical_delete BEFORE the storage mark, so a crash
                //    between the two replays the delete (uncommitted deletes are
                //    filtered by replay). The row_ids come from the upstream scan, so
                //    they are fully known before any storage mutation — unlike INSERT
                //    (whose final count depends on dedup), DELETE has no post-op
                //    dependency, so it adopts the same WAL-first ordering the catalog
                //    delete uses (delete_pg_catalog_rows_inner).
                if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
                    std::pmr::vector<int64_t> wal_row_ids(res);
                    wal_row_ids.reserve(modified_size);
                    for (size_t i = 0; i < modified_size; i++) {
                        wal_row_ids.push_back(static_cast<int64_t>(ids[i]));
                    }
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
                    auto [_df2, dff] = actor_zeta::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::flush,
                                                        ctx->session,
                                                        wal_id);
                    ctx->add_pending_disk_future(std::move(dff));
                }

                // 2. storage_delete_rows — mark the rows deleted under this txn (MVCC).
                vector_t row_ids(res, types::logical_type::BIGINT, modified_size);
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

                // 3. Mirror to index (old data). BOTH paths stage the MATCHED old rows +
                //    their absolute ids into index_old_chunks_/index_old_row_ids_: the
                //    SIMPLE path via consume_batch_ (push()), the USING-join path in its
                //    match loop. So the index delete_rows always receives the matched
                //    rows paired with their own ids — never the first-N scan rows — even
                //    when streaming leaves left_->output() empty.
                if (mirror_index) {
                    // Send an OWNED deep copy of the staged old rows across the mailbox.
                    // record_flush() (below) deep-copies index_old_chunks_ into
                    // constraint_input_ when a parent constraint is present, so the
                    // staged chunks must stay executor-owned — never handed to the
                    // manager_index actor by move. Deep-copy each
                    // (<=DEFAULT_VECTOR_CAPACITY) chunk into fresh FLAT vectors instead.
                    // index_old_row_ids_ carries no shared buffers, so it is moved; the
                    // copied chunks stay aligned to it row-for-row.
                    chunks_vector_t index_old_copy(res);
                    index_old_copy.reserve(index_old_chunks_.size());
                    for (const auto& c : index_old_chunks_) {
                        data_chunk_t owned(res, c.types(), c.size() == 0 ? 1 : c.size());
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
                    auto index_error = co_await std::move(ixf);
                    if (index_error.contains_error()) {
                        // Rows removed from the table but still present in the index: the next
                        // index scan would return them. Fail the statement instead.
                        co_return dml_detail::flush_outcome_t{std::move(index_error), false, 0, 0};
                    }
                }

                affected_rows_ += static_cast<uint64_t>(modified_size);
                // DELETE appends nothing: no append range on the outcome.
                co_return dml_detail::flush_outcome_t{core::error_t::no_error(), false, 0, 0};
            };

            auto outcome = co_await op(resource_);
            // The rows a parent fk_cascade must observe are the OLD (about-to-delete)
            // rows it reads to find referencing children — index_old_chunks_. Pass them
            // as constraint_rows; record_flush accumulates them into constraint_input_
            // ONLY when dml_has_parent_constraint (bounded memory otherwise). The deep
            // copy for the index send above left index_old_chunks_ intact for this read.
            auto err = dml_detail::record_flush(ctx,
                                                resource_,
                                                table_oid_,
                                                outcome,
                                                ctx->dml_has_parent_constraint,
                                                constraint_input_,
                                                index_old_chunks_);
            // Record the delete marker ONCE across all flushes: COMMIT/ABORT key the
            // MVCC swap/revert on the txn id, not on per-flush ranges. Recorded BEFORE
            // the flush-error check so a late flush failure still leaves the marker for
            // the failed-statement abort tail to un-stamp the already-stamped marks.
            if (!delete_marker_recorded_) {
                ctx->dml_deletes.push_back(components::table::dml_delete_range_t{table_oid_, ctx->txn.transaction_id});
                delete_marker_recorded_ = true;
            }

            if (err.contains_error()) {
                set_error(err);
                mark_failed();
                co_return;
            }

            // Clear the flushed slice (bounded memory). Keep returning_staged_ — it is
            // the RETURNING accumulator, drained only on the final call.
            modified_ = operators::make_operator_write_data(resource_);
            index_old_chunks_.clear();
            index_old_row_ids_.clear();
        }

        // Mid-pump flush: emit nothing, keep accumulating for the next call.
        if (!is_final) {
            co_return;
        }

        // FINAL: with RETURNING, drain the staged RETURNING accumulator. Without
        // RETURNING, emit a typed chunk batch whose cardinalities sum to the total
        // affected-row count accumulated across every flush. Nothing deleted and no
        // RETURNING => leave output_ null (a 0-affected DELETE emits no result rows).
        if (!returning_.empty()) {
            if (returning_staged_.empty()) {
                // Nothing matched, but we still have to return correct columns
                auto [_rt, rtf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_types,
                                                   ctx->session,
                                                   table_oid_);
                auto returning_types = co_await std::move(rtf);
                data_chunk_t empty(resource_, returning_types, 0);
                empty.set_cardinality(0);
                auto proj = evaluate_projection(resource_,
                                                returning_,
                                                &empty,
                                                ctx->parameters,
                                                ctx->execution_context,
                                                &returning_graph_);
                if (proj.has_error()) {
                    set_error(proj.error());
                    mark_failed();
                    co_return;
                }
                returning_staged_.emplace_back(std::move(proj.value()));
            }
            set_output(make_operator_data(resource_, std::move(returning_staged_)));
        } else if (affected_rows_ > 0) {
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_types,
                                             ctx->session,
                                             table_oid_);
            auto types = co_await std::move(tf);
            // The result carries only the affected-row count as cardinality (no row data),
            // emitted as ≤DEFAULT_VECTOR_CAPACITY-row chunks shaped by the table's types.
            set_output(make_operator_data(resource_,
                                          dml_detail::make_affected_count_chunks(resource_, affected_rows_, types)));
        }
        mark_executed();
    }

} // namespace components::operators
