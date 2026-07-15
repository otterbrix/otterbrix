#include "operator_update.hpp"
#include "dml_util.hpp"
#include "predicates/predicate.hpp"
#include <atomic>
#include <cassert>
#include <components/vector/vector_operations.hpp>

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_update_storage_update_sends{0};
    } // namespace
    uint64_t update_storage_update_sends() noexcept {
        return g_update_storage_update_sends.load(std::memory_order_relaxed);
    }
#endif

    operator_update::operator_update(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::catalog::oid_t table_oid,
                                     std::pmr::vector<expressions::update_expr_ptr> updates,
                                     bool upsert,
                                     std::pmr::vector<select_column_t> returning,
                                     expressions::expression_ptr expr,
                                     std::int64_t affected_bound)
        : read_write_operator_t(resource, log, operator_type::update)
        , table_oid_(table_oid)
        , updates_(std::move(updates))
        , expr_(std::move(expr))
        , upsert_(upsert)
        , returning_(std::move(returning))
        , returning_from_chunks_(resource)
        , affected_bound_(affected_bound) {}

    namespace {
        // Applies all update expressions to out_chunk[0..match_count) and
        // records the modified rows in the modified_ list.
        void apply_updates(std::pmr::memory_resource* resource,
                           const std::pmr::vector<expressions::update_expr_ptr>& updates,
                           vector::data_chunk_t& out_chunk,
                           const vector::data_chunk_t& from_chunk,
                           uint64_t match_count,
                           const logical_plan::storage_parameters& parameters,
                           core::date::timezone_offset_t session_tz,
                           operators::operator_write_data_ptr& modified) {
            std::pmr::vector<bool> any_modified(match_count, false, resource);
            for (const auto& expr : updates) {
                auto row_flags = expr->execute(resource, out_chunk, from_chunk, match_count, &parameters, session_tz);
                for (uint64_t i = 0; i < match_count; i++) {
                    if (i < row_flags.size() && row_flags[i]) {
                        any_modified[i] = true;
                    }
                }
            }
            for (uint64_t i = 0; i < match_count; i++) {
                if (any_modified[i]) {
                    modified->append(i);
                }
            }
        }
    } // anonymous namespace

    void operator_update::ensure_simple_init_() {
        if (simple_init_done_) {
            return;
        }
        modified_ = operators::make_operator_write_data(resource_);
        // Accumulator for the NEW updated rows; consume_batch_ appends one out_chunk
        // per matched batch. await_async_and_resume iterates output_->chunks().
        output_ = operators::make_operator_data(resource_, chunks_vector_t{resource_});
        simple_init_done_ = true;
    }

    core::error_t operator_update::consume_batch_(pipeline::context_t* pipeline_context,
                                                  const vector::data_chunk_t& chunk) {
        using components::vector::data_chunk_t;
        ensure_simple_init_();
        if (chunk.size() == 0) {
            return core::error_t::no_error();
        }
        auto* resource = resource_;
        auto types = chunk.types();

        // expr_ is null for the simple predicate-scan UPDATE (the scan pushed the
        // WHERE), so create_all_true_predicate matches every scan row; a non-null
        // expr_ is honored for completeness.
        auto predicate = expr_ ? predicates::create_predicate(resource,
                                                              pipeline_context->function_registry,
                                                              expr_,
                                                              types,
                                                              types,
                                                              &pipeline_context->parameters,
                                                              pipeline_context->session_tz)
                               : predicates::create_all_true_predicate(resource);

        data_chunk_t out_chunk(resource, types, chunk.size());
        size_t index = 0;
        for (size_t i = 0; i < chunk.size(); ++i) {
            auto res = predicate->check(chunk, i);
            if (res.has_error()) {
                return res.error();
            }
            if (!res.value()) {
                continue;
            }
            if (chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                out_chunk.row_ids.data<int64_t>()[index] =
                    static_cast<int64_t>(chunk.data.front().indexing().get_index(i));
            } else {
                out_chunk.row_ids.data<int64_t>()[index] = chunk.row_ids.data<int64_t>()[i];
            }
            for (size_t k = 0; k < chunk.column_count(); ++k) {
                vector::vector_ops::copy(chunk.data[k], out_chunk.data[k], i + 1, i, index);
            }
            vector::validate_chunk_capacity(out_chunk, ++index);
        }
        out_chunk.set_cardinality(index);
        if (index == 0) {
            return core::error_t::no_error();
        }

        // Capture the matched OLD rows BEFORE apply_updates mutates out_chunk in
        // place — these are the pre-update rows for the index mirror, aligned
        // row-for-row (and by row_id) with the NEW rows appended to output_.
        // out_chunk.copy() copies both the columns and row_ids for out_chunk.size()
        // (== index) rows and sets old_chunk's cardinality.
        data_chunk_t old_chunk(resource, types, index);
        out_chunk.copy(old_chunk, 0);
        index_old_chunks_.emplace_back(std::move(old_chunk));

        apply_updates(resource,
                      updates_,
                      out_chunk,
                      out_chunk,
                      index,
                      pipeline_context->parameters,
                      pipeline_context->session_tz,
                      modified_);
        output_->append_chunk(std::move(out_chunk));
        return core::error_t::no_error();
    }

    core::error_t operator_update::consume_join_batch_(pipeline::context_t* pipeline_context,
                                                       const vector::data_chunk_t& chunk_left,
                                                       const chunks_vector_t& right_chunks) {
        // UPDATE ... FROM shared core (R6: one implementation, two entry points).
        // Probes ONE LEFT (target) scan batch against the fully-materialized RIGHT
        // (FROM) build chunks: a semi-join (a target row is updated once regardless
        // of how many FROM rows it matches). Per matched LEFT row it builds the
        // updated out_chunk (matched columns, SET applied), accumulates it into
        // output_ + modified_, stages the matched OLD rows for the index
        // mirror (aligned by row_id with the NEW rows), and — for RETURNING — keeps
        // the matched FROM rows in lockstep so a joined RETURNING column reads them.
        // push() calls it per LEFT batch. await_async_and_resume drains it all.
        using components::vector::data_chunk_t;
        ensure_simple_init_();
        if (chunk_left.size() == 0) {
            return core::error_t::no_error();
        }
        auto* resource = resource_;
        auto types_left = chunk_left.types();
        std::pmr::vector<types::complex_logical_type> types_right(resource);
        for (const auto& rc : right_chunks) {
            if (rc.size() > 0) {
                types_right = rc.types();
                break;
            }
        }

        auto predicate = expr_ ? predicates::create_predicate(resource,
                                                              pipeline_context->function_registry,
                                                              expr_,
                                                              types_left,
                                                              types_right,
                                                              &pipeline_context->parameters,
                                                              pipeline_context->session_tz)
                               : predicates::create_all_true_predicate(resource);

        data_chunk_t out_chunk(resource, types_left, chunk_left.size());
        data_chunk_t right_chunk(resource, types_right, chunk_left.size());
        size_t index = 0;
        for (size_t i = 0; i < chunk_left.size(); ++i) {
            // Matched-row bound (UPDATE ... FROM ... LIMIT n): stop once the running matched
            // total (already-flushed matched_total_ + this batch's index) reaches the bound.
            // -1 = unbounded.
            if (affected_bound_ >= 0 && matched_total_ + index >= static_cast<uint64_t>(affected_bound_)) {
                break;
            }
            bool row_matched = false;
            for (const auto& chunk_right : right_chunks) {
                if (chunk_right.size() == 0) {
                    continue;
                }
                auto results = predicates::batch_check_1vN(predicate, chunk_left, chunk_right, i, chunk_right.size());
                if (results.has_error()) {
                    return results.error();
                }
                for (size_t j = 0; j < chunk_right.size(); ++j) {
                    if (!results.value()[j]) {
                        continue;
                    }
                    // Storage / index update keys on the ABSOLUTE table row id of the
                    // matched left row; mirror the simple path's DICTIONARY fallback.
                    if (chunk_left.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
                        out_chunk.row_ids.data<int64_t>()[index] =
                            static_cast<int64_t>(chunk_left.data.front().indexing().get_index(i));
                    } else {
                        out_chunk.row_ids.data<int64_t>()[index] = chunk_left.row_ids.data<int64_t>()[i];
                    }
                    for (size_t k = 0; k < chunk_left.column_count(); ++k) {
                        vector::vector_ops::copy(chunk_left.data[k], out_chunk.data[k], i + 1, i, index);
                    }
                    for (size_t k = 0; k < chunk_right.column_count(); ++k) {
                        vector::vector_ops::copy(chunk_right.data[k], right_chunk.data[k], j + 1, j, index);
                    }
                    ++index;
                    vector::validate_chunk_capacity(out_chunk, index);
                    vector::validate_chunk_capacity(right_chunk, index);
                    // UPDATE ... FROM is a semi-join: a target row is updated once
                    // regardless of how many FROM rows it matches. Stop after the
                    // first matching FROM row.
                    row_matched = true;
                    break;
                }
                if (row_matched) {
                    break;
                }
            }
        }
        // Count matched left rows at MATCH time so the bound survives mid-pump flushes.
        matched_total_ += index;
        out_chunk.set_cardinality(index);
        right_chunk.set_cardinality(index);
        if (index == 0) {
            return core::error_t::no_error();
        }

        // Capture the matched OLD rows BEFORE apply_updates mutates out_chunk in
        // place — pre-update rows for the index mirror, aligned row-for-row (and by
        // row_id) with the NEW rows accumulated in output_.
        data_chunk_t old_chunk(resource, types_left, index);
        out_chunk.copy(old_chunk, 0);
        index_old_chunks_.emplace_back(std::move(old_chunk));

        apply_updates(resource,
                      updates_,
                      out_chunk,
                      right_chunk,
                      index,
                      pipeline_context->parameters,
                      pipeline_context->session_tz,
                      modified_);
        output_->append_chunk(std::move(out_chunk));
        // Keep the matched FROM rows aligned with the updated rows so RETURNING can
        // project joined (right-side) columns.
        if (!returning_.empty()) {
            returning_from_chunks_.emplace_back(std::move(right_chunk));
        }
        return core::error_t::no_error();
    }

    core::error_t
    operator_update::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& /*out*/) {
        // STREAMING DML SINK: fold one scan batch into the updated-rows accumulator
        // (output_), modified_, and the index-old staging. Emits
        // nothing; await_async_and_resume drains the staged state into the single
        // WAL->storage->index commit. FROM-join shape: probe the LEFT batch against
        // the materialized RIGHT (FROM) build chunks; otherwise the simple fold.
        if (right_ && right_->output()) {
            return consume_join_batch_(ctx, input, right_->output()->chunks());
        }
        return consume_batch_(ctx, input);
    }

    actor_zeta::unique_future<void> operator_update::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;
        using components::vector::vector_t;

        // BOUNDED DML SINK. The executor drives this INCREMENTALLY: once
        // per mid-pump "buffer full" (dml_flush_is_final==false) and once at the
        // post-pump finalize (==true). Each drive flushes whatever push() folded into
        // output_ since the last flush; only the FINAL drive emits the RETURNING /
        // affected-count result + mark_executed. With dml_flush_row_threshold==0 the
        // executor drives await exactly once with is_final==true, collapsing to a
        // single flush.
        const bool is_final = ctx->dml_flush_is_final;

        if (output_ && output_->size() > 0) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz, table_oid_};
            // See operator_insert comment on db_oid temporary hardcode.
            constexpr auto db_oid = components::catalog::well_known_oid::main_database;
            const bool mirror_index = ctx->index_address != actor_zeta::address_t::empty_address();

            // STREAMING invariant: consume_batch_/consume_join_batch_ stage exactly one
            // OLD-row chunk per accumulated updated chunk, so index_old_chunks_ is in
            // lockstep with output_->chunks() (index_old_chunks_[k] is the old version
            // of output_->chunks()[k]). Assert the staging held rather than silently
            // walking left_->output() (which is empty when streaming) mid-flush.
            assert(index_old_chunks_.size() == output_->chunks().size());

            // ONE flush of the currently-buffered rows via a NAMED coroutine lambda.
            // UPDATE = MVCC delete-old + append-new: the operator OWNS its WAL
            // (write_physical_update) and records BOTH an append range (via
            // record_flush, below) and a delete marker. The new-row segments append
            // sequentially within the txn, so they coalesce into one range; gather the
            // whole batch up front, then one send per service.
            auto op = [&]([[maybe_unused]] std::pmr::memory_resource* res)
                -> actor_zeta::unique_future<dml_detail::flush_outcome_t> {
                auto copy_of = [this](const data_chunk_t& src) {
                    data_chunk_t dst(resource_, src.types(), src.size());
                    src.copy(dst, 0);
                    return dst;
                };

                chunks_vector_t update_data(resource_);               // storage_update payload (mutated)
                std::pmr::vector<vector_t> update_row_ids(resource_); // storage_update row_ids, one per chunk
                chunks_vector_t wal_chunks(resource_);                // WAL payload (submitted new rows)
                std::pmr::vector<int64_t> wal_row_ids(resource_);     // WAL row_ids, flat
                chunks_vector_t idx_old(resource_);                   // index: old row versions, one per chunk
                chunks_vector_t idx_new(resource_);                   // index: new rows, one per chunk
                std::pmr::vector<int64_t> idx_row_ids(resource_);     // index row_ids, flat

                size_t out_chunk_idx = 0;
                for (auto& out_chunk : output_->chunks()) {
                    if (out_chunk.size() == 0) {
                        continue;
                    }
                    const uint64_t n = out_chunk.size();

                    // storage_update needs a row_ids vector_t + payload copy per chunk.
                    vector_t row_ids(resource_, types::logical_type::BIGINT, n);
                    for (uint64_t i = 0; i < n; i++) {
                        row_ids.data<int64_t>()[i] = out_chunk.row_ids.data<int64_t>()[i];
                    }
                    update_row_ids.emplace_back(std::move(row_ids));
                    update_data.emplace_back(copy_of(out_chunk));

                    // WAL needs the submitted new rows + their flat row_ids.
                    wal_chunks.emplace_back(copy_of(out_chunk));
                    for (uint64_t i = 0; i < n; i++) {
                        wal_row_ids.push_back(out_chunk.row_ids.data<int64_t>()[i]);
                    }

                    // Index needs the n old row versions + the new rows and their ids.
                    // index_old_chunks_[out_chunk_idx] is this updated chunk's OLD
                    // version, staged in lockstep by push() (asserted above).
                    if (mirror_index) {
                        idx_old.emplace_back(std::move(index_old_chunks_[out_chunk_idx]));
                        idx_new.emplace_back(copy_of(out_chunk));
                        for (uint64_t i = 0; i < n; i++) {
                            idx_row_ids.push_back(out_chunk.row_ids.data<int64_t>()[i]);
                        }
                    }
                    ++out_chunk_idx;
                }

                // 1. RETURNING is a pure LOCAL projection over the already-built updated
                //    chunks (paired with their lockstep FROM chunks) — run it BEFORE any
                //    storage mutation, so a projection error fails the statement CLEANLY:
                //    zero storage writes, no WAL update record, nothing to revert. The
                //    projected chunks accumulate across flushes; the FINAL drive emits them.
                if (!returning_.empty()) {
                    for (size_t i = 0; i < output_->chunks().size(); ++i) {
                        auto& out_chunk = output_->chunks()[i];
                        if (out_chunk.size() == 0) {
                            continue;
                        }
                        data_chunk_t* right_batch =
                            i < returning_from_chunks_.size() ? &returning_from_chunks_[i] : nullptr;
                        auto proj = evaluate_projection(resource_,
                                                        returning_,
                                                        &out_chunk,
                                                        ctx->parameters,
                                                        ctx->session_tz,
                                                        right_batch);
                        if (proj.has_error()) {
                            co_return dml_detail::flush_outcome_t{proj.error()};
                        }
                        returning_accum_.emplace_back(std::move(proj.value()));
                    }
                }

                // 2. storage_update (MVCC: delete old + insert new) — one batched send.
                //    The reply carries any write_conflict / out_of_memory from the
                //    table-layer MVCC update as a value; surface it as a clean error so
                //    the txn aborts gracefully.
#ifdef DEV_MODE
                g_update_storage_update_sends.fetch_add(1, std::memory_order_relaxed);
#endif
                auto [_u, uf] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::storage_update,
                                                 exec_ctx,
                                                 table_oid_,
                                                 std::move(update_row_ids),
                                                 std::move(update_data));
                auto update_result = co_await std::move(uf);
                if (update_result.has_error()) {
                    co_return dml_detail::flush_outcome_t{update_result.error()};
                }
                auto [range_start, total_count] = update_result.value();

                // 3. WAL physical_update: one record for THIS flushed range. UPDATE
                //    owns its WAL write (unlike INSERT's WAL-first storage_append).
                if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
                    const uint64_t wal_count = wal_row_ids.size();
                    auto [_w, wf] = actor_zeta::send(ctx->wal_address,
                                                     &services::wal::manager_wal_replicate_t::write_physical_update,
                                                     ctx->session,
                                                     table_oid_,
                                                     std::move(wal_row_ids),
                                                     std::move(wal_chunks),
                                                     wal_count,
                                                     ctx->txn.transaction_id,
                                                     db_oid);
                    auto wal_id = co_await std::move(wf);
                    auto [_df, dff] = actor_zeta::send(ctx->disk_address,
                                                       &services::disk::manager_disk_t::flush,
                                                       ctx->session,
                                                       wal_id);
                    ctx->add_pending_disk_future(std::move(dff));
                }

                // 4. Mirror to index (old + new data) — one batched send. idx_old came
                //    from the streaming staging (index_old_chunks_), aligned row-for-row
                //    + by row_id with the new rows.
                if (mirror_index) {
                    auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                                       &services::index::manager_index_t::update_rows,
                                                       exec_ctx,
                                                       table_oid_,
                                                       std::move(idx_old),
                                                       std::move(idx_new),
                                                       std::move(idx_row_ids),
                                                       range_start);
                    co_await std::move(ixf);
                }

                // 5. Without RETURNING the affected count accumulates across flushes
                //    (the RETURNING projection already ran in step 1, pre-mutation).
                if (returning_.empty()) {
                    affected_rows_ += total_count;
                }

                co_return dml_detail::flush_outcome_t{core::error_t::no_error(), true, range_start, total_count};
            };

            auto outcome = co_await op(resource_);
            // COMMON post-storage bookkeeping (dml_util): record the append range into
            // the unified append channel and — only under a parent constraint — accumulate
            // a persistent copy of the just-written NEW rows into constraint_input_ (so
            // the constraint validates the full set at finalize). constraint_rows =
            // output_->chunks(): op only COPIED from output_, so the NEW rows are intact.
            auto err = dml_detail::record_flush(ctx,
                                                resource_,
                                                table_oid_,
                                                outcome,
                                                ctx->dml_has_parent_constraint,
                                                constraint_input_,
                                                output_->chunks());
            if (err.contains_error()) {
                set_error(err);
                mark_failed();
                co_return;
            }

            // UPDATE = delete-old + append-new: record the MVCC delete tombstone ONCE
            // across all flushes (append ranges are per-flush via record_flush; the
            // delete marker is a single per-txn/table tombstone).
            if (!delete_marker_recorded_) {
                ctx->dml_deletes.push_back(
                    components::table::dml_delete_range_t{table_oid_, ctx->txn.transaction_id});
                delete_marker_recorded_ = true;
            }

            // Release the flushed batch: the accumulated updated rows and the lockstep
            // staging that fed THIS flush. output_/index_old_chunks_/returning_from_
            // chunks_ stay in lockstep so the next flush starts clean and bounded.
            output_->chunks().clear();
            index_old_chunks_.clear();
            returning_from_chunks_.clear();
        }

        // MID-PUMP flush: more batches may still arrive — do NOT emit the result or
        // mark executed. Only the final drive finalizes.
        if (!is_final) {
            co_return;
        }

        // FINAL. output_ was cleared per flush, so it cannot double as the
        // affected-count carrier: emit an explicit result — affected-count chunks
        // without RETURNING, the accumulated projection with it.
        if (returning_.empty()) {
            if (affected_rows_ > 0) {
                // Column-less chunks whose cardinalities sum to the affected-row count
                // (the cursor totals chunk sizes).
                set_output(make_operator_data(
                    resource_, dml_detail::make_affected_count_chunks(resource_, affected_rows_, {})));
            } else {
                set_output(nullptr);
            }
        } else {
            set_output(make_operator_data(resource_, std::move(returning_accum_)));
        }
        mark_executed();
    }

} // namespace components::operators
