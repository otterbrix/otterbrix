#include "operator_update.hpp"
#include "predicates/predicate.hpp"
#include <components/vector/vector_operations.hpp>

#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <services/wal/manager_wal_replicate.hpp>

namespace components::operators {

    operator_update::operator_update(std::pmr::memory_resource* resource,
                                     log_t log,
                                     components::catalog::oid_t table_oid,
                                     std::pmr::vector<expressions::update_expr_ptr> updates,
                                     bool upsert,
                                     std::pmr::vector<select_column_t> returning,
                                     expressions::expression_ptr expr)
        : read_write_operator_t(resource, log, operator_type::update)
        , table_oid_(table_oid)
        , updates_(std::move(updates))
        , expr_(std::move(expr))
        , upsert_(upsert)
        , returning_(std::move(returning))
        , returning_from_chunks_(resource) {}

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

        if (!output_) {
            mark_executed();
            co_return;
        }
        // Snapshot the NEW (post-SET) updated rows for a parent fk_check /
        // check_constraint BEFORE a RETURNING projection replaces output_. The
        // streaming async-finalize drive runs this DML's await first, then the
        // constraint's; the constraint validates these new values against the
        // parent table / NOT-NULL rules.
        constraint_input_ = output_;
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz, table_oid_};
        // See operator_insert comment on db_oid temporary hardcode.
        constexpr auto db_oid = components::catalog::well_known_oid::main_database;

        // Hold the updated chunks alive while we ship them; set_output rebinds output_.
        auto input = output_;

        // Old-row cursor over the scan (left) output. update_rows pairs old_data[i]
        // with new_data[i]/row_ids[i] positionally.
        //
        // Old-row source: the streaming SIMPLE path (consume_batch_ via push())
        // staged the matched OLD rows into index_old_chunks_, one staged chunk per
        // accumulated updated chunk, in lockstep — so index_old_chunks_[k] is the
        // old version of input->chunks()[k]. That is the only source available when
        // streaming leaves left_->output() empty. For any non-streaming caller that
        // did not stage (index_old_chunks_ empty), fall back to walking the scan
        // (left_->output()) cursor in matched order, exactly as before.
        const bool have_staged_old = !index_old_chunks_.empty();
        const chunks_vector_t* scan_chunks = nullptr;
        if (auto scan_out = left_ ? left_->output() : nullptr) {
            scan_chunks = &scan_out->chunks();
        }
        size_t scan_chunk_idx = 0;
        uint64_t scan_row_in_chunk = 0;
        size_t out_chunk_idx = 0;

        // UPDATE = delete-old + append-new. The new-row segments append sequentially
        // within the txn, so they are contiguous and coalesce into one range. Gather the
        // whole batch up front, then issue a single send per service.
        chunks_vector_t update_data(resource_);               // storage_update payload (mutated)
        std::pmr::vector<vector_t> update_row_ids(resource_); // storage_update row_ids, one per chunk
        chunks_vector_t wal_chunks(resource_);                // WAL payload (submitted new rows)
        std::pmr::vector<int64_t> wal_row_ids(resource_);     // WAL row_ids, flat
        chunks_vector_t idx_old(resource_);                   // index: old row versions, one per chunk
        chunks_vector_t idx_new(resource_);                   // index: new rows, one per chunk
        std::pmr::vector<int64_t> idx_row_ids(resource_);     // index row_ids, flat
        chunks_vector_t projected(resource_);
        // RETURNING: the matched FROM (right) rows were gathered in lockstep, one right
        // chunk per updated chunk (returning_from_chunks_), aligned by index.
        size_t right_idx = 0;
        const bool mirror_index =
            ctx->index_address != actor_zeta::address_t::empty_address() && (have_staged_old || scan_chunks != nullptr);

        auto copy_of = [this](const data_chunk_t& src) {
            data_chunk_t dst(resource_, src.types(), src.size());
            src.copy(dst, 0);
            return dst;
        };

        for (auto& out_chunk : input->chunks()) {
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

            // Index needs the n old row versions plus the new rows and their row_ids.
            // STREAMING: the staged index_old_chunks_[out_chunk_idx] is the old
            // version of this updated chunk (one staged chunk per push() in lockstep).
            // FALLBACK (no staging): walk the scan cursor in matched order to
            // assemble this chunk's old data.
            if (mirror_index) {
                if (have_staged_old && out_chunk_idx < index_old_chunks_.size()) {
                    idx_old.emplace_back(std::move(index_old_chunks_[out_chunk_idx]));
                } else {
                    data_chunk_t old_data(resource_, out_chunk.types(), n);
                    uint64_t filled = 0;
                    while (scan_chunks != nullptr && filled < n && scan_chunk_idx < scan_chunks->size()) {
                        const auto& sc = (*scan_chunks)[scan_chunk_idx];
                        if (scan_row_in_chunk >= sc.size()) {
                            ++scan_chunk_idx;
                            scan_row_in_chunk = 0;
                            continue;
                        }
                        const uint64_t take = std::min<uint64_t>(sc.size() - scan_row_in_chunk, n - filled);
                        for (uint64_t col = 0; col < sc.column_count() && col < old_data.column_count(); ++col) {
                            vector::vector_ops::copy(sc.data[col], old_data.data[col], take, scan_row_in_chunk, filled);
                        }
                        vector::vector_ops::copy(sc.row_ids, old_data.row_ids, take, scan_row_in_chunk, filled);
                        filled += take;
                        scan_row_in_chunk += take;
                    }
                    old_data.set_cardinality(filled);
                    idx_old.emplace_back(std::move(old_data));
                }
                idx_new.emplace_back(copy_of(out_chunk));
                for (uint64_t i = 0; i < n; i++) {
                    idx_row_ids.push_back(out_chunk.row_ids.data<int64_t>()[i]);
                }
            }
            ++out_chunk_idx;

            // RETURNING projection is local (no storage read-back): project the updated
            // chunk now, paired with its lockstep FROM (right) chunk.
            if (!returning_.empty()) {
                data_chunk_t* right_batch =
                    right_idx < returning_from_chunks_.size() ? &returning_from_chunks_[right_idx] : nullptr;
                auto proj = evaluate_projection(resource_,
                                                returning_,
                                                &out_chunk,
                                                ctx->parameters,
                                                ctx->session_tz,
                                                right_batch);
                if (proj.has_error()) {
                    set_error(proj.error());
                    mark_executed();
                    co_return;
                }
                projected.emplace_back(std::move(proj.value()));
                ++right_idx;
            }
        }

        // No matched rows (e.g. a streaming UPDATE whose predicate selected
        // nothing): the executor drives await unconditionally for an async-finalize
        // sink, so guard the empty case here — nothing to write. output_ already
        // carries the 0-row affected count, but a RETURNING projection still emits
        // the (empty) projected vector.
        if (update_data.empty()) {
            if (!returning_.empty()) {
                set_output(make_operator_data(resource_, std::move(projected)));
            }
            mark_executed();
            co_return;
        }

        // 1. storage_update (MVCC: delete old + insert new) — one batched send.
        auto [_u, uf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_update,
                                         exec_ctx,
                                         table_oid_,
                                         std::move(update_row_ids),
                                         std::move(update_data));
        // The update reply carries any write_conflict / out_of_memory the table-layer MVCC
        // update surfaced; surface it as a clean error cursor (the executor turns has_error()
        // into an error cursor) so the txn aborts gracefully.
        auto update_result = co_await std::move(uf);
        if (update_result.has_error()) {
            set_error(update_result.error());
            mark_failed();
            co_return;
        }
        auto [range_start, total_count] = update_result.value();

        // 2. WAL physical_update: one record for the whole range.
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
            auto [_df, dff] =
                actor_zeta::send(ctx->disk_address, &services::disk::manager_disk_t::flush, ctx->session, wal_id);
            ctx->add_pending_disk_future(std::move(dff));
        }

        // 3. Mirror to index (old + new data) — one batched send. idx_old/idx_new/
        //    idx_row_ids were assembled per chunk above: the old versions come from
        //    the streaming staging (index_old_chunks_) or the scan cursor fallback,
        //    aligned row-for-row + by row_id with the new rows.
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

        // 4. Record swap-info on context. UPDATE = delete-old + append-new, so both
        // append_row_* and delete_txn_id must be populated.
        ctx->dml_append_row_start = range_start;
        ctx->dml_append_row_count = total_count;
        ctx->dml_delete_txn_id = ctx->txn.transaction_id;
        ctx->dml_table_oid = table_oid_;

        // RETURNING: each updated chunk was projected into `projected` in the gather
        // loop above (paired with its lockstep FROM chunk), already batched into
        // ≤DEFAULT_VECTOR_CAPACITY chunks. Without RETURNING, output_ keeps the
        // updated rows.
        if (!returning_.empty()) {
            set_output(make_operator_data(resource_, std::move(projected)));
        }
        mark_executed();
    }

} // namespace components::operators
