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
        delete_wal_apply_gate_t* g_delete_wal_apply_gate = nullptr;
    } // namespace
    uint64_t delete_scanned_columns() noexcept { return g_delete_scanned_columns.load(std::memory_order_relaxed); }
    void dev_set_delete_wal_apply_gate(delete_wal_apply_gate_t* gate) { g_delete_wal_apply_gate = gate; }
    delete_wal_apply_gate_t* dev_delete_wal_apply_gate() { return g_delete_wal_apply_gate; }
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

        vector::vector_t batch_ids(resource_, types::logical_type::BIGINT, chunk.size());
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

        // Staged chunk row k pairs with index_old_row_ids_[k]; manager_index_t::delete_rows relies on that alignment.
        {
            data_chunk_t old_matched(resource_, types, index);
            chunk.copy(old_matched, matched_indexing, index);
            old_matched.set_cardinality(index);
            index_old_chunks_.emplace_back(std::move(old_matched));
            for (size_t i = 0; i < index; i++) {
                index_old_row_ids_.push_back(batch_ids.data<int64_t>()[i]);
            }
        }

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
        // RIGHT stays per-chunk: merging it could overflow a single chunk past DEFAULT_VECTOR_CAPACITY.
        using components::vector::data_chunk_t;
        ensure_simple_init_();
        if (chunk_left.size() == 0) {
            return core::error_t::no_error();
        }
        const bool collect_returning = !returning_.empty();
        auto types_left = chunk_left.types();
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

        vector::vector_t batch_ids(resource_, types::logical_type::BIGINT, chunk_left.size());
        vector::indexing_vector_t matched_indexing(resource_);
        matched_indexing.reset(chunk_left.size());
        // Gathered per-row, not via an indexing gather — a batch can match more right rows than the chunk holds.
        data_chunk_t affected_right(resource_, types_right, chunk_left.size());

        size_t index = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            // Stops once matched_total_ + this batch's index reaches affected_bound_; -1 = unbounded.
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
                    // Keys on the absolute row id, not the loop index — they diverge with gaps or row groups.
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
                    row_matched = true;
                    break;
                }
                if (row_matched) {
                    break;
                }
            }
        }
        matched_total_ += index;
        if (index == 0) {
            return core::error_t::no_error();
        }
        affected_right.set_cardinality(index);

        for (size_t i = 0; i < index; i++) {
            modified_->append(static_cast<size_t>(batch_ids.data<int64_t>()[i]));
        }

        {
            data_chunk_t old_matched(resource_, types_left, index);
            chunk_left.copy(old_matched, matched_indexing, index);
            old_matched.set_cardinality(index);
            index_old_chunks_.emplace_back(std::move(old_matched));
            for (size_t i = 0; i < index; i++) {
                index_old_row_ids_.push_back(batch_ids.data<int64_t>()[i]);
            }
        }

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
#ifdef DEV_MODE
        for (const auto& column : input.data) {
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

        // Called once per buffer-full during the pump and once at finalize (dml_flush_is_final); only the
        // final call emits output and mark_executed. threshold==0 collapses to a single final call.
        const bool is_final = ctx->dml_flush_is_final;

        // Bypasses the predicate-scan/storage/index path; pg_catalog_delete_tables lets
        // operator_commit_transaction revert/publish the MVCC tombstone. Buffers nothing, so never mid-flushed.
        if (oid_col_idx_ >= 0) {
            components::execution_context_t exec_ctx{ctx->session,
                                                     ctx->txn,
                                                     ctx->execution_context.timezone_offset,
                                                     table_oid_};
            auto [_c, cf] = actor_zeta::otterbrix::send(ctx->disk_address,
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

        // DELETE writes its own WAL, unlike INSERT where the disk agent owns it.
        if (modified_ && modified_->size() > 0) {
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

                // Storage first, then WAL (same order operator_update uses): a mutation's WAL id must never
                // be allocated before the mutation is applied, or a checkpoint can advance the durable floor
                // past a delete never folded into the .otbx (test_delete_floor_resurrection).
                vector_t row_ids(res, types::logical_type::BIGINT, modified_size);
                for (size_t i = 0; i < modified_size; i++) {
                    row_ids.data<int64_t>()[i] = static_cast<int64_t>(ids[i]);
                }
                auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::storage_delete_rows,
                                                            exec_ctx,
                                                            table_oid_,
                                                            std::move(row_ids),
                                                            static_cast<uint64_t>(modified_size));
                auto deleted_r = co_await std::move(df);
                if (deleted_r.has_error()) {
                    co_return dml_detail::flush_outcome_t{deleted_r.error(), false, 0, 0};
                }

                // The delete marker recorded before this error check lets the abort tail un-stamp the rows
                // on a refused WAL record, so no committed delete lacks its journal record.
                if (ctx->wal_address != actor_zeta::address_t::empty_address()) {
                    std::pmr::vector<int64_t> wal_row_ids(res);
                    wal_row_ids.reserve(modified_size);
                    for (size_t i = 0; i < modified_size; i++) {
                        wal_row_ids.push_back(static_cast<int64_t>(ids[i]));
                    }
                    auto count = static_cast<uint64_t>(wal_row_ids.size());
                    constexpr auto db_oid = components::catalog::well_known_oid::main_database;
                    auto [_w, wf] =
                        actor_zeta::otterbrix::send(ctx->wal_address,
                                                    &services::wal::manager_wal_replicate_t::write_physical_delete,
                                                    ctx->session,
                                                    table_oid_,
                                                    std::move(wal_row_ids),
                                                    count,
                                                    ctx->txn.transaction_id,
                                                    db_oid);
                    auto wal_result = co_await std::move(wf);
                    if (wal_result.has_error()) {
                        co_return dml_detail::flush_outcome_t{wal_result.error(), false, 0, 0};
                    }
                }

#ifdef DEV_MODE
                while (auto* gate = dev_delete_wal_apply_gate()) {
                    if (!gate->hold(table_oid_)) {
                        break;
                    }
                    auto [_g, gf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                               &services::disk::manager_disk_t::storage_total_rows,
                                                               ctx->session,
                                                               table_oid_);
                    auto ping = co_await std::move(gf);
                    if (ping.has_error()) {
                        break;
                    }
                }
#endif

                // Both paths stage the matched old rows into index_old_chunks_/index_old_row_ids_, so
                // delete_rows always gets the matched rows paired with their own ids, never the first-N scan rows.
                if (mirror_index) {
                    chunks_vector_t index_old_copy(res);
                    index_old_copy.reserve(index_old_chunks_.size());
                    for (const auto& c : index_old_chunks_) {
                        data_chunk_t owned(res, c.types(), c.size() == 0 ? 1 : c.size());
                        if (c.size() > 0) {
                            c.copy(owned, 0);
                        }
                        index_old_copy.emplace_back(std::move(owned));
                    }
                    auto [_ix, ixf] = actor_zeta::otterbrix::send(ctx->index_address,
                                                                  &services::index::manager_index_t::delete_rows,
                                                                  exec_ctx,
                                                                  table_oid_,
                                                                  std::move(index_old_copy),
                                                                  std::move(index_old_row_ids_));
                    auto index_error = co_await std::move(ixf);
                    if (index_error.contains_error()) {
                        co_return dml_detail::flush_outcome_t{std::move(index_error), false, 0, 0};
                    }
                }

                affected_rows_ += static_cast<uint64_t>(modified_size);
                co_return dml_detail::flush_outcome_t{core::error_t::no_error(), false, 0, 0};
            };

            auto outcome = co_await op(resource_);
            // fk_cascade needs the OLD rows (index_old_chunks_) to find referencing children; record_flush
            // accumulates them into constraint_input_ only when dml_has_parent_constraint, to bound memory.
            auto err = dml_detail::record_flush(ctx,
                                                resource_,
                                                table_oid_,
                                                outcome,
                                                ctx->dml_has_parent_constraint,
                                                constraint_input_,
                                                index_old_chunks_);
            // Recorded once per txn, before the flush-error check, so a late failure still leaves the marker
            // for the abort tail to un-stamp; COMMIT/ABORT key the MVCC swap/revert on the txn id, not the flush.
            if (!delete_marker_recorded_) {
                ctx->dml_deletes.push_back(components::table::dml_delete_range_t{table_oid_, ctx->txn.transaction_id});
                delete_marker_recorded_ = true;
            }

            if (err.contains_error()) {
                set_error(err);
                mark_failed();
                co_return;
            }

            modified_ = operators::make_operator_write_data(resource_);
            index_old_chunks_.clear();
            index_old_row_ids_.clear();
        }

        if (!is_final) {
            co_return;
        }

        // A 0-affected DELETE without RETURNING leaves output_ null, emitting no result rows.
        if (!returning_.empty()) {
            if (!returning_staged_.empty()) {
                set_output(make_operator_data(resource_, std::move(returning_staged_)));
            }
        } else if (affected_rows_ > 0) {
            auto [_t, tf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::storage_types,
                                                        ctx->session,
                                                        table_oid_);
            auto types_r = co_await std::move(tf);
            if (types_r.has_error()) {
                set_error(types_r.error());
                mark_failed();
                co_return;
            }
            auto types = std::move(types_r.value());
            set_output(make_operator_data(resource_,
                                          dml_detail::make_affected_count_chunks(resource_, affected_rows_, types)));
        }
        mark_executed();
    }

} // namespace components::operators
