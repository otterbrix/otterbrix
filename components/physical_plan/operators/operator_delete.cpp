#include "operator_delete.hpp"

#include "dml_util.hpp"
#include "join_utils.hpp"
#include "scan/guard_chunk.hpp"
#include <atomic>
#include <components/vector/vector_operations.hpp>
#include <optional>

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
                                     table::storage_address_t address,
                                     std::pmr::string name,
                                     std::pmr::vector<projected_column_t> returning,
                                     expressions::expression_ptr expr,
                                     logical_plan::row_cap_t affected_bound)
        : read_write_operator_t(resource, log, operator_type::remove)
        , table_oid_(table_oid)
        , address_(std::move(address))
        , name_(std::move(name), resource)
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
        , name_(resource)
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
        auto types = chunk.types();

        // all_true/all_false can skip graph
        if (condition_ == expressions::condition_kind::never) {
            return core::error_t::no_error();
        }
        std::optional<vector::data_chunk_t> produced;
        if (condition_ == expressions::condition_kind::computed) {
            if (!graph_) {
                auto built = expressions::build_condition_graph(resource_,
                                                                pipeline_context->parameters,
                                                                expression_.get(),
                                                                types);
                if (built.has_error()) {
                    return built.error();
                }
                graph_ = std::move(built.value());
            }
            auto decided = expressions::run_graph(graph_.get(),
                                                  pipeline_context->parameters,
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
            batch_ids.data<int64_t>()[index] = dml_detail::row_id_of(chunk, i);
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
        index_old_chunks_.emplace_back(dml_detail::stage_matched(resource_, chunk, matched_indexing, index));
        for (size_t i = 0; i < index; i++) {
            index_old_row_ids_.push_back(batch_ids.data<int64_t>()[i]);
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
                                                                pipeline_context->parameters,
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
        // Where output row k's right-side partner lives: (right chunk, row).
        std::pmr::vector<std::pair<uint32_t, uint32_t>> partners(resource_);
        if (collect_returning) {
            partners.reserve(chunk_left.size());
        }
        size_t index = 0;
        for (size_t i = 0; i < chunk_left.size(); i++) {
            // Stops once matched_total_ + this batch's index reaches affected_bound_.
            if (!affected_bound_.admits(matched_total_ + index)) {
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
                                                          pipeline_context->parameters,
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
                    batch_ids.data<int64_t>()[index] = dml_detail::row_id_of(chunk_left, i);
                    matched_indexing.set_index(index, i);
                    if (collect_returning) {
                        partners.emplace_back(static_cast<uint32_t>(ci), static_cast<uint32_t>(j));
                    }
                    index++;
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
        for (size_t i = 0; i < index; i++) {
            modified_->append(static_cast<size_t>(batch_ids.data<int64_t>()[i]));
        }

        if (!collect_returning) {
            index_old_chunks_.emplace_back(dml_detail::stage_matched(resource_, chunk_left, matched_indexing, index));
            for (size_t i = 0; i < index; i++) {
                index_old_row_ids_.push_back(batch_ids.data<int64_t>()[i]);
            }
            return core::error_t::no_error();
        }
        // RETURNING reads each deleted row with the USING row it matched: one staged pair per right chunk the
        // partners come from, in order of first appearance.
        std::pmr::vector<uint32_t> chunk_order(resource_);
        std::pmr::vector<std::pmr::vector<uint32_t>> rows_of(right_chunks.size(),
                                                             std::pmr::vector<uint32_t>(resource_),
                                                             resource_);
        for (size_t k = 0; k < partners.size(); ++k) {
            auto& rows = rows_of[partners[k].first];
            if (rows.empty()) {
                chunk_order.push_back(partners[k].first);
            }
            rows.push_back(static_cast<uint32_t>(k));
        }
        for (uint32_t ci : chunk_order) {
            const auto& rows = rows_of[ci];
            vector::indexing_vector_t left_selection(resource_, rows.size());
            vector::indexing_vector_t right_selection(resource_, rows.size());
            for (size_t n = 0; n < rows.size(); ++n) {
                left_selection.set_index(n, matched_indexing.get_index(rows[n]));
                right_selection.set_index(n, partners[rows[n]].second);
                index_old_row_ids_.push_back(batch_ids.data<int64_t>()[rows[n]]);
            }
            index_old_chunks_.emplace_back(
                dml_detail::stage_matched(resource_, chunk_left, left_selection, rows.size()));
            returning_from_chunks_.emplace_back(
                dml_detail::stage_matched(resource_, right_chunks[ci], right_selection, rows.size()));
        }
        return core::error_t::no_error();
    }

    core::error_t
    operator_delete::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& /*out*/) {
#ifdef DEV_MODE
        g_delete_scanned_columns.fetch_add(input.column_count(), std::memory_order_relaxed);
#endif
        if (right_ && right_->output()) {
            return consume_join_batch_(ctx, input, right_->output()->chunks());
        }
        return consume_batch_(ctx, input);
    }

    std::pmr::string operator_delete::explain_label_impl() const {
        return foreign_label(resource_, address_, "Foreign Delete", name_);
    }

    actor_zeta::unique_future<core::error_t> operator_delete::open_impl(pipeline::context_t* ctx) {
        if (oid_col_idx_ >= 0) {
            co_return core::error_t::no_error();
        }
        if (auto refused = dml_detail::check_constant_returning(resource_, returning_, *ctx);
            refused.contains_error()) {
            set_error(refused);
            mark_failed();
            co_return refused;
        }
        co_return core::error_t::no_error();
    }

    actor_zeta::unique_future<void> operator_delete::await_async_and_resume(pipeline::context_t* ctx) {
        using components::vector::data_chunk_t;

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

        if (modified_ && modified_->size() > 0) {
            components::execution_context_t exec_ctx{ctx->session,
                                                     ctx->txn,
                                                     ctx->execution_context.timezone_offset,
                                                     table_oid_};
            // DELETE writes its own WAL, unlike INSERT where the disk agent owns it.
            const bool mirror_index =
                table_has_indexes_ && ctx->index_address != actor_zeta::address_t::empty_address();
            const bool returning = !returning_.empty();
            auto rounds = dml_detail::split_parallel_rounds(resource_,
                                                            {&index_old_chunks_, &returning_from_chunks_},
                                                            address_.max_rows_per_write());
            std::size_t next_row_id = 0;
            for (auto& round : rounds) {
                auto& old_rows = round[0];
                auto& from_rows = round[1];
                uint64_t round_rows = 0;
                for (const auto& chunk : old_rows) {
                    round_rows += chunk.size();
                }
                // Storage first, then WAL (same order operator_update uses): a mutation's WAL id must never
                // be allocated before the mutation is applied, or a checkpoint can advance the durable floor
                // past a delete never folded into the .otbx (test_delete_floor_resurrection).
                std::pmr::vector<int64_t> round_row_ids(resource_);
                round_row_ids.reserve(round_rows);
                auto row_ids = table::make_row_id_batch(resource_, round_rows, [&] {
                    const auto id = index_old_row_ids_[next_row_id++];
                    round_row_ids.push_back(id);
                    return id;
                });
                ctx->join_transaction(address_, table_oid_, true);
                auto [_d, df] = actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_delete_rows>(
                    address_,
                    exec_ctx,
                    table_oid_,
                    std::move(row_ids),
                    returning);
                auto deleted_r = co_await std::move(df);

                dml_detail::flush_outcome_t outcome{};
                bool stamped_deletes = false;
                if (deleted_r.has_error()) {
                    outcome.error = deleted_r.error();
                } else {
                    auto& receipt = deleted_r.value();
                    stamped_deletes = receipt.awaits_publish;
                    // The delete marker recorded below lets the abort tail un-stamp the rows on a refused WAL
                    // record, so no committed delete lacks its journal record.
                    if (receipt.awaits_publish && ctx->wal_address != actor_zeta::address_t::empty_address()) {
                        auto count = static_cast<uint64_t>(round_row_ids.size());
                        constexpr auto db_oid = components::catalog::well_known_oid::main_database;
                        auto [_w, wf] =
                            actor_zeta::otterbrix::send(ctx->wal_address,
                                                        &services::wal::manager_wal_replicate_t::write_physical_delete,
                                                        ctx->session,
                                                        table_oid_,
                                                        round_row_ids,
                                                        count,
                                                        ctx->txn.transaction_id,
                                                        ctx->txn.sub_id,
                                                        db_oid);
                        auto wal_result = co_await std::move(wf);
                        if (wal_result.has_error()) {
                            outcome.error = wal_result.error();
                        }
                    }

#ifdef DEV_MODE
                    while (auto* gate = dev_delete_wal_apply_gate()) {
                        if (!gate->hold(table_oid_)) {
                            break;
                        }
                        auto [_g, gf] =
                            actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_total_rows>(
                                address_,
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
                    if (!outcome.error.contains_error() && mirror_index && !old_rows.empty()) {
                        chunks_vector_t index_old_copy(resource_);
                        index_old_copy.reserve(old_rows.size());
                        for (const auto& c : old_rows) {
                            data_chunk_t owned(resource_, c.types(), c.size() == 0 ? 1 : c.size());
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
                                                                      round_row_ids);
                        if (auto index_error = co_await std::move(ixf); index_error.contains_error()) {
                            outcome.error = std::move(index_error);
                        }
                    }

                    if (!outcome.error.contains_error()) {
                        written_ += receipt.count;
                        uint64_t offset = 0;
                        for (auto& returned : receipt.returned) {
                            if (returned.size() == 0) {
                                continue;
                            }
                            if (auto refused = check_chunk_width(resource_, returned); refused.contains_error()) {
                                outcome.error = std::move(refused);
                                break;
                            }
                            auto rows = dml_detail::as_scanned(resource_,
                                                               vector::adopted(resource_, std::move(returned)),
                                                               scan_columns_);
                            std::optional<data_chunk_t> partners;
                            if (!from_rows.empty()) {
                                partners.emplace(dml_detail::rows_at(resource_, from_rows, offset, rows.size()));
                            }
                            offset += rows.size();
                            auto proj = evaluate_projection(resource_,
                                                            returning_,
                                                            &rows,
                                                            ctx->parameters,
                                                            ctx->execution_context,
                                                            &returning_graph_,
                                                            partners.has_value() ? &*partners : nullptr);
                            if (proj.has_error()) {
                                outcome.error = proj.error();
                                break;
                            }
                            returning_staged_.emplace_back(std::move(proj.value()));
                        }
                    }
                }

                // fk_cascade needs the OLD rows (index_old_chunks_) to find referencing children; record_flush
                // accumulates them into constraint_input_ only when dml_has_parent_constraint, to bound memory.
                auto err = dml_detail::record_flush(ctx,
                                                    resource_,
                                                    table_oid_,
                                                    outcome,
                                                    ctx->dml_has_parent_constraint,
                                                    constraint_input_,
                                                    old_rows);
                // Recorded once per txn, before the flush-error check, so a late failure still leaves the marker
                // for the abort tail to un-stamp; COMMIT/ABORT key the MVCC swap/revert on the txn id, not the flush.
                if ((stamped_deletes || (deleted_r.has_error() && ctx->is_disk(address_))) &&
                    !delete_marker_recorded_) {
                    ctx->dml_deletes.push_back(
                        components::table::dml_delete_range_t{table_oid_, ctx->txn.transaction_id});
                    delete_marker_recorded_ = true;
                }
                if (err.contains_error()) {
                    modified_ = operators::make_operator_write_data(resource_);
                    index_old_chunks_.clear();
                    index_old_row_ids_.clear();
                    returning_from_chunks_.clear();
                    set_error(err);
                    mark_failed();
                    co_return;
                }
            }

            modified_ = operators::make_operator_write_data(resource_);
            index_old_chunks_.clear();
            index_old_row_ids_.clear();
            returning_from_chunks_.clear();
        }

        if (!is_final) {
            co_return;
        }

        // Without RETURNING output_ stays null: the count is written().
        if (!returning_.empty()) {
            if (returning_staged_.empty()) {
                // Nothing matched, but we still have to return correct columns
                auto empty = dml_detail::as_scanned(resource_, data_chunk_t(resource_, table_types_, 0), scan_columns_);
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
        }
        mark_executed();
    }

} // namespace components::operators
