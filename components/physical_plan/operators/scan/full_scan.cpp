#include "full_scan.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/expressions/execution_graph_builder.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    core::result_wrapper_t<std::unique_ptr<table::table_filter_t>>
    transform_predicate(std::pmr::memory_resource* resource,
                        const expressions::compare_expression_ptr& expression,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        const logical_plan::storage_parameters* parameters,
                        const components::graph_execution_context& context) {
        if (!expression || expression->type() == expressions::compare_type::all_true) {
            return std::unique_ptr<table::table_filter_t>{};
        }
        // pushing graph that is always false is pessimization, and should be caught earlier
        if (expression->type() == expressions::compare_type::all_false) {
            return core::error_t{core::error_code_t::physical_plan_error,
                                 std::pmr::string{"all_false predicate reached filter construction", resource}};
        }

        const auto condition = expressions::classify_condition(expression);
        std::unique_ptr<execution_graph::execution_graph_t> graph;
        if (condition == expressions::condition_kind::computed) {
            auto built = expressions::build_condition_graph(resource, parameters->parameters, expression.get(), types);
            if (built.has_error()) {
                return built.error();
            }
            graph = std::move(built.value());
        }
        types::parameter_map_t snapshot{resource};
        snapshot.insert(parameters->parameters.begin(), parameters->parameters.end());
        return std::unique_ptr<table::table_filter_t>(
            std::make_unique<table::table_filter_t>(std::move(snapshot), context, std::move(graph), condition));
    }

    full_scan::full_scan(std::pmr::memory_resource* resource,
                         log_t log,
                         components::catalog::oid_t table_oid,
                         const expressions::compare_expression_ptr& expression,
                         logical_plan::limit_t limit,
                         std::vector<size_t> projected_cols)
        : read_only_operator_t(resource, log, operator_type::full_scan)
        , table_oid_(table_oid)
        , expression_(expression)
        , limit_(limit)
        , projected_cols_(std::move(projected_cols)) {}

    vector::data_chunk_t full_scan::make_drain_chunk(const std::pmr::vector<types::complex_logical_type>& types) {
        if (projected_cols_.empty()) {
            return vector::data_chunk_t{resource_, types, 0};
        }
        // Pruned-scan contract (PR #477): pruned scans emit FULL-WIDTH chunks whose
        // non-projected columns are buffer-less placeholders, so column ordinals stay
        // stable plan-wide (expression key paths are never remapped after prune_columns).
        // The schema'd 0-row empty-guard must honor the same shape as real batches —
        // operators above index it by table ordinal. An empty `types` (the 0-column
        // drain sentinel) still degrades to a 0-column chunk here.
        return vector::data_chunk_t{resource_, types, projected_cols_, 0};
    }

    // --- Push-based streaming pipeline source (PER-BATCH FETCH-NEXT, bounded) ---
    // FIRST call: one-time setup (short-circuits, build the filter, the storage_types await for the
    //   empty-guard schema), then OPEN the cursor (storage_fetch_next_batch, cursor_id==0, passing
    //   the filter + offset+limit head cap) and return its first batch.
    // SUBSEQUENT calls: ADVANCE the SAME cursor (cursor_id_!=0, no filter) and return one batch.
    // Each call does at most ONE cross-actor fetch await; the N awaits are sequential across calls
    // in this nested operator coroutine (driven by execute_pipeline), so the single-slot awaited
    // continuation is republished+cleared between awaits — no lost-wakeup. Peak scan memory = one
    // batch (zero pins survive a round-trip; the agent re-seeks a transient scan state from a
    // stored position).
    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    full_scan::source_next(pipeline::context_t* ctx) {
        if (drained_) {
            co_return make_drain_chunk(std::pmr::vector<types::complex_logical_type>{resource_});
        }

        // No-table sentinel (no-FROM SELECT): emit ONE synthetic single-row batch
        // carrying one placeholder column (not the 0-column drain sentinel), then drain.
        // operator_select_t projects its constant/arithmetic columns over this one row to
        // produce the single constants row (the placeholder is ignored), matching the
        // legacy virtual-row path. No disk round-trip.
        if (table_oid_ == components::catalog::INVALID_OID) {
            drained_ = true;
            std::pmr::vector<types::complex_logical_type> types(resource_);
            types.emplace_back(types::logical_type::BOOLEAN);
            vector::data_chunk_t row{resource_, types, 1};
            row.set_cardinality(1);
            co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(row));
        }

        if (!opened_) {
            opened_ = true;

            // Short-circuit: all_false → empty result, immediately drained.
            if (expression_ && expression_->type() == expressions::compare_type::all_false) {
                drained_ = true;
                co_return make_drain_chunk(std::pmr::vector<types::complex_logical_type>{resource_});
            }

            // Short-circuit: null parameter in a scalar comparison — SQL NULL semantics.
            // col OP NULL → always false → empty. col OP ALL(empty) is vacuously true → scan all.
            bool null_param_skip_filter = false;
            if (expression_ && !expression_->is_union() && expression_->type() != expressions::compare_type::is_null &&
                expression_->type() != expressions::compare_type::is_not_null &&
                std::holds_alternative<core::parameter_id_t>(expression_->right())) {
                auto pid = std::get<core::parameter_id_t>(expression_->right());
                auto it = ctx->parameters.parameters.find(pid);
                if (it != ctx->parameters.parameters.end() && it->second.is_null()) {
                    if (expression_->type() != expressions::compare_type::all) {
                        drained_ = true;
                        co_return make_drain_chunk(std::pmr::vector<types::complex_logical_type>{resource_});
                    }
                    null_param_skip_filter = true;
                }
            }

            // Get types to build the filter (await 1). Cached for the no-data empty-guard below.
            auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_types,
                                             ctx->session,
                                             table_oid_);
            guard_types_ = co_await std::move(tf);

            std::unique_ptr<table::table_filter_t> filter;
            if (!null_param_skip_filter) {
                auto filter_result =
                    transform_predicate(resource_, expression_, guard_types_, &ctx->parameters, ctx->execution_context);
                if (filter_result.has_error()) {
                    set_error(filter_result.error());
                    mark_failed();
                    co_return core::result_wrapper_t<vector::data_chunk_t>(filter_result.error());
                }
                filter = std::move(filter_result.value());
            }

            // OPEN the cursor: the read-cap (offset+limit head cap) is pushed down as the agent's
            // post-filter matched-row COUNT cap. SELECT OFFSET is applied by operator_limit above,
            // so every scan receives offset()==0 and head_cap() == limit here.
            const int64_t scan_limit = limit_.head_cap();

            auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_fetch_next_batch,
                                             ctx->session,
                                             table_oid_,
                                             cursor_id_, // 0 == OPEN
                                             std::move(filter),
                                             scan_limit,
                                             projected_cols_,
                                             ctx->txn);
            auto fetch_result = co_await std::move(sf);
            if (fetch_result.has_error()) {
                set_error(fetch_result.error());
                mark_failed();
                co_return fetch_result.convert_error<vector::data_chunk_t>();
            }
            auto reply = std::move(fetch_result.value());
            cursor_id_ = reply.cursor_id;
            co_return co_await emit_or_skip(ctx, std::move(reply.batch));
        }

        // ADVANCE: read one more batch from the open cursor (filter dropped — the agent owns it).
        auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_fetch_next_batch,
                                         ctx->session,
                                         table_oid_,
                                         cursor_id_,
                                         std::unique_ptr<table::table_filter_t>(nullptr),
                                         int64_t{-1},
                                         projected_cols_,
                                         ctx->txn);
        auto fetch_result = co_await std::move(sf);
        if (fetch_result.has_error()) {
            set_error(fetch_result.error());
            mark_failed();
            co_return fetch_result.convert_error<vector::data_chunk_t>();
        }
        auto reply = std::move(fetch_result.value());
        co_return co_await emit_or_skip(ctx, std::move(reply.batch));
    }

    // Apply the drained empty-guard to one fetched batch. (OFFSET is applied by operator_limit
    // above; every scan receives offset()==0, so there is no per-batch skip / re-fetch.)
    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    full_scan::emit_or_skip(pipeline::context_t* /*ctx*/, std::unique_ptr<vector::data_chunk_t> batch) {
        const uint64_t sz = batch ? batch->size() : 0;

        // Drained: the agent replied a cardinality-0 batch (and erased its cursor).
        if (sz == 0) {
            drained_ = true;
            // Emit ONE schema'd 0-row guard the first time the source produces nothing, so a
            // scalar aggregate emits COUNT=0 and an OUTER join NULL-pads.
            if (!emitted_any_) {
                emitted_any_ = true;
                co_return make_drain_chunk(guard_types_);
            }
            co_return make_drain_chunk(std::pmr::vector<types::complex_logical_type>{resource_});
        }

        emitted_any_ = true;
        co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(*batch));
    }

} // namespace components::operators