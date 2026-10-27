#include "full_scan.hpp"
#include "guard_chunk.hpp"

#include <components/expressions/compare_expression.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    core::result_wrapper_t<std::unique_ptr<table::pushed_filter_t>>
    transform_predicate(std::pmr::memory_resource* resource,
                        std::pmr::memory_resource* target,
                        const expressions::compare_expression_ptr& expression,
                        const logical_plan::storage_parameters* parameters,
                        const components::graph_execution_context& context) {
        if (!expression || expression->type() == expressions::compare_type::all_true) {
            return std::unique_ptr<table::pushed_filter_t>{};
        }
        // pushing graph that never selects a row is pessimization, and should be caught earlier
        if (expression->type() == expressions::compare_type::all_false ||
            expression->type() == expressions::compare_type::all_unknown) {
            return core::error_t{
                core::error_code_t::physical_plan_error,
                std::pmr::string{"a predicate that selects nothing reached filter construction", resource}};
        }
        return table::make_pushed_filter(target, expression, *parameters, context);
    }

    full_scan::full_scan(std::pmr::memory_resource* resource,
                         log_t log,
                         components::catalog::oid_t table_oid,
                         table::storage_address_t address,
                         std::pmr::string name,
                         const expressions::compare_expression_ptr& expression,
                         logical_plan::row_cap_t cap,
                         std::vector<size_t> projected_cols)
        : read_only_operator_t(resource, log, operator_type::full_scan)
        , table_oid_(table_oid)
        , address_(std::move(address))
        , name_(std::move(name), resource)
        , expression_(expression)
        , cap_(cap)
        , projected_cols_(std::move(projected_cols)) {}

    std::pmr::string full_scan::explain_label_impl() const {
        // explain_plan names the relation after the line, as for every scan.
        return foreign_label(resource_, address_, "Foreign Scan", {});
    }

    actor_zeta::unique_future<core::error_t> full_scan::open_impl(pipeline::context_t* ctx) {
        opened_ = true;
        // No-table sentinel (no-FROM SELECT): source_next makes its one row, no storage is asked.
        if (table_oid_ == components::catalog::INVALID_OID) {
            co_return core::error_t::no_error();
        }
        const bool selects_nothing = expression_ && (expression_->type() == expressions::compare_type::all_false ||
                                                     expression_->type() == expressions::compare_type::all_unknown);
        // col OP NULL is never true, and so is col OP ANY|ALL (NULL).
        const bool null_operand = expression_ && !expression_->is_union() &&
                                  expression_->type() != expressions::compare_type::is_null &&
                                  expression_->type() != expressions::compare_type::is_not_null &&
                                  std::holds_alternative<core::parameter_id_t>(expression_->right()) &&
                                  ctx->parameters.is_null(components::expressions::as_parameter(expression_->right()));

        if (selects_nothing || null_operand) {
            drained_ = true;
            co_return core::error_t::no_error();
        }

        // The read-cap (offset+limit) is pushed down as the storage's post-filter matched-row count cap. SELECT
        // OFFSET is applied by operator_limit above.
        const int64_t scan_limit = cap_.to_disk();
        actor_zeta::unique_future<core::result_wrapper_t<services::disk::fetch_batch_t>> opening;
        if (expression_) {
            auto filter = transform_predicate(resource_,
                                              ctx->disk_address.resource(),
                                              expression_,
                                              &ctx->parameters,
                                              ctx->execution_context);
            if (filter.has_error()) {
                set_error(filter.error());
                mark_failed();
                co_return filter.error();
            }
            auto [_s, sf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::storage_open_filtered_scan,
                                                        ctx->session,
                                                        table_oid_,
                                                        std::move(filter.value()),
                                                        scan_limit,
                                                        projected_cols_,
                                                        ctx->txn);
            opening = std::move(sf);
        } else {
            ctx->join_transaction(address_, table_oid_, false);
            auto [_s, sf] = actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_fetch_next_batch>(
                address_,
                ctx->session,
                table_oid_,
                uint64_t{0},
                scan_limit,
                projected_cols_,
                ctx->txn);
            opening = std::move(sf);
        }
        auto reply = co_await std::move(opening);
        if (reply.has_error()) {
            set_error(reply.error());
            mark_failed();
            co_return reply.error();
        }
        cursor_id_ = reply.value().cursor_id;
        first_batch_ = std::move(reply.value().batch);
        if (cursor_id_ == 0 && first_batch_ && first_batch_->size() != 0) {
            std::pmr::string what{"the storage of table \"", resource_};
            what += name_;
            what += "\" answered a page without a cursor";
            core::error_t refused{core::error_code_t::invalid_parameter, std::move(what)};
            set_error(refused);
            mark_failed();
            co_return refused;
        }
        co_return core::error_t::no_error();
    }

    // Each call does at most one cross-actor fetch await; the N awaits are sequential across calls in
    // this nested operator coroutine (driven by execute_pipeline), so the single-slot awaited
    // continuation is republished+cleared between awaits — no lost-wakeup. Peak scan memory is one
    // batch: no pins survive a round-trip, and the agent re-seeks a transient scan state from a stored
    // position.
    actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
    full_scan::source_next(pipeline::context_t* ctx) {
        if (table_oid_ == components::catalog::INVALID_OID) {
            if (drained_) {
                co_return std::nullopt;
            }
            // No-table sentinel (no-FROM SELECT): one synthetic single-row batch with one placeholder column;
            // operator_select_t projects its constant/arithmetic columns over this row.
            drained_ = true;
            std::pmr::vector<types::complex_logical_type> types(resource_);
            types.emplace_back(types::logical_type::BOOLEAN);
            vector::data_chunk_t row{resource_, types, 1};
            row.set_cardinality(1);
            co_return std::move(row);
        }
        if (drained_) {
            if (!emitted_any_) {
                emitted_any_ = true;
                co_return make_guard_chunk(resource_, page_types_, {});
            }
            co_return std::nullopt;
        }
        if (first_batch_) {
            co_return emit_or_skip(std::move(first_batch_));
        }

#ifdef DEV_MODE
        // Between-batches pause gate (see services::disk::scan_advance_gate_t). Polling by one
        // cross-actor round-trip per ask keeps every mailbox free while the gate holds: the
        // await parks this nested coroutine, it never blocks an actor thread.
        while (true) {
            auto* gate = services::disk::dev_scan_advance_gate();
            if (gate == nullptr || !gate->hold(table_oid_, cursor_id_)) {
                break;
            }
            auto [_g, gf] =
                actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_total_rows>(address_,
                                                                                                   ctx->session,
                                                                                                   table_oid_);
            auto ping = co_await std::move(gf);
            if (ping.has_error()) {
                // The poll target is gone — the world is tearing down; holding forever
                // would hang it. Let the fetch below answer for the cursor.
                break;
            }
        }
#endif
        // ADVANCE: read one more batch from the open cursor (the disk keeps its filter with the cursor).
        auto [_s, sf] =
            actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_fetch_next_batch>(address_,
                                                                                                     ctx->session,
                                                                                                     table_oid_,
                                                                                                     cursor_id_,
                                                                                                     int64_t{-1},
                                                                                                     projected_cols_,
                                                                                                     ctx->txn);
        auto fetch_result = co_await std::move(sf);
        if (fetch_result.has_error()) {
            set_error(fetch_result.error());
            mark_failed();
            co_return fetch_result.convert_error<std::optional<vector::data_chunk_t>>();
        }
        co_return emit_or_skip(std::move(fetch_result.value().batch));
    }

    actor_zeta::unique_future<void> full_scan::release_cursor(pipeline::context_t* ctx) {
        // Nothing to release: never opened, already drained (the agent erased its own entry),
        // or already released.
        if (cursor_id_ == 0 || drained_) {
            co_return;
        }
        const uint64_t id = cursor_id_;
        // Clear first so a re-entry cannot double-send, and so the operator cannot be left
        // pointing at a cursor the agent has dropped.
        cursor_id_ = 0;
        drained_ = true;
        auto [_s, cf] =
            actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_close_cursor>(address_,
                                                                                                 ctx->session,
                                                                                                 table_oid_,
                                                                                                 id);
        co_await std::move(cf);
        co_return;
    }

    // Applies the drained empty-guard to one fetched batch; per-batch OFFSET skip never applies here
    // (operator_limit already owns it, see source_next).
    core::result_wrapper_t<std::optional<vector::data_chunk_t>>
    full_scan::emit_or_skip(std::unique_ptr<vector::data_chunk_t> batch) {
        const uint64_t sz = batch ? batch->size() : 0;

        // Drained: the agent replied a cardinality-0 batch (and erased its cursor).
        if (sz == 0) {
            drained_ = true;
            // Emit one schema'd 0-row guard the first time the source produces nothing, so a
            // scalar aggregate emits COUNT=0 and an OUTER join NULL-pads.
            if (!emitted_any_) {
                emitted_any_ = true;
                return make_guard_chunk(resource_, page_types_, {});
            }
            return std::optional<vector::data_chunk_t>{};
        }

        if (auto refused = check_scan_page(resource_, name_, page_types_, *batch); refused.contains_error()) {
            return refused;
        }
        emitted_any_ = true;
        return std::optional<vector::data_chunk_t>{vector::adopted(resource_, std::move(*batch))};
    }

} // namespace components::operators
