#include "full_scan.hpp"

#include <services/disk/manager_disk.hpp>

namespace components::operators {

    core::result_wrapper_t<std::unique_ptr<table::table_filter_t>>
    transform_predicate(std::pmr::memory_resource* resource,
                        const expressions::compare_expression_ptr& expression,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        const logical_plan::storage_parameters* parameters,
                        core::date::timezone_offset_t session_tz) {
        if (!expression || expression->type() == expressions::compare_type::all_true) {
            return std::unique_ptr<table::table_filter_t>{};
        }
        if (expression->type() == expressions::compare_type::all_false) {
            assert(false && "all_false should be short-circuited in await_async_and_resume");
        }
        switch (expression->type()) {
            case expressions::compare_type::union_and: {
                auto filter = std::make_unique<table::conjunction_and_filter_t>();
                for (const auto& child : expression->children()) {
                    auto child_result =
                        transform_predicate(resource,
                                            reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                            types,
                                            parameters,
                                            session_tz);
                    if (child_result.has_error()) {
                        return child_result;
                    }
                    if (child_result.value()) {
                        filter->child_filters.emplace_back(std::move(child_result.value()));
                    }
                }
                if (filter->child_filters.size() < 2) {
                    return core::error_t{
                        core::error_code_t::physical_plan_error,
                        std::pmr::string{"incomplete AND filter — expression construction error", resource}};
                }
                return std::unique_ptr<table::table_filter_t>(std::move(filter));
            }
            case expressions::compare_type::union_or: {
                auto filter = std::make_unique<table::conjunction_or_filter_t>();
                for (const auto& child : expression->children()) {
                    auto child_result =
                        transform_predicate(resource,
                                            reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                            types,
                                            parameters,
                                            session_tz);
                    if (child_result.has_error()) {
                        return child_result;
                    }
                    if (child_result.value()) {
                        filter->child_filters.emplace_back(std::move(child_result.value()));
                    }
                }
                if (filter->child_filters.size() < 2) {
                    return core::error_t{
                        core::error_code_t::physical_plan_error,
                        std::pmr::string{"incomplete OR filter — expression construction error", resource}};
                }
                return std::unique_ptr<table::table_filter_t>(std::move(filter));
            }
            case expressions::compare_type::union_not: {
                auto filter = std::make_unique<table::conjunction_not_filter_t>();
                filter->child_filters.reserve(expression->children().size());
                for (const auto& child : expression->children()) {
                    auto child_result =
                        transform_predicate(resource,
                                            reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                            types,
                                            parameters,
                                            session_tz);
                    if (child_result.has_error()) {
                        return child_result;
                    }
                    if (child_result.value()) {
                        filter->child_filters.emplace_back(std::move(child_result.value()));
                    }
                }
                if (filter->child_filters.empty()) {
                    return core::error_t{
                        core::error_code_t::physical_plan_error,
                        std::pmr::string{"empty NOT filter — expression construction error", resource}};
                }
                return std::unique_ptr<table::table_filter_t>(std::move(filter));
            }
            case expressions::compare_type::any:
            case expressions::compare_type::all: {
                const auto& path = std::get<expressions::key_t>(expression->left()).path();
                auto param_id = std::get<core::parameter_id_t>(expression->right());
                std::pmr::vector<uint64_t> indices(path.begin(), path.end(), path.get_allocator().resource());
                if (parameters->parameters.find(param_id) == parameters->parameters.end()) {
                    return core::error_t{
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"parameter not found in expression to filter conversion", resource}};
                }
                auto inner_op = expression->inner_op();
                if (inner_op == expressions::compare_type::invalid) {
                    inner_op = expressions::compare_type::eq;
                }
                // For a subscript path (v[i]) the comparison is against the element
                // type, not the ARRAY/LIST column type; type_from_path resolves it.
                const auto& col_type = types::complex_logical_type::type_from_path(types, path);
                const auto& arr = parameters->parameters.at(param_id).children();
                const bool is_any = expression->type() == expressions::compare_type::any;
                auto filter = is_any ? std::unique_ptr<table::conjunction_filter_t>(
                                           std::make_unique<table::conjunction_or_filter_t>())
                                     : std::unique_ptr<table::conjunction_filter_t>(
                                           std::make_unique<table::conjunction_and_filter_t>());
                filter->child_filters.reserve(arr.size());
                for (const auto& val : arr) {
                    auto coerced = val.type() == col_type ? val : val.cast_as(col_type, session_tz);
                    if (coerced.is_null()) {
                        continue;
                    }
                    filter->child_filters.emplace_back(
                        std::make_unique<table::constant_filter_t>(inner_op, coerced, indices));
                }
                return filter;
            }
            case expressions::compare_type::invalid:
                return core::error_t{
                    core::error_code_t::physical_plan_error,
                    std::pmr::string{"unsupported compare_type in expression to filter conversion", resource}};
            case expressions::compare_type::is_null:
            case expressions::compare_type::is_not_null: {
                const auto& path = std::get<expressions::key_t>(expression->left()).path();
                std::pmr::vector<uint64_t> indices(path.begin(), path.end(), path.get_allocator().resource());
                return std::unique_ptr<table::table_filter_t>(
                    std::make_unique<table::is_null_filter_t>(expression->type(), std::move(indices)));
            }
            default: {
                assert(std::holds_alternative<expressions::key_t>(expression->left()));
                const auto& path = std::get<expressions::key_t>(expression->left()).path();
                auto id = std::get<core::parameter_id_t>(expression->right());
                std::pmr::vector<uint64_t> indices(path.begin(), path.end(), path.get_allocator().resource());
                auto it = parameters->parameters.find(id);
                if (it == parameters->parameters.end()) {
                    return core::error_t{
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"parameter not found in expression to filter conversion", resource}};
                }
                // Coerce STRING parameter to ENUM ordinal when the target column is an ENUM:
                // compare semantics see int32 storage on both sides, so the literal must be
                // resolved to its ordinal up-front (else the filter matches 0 rows).
                // For a subscript path (v[i]) this resolves to the element type, so the
                // constant is coerced to what the per-element compare actually sees.
                const auto& col_type = types::complex_logical_type::type_from_path(types, path);
                const auto& param_value = it->second;
                if (col_type.type() == types::logical_type::ENUM &&
                    param_value.type().type() == types::logical_type::STRING_LITERAL) {
                    auto key = param_value.value<std::string_view>();
                    auto coerced = types::logical_value_t::create_enum(resource, col_type, key);
                    if (coerced.type().type() == types::logical_type::NA) {
                        return core::error_t{core::error_code_t::invalid_parameter,
                                             std::pmr::string{std::string{"enum value '"} + std::string{key} +
                                                                  "' not found in ENUM column",
                                                              resource}};
                    }
                    // Storage holds the ordinal as int32 (ENUM physical_type=INT32).
                    // constant_filter_t's compare path doesn't auto-coerce ENUM<->INT32,
                    // so wrap the ordinal as a plain INT32 logical_value_t.
                    types::logical_value_t ordinal_val{resource, coerced.value<int32_t>()};
                    return std::unique_ptr<table::table_filter_t>(
                        std::make_unique<table::constant_filter_t>(expression->type(),
                                                                   std::move(ordinal_val),
                                                                   std::move(indices)));
                }
                if (!param_value.is_null() && param_value.type() != col_type) {
                    auto coerced = param_value.cast_as(col_type, session_tz);
                    if (!coerced.is_null()) {
                        return std::unique_ptr<table::table_filter_t>(
                            std::make_unique<table::constant_filter_t>(expression->type(),
                                                                       std::move(coerced),
                                                                       std::move(indices)));
                    }
                }
                return std::unique_ptr<table::table_filter_t>(
                    std::make_unique<table::constant_filter_t>(expression->type(), it->second, std::move(indices)));
            }
        }
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

    actor_zeta::unique_future<void> full_scan::await_async_and_resume(pipeline::context_t* ctx) {
        if (log_.is_valid()) {
            trace(log(), "full_scan::await_async_and_resume on oid={}", static_cast<unsigned>(table_oid_));
        }

        // Short-circuit: if expression is all_false, return empty result immediately
        if (expression_ && expression_->type() == expressions::compare_type::all_false) {
            output_ = make_operator_data(resource_, std::pmr::vector<types::complex_logical_type>{resource_});
            mark_executed();
            co_return;
        }

        // Short-circuit: null parameter in a scalar comparison — SQL NULL semantics.
        // col OP NULL → always false → return empty immediately.
        // col OP ALL(empty) is vacuously true → skip filter, scan all rows.
        // Excludes is_null/is_not_null which use a dummy null parameter on the right.
        bool null_param_skip_filter = false;
        if (expression_ && !expression_->is_union() && expression_->type() != expressions::compare_type::is_null &&
            expression_->type() != expressions::compare_type::is_not_null &&
            std::holds_alternative<core::parameter_id_t>(expression_->right())) {
            auto pid = std::get<core::parameter_id_t>(expression_->right());
            auto it = ctx->parameters.parameters.find(pid);
            if (it != ctx->parameters.parameters.end() && it->second.is_null()) {
                if (expression_->type() != expressions::compare_type::all) {
                    output_ = make_operator_data(resource_, std::pmr::vector<types::complex_logical_type>{resource_});
                    mark_executed();
                    co_return;
                }
                null_param_skip_filter = true;
            }
        }

        // Get types to build filter
        auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_types,
                                         ctx->session,
                                         table_oid_);
        auto types = co_await std::move(tf);

        // Build filter from expression
        std::unique_ptr<table::table_filter_t> filter;
        if (!null_param_skip_filter) {
            auto filter_result = transform_predicate(resource_, expression_, types, &ctx->parameters, ctx->session_tz);
            if (filter_result.has_error()) {
                set_error(filter_result.error());
                mark_failed();
                co_return;
            }
            filter = std::move(filter_result.value());
        }

        // Scan from storage — batched + projected (PR #477+#483).
        int64_t offset_val = limit_.offset();
        int64_t limit_val = limit_.limit();
        int64_t scan_limit = (limit_val < 0) ? limit_val : limit_val + offset_val;
        auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_scan_batched,
                                         ctx->session,
                                         table_oid_,
                                         std::move(filter),
                                         scan_limit,
                                         projected_cols_,
                                         ctx->txn);
        // The scan reply carries any buffer-pool OOM / data_corruption the table-layer scan
        // surfaced. Surface it as a clean error cursor; the executor turns has_error() into an
        // error cursor.
        auto scan_result = co_await std::move(sf);
        if (scan_result.has_error()) {
            set_error(scan_result.error());
            mark_failed();
            co_return;
        }
        auto batches = std::move(scan_result.value());

        // Skip offset rows across batches.
        if (offset_val > 0) {
            uint64_t remaining = static_cast<uint64_t>(offset_val);
            size_t skip_count = 0;
            for (; skip_count < batches.size() && remaining > 0; ++skip_count) {
                auto sz = batches[skip_count].size();
                if (sz <= remaining) {
                    remaining -= sz;
                    continue;
                }
                batches[skip_count] = batches[skip_count].partial_copy(resource_, remaining, sz - remaining);
                remaining = 0;
                break;
            }
            if (skip_count > 0) {
                batches.erase(batches.begin(), batches.begin() + static_cast<std::ptrdiff_t>(skip_count));
            }
        }

        // Maintain the operator_data_t invariant: at least one (possibly empty)
        // chunk. storage_scan_batched can return an empty vector at SSB-scale when
        // the disk service get_storage(table_oid) hits an oid-resolution race with
        // CSV ingest commit. Without this guard, operator_join.cpp:125 asserts.
        // Schema is taken from the projected scan signature so OUTER joins can
        // still emit NULL-padded rows from the non-empty side.
        if (batches.empty()) {
            std::pmr::vector<types::complex_logical_type> projected_types(resource_);
            if (projected_cols_.empty()) {
                projected_types = types;
            } else {
                projected_types.reserve(projected_cols_.size());
                for (auto idx : projected_cols_) {
                    if (idx < types.size()) {
                        projected_types.push_back(types[idx]);
                    }
                }
            }
            batches.emplace_back(resource_, projected_types, 0);
        }

        output_ = make_operator_data(resource_, std::move(batches));
        mark_executed();
        co_return;
    }

    vector::data_chunk_t
    full_scan::make_drain_chunk(const std::pmr::vector<types::complex_logical_type>& types) {
        std::pmr::vector<types::complex_logical_type> projected_types(resource_);
        if (projected_cols_.empty()) {
            projected_types = types;
        } else {
            projected_types.reserve(projected_cols_.size());
            for (auto idx : projected_cols_) {
                if (idx < types.size()) {
                    projected_types.push_back(types[idx]);
                }
            }
        }
        return vector::data_chunk_t{resource_, projected_types, 0};
    }

    // --- Push-based streaming pipeline source (PER-BATCH FETCH-NEXT, bounded) ---
    // FIRST call: one-time setup (short-circuits, build the filter, the storage_types await for the
    //   empty-guard schema), then OPEN the cursor (storage_fetch_next_batch, cursor_id==0, passing
    //   the filter + offset+limit head cap) and return its first batch.
    // SUBSEQUENT calls: ADVANCE the SAME cursor (cursor_id_!=0, no filter) and return one batch.
    // Each call does at most ONE cross-actor fetch await; the N awaits are sequential across calls
    // in this nested operator coroutine (driven by execute_pipeline), so the single-slot awaited
    // continuation is republished+cleared between awaits — no lost-wakeup (same shape as
    // await_async_and_resume's two sequential awaits). Peak scan memory = one batch (zero pins
    // survive a round-trip; the agent re-seeks a transient scan state from a stored position).
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
            if (expression_ && !expression_->is_union() &&
                expression_->type() != expressions::compare_type::is_null &&
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
                    transform_predicate(resource_, expression_, guard_types_, &ctx->parameters, ctx->session_tz);
                if (filter_result.has_error()) {
                    set_error(filter_result.error());
                    mark_failed();
                    co_return core::result_wrapper_t<vector::data_chunk_t>(filter_result.error());
                }
                filter = std::move(filter_result.value());
            }

            // OPEN the cursor: offset+limit pushed down as the agent's post-filter matched-row cap;
            // the head OFFSET rows are skipped per-batch below (the agent caps but does not skip).
            const int64_t offset_val = limit_.offset();
            const int64_t limit_val = limit_.limit();
            const int64_t scan_limit = (limit_val < 0) ? limit_val : limit_val + offset_val;
            remaining_offset_ = offset_val > 0 ? static_cast<uint64_t>(offset_val) : 0;

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

    // Apply per-batch OFFSET skip and the drained empty-guard to one fetched batch. A re-fetch is
    // needed only while OFFSET still consumes whole batches, so this loops over ADVANCE fetches.
    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    full_scan::emit_or_skip(pipeline::context_t* ctx, std::unique_ptr<vector::data_chunk_t> batch) {
        while (true) {
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

            // Skip OFFSET rows from the head of the stream.
            if (remaining_offset_ > 0) {
                if (sz <= remaining_offset_) {
                    remaining_offset_ -= sz; // whole batch consumed by OFFSET — fetch the next one
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
                    batch = std::move(fetch_result.value().batch);
                    continue;
                }
                auto trimmed = batch->partial_copy(resource_, remaining_offset_, sz - remaining_offset_);
                remaining_offset_ = 0;
                emitted_any_ = true;
                co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(trimmed));
            }

            emitted_any_ = true;
            co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(*batch));
        }
    }

} // namespace components::operators
