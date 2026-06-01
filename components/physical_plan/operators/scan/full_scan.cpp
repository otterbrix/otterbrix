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
                const auto& col_type = types[indices[0]];
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
                const auto& col_type = types[indices[0]];
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

    void full_scan::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (table_oid_ == components::catalog::INVALID_OID)
            return;
        async_wait();
    }

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
        auto batches = co_await std::move(sf);

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

} // namespace components::operators
