#include "full_scan.hpp"

#include <services/disk/manager_disk.hpp>

namespace components::operators {

    std::unique_ptr<table::table_filter_t>
    transform_predicate(const expressions::compare_expression_ptr& expression,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        const logical_plan::storage_parameters* parameters) {
        if (!expression || expression->type() == expressions::compare_type::all_true) {
            return nullptr;
        }
        if (expression->type() == expressions::compare_type::all_false) {
            assert(false && "all_false should be short-circuited in await_async_and_resume");
        }
        switch (expression->type()) {
            case expressions::compare_type::union_and: {
                auto filter = std::make_unique<table::conjunction_and_filter_t>();
                for (const auto& child : expression->children()) {
                    auto child_filter =
                        transform_predicate(reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                            types,
                                            parameters);
                    if (child_filter) {
                        filter->child_filters.emplace_back(std::move(child_filter));
                    }
                }
                if (filter->child_filters.size() < 2) {
                    throw std::runtime_error("incomplete AND filter — expression construction error");
                }
                return filter;
            }
            case expressions::compare_type::union_or: {
                auto filter = std::make_unique<table::conjunction_or_filter_t>();
                for (const auto& child : expression->children()) {
                    auto child_filter =
                        transform_predicate(reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                            types,
                                            parameters);
                    if (child_filter) {
                        filter->child_filters.emplace_back(std::move(child_filter));
                    }
                }
                if (filter->child_filters.size() < 2) {
                    throw std::runtime_error("incomplete OR filter — expression construction error");
                }
                return filter;
            }
            case expressions::compare_type::union_not: {
                auto filter = std::make_unique<table::conjunction_not_filter_t>();
                filter->child_filters.reserve(expression->children().size());
                for (const auto& child : expression->children()) {
                    auto child_filter =
                        transform_predicate(reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                            types,
                                            parameters);
                    if (child_filter) {
                        filter->child_filters.emplace_back(std::move(child_filter));
                    }
                }
                if (filter->child_filters.empty()) {
                    throw std::runtime_error("empty NOT filter — expression construction error");
                }
                return filter;
            }
            case expressions::compare_type::invalid:
                throw std::runtime_error("unsupported compare_type in expression to filter conversion");
            case expressions::compare_type::is_null:
            case expressions::compare_type::is_not_null: {
                const auto& path = std::get<expressions::key_t>(expression->left()).path();
                std::pmr::vector<uint64_t> indices(path.begin(), path.end(), path.get_allocator().resource());
                return std::make_unique<table::is_null_filter_t>(expression->type(), std::move(indices));
            }
            default: {
                const auto& path = std::get<expressions::key_t>(expression->left()).path();
                auto id = std::get<core::parameter_id_t>(expression->right());
                std::pmr::vector<uint64_t> indices(path.begin(), path.end(), path.get_allocator().resource());
                auto it = parameters->parameters.find(id);
                if (it == parameters->parameters.end()) {
                    throw std::runtime_error("parameter not found in expression to filter conversion");
                }
                return std::make_unique<table::constant_filter_t>(expression->type(), it->second, std::move(indices));
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

        // Get types to build filter
        auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_types,
                                         ctx->session,
                                         table_oid_);
        auto types = co_await std::move(tf);

        // Build filter from expression
        auto filter = transform_predicate(expression_, types, &ctx->parameters);

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
