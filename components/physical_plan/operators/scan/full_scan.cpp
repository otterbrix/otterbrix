#include "full_scan.hpp"

#include <components/physical_plan/operators/transformation.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    std::unique_ptr<table::table_filter_t>
    transform_predicate(const expressions::compare_expression_ptr& expression,
                        const std::pmr::vector<types::complex_logical_type>& types,
                        const logical_plan::storage_parameters* parameters) {
        if (!expression || expression->type() == expressions::compare_type::all_true) {
            return nullptr;
        }
        switch (expression->type()) {
            case expressions::compare_type::union_and: {
                auto filter = std::make_unique<table::conjunction_and_filter_t>();
                filter->child_filters.reserve(expression->children().size());
                for (const auto& child : expression->children()) {
                    filter->child_filters.emplace_back(
                        transform_predicate(reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                            types,
                                            parameters));
                }
                return filter;
            }
            case expressions::compare_type::union_or: {
                auto filter = std::make_unique<table::conjunction_or_filter_t>();
                filter->child_filters.reserve(expression->children().size());
                for (const auto& child : expression->children()) {
                    filter->child_filters.emplace_back(
                        transform_predicate(reinterpret_cast<const expressions::compare_expression_ptr&>(child),
                                            types,
                                            parameters));
                }
                return filter;
            }
            case expressions::compare_type::invalid:
                throw std::runtime_error("unsupported compare_type in expression to filter conversion");
            default: {
                std::vector<uint64_t> indices;
                // pointer + size to avoid std::vector and std::pmr::vector clashing
                auto* local_types = types.data();
                size_t size = types.size();
                bool path_valid = true;
                for (size_t i = 0; i < expression->primary_key().storage().size(); i++) {
                    auto it =
                        std::find_if(local_types, local_types + size, [&](const types::complex_logical_type& type) {
                            return core::pmr::operator==(type.alias(), expression->primary_key().storage()[i]);
                        });
                    if (it == local_types + size) {
                        path_valid = false;
                        break;
                    }
                    indices.emplace_back(it - local_types);
                    // if it isn't the last one
                    if (i + 1 != expression->primary_key().storage().size()) {
                        if (it->child_types().empty()) {
                            path_valid = false;
                            break;
                        }
                        local_types = it->child_types().data();
                        size = it->child_types().size();
                    }
                }
                if (!path_valid) {
                    return nullptr;
                }
                return std::make_unique<table::constant_filter_t>(expression->type(),
                                                                  parameters->parameters.at(expression->value()),
                                                                  std::move(indices));
            }
        }
    }

    full_scan::full_scan(std::pmr::memory_resource* resource, log_t* log, collection_full_name_t name,
                         const expressions::compare_expression_ptr& expression,
                         logical_plan::limit_t limit)
        : read_only_operator_t(resource, log, operator_type::full_scan)
        , name_(std::move(name))
        , expression_(expression)
        , limit_(limit) {}

    void full_scan::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (name_.empty()) return;
        async_wait();
    }

    actor_zeta::unique_future<void> full_scan::await_async_and_resume(pipeline::context_t* ctx) {
        if (log_) {
            trace(log(), "full_scan::await_async_and_resume on {}", name_.to_string());
        }

        // Get types to build filter
        auto [_t, tf] = actor_zeta::send(ctx->disk_address,
            &services::disk::manager_disk_t::storage_types, ctx->session, name_);
        auto types = co_await std::move(tf);

        // Build filter from expression
        auto filter = transform_predicate(expression_, types, &ctx->parameters);

        // Scan from storage
        int limit_val = limit_.limit();
        auto [_s, sf] = actor_zeta::send(ctx->disk_address,
            &services::disk::manager_disk_t::storage_scan, ctx->session, name_,
            std::move(filter), limit_val);
        auto data = co_await std::move(sf);

        if (data) {
            output_ = make_operator_data(resource_, std::move(*data));
        } else {
            output_ = make_operator_data(resource_,
                std::pmr::vector<types::complex_logical_type>{resource_});
        }
        mark_executed();
        co_return;
    }

} // namespace components::operators
