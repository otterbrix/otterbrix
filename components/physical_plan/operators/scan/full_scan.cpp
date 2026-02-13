#include "full_scan.hpp"

#include <components/physical_plan/operators/transformation.hpp>
#include <services/collection/collection.hpp>

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
                // pointer + size to avoid std::vector and std::pmr::vector clashing
                auto* local_types = types.data();
                size_t size = types.size();
                const auto& path = std::get<expressions::key_t>(expression->left()).path();
                std::vector<uint64_t> indices;
                indices.reserve(path.size());
                auto id = std::get<core::parameter_id_t>(expression->right());
                for (size_t i = 0; i < path.size(); i++) {
                    indices.emplace_back(path[i]);
                }
                return std::make_unique<table::constant_filter_t>(expression->type(),
                                                                  parameters->parameters.at(id),
                                                                  std::move(indices));
            }
        }
    }

    full_scan::full_scan(services::collection::context_collection_t* context,
                         const expressions::compare_expression_ptr& expression,
                         logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , expression_(expression)
        , limit_(limit) {}

    void full_scan::on_execute_impl(pipeline::context_t* pipeline_context) {
        trace(context_->log(), "full_scan");
        int count = 0;
        if (!limit_.check(count)) {
            return; //limit = 0
        }

        auto types = context_->table_storage().table().copy_types();
        output_ = operators::make_operator_data(context_->resource(), types);
        std::vector<table::storage_index_t> column_indices;
        column_indices.reserve(context_->table_storage().table().column_count());
        for (size_t i = 0; i < context_->table_storage().table().column_count(); i++) {
            column_indices.emplace_back(static_cast<int64_t>(i));
        }
        table::table_scan_state state(context_->resource());
        auto filter =
            transform_predicate(expression_, types, pipeline_context ? &pipeline_context->parameters : nullptr);
        context_->table_storage().table().initialize_scan(state, column_indices, filter.get());
        // TODO: check limit inside scan
        context_->table_storage().table().scan(output_->data_chunk(), state);
        if (limit_.limit() >= 0) {
            output_->data_chunk().set_cardinality(
                std::min(output_->data_chunk().size(), static_cast<uint64_t>(limit_.limit())));
        }
    }

} // namespace components::operators
