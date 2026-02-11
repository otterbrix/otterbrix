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
