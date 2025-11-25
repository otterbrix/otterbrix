#include "index_scan.hpp"
#include <components/index/disk/route.hpp>
#include <services/collection/collection.hpp>

namespace components::table::operators {

    using range = index::index_t::range;

    std::vector<range> search_range_by_index(index::index_t* index,
                                             const expressions::compare_expression_ptr& expr,
                                             const logical_plan::storage_parameters* parameters) {
        using expressions::compare_type;
        using logical_plan::get_parameter;
        auto value = get_parameter(parameters, expr->value());
        switch (expr->type()) {
            case compare_type::eq:
                return {index->find(value)};
            case compare_type::ne:
                return {index->lower_bound(value), index->upper_bound(value)};
            case compare_type::gt:
                return {index->upper_bound(value)};
            case compare_type::lt:
                return {index->lower_bound(value)};
            case compare_type::gte:
                return {index->find(value), index->upper_bound(value)};
            case compare_type::lte:
                return {index->lower_bound(value), index->find(value)};
            default:
                //todo: error
                return {{index->cend(), index->cend()}, {index->cend(), index->cend()}};
        }
    }

    base::operators::operator_data_ptr search_by_index(index::index_t* index,
                                                       const expressions::compare_expression_ptr& expr,
                                                       const logical_plan::limit_t& limit,
                                                       const logical_plan::storage_parameters* parameters,
                                                       table::data_table_t& table) {
        auto ranges = search_range_by_index(index, expr, parameters);
        size_t rows = 0;
        for (const auto& range : ranges) {
            rows += std::distance(range.first, range.second);
        }
        size_t count = 0;
        rows = limit.limit() == logical_plan::limit_t::unlimit().limit() ? rows : std::min<size_t>(rows, limit.limit());
        vector::vector_t row_ids(index->resource(), types::logical_type::BIGINT, rows);
        for (const auto& range : ranges) {
            for (auto it = range.first; it != range.second; ++it) {
                if (!limit.check(count)) {
                    break;
                }
                row_ids.set_value(count, types::logical_value_t{it->row_index});
                ++count;
            }
        }

        table::column_fetch_state state;
        std::vector<table::storage_index_t> column_indices;
        column_indices.reserve(table.column_count());
        for (int64_t i = 0; i < table.column_count(); i++) {
            column_indices.emplace_back(i);
        }

        auto result = base::operators::make_operator_data(index->resource(), table.copy_types(), rows);
        table.fetch(result->data_chunk(), column_indices, row_ids, rows, state);
        result->data_chunk().row_ids = row_ids;
        return result;
    }

    index_scan::index_scan(services::collection::context_collection_t* context,
                           expressions::compare_expression_ptr expr,
                           logical_plan::limit_t limit)
        : read_only_operator_t(context, operator_type::match)
        , expr_(std::move(expr))
        , limit_(limit) {}

    void index_scan::on_execute_impl(pipeline::context_t* pipeline_context) {
        trace(context_->log(), "index_scan by field \"{}\"", expr_->primary_key().as_string());
        auto* index = index::search_index(context_->index_engine(), {expr_->primary_key()});
        context_->table_storage().table();
        if (index && index->is_disk()) {
            trace(context_->log(), "index_scan: send query into disk");
            auto value = logical_plan::get_parameter(&pipeline_context->parameters, expr_->value());
            pipeline_context->send(index->disk_agent(),
                                   services::index::handler_id(services::index::route::find),
                                   value,
                                   expr_->type());
            async_wait();
        } else {
            trace(context_->log(), "index_scan: prepare result");
            if (!limit_.check(0)) {
                return; //limit = 0
            }
            if (index) {
                output_ = search_by_index(index,
                                          expr_,
                                          limit_,
                                          &pipeline_context->parameters,
                                          context_->table_storage().table());
            } else {
                output_ = base::operators::make_operator_data(context_->resource(),
                                                              context_->table_storage().table().copy_types());
            }
        }
    }

    void index_scan::on_resume_impl(pipeline::context_t* pipeline_context) {
        trace(context_->log(), "resume index_scan by field \"{}\"", expr_->primary_key().as_string());
        auto* index = index::search_index(context_->index_engine(), {expr_->primary_key()});
        trace(context_->log(), "index_scan: prepare result");
        if (!limit_.check(0)) {
            return; //limit = 0
        }
        if (index) {
            output_ =
                search_by_index(index, expr_, limit_, &pipeline_context->parameters, context_->table_storage().table());
        } else {
            output_ = base::operators::make_operator_data(context_->resource(),
                                                          context_->table_storage().table().copy_types());
        }
    }

} // namespace components::table::operators
