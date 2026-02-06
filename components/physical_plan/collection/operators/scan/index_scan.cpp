#include "index_scan.hpp"
#include <services/disk/index_agent_disk.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/collection/collection.hpp>
#include <core/executor.hpp>

namespace components::collection::operators {

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

    void search_by_index(index::index_t* index,
                         const expressions::compare_expression_ptr& expr,
                         const logical_plan::limit_t& limit,
                         const logical_plan::storage_parameters* parameters,
                         base::operators::operator_data_ptr& result) {
        auto ranges = search_range_by_index(index, expr, parameters);
        int count = 0;
        for (const auto& range : ranges) {
            for (auto it = range.first; it != range.second; ++it) {
                if (!limit.check(count)) {
                    return;
                }
                result->append(it->doc); //todo: check nullptr and get by id
                ++count;
            }
        }
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
        if (index && index->is_disk() && index->disk_manager()) {
            trace(context_->log(), "index_scan: send query into disk (future-based)");
            auto value = logical_plan::get_parameter(&pipeline_context->parameters, expr_->value());
            auto session_copy = pipeline_context->session;
            auto agent_copy = index->disk_agent();
            auto value_copy = value;
            auto [_, future] = actor_zeta::send(index->disk_manager(),
                             &services::disk::manager_disk_t::index_find_by_agent,
                             std::move(session_copy),
                             std::move(agent_copy),
                             std::move(value_copy),
                             expr_->type());
            bool tmp_disk_future_ready_ = future.available();
            disk_future_ = std::make_unique<actor_zeta::unique_future<services::disk::index_disk_t::result> >(std::move(future));
            disk_future_ready_ = tmp_disk_future_ready_;
            async_wait();
        } else {
            trace(context_->log(), "index_scan: prepare result");
            if (!limit_.check(0)) {
                return; //limit = 0
            }
            output_ = base::operators::make_operator_data(context_->resource());
            if (index) {
                search_by_index(index, expr_, limit_, &pipeline_context->parameters, output_);
            }
        }
    }

    void index_scan::on_resume_impl(pipeline::context_t* pipeline_context) {
        trace(context_->log(), "resume index_scan by field \"{}\"", expr_->primary_key().as_string());
        auto* index = index::search_index(context_->index_engine(), {expr_->primary_key()});

        if (index && index->is_disk() && !disk_result_.empty()) {
            trace(context_->log(), "index_scan: sync_index_from_disk, result size: {}", disk_result_.size());
            components::index::sync_index_from_disk(context_->index_engine(),
                                                    index->disk_agent(),
                                                    disk_result_,
                                                    context_->document_storage());
        }

        trace(context_->log(), "index_scan: prepare result");
        if (!limit_.check(0)) {
            return; //limit = 0
        }
        output_ = base::operators::make_operator_data(context_->resource());
        if (index) {
            search_by_index(index, expr_, limit_, &pipeline_context->parameters, output_);
        }
    }

    actor_zeta::unique_future<void> index_scan::await_async_and_resume(pipeline::context_t* ctx) {
        if (disk_future_) {
            trace(context_->log(), "index_scan: await disk future (unique_future)");
            disk_result_ = co_await std::move(*disk_future_);
            trace(context_->log(), "index_scan: disk future resolved, result size: {}", disk_result_.size());
        }
        on_resume(ctx);
        co_return;
    }

} // namespace components::collection::operators
