#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_vector_search.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <expressions/compare_expression.hpp>
#include <vector_search/distance_metrics.hpp>

#include <string>
#include <vector>

namespace components::operators {

    class operator_vector_search_t final : public read_only_operator_t {
    public:
        operator_vector_search_t(std::pmr::memory_resource* resource,
                                 log_t log,
                                 components::catalog::oid_t table_oid,
                                 std::string column_name,
                                 std::vector<double> query_vector,
                                 std::size_t k,
                                 vector_search::metric_type metric,
                                 const expressions::compare_expression_ptr& filter,
                                 vector_search::filter_strategy strategy,
                                 bool descending = false);

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        components::catalog::oid_t table_oid_;
        std::string column_name_;
        std::vector<double> query_vector_;
        std::size_t k_;
        vector_search::metric_type metric_;
        expressions::compare_expression_ptr filter_;
        vector_search::filter_strategy strategy_;
        bool descending_;  // DESC: exact K-farthest scan
    };

} // namespace components::operators
