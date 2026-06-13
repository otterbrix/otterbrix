#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/expressions/compare_expression.hpp>
#include <string>
#include <vector>
#include <vector_search/distance_metrics.hpp>

namespace components::logical_plan {

    class node_vector_search_t final : public node_t {
    public:
        node_vector_search_t(std::pmr::memory_resource* resource,
                             core::dbname_t dbname,
                             core::relname_t relname,
                             std::string column_name,
                             std::vector<double> query_vector,
                             std::size_t k,
                             vector_search::metric_type metric,
                             vector_search::filter_strategy strategy = vector_search::filter_strategy::post_filter,
                             bool descending = false);

        // Role-named accessors. The vector_search node carries the source table
        // identity through the parser-window so the executor's catalog-resolve
        // wrap and enrich can stamp table_oid(); resolved-stage code uses
        // table_oid().
        const core::dbname_t& dbname() const noexcept { return dbname_; }
        const core::relname_t& relname() const noexcept { return relname_; }

        const std::string& column_name() const noexcept { return column_name_; }
        const std::vector<double>& query_vector() const noexcept { return query_vector_; }
        std::size_t k() const noexcept { return k_; }
        vector_search::metric_type metric() const noexcept { return metric_; }
        vector_search::filter_strategy strategy() const noexcept { return strategy_; }
        // DESC: exact K-farthest scan, no index.
        bool descending() const noexcept { return descending_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        core::dbname_t dbname_;
        core::relname_t relname_;
        std::string column_name_;
        std::vector<double> query_vector_;
        std::size_t k_;
        vector_search::metric_type metric_;
        vector_search::filter_strategy strategy_;
        bool descending_;
    };

    using node_vector_search_ptr = boost::intrusive_ptr<node_vector_search_t>;

    node_vector_search_ptr
    make_node_vector_search(std::pmr::memory_resource* resource,
                            core::dbname_t dbname,
                            core::relname_t relname,
                            std::string column_name,
                            std::vector<double> query_vector,
                            std::size_t k,
                            vector_search::metric_type metric,
                            const expressions::compare_expression_ptr& filter = nullptr,
                            vector_search::filter_strategy strategy = vector_search::filter_strategy::pre_filter,
                            bool descending = false);

} // namespace components::logical_plan
