#include "node_vector_search.hpp"

#include <sstream>

namespace components::logical_plan {

    node_vector_search_t::node_vector_search_t(std::pmr::memory_resource* resource,
                                               core::dbname_t dbname,
                                               core::relname_t relname,
                                               std::string column_name,
                                               std::vector<double> query_vector,
                                               std::size_t k,
                                               vector_search::metric_type metric,
                                               bool descending,
                                               std::optional<core::parameter_id_t> k_param)
        : node_t(resource, node_type::vector_search_t)
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , column_name_(std::move(column_name))
        , query_vector_(std::move(query_vector))
        , k_(k)
        , metric_(metric)
        , descending_(descending)
        , k_param_(k_param) {}

    hash_t node_vector_search_t::hash_impl() const { return 0; }

    std::string node_vector_search_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$vector_search: {column: \"" << column_name_ << "\", k: " << k_ << ", metric: \""
               << vector_search::metric_to_string(metric_) << "\""
               << ", query_dim: " << query_vector_.size() << "}";
        return stream.str();
    }

    node_vector_search_ptr make_node_vector_search(std::pmr::memory_resource* resource,
                                                   core::dbname_t dbname,
                                                   core::relname_t relname,
                                                   std::string column_name,
                                                   std::vector<double> query_vector,
                                                   std::size_t k,
                                                   vector_search::metric_type metric,
                                                   const expressions::compare_expression_ptr& filter,
                                                   bool descending,
                                                   std::optional<core::parameter_id_t> k_param) {
        node_vector_search_ptr node = new node_vector_search_t{resource,
                                                               std::move(dbname),
                                                               std::move(relname),
                                                               std::move(column_name),
                                                               std::move(query_vector),
                                                               k,
                                                               metric,
                                                               descending,
                                                               k_param};
        if (filter) {
            node->append_expression(filter);
        }
        return node;
    }

} // namespace components::logical_plan
