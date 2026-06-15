#include "create_plan_vector_search.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_vector_search.hpp>
#include <components/physical_plan/operators/operator_vector_search.hpp>
#include <components/types/logical_value.hpp>

#include <stdexcept>

namespace services::planner::impl {

    namespace {
        std::size_t resolve_k(const components::logical_plan::node_vector_search_t* vs_node,
                              const components::logical_plan::storage_parameters* params) {
            if (!vs_node->k_param() || !params) {
                return vs_node->k();
            }
            auto it = params->parameters.find(*vs_node->k_param());
            if (it == params->parameters.end()) {
                throw std::logic_error("vector_search: LIMIT parameter is not bound");
            }
            const auto& v = it->second;
            int64_t lim = -1;
            switch (v.type().type()) {
                case components::types::logical_type::BIGINT:
                    lim = v.value<int64_t>();
                    break;
                case components::types::logical_type::INTEGER:
                    lim = v.value<int32_t>();
                    break;
                case components::types::logical_type::SMALLINT:
                    lim = v.value<int16_t>();
                    break;
                case components::types::logical_type::TINYINT:
                    lim = v.value<int8_t>();
                    break;
                default:
                    throw std::logic_error("vector_search: LIMIT parameter has unsupported type");
            }
            if (lim < 0) {
                throw std::logic_error("vector_search: LIMIT parameter must be non-negative");
            }
            return static_cast<std::size_t>(lim);
        }
    } // namespace

    components::operators::operator_ptr
    create_plan_vector_search(const context_storage_t& context,
                              const components::logical_plan::node_ptr& node,
                              const components::logical_plan::storage_parameters* params,
                              const std::vector<size_t>& projected_cols) {
        auto vs_node = static_cast<const components::logical_plan::node_vector_search_t*>(node.get());

        components::expressions::compare_expression_ptr filter;
        if (!node->expressions().empty()) {
            filter = boost::dynamic_pointer_cast<components::expressions::compare_expression_t>(
                node->expressions()[0]);
        }

        const std::size_t k = resolve_k(vs_node, params);

        std::size_t vector_col_chunk = static_cast<std::size_t>(-1);
        if (const auto* md = context.table_metadata_for(node->table_oid())) {
            for (const auto& col : md->columns) {
                if (col.attname == vs_node->column_name() && col.chunk_position >= 0) {
                    vector_col_chunk = static_cast<std::size_t>(col.chunk_position);
                    break;
                }
            }
        }

        if (context.has_table_oid(node->table_oid())) {
            return boost::intrusive_ptr(
                new components::operators::operator_vector_search_t(context.resource,
                                                                    context.log.clone(),
                                                                    node->table_oid(),
                                                                    vs_node->column_name(),
                                                                    vs_node->query_vector(),
                                                                    k,
                                                                    vs_node->metric(),
                                                                    filter,
                                                                    vs_node->strategy(),
                                                                    vs_node->descending(),
                                                                    projected_cols,
                                                                    vector_col_chunk));
        } else {
            return boost::intrusive_ptr(
                new components::operators::operator_vector_search_t(nullptr,
                                                                    log_t{},
                                                                    node->table_oid(),
                                                                    vs_node->column_name(),
                                                                    vs_node->query_vector(),
                                                                    k,
                                                                    vs_node->metric(),
                                                                    filter,
                                                                    vs_node->strategy(),
                                                                    vs_node->descending(),
                                                                    projected_cols,
                                                                    vector_col_chunk));
        }
    }

} // namespace services::planner::impl
