#include "create_plan_vector_search.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_vector_search.hpp>
#include <components/physical_plan/operators/operator_vector_search.hpp>
#include <components/types/logical_value.hpp>

namespace services::planner::impl {

    namespace {
        std::size_t resolve_k(const components::logical_plan::node_vector_search_t* vs_node,
                              const components::logical_plan::storage_parameters* params) {
            if (!vs_node->k_param() || !params) {
                return vs_node->k();
            }
            auto it = params->parameters.find(*vs_node->k_param());
            if (it == params->parameters.end()) {
                return vs_node->k();
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
                    break;
            }
            return lim >= 0 ? static_cast<std::size_t>(lim) : vs_node->k();
        }
    } // namespace

    components::operators::operator_ptr
    create_plan_vector_search(const context_storage_t& context,
                              const components::logical_plan::node_ptr& node,
                              const components::logical_plan::storage_parameters* params) {
        auto vs_node = static_cast<const components::logical_plan::node_vector_search_t*>(node.get());

        components::expressions::compare_expression_ptr filter;
        if (!node->expressions().empty()) {
            filter = boost::dynamic_pointer_cast<components::expressions::compare_expression_t>(
                node->expressions()[0]);
        }

        const std::size_t k = resolve_k(vs_node, params);

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
                                                                    vs_node->descending()));
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
                                                                    vs_node->descending()));
        }
    }

} // namespace services::planner::impl
