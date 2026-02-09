#include "relation.hpp"
#include <components/expressions/aggregate_expression.hpp>
#include <core/typedefs.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/expression.hpp>

#include <magic_enum.hpp>

using namespace components;

using namespace components::logical_plan;
using components::table::column_definition_t;
using namespace components::expressions;



namespace otterbrix {
    
    class ColumnsVisitor {
    public:
        static inline const std::string error_str = "#";

        static components::types::complex_logical_type find_type(
                string name, const vector<column_definition_t>& initial) {
            for (const auto& col : initial) {
                if (col.name() == name) {
                    return col.type();
                }
            }
            return components::types::logical_type::UNKNOWN;
            
        }

        static std::pair<string, bool> find_param_name(
                const std::variant<core::parameter_id_t, expressions::key_t, expression_ptr>& param) {

            return std::visit([](const auto& expr) {
                using type = std::decay_t<decltype(expr)>;
                if constexpr (std::is_same_v<type, expressions::key_t>) {
                    return std::make_pair(expr.as_string(), true);
                } else if constexpr (std::is_same_v<type, core::parameter_id_t> || 
                    std::is_same_v<type, expression_ptr>) {
                    return std::make_pair(error_str, false);
                }

                throw std::runtime_error("Unknown parameter type for nodes");
            }, param);
        }


        static column_definition_t process_aggregate(
            aggregate_expression_ptr aggregate_expr, 
            const vector<column_definition_t>& initial) {
                string name = error_str;
                components::types::complex_logical_type type = components::types::logical_type::UNKNOWN;
                if (aggregate_expr->params().size() > 1) {
                    return column_definition_t(name, type);
                }
                bool is_count = aggregate_expr->type() == expressions::aggregate_type::count;
                if (is_count) {
                    name = (aggregate_expr->key().is_null()?"count":aggregate_expr->key().as_string());
                    type = types::logical_type::INTEGER;
                } else {

                    const auto& param = aggregate_expr->params().front();
                    auto founded_name = find_param_name(param);
                
                    if (aggregate_expr->key().is_null()) {
                        string agg_str = string(magic_enum::enum_name(aggregate_expr->type()));
                        name =  agg_str + 
                        "(" + founded_name.first +")";
                    } else {
                        name = aggregate_expr->key().as_string();
                    }   
                    auto base_type = find_type(founded_name.first, initial); 
                    switch (aggregate_expr->type()) {
                        case aggregate_type::avg:
                            type = types::logical_type::FLOAT;
                            break;
                        default:
                            type = base_type;
                    }
                }
                return column_definition_t(name, type); 
        } 

        static column_definition_t process_scalar(
            scalar_expression_ptr scalar_expr, 
            const vector<column_definition_t>& initial) {
                string name = error_str;
                components::types::complex_logical_type type = components::types::logical_type::UNKNOWN;

                if (scalar_expr->type() != scalar_type::get_field) {
                    return column_definition_t(name, type);
                }
                if (scalar_expr->params().size() > 1) {
                    return column_definition_t(name, type);
                }
                if (!scalar_expr->key().is_null()) {
                    name = scalar_expr->key().as_string();
                }

                if (scalar_expr->params().size() == 1) {
                    auto param_name = find_param_name(scalar_expr->params().front());
                    type = find_type(param_name.first, initial);
                } else {
                    type = find_type(name, initial);
                }

                /*if (scalar_expr->key().is_null()) {
                    name = founded_name.first;
                } else {
                    name = scalar_expr->key().as_string();
                }*/
                return column_definition_t(name, type);
        }
    public:
        vector<column_definition_t> operator()(const Relation::Data& data) {
            vector<column_definition_t> result;
            result.reserve(data.columns->size());
            for (idx_t i = 0; i < data.columns->size(); i++) {
                const auto& col = data.columns->at(i);
                result.emplace_back(col.name(), col.type());
            }
            return result;
        }

        vector<column_definition_t> operator()(const Relation::Join& join) {
            vector<column_definition_t> result;
            auto left = join.left->GetColumns();
            auto right = join.right->GetColumns();
            result.reserve(left.size() + right.size());
 
            for (const auto& col : left) {
                result.emplace_back(col.name(), col.type());
            }
            for (const auto& col : right) {
                result.emplace_back(col.name(), col.type());
            }
            return result;
        }

        vector<column_definition_t> operator()(const Relation::Aggregate& aggregate) {
            // define types
            auto initial = aggregate.resource->GetColumns();
            vector<column_definition_t> result;
            auto group = aggregate.group;
            if (!group) {
                result.reserve(initial.size());
                for (const auto& col : initial) {
                    result.emplace_back(col.name(), col.type());
                }
                return result;
            }
            const auto& exprs = group->expressions();
            result.reserve(exprs.size());

            for (const auto& expr : exprs) {
                switch (expr->group()) {
                    case expression_group::aggregate:
                        result.push_back(process_aggregate(
                            boost::static_pointer_cast<aggregate_expression_t>(expr),
                            initial));
                        break;
                    case expression_group::scalar:
                        result.push_back(process_scalar(
                            boost::static_pointer_cast<scalar_expression_t>(expr),
                            initial));
                        break;
                    default:
                        result.emplace_back(error_str, components::types::logical_type::UNKNOWN);                        
                }
            }
            return result;

        }

    };



    Relation::Relation(node_data_ptr data,
            shared_ptr<ExternalDependency> external_dependency,
            unique_ptr<vector<column_definition_t>> columns) 
            : relation(Relation::Data(data, external_dependency, std::move(columns))){
    }

    
    Relation::Relation(shared_ptr<Relation> resource, 
            node_group_ptr group, 
            node_match_ptr match, 
            node_sort_ptr sort, string name) 
            : relation(Aggregate(resource, group, match, sort, std::move(name))) {
    }


    Relation::Relation(shared_ptr<Relation> left, shared_ptr<Relation> right, 
            unique_ptr<vector<components::expressions::expression_ptr>> conditions, 
            logical_plan::join_type join_type) 
            : relation(Relation::Join(left, right, std::move(conditions), join_type)) {

    }

    Relation::Relation(Relation&& other) noexcept = default;


    vector<components::table::column_definition_t> Relation::GetColumns() {
        return std::visit(ColumnsVisitor(), relation);
    }

} // namespace otterbrix
