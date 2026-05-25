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
                bool is_count = aggregate_expr->function_name() == "count";
                if (is_count) {
                    name = "count";
                    type = types::logical_type::UBIGINT;
                } else {

                    const auto& param = aggregate_expr->params().front();
                    auto founded_name = find_param_name(param);
                
                    if (aggregate_expr->key().is_null()) {
                        string agg_str = aggregate_expr->function_name();
                        name =  agg_str + 
                        "(" + founded_name.first +")";
                    } else {
                        name = aggregate_expr->key().as_string();
                    }   
                    auto base_type = find_type(founded_name.first, initial);
                    if (aggregate_expr->function_name() == "avg") {
                        type = types::logical_type::DOUBLE;
                    } else {
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
            // Match operator_join_t output schema: right-side columns whose name
            // already appears on the left are collapsed into the existing slot
            // (USING-style dedup). Reading past the engine's actual chunk size
            // would segfault in fetchall.
            for (const auto& col : right) {
                bool duplicate = false;
                for (const auto& existing : result) {
                    if (existing.name() == col.name()) {
                        duplicate = true;
                        break;
                    }
                }
                if (!duplicate) {
                    result.emplace_back(col.name(), col.type());
                }
            }
            return result;
        }

        vector<column_definition_t> operator()(const Relation::Limit& limit) {
            return limit.resource->GetColumns();
        }

        vector<column_definition_t> operator()(const Relation::Aggregate& aggregate) {
            // define types
            auto initial = aggregate.resource->GetColumns();
            vector<column_definition_t> result;
            auto group = aggregate.group;
            auto select = aggregate.select;

            // Pure projection (select-only): output schema is exactly the SELECT exprs,
            // independent of upstream group state.
            if (select && !group) {
                const auto& exprs = select->expressions();
                result.reserve(exprs.size());
                for (const auto& expr : exprs) {
                    switch (expr->group()) {
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
            node_sort_ptr sort,
            node_select_ptr select, string name)
            : relation(Aggregate(resource, group, match, sort, select, std::move(name))) {
    }


    Relation::Relation(shared_ptr<Relation> left, shared_ptr<Relation> right, 
            unique_ptr<vector<components::expressions::expression_ptr>> conditions, 
            logical_plan::join_type join_type) 
            : relation(Relation::Join(left, right, std::move(conditions), join_type)) {

    }

    Relation::Relation(shared_ptr<Relation> resource, int64_t limit_count)
            : relation(Limit(resource, limit_count)) {
    }

    Relation::Relation(Relation&& other) noexcept = default;


    vector<components::table::column_definition_t> Relation::GetColumns() {
        return std::visit(ColumnsVisitor(), relation);
    }

} // namespace otterbrix
