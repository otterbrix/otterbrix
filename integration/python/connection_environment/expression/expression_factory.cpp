#include "expression_factory.hpp"

#include <integration/cpp/otterbrix.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>

#include <util/util.hpp>

#include <magic_enum.hpp>
#include <stdexcept>

using namespace components;
using namespace components::expressions;

namespace otterbrix {


    ExpressionFactory::ExpressionFactory(const boost::intrusive_ptr<otterbrix_t>& space)
        : counter(0), space(space) {}

    ExpressionFactory::~ExpressionFactory() = default;

    void ExpressionFactory::SetNullSpace() {
        space = nullptr;
    }

    Expression ExpressionFactory::MakeConstant(components::types::logical_value_t&& value) {
        return Expression(AddValue(std::move(value)));
    }

    Expression ExpressionFactory::MakeCountExpression() {
        return make_aggregate_expression(space->dispatcher()->resource(), "count", expressions::key_t(space->dispatcher()->resource(), "count"));
    }

    Expression ExpressionFactory::SortExpression(const string& arg) {
        return make_sort_expression(expressions::key_t(space->dispatcher()->resource(), arg), sort_order::asc);
    }

    Expression ExpressionFactory::SortExpression(const Expression& arg, sort_order order) {
        return std::visit([order](const auto& expr) -> Expression {
            using T = std::decay_t<decltype(expr)>;
            if constexpr (std::is_same_v<T, expressions::key_t>) {
                return Expression(make_sort_expression(expr, order));
            } else if constexpr (std::is_same_v<T, expressions::expression_ptr>) {
                if (expr->group() == expression_group::sort) {
                    return Expression(expr);
                } else {
                    throw std::runtime_error("Invalid argument for sort expression. OtterBrix doesn\'t support this type of expression");
                }
            } else if constexpr (std::is_same_v<T, core::parameter_id_t>) {
                throw std::runtime_error("Invalid argument for sort expression. OtterBrix doesn\'t support this type of expression");
            } else {
                throw std::runtime_error("Implementation Error. Undefined expression during sort expession processing");
            }}, arg);
    }

    Expression ExpressionFactory::AggregationUnaryExpression(const string& function_name,
        const Expression& expr) {
        auto* resource = space->dispatcher()->resource();
        string sub_name = std::visit([](const auto& param) -> string {
                using T = std::decay_t<decltype(param)>;
                if constexpr (std::is_same_v<T, expressions::key_t>) {
                    return param.as_string();
                } else if (std::is_same_v<T, expressions::expression_ptr> ||
                           std::is_same_v<T, core::parameter_id_t>) {
                    throw std::runtime_error("Current configuration support only column names as argument of aggregation function");
                } else {
                    throw std::runtime_error("Implementation Error. Undefined parameter of aggregaton function");
                }
            },
            expr);

        string agg_str = function_name + "(" + sub_name + ")";
        auto aggregation_expression =
            expressions::make_aggregate_expression(resource, function_name, expressions::key_t(resource, agg_str));

        // Spark-style avg over integers uses floating accumulator:
        // https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L63-L79
        // But grouped_aggregate truncates back to the column type when the input vector is integral:
        // https://github.com/prawwtocol/otterbrix/blob/main/components/physical_plan/operators/aggregate/grouped_aggregate.cpp#L265
        // Multiply by 1.0 so arithmetic promotes to DOUBLE
        if (function_name == "avg") {
            Expression one = MakeConstant(types::logical_value_t(resource, 1.0));
            Expression scaled = ScalarBinaryExpression(scalar_type::multiply, expr, std::move(one));
            std::visit(
                [&](const auto& inner) {
                    using T = std::decay_t<decltype(inner)>;
                    if constexpr (std::is_same_v<T, expressions::expression_ptr>) {
                        aggregation_expression->append_param(inner);
                    } else {
                        throw std::runtime_error("avg: internal multiply expression expected");
                    }
                },
                scaled);
            return Expression(aggregation_expression);
        }

        aggregation_expression->append_param(expr);
        return Expression(aggregation_expression);
    }

    Expression ExpressionFactory::ScalarUnaryExpression(components::expressions::scalar_type type, 
        const Expression& expr) {
        return std::visit([&type, resource = space->dispatcher()->resource()](const auto& arg) -> Expression {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (
                (std::is_same_v<T, expressions::key_t> || std::is_same_v<T, core::parameter_id_t> || 
                    std::is_same_v<T, expressions::expression_ptr>)) {
                auto scalar_expr = expressions::make_scalar_expression(resource, type);
                scalar_expr->append_param(arg);
                return Expression(boost::static_pointer_cast<expressions::expression_i>(scalar_expr));
            }

        }, expr); 
    }

    Expression ExpressionFactory::ScalarBinaryExpression(components::expressions::scalar_type type, 
        const Expression& left, const Expression& right) {
        return std::visit([&type, resource = space->dispatcher()->resource()](const auto& arg1, const auto& arg2) -> Expression {
            using T1 = std::decay_t<decltype(arg1)>;
            using T2 = std::decay_t<decltype(arg2)>;

            if constexpr (
                (std::is_same_v<T1, expressions::key_t> || std::is_same_v<T1, core::parameter_id_t> || 
                    std::is_same_v<T1, expressions::expression_ptr>) && 
                (std::is_same_v<T2, expressions::key_t> || std::is_same_v<T2, core::parameter_id_t> || 
                    std::is_same_v<T2, expressions::expression_ptr>)) {
                expressions::scalar_expression_ptr scalar_expr = expressions::make_scalar_expression(resource, type);

                scalar_expr->append_param(arg1);
                scalar_expr->append_param(arg2);
                return Expression(boost::static_pointer_cast<expressions::expression_i>(scalar_expr));
            }
            throw std::runtime_error("Undefined argument for scalar binary expression");

        }, left, right);
        
    }

    Expression ExpressionFactory::ComparisonExpression(expressions::compare_type type, 
        const Expression& left, const Expression& right) {

        return std::visit([&type, resource = space->dispatcher()->resource()](const auto& arg1, const auto& arg2) -> Expression {
            using T1 = std::decay_t<decltype(arg1)>;
            using T2 = std::decay_t<decltype(arg2)>;
            if constexpr (std::is_same_v<T1, expressions::key_t> && 
                (std::is_same_v<T2, expressions::key_t> || std::is_same_v<T2, core::parameter_id_t>)) {
                auto compare_expression = expressions::make_compare_expression(resource, type, arg1, arg2);
                return Expression(boost::static_pointer_cast<expressions::expression_i>(compare_expression));
            }
            throw std::runtime_error("Incorrect arguments for the compare expression. OtteBrix doesn\'t implement \'not field\' comp_op \'expr\'");
            
        }, left, right);
    
    }
    
    Expression ExpressionFactory::ExpressionWithAlias(const Expression& expr, const string& alias) {
        return std::visit([&alias, resource = space->dispatcher()->resource()](const auto& expr) -> Expression {
            using T = std::decay_t<decltype(expr)>;
            if constexpr (std::is_same_v<T, expressions::key_t>) {
                expressions::scalar_expression_ptr scalar_expr = 
                    expressions::make_scalar_expression(resource, 
                        expressions::scalar_type::get_field, expressions::key_t(resource, alias));
                scalar_expr->append_param(expr);
                return Expression(boost::static_pointer_cast<expressions::expression_i>(scalar_expr));
            } else  if constexpr (std::is_same_v<T, expressions::expression_ptr>) {
                if (expr->group() == expression_group::aggregate) {
                    const auto& agg = boost::static_pointer_cast<expressions::aggregate_expression_t>(expr);
                    auto alias_expr = make_aggregate_expression(resource, agg->function_name(), expressions::key_t(resource, alias));
                    for (const auto& param : agg->params()) {
                        alias_expr->append_param(param);
                    }
                    return Expression(boost::static_pointer_cast<expressions::expression_i>(alias_expr));
                } 
                throw std::runtime_error("Incorrect argument for the alias expression. Coulnd\'t support difficult expressions");
                
            }
            throw std::runtime_error("Incorrect argument for the alias expression. OtteBrix doesn\'t implement naming of \'not field\'");
        }, expr);
                
    }

    Expression ExpressionFactory::ComparisonNotExpression(const Expression& expr) {
        auto not_expr = make_compare_union_expression(space->dispatcher()->resource(), expressions::compare_type::union_not);
        not_expr->append_child(UnionExpressionToExpressionPtr(expr));
        return Expression(boost::static_pointer_cast<expressions::expression_i>(not_expr));
    }

    Expression ExpressionFactory::ComparisonUnionExpression(expressions::compare_type type, 
        const Expression& left, const Expression& right) {
        auto union_expr = make_compare_union_expression(space->dispatcher()->resource(), type);
        union_expr->append_child(UnionExpressionToExpressionPtr(left));
        union_expr->append_child(UnionExpressionToExpressionPtr(right));
        return Expression(boost::static_pointer_cast<expressions::expression_i>(union_expr));
        
    }

    Expression ExpressionFactory::TrueExpression() {
        return Expression(make_compare_expression(space->dispatcher()->resource(), compare_type::all_true));
    }

    expressions::compare_expression_ptr ExpressionFactory::UnionExpressionToExpressionPtr(const Expression& expr) {
        return std::visit([resource = space->dispatcher()->resource()](const auto& expr) -> expressions::compare_expression_ptr {
            using T = std::decay_t<decltype(expr)>;
            if constexpr (std::is_same_v<T, expressions::expression_ptr>) {
                if (expr->group() == expressions::expression_group::compare) {
                    return boost::static_pointer_cast<expressions::compare_expression_t>(expr);
                }
            }
            
            throw std::runtime_error("Incorrect arguments for the compare union expression. Should be bool expression");
        }, expr);
    }



    string ExpressionFactory::ConvertToString(const Expression& expr) {
        return std::visit([this](const auto& expr) -> string {
            using T = std::decay_t<decltype(expr)>;
            if constexpr (std::is_same_v<T, expressions::key_t>) {
                return expr.as_string();
            } else if constexpr (std::is_same_v<T, expressions::expression_ptr>) {
                return expr->to_string();
            } else if constexpr (std::is_same_v<T, core::parameter_id_t>){ 
                const auto& value = this->values.at(expr);
                return util::LogicalValueToString(value);
            }
            std::runtime_error("Implementation Error: Couldn\'t convert PyExpression to string");
        
        }, expr);

    }

    core::parameter_id_t ExpressionFactory::AddValue(components::types::logical_value_t&& value) {
        auto param = core::parameter_id_t(counter);
        counter++;
        values.emplace(param, value);
        return param;
    }

    components::logical_plan::parameter_node_ptr ExpressionFactory::GetParams() {
        auto params = logical_plan::make_parameter_node(space->dispatcher()->resource());
        for (const auto& param : values) {
            switch (param.second.type().to_physical_type()) {
                case components::types::physical_type::BOOL:
                    params->add_parameter(param.first, param.second.value<bool>());
                    break;
                case components::types::physical_type::UINT8:
                    params->add_parameter(param.first, param.second.value<uint8_t>());
                    break;
                case components::types::physical_type::INT8:
                    params->add_parameter(param.first, param.second.value<int8_t>());
                    break;
                case components::types::physical_type::UINT16:
                    params->add_parameter(param.first, param.second.value<uint16_t>());
                    break;
                case components::types::physical_type::INT16:
                    params->add_parameter(param.first, param.second.value<int16_t>());
                    break;
                case components::types::physical_type::UINT32:
                    params->add_parameter(param.first, param.second.value<uint32_t>());
                    break;
                case components::types::physical_type::INT32:
                    params->add_parameter(param.first, param.second.value<int32_t>());
                    break;
                case components::types::physical_type::UINT64:
                    params->add_parameter(param.first, param.second.value<uint64_t>());
                    break;
                case components::types::physical_type::INT64:
                    params->add_parameter(param.first, param.second.value<int64_t>());
                    break;
                case components::types::physical_type::UINT128:
                    params->add_parameter(param.first, param.second.value<int64_t>());
                    break;
                case components::types::physical_type::FLOAT:
                    params->add_parameter(param.first, param.second.value<float>());
                    break;
                case components::types::physical_type::DOUBLE:
                    params->add_parameter(param.first, param.second.value<double>());
                    break;
                case components::types::physical_type::STRING:
                    params->add_parameter(param.first, string(param.second.value<std::string_view>()));
                    break;
                default:
                    throw std::runtime_error("Couldn\'t convert logical value to document value");
            }
        }
        return params;

    }
} // namespace otterbrix
