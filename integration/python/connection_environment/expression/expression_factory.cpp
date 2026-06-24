#include "expression_factory.hpp"

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <integration/cpp/otterbrix.hpp>

#include <util/util.hpp>

#include <stdexcept>
#include <string>

using namespace components;
using namespace components::expressions;

namespace otterbrix {

    expression_factory_t::expression_factory_t(const boost::intrusive_ptr<otterbrix_t>& space)
        : counter(0)
        , space(space) {}

    expression_factory_t::~expression_factory_t() = default;

    void expression_factory_t::set_null_space() { space = nullptr; }

    expression_wrapper_t expression_factory_t::make_constant(components::types::logical_value_t&& value) {
        return expression_wrapper_t(add_value(std::move(value)));
    }

    expression_wrapper_t expression_factory_t::make_count_expression() {
        auto* resource = space->dispatcher()->resource();
        return expression_wrapper_t(boost::static_pointer_cast<expressions::expression_i>(
            make_aggregate_expression(resource, "count", expressions::key_t(resource, "count"))));
    }

    expression_wrapper_t expression_factory_t::sort_expression(const std::string& arg) {
        auto* resource = space->dispatcher()->resource();
        return expression_wrapper_t(boost::static_pointer_cast<expressions::expression_i>(
            make_sort_expression(expressions::key_t(resource, arg), sort_order::asc)));
    }

    namespace {
        core::error_t invalid_argument(std::pmr::memory_resource* resource, const char* message) {
            return core::error_t(core::error_code_t::invalid_parameter, std::pmr::string{message, resource});
        }
    } // namespace

    core::result_wrapper_t<expression_wrapper_t> expression_factory_t::sort_expression(const expression_wrapper_t& arg,
                                                                                       sort_order order) {
        auto* resource = space->dispatcher()->resource();
        if (arg.is_key()) {
            return expression_wrapper_t(make_sort_expression(arg.key(), order));
        }
        if (arg.is_expression()) {
            if (arg.expression()->group() == expression_group::sort) {
                return expression_wrapper_t(arg.expression());
            }
            return invalid_argument(
                resource,
                "Invalid argument for sort expression. OtterBrix doesn't support this type of expression");
        }
        // parameter
        return invalid_argument(
            resource,
            "Invalid argument for sort expression. OtterBrix doesn't support this type of expression");
    }

    core::result_wrapper_t<expression_wrapper_t>
    expression_factory_t::aggregation_unary_expression(const std::string& function_name,
                                                       const expression_wrapper_t& expr) {
        auto* resource = space->dispatcher()->resource();
        if (!expr.is_key()) {
            return invalid_argument(
                resource,
                "Current configuration support only column names as argument of aggregation function");
        }
        std::string sub_name = expr.key().as_string();

        std::string agg_str = function_name + "(" + sub_name + ")";
        auto aggregation_expression =
            expressions::make_aggregate_expression(resource, function_name, expressions::key_t(resource, agg_str));

        // Spark-style avg over integers uses floating accumulator:
        // https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala#L63-L79
        // But the avg aggregate kernel truncates back to the column type when the input is
        // integral (sum/count are the same integral type), so multiply by 1.0 to promote to DOUBLE.
        if (function_name == "avg") {
            expression_wrapper_t one = make_constant(types::logical_value_t(resource, 1.0));
            auto scaled = scalar_binary_expression(scalar_type::multiply, expr, std::move(one));
            if (scaled.has_error()) {
                return scaled.error();
            }
            if (!scaled.value().is_expression()) {
                return invalid_argument(resource, "avg: internal multiply expression expected");
            }
            aggregation_expression->append_param(scaled.value().expression());
            return expression_wrapper_t(aggregation_expression);
        }

        aggregation_expression->append_param(expr.key());
        return expression_wrapper_t(aggregation_expression);
    }

    expression_wrapper_t expression_factory_t::scalar_unary_expression(components::expressions::scalar_type type,
                                                                       const expression_wrapper_t& expr) {
        auto* resource = space->dispatcher()->resource();
        auto scalar_expr = expressions::make_scalar_expression(resource, type);
        if (expr.is_key()) {
            scalar_expr->append_param(expr.key());
        } else if (expr.is_parameter()) {
            scalar_expr->append_param(expr.parameter());
        } else {
            scalar_expr->append_param(expr.expression());
        }
        return expression_wrapper_t(boost::static_pointer_cast<expressions::expression_i>(scalar_expr));
    }

    namespace {
        // append an expression_wrapper_t's payload to a node that takes param_storage children (scalar/compare).
        template<class NODE>
        void append_expression(NODE& node, const expression_wrapper_t& expr) {
            if (expr.is_key()) {
                node->append_param(expr.key());
            } else if (expr.is_parameter()) {
                node->append_param(expr.parameter());
            } else {
                node->append_param(expr.expression());
            }
        }
    } // namespace

    core::result_wrapper_t<expression_wrapper_t>
    expression_factory_t::scalar_binary_expression(components::expressions::scalar_type type,
                                                   const expression_wrapper_t& left,
                                                   const expression_wrapper_t& right) {
        auto* resource = space->dispatcher()->resource();
        expressions::scalar_expression_ptr scalar_expr = expressions::make_scalar_expression(resource, type);
        append_expression(scalar_expr, left);
        append_expression(scalar_expr, right);
        return expression_wrapper_t(boost::static_pointer_cast<expressions::expression_i>(scalar_expr));
    }

    core::result_wrapper_t<expression_wrapper_t>
    expression_factory_t::comparison_expression(expressions::compare_type type,
                                                const expression_wrapper_t& left,
                                                const expression_wrapper_t& right) {
        auto* resource = space->dispatcher()->resource();
        if (left.is_key() && (right.is_key() || right.is_parameter())) {
            expressions::compare_expression_ptr compare_expression =
                right.is_key() ? expressions::make_compare_expression(resource, type, left.key(), right.key())
                               : expressions::make_compare_expression(resource, type, left.key(), right.parameter());
            return expression_wrapper_t(boost::static_pointer_cast<expressions::expression_i>(compare_expression));
        }
        return invalid_argument(
            resource,
            "Incorrect arguments for the compare expression. OtteBrix doesn't implement 'not field' comp_op 'expr'");
    }

    core::result_wrapper_t<expression_wrapper_t>
    expression_factory_t::expression_with_alias(const expression_wrapper_t& expr, const std::string& alias) {
        auto* resource = space->dispatcher()->resource();
        if (expr.is_key()) {
            expressions::scalar_expression_ptr scalar_expr =
                expressions::make_scalar_expression(resource,
                                                    expressions::scalar_type::get_field,
                                                    expressions::key_t(resource, alias));
            scalar_expr->append_param(expr.key());
            return expression_wrapper_t(boost::static_pointer_cast<expressions::expression_i>(scalar_expr));
        }
        if (expr.is_expression()) {
            if (expr.expression()->group() == expression_group::aggregate) {
                const auto& agg = boost::static_pointer_cast<expressions::aggregate_expression_t>(expr.expression());
                auto alias_expr =
                    make_aggregate_expression(resource, agg->function_name(), expressions::key_t(resource, alias));
                for (const auto& param : agg->params()) {
                    alias_expr->append_param(param);
                }
                return expression_wrapper_t(boost::static_pointer_cast<expressions::expression_i>(alias_expr));
            }
            return invalid_argument(
                resource,
                "Incorrect argument for the alias expression. Coulnd't support difficult expressions");
        }
        return invalid_argument(
            resource,
            "Incorrect argument for the alias expression. OtteBrix doesn't implement naming of 'not field'");
    }

    core::result_wrapper_t<expression_wrapper_t>
    expression_factory_t::comparison_not_expression(const expression_wrapper_t& expr) {
        auto not_expr =
            make_compare_union_expression(space->dispatcher()->resource(), expressions::compare_type::union_not);
        auto child = union_expression_to_expression_ptr(expr);
        if (child.has_error()) {
            return child.error();
        }
        not_expr->append_child(child.value());
        return expression_wrapper_t(boost::static_pointer_cast<expressions::expression_i>(not_expr));
    }

    core::result_wrapper_t<expression_wrapper_t>
    expression_factory_t::comparison_union_expression(expressions::compare_type type,
                                                      const expression_wrapper_t& left,
                                                      const expression_wrapper_t& right) {
        auto union_expr = make_compare_union_expression(space->dispatcher()->resource(), type);
        auto left_child = union_expression_to_expression_ptr(left);
        if (left_child.has_error()) {
            return left_child.error();
        }
        auto right_child = union_expression_to_expression_ptr(right);
        if (right_child.has_error()) {
            return right_child.error();
        }
        union_expr->append_child(left_child.value());
        union_expr->append_child(right_child.value());
        return expression_wrapper_t(boost::static_pointer_cast<expressions::expression_i>(union_expr));
    }

    expression_wrapper_t expression_factory_t::true_expression() {
        return expression_wrapper_t(make_compare_expression(space->dispatcher()->resource(), compare_type::all_true));
    }

    core::result_wrapper_t<expressions::compare_expression_ptr>
    expression_factory_t::union_expression_to_expression_ptr(const expression_wrapper_t& expr) {
        auto* resource = space->dispatcher()->resource();
        if (expr.is_expression() && expr.expression()->group() == expressions::expression_group::compare) {
            return boost::static_pointer_cast<expressions::compare_expression_t>(expr.expression());
        }
        return invalid_argument(resource,
                                "Incorrect arguments for the compare union expression. Should be bool expression");
    }

    core::result_wrapper_t<std::string> expression_factory_t::convert_to_string(const expression_wrapper_t& expr) {
        if (expr.is_key()) {
            return expr.key().as_string();
        }
        if (expr.is_expression()) {
            return expr.expression()->to_string();
        }
        // parameter
        const auto& value = this->values.at(expr.parameter());
        return util::logical_value_to_string(value);
    }

    core::parameter_id_t expression_factory_t::add_value(components::types::logical_value_t&& value) {
        auto param = core::parameter_id_t(counter);
        counter++;
        values.emplace(param, value);
        return param;
    }

    components::logical_plan::parameter_node_ptr expression_factory_t::get_params() {
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
                    params->add_parameter(param.first, std::string(param.second.value<std::string_view>()));
                    break;
                default:
                    throw std::runtime_error("Couldn\'t convert logical value to document value");
            }
        }
        return params;
    }
} // namespace otterbrix
