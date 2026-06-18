#include "substrait_expression_importer.hpp"

#include <variant>
#include <vector>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>

namespace components::logical_plan::substrait_adapter {

    from_substrait_context_t::from_substrait_context_t(std::pmr::memory_resource* resource, import_profile_t profile_)
        : params(make_parameter_node(resource))
        , profile(profile_) {}

    bool from_substrait_context_t::is_external_canonical() const {
        return profile == import_profile_t::external_canonical;
    }

    std::string get_function_name(const from_substrait_context_t& ctx, uint32_t ref) {
        auto it = ctx.functions.find(ref);
        if (it == ctx.functions.end()) {
            return "";
        }
        return it->second;
    }

    components::types::logical_value_t logical_value_from_literal(std::pmr::memory_resource* resource,
                                                                  const substrait::Expression_Literal& literal) {
        using components::types::logical_value_t;
        using components::types::complex_logical_type;
        using components::types::logical_type;
        if (literal.has_null()) {
            return logical_value_t{resource, logical_type::NA};
        }
        switch (literal.literal_type_case()) {
            case substrait::Expression_Literal::kBoolean:
                return logical_value_t{resource, literal.boolean()};
            case substrait::Expression_Literal::kI8:
                return logical_value_t{resource, static_cast<int8_t>(literal.i8())};
            case substrait::Expression_Literal::kI16:
                return logical_value_t{resource, static_cast<int16_t>(literal.i16())};
            case substrait::Expression_Literal::kI32:
                return logical_value_t{resource, static_cast<int32_t>(literal.i32())};
            case substrait::Expression_Literal::kI64:
                return logical_value_t{resource, static_cast<int64_t>(literal.i64())};
            case substrait::Expression_Literal::kFp32:
                return logical_value_t{resource, static_cast<float>(literal.fp32())};
            case substrait::Expression_Literal::kFp64:
                return logical_value_t{resource, static_cast<double>(literal.fp64())};
            case substrait::Expression_Literal::kString:
                return logical_value_t{resource, literal.string()};
            case substrait::Expression_Literal::kVarChar:
                return logical_value_t{resource, literal.var_char().value()};
            case substrait::Expression_Literal::kBinary:
                return logical_value_t{resource, literal.binary()};
            case substrait::Expression_Literal::kList: {
                std::vector<logical_value_t> values;
                values.reserve(static_cast<size_t>(literal.list().values_size()));
                for (const auto& item : literal.list().values()) {
                    values.emplace_back(logical_value_from_literal(resource, item));
                }
                auto child_type = values.empty() ? complex_logical_type(logical_type::NA) : values.front().type();
                return logical_value_t::create_list(resource, child_type, values);
            }
            case substrait::Expression_Literal::kStruct: {
                std::vector<logical_value_t> fields;
                fields.reserve(static_cast<size_t>(literal.struct_().fields_size()));
                for (const auto& field : literal.struct_().fields()) {
                    fields.emplace_back(logical_value_from_literal(resource, field));
                }
                return logical_value_t::create_struct(resource, "struct", fields);
            }
            case substrait::Expression_Literal::kMap: {
                std::vector<logical_value_t> keys;
                std::vector<logical_value_t> values;
                keys.reserve(static_cast<size_t>(literal.map().key_values_size()));
                values.reserve(static_cast<size_t>(literal.map().key_values_size()));
                for (const auto& item : literal.map().key_values()) {
                    keys.emplace_back(item.has_key() ? logical_value_from_literal(resource, item.key())
                                                     : logical_value_t{resource, logical_type::NA});
                    values.emplace_back(item.has_value() ? logical_value_from_literal(resource, item.value())
                                                        : logical_value_t{resource, logical_type::NA});
                }
                auto key_type = keys.empty() ? complex_logical_type(logical_type::NA) : keys.front().type();
                auto value_type = values.empty() ? complex_logical_type(logical_type::NA) : values.front().type();
                return logical_value_t::create_map(resource, key_type, value_type, keys, values);
            }
            case substrait::Expression_Literal::kEmptyList:
                return logical_value_t::create_list(resource, complex_logical_type(logical_type::NA), {});
            case substrait::Expression_Literal::kEmptyMap:
                return logical_value_t::create_map(resource,
                                                   complex_logical_type(logical_type::NA),
                                                   complex_logical_type(logical_type::NA),
                                                   {},
                                                   {});
            default:
                return logical_value_t{resource, logical_type::NA};
        }
    }

    expressions::param_storage param_from_expression(std::pmr::memory_resource* resource,
                                                     const substrait::Expression& expr,
                                                     const field_mapping_t& mapping,
                                                     from_substrait_context_t& ctx) {
        if (expr.has_selection() && expr.selection().has_direct_reference() &&
            expr.selection().direct_reference().has_struct_field()) {
            auto idx = expr.selection().direct_reference().struct_field().field();
            if (mapping.contains(idx)) {
                auto key = expressions::key_t(resource, mapping.name_or_empty(idx));
                if (mapping.left_size >= 0) {
                    key.set_side(idx < mapping.left_size ? expressions::side_t::left : expressions::side_t::right);
                }
                return key;
            }
        }
        if (expr.has_dynamic_parameter()) {
            return core::parameter_id_t{static_cast<uint16_t>(expr.dynamic_parameter().parameter_reference())};
        }
        if (expr.has_literal()) {
            auto id = ctx.params->next_id();
            ctx.params->add_parameter(id, logical_value_from_literal(resource, expr.literal()));
            return id;
        }
        return expressions::key_t(resource);
    }

    expressions::expression_ptr expression_with_alias(std::pmr::memory_resource* resource,
                                                      const expressions::expression_ptr& expr,
                                                      const std::string& alias) {
        using namespace components::expressions;
        if (!expr || alias.empty()) {
            return expr;
        }
        auto key = key_t(resource, alias);
        switch (expr->group()) {
            case expression_group::scalar: {
                auto* scalar = static_cast<const scalar_expression_t*>(expr.get());
                auto out = make_scalar_expression(resource, scalar->type(), key);
                for (const auto& p : scalar->params()) {
                    out->append_param(p);
                }
                return out;
            }
            case expression_group::aggregate: {
                auto* aggr = static_cast<const aggregate_expression_t*>(expr.get());
                auto out = make_aggregate_expression(resource, aggr->function_name(), key);
                out->set_distinct(aggr->is_distinct());
                for (const auto& p : aggr->params()) {
                    out->append_param(p);
                }
                return out;
            }
            default:
                return expr;
        }
    }

    expressions::expression_ptr expression_from_substrait(std::pmr::memory_resource* resource,
                                                          const substrait::Expression& expr,
                                                          const field_mapping_t& mapping,
                                                          from_substrait_context_t& ctx) {
        using namespace components::expressions;
        switch (expr.rex_type_case()) {
            case substrait::Expression::kSelection: {
                if (expr.selection().has_direct_reference() &&
                    expr.selection().direct_reference().has_struct_field()) {
                    auto idx = expr.selection().direct_reference().struct_field().field();
                    if (mapping.contains(idx)) {
                        auto key = key_t(resource, mapping.name_or_empty(idx));
                        if (mapping.left_size >= 0) {
                            key.set_side(idx < mapping.left_size ? side_t::left : side_t::right);
                        }
                        auto expr_out = make_scalar_expression(resource, scalar_type::get_field);
                        expr_out->append_param(key);
                        return expr_out;
                    }
                }
                break;
            }
            case substrait::Expression::kDynamicParameter: {
                auto expr_out = make_scalar_expression(resource, scalar_type::get_field);
                expr_out->append_param(
                    core::parameter_id_t{static_cast<uint16_t>(expr.dynamic_parameter().parameter_reference())});
                return expr_out;
            }
            case substrait::Expression::kLiteral: {
                auto expr_out = make_scalar_expression(resource, scalar_type::get_field);
                expr_out->append_param(param_from_expression(resource, expr, mapping, ctx));
                return expr_out;
            }
            case substrait::Expression::kScalarFunction: {
                auto func_name = get_function_name(ctx, expr.scalar_function().function_reference());
                if (func_name == "and" || func_name == "or" || func_name == "not") {
                    auto type = func_name == "and"   ? compare_type::union_and
                                : func_name == "or" ? compare_type::union_or
                                                     : compare_type::union_not;
                    auto out = make_compare_union_expression(resource, type);
                    for (const auto& arg : expr.scalar_function().arguments()) {
                        out->append_child(expression_from_substrait(resource, arg.value(), mapping, ctx));
                    }
                    return out;
                }
                if (func_name == "eq" || func_name == "ne" || func_name == "gt" || func_name == "lt" ||
                    func_name == "gte" || func_name == "lte" || func_name == "regex") {
                    compare_type type = compare_type::invalid;
                    if (func_name == "eq")
                        type = compare_type::eq;
                    else if (func_name == "ne")
                        type = compare_type::ne;
                    else if (func_name == "gt")
                        type = compare_type::gt;
                    else if (func_name == "lt")
                        type = compare_type::lt;
                    else if (func_name == "gte")
                        type = compare_type::gte;
                    else if (func_name == "lte")
                        type = compare_type::lte;
                    else if (func_name == "regex")
                        type = compare_type::regex;

                    if (expr.scalar_function().arguments_size() >= 2) {
                        auto left =
                            param_from_expression(resource, expr.scalar_function().arguments(0).value(), mapping, ctx);
                        auto right =
                            param_from_expression(resource, expr.scalar_function().arguments(1).value(), mapping, ctx);
                        return make_compare_expression(resource, type, left, right);
                    }
                }

                auto scalar_kind = get_scalar_type(func_name);
                if (scalar_kind != scalar_type::invalid) {
                    auto out = make_scalar_expression(resource, scalar_kind);
                    for (const auto& arg : expr.scalar_function().arguments()) {
                        out->append_param(param_from_expression(resource, arg.value(), mapping, ctx));
                    }
                    return out;
                }
                if (is_known_aggregate_function(func_name)) {
                    auto out = make_aggregate_expression(resource, normalize_aggregate_function_name(func_name));
                    if (expr.scalar_function().arguments_size() > 0) {
                        for (const auto& arg : expr.scalar_function().arguments()) {
                            out->append_param(param_from_expression(resource, arg.value(), mapping, ctx));
                        }
                    }
                    return out;
                }
                break;
            }
            default:
                break;
        }
        return make_scalar_expression(resource, scalar_type::get_field);
    }

} // namespace components::logical_plan::substrait_adapter
