#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {

    expression_ptr transformer::transform_a_expr_arithmetic(
        A_Expr* node, const name_collection_t& names, logical_plan::parameter_node_t* params) {
        auto op_str = std::string_view(strVal(node->name->lst.front().data));
        auto stype = get_arithmetic_scalar_type(op_str);

        auto expr = make_scalar_expression(resource_, stype);

        if (node->lexpr) {
            expr->append_param(transform_a_expr_operand(node->lexpr, names, params));
        } else {
            // Unary minus: 0 - x
            auto zero_id = params->add_parameter(types::logical_value_t(resource_, int64_t(0)));
            expr->append_param(zero_id);
        }
        expr->append_param(transform_a_expr_operand(node->rexpr, names, params));
        return expr;
    }

    param_storage transformer::transform_a_expr_operand(
        Node* node, const name_collection_t& names, logical_plan::parameter_node_t* params) {
        switch (nodeTag(node)) {
            case T_ColumnRef: {
                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node), names);
                key.deduce_side(names);
                return key.field;
            }
            case T_A_Indirection: {
                auto key = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(node), names);
                key.deduce_side(names);
                return key.field;
            }
            case T_ParamRef:
            case T_A_Const:
            case T_TypeCast:
            case T_RowExpr:
            case T_A_ArrayExpr:
                return add_param_value(node, params);
            case T_A_Expr: {
                auto sub_expr = pg_ptr_cast<A_Expr>(node);
                if (sub_expr->kind == AEXPR_OP) {
                    auto sub_op = std::string_view(strVal(sub_expr->name->lst.front().data));
                    if (is_arithmetic_operator(sub_op)) {
                        return transform_a_expr_arithmetic(sub_expr, names, params);
                    }
                }
                throw parser_exception_t{"Unsupported A_Expr in arithmetic operand", ""};
            }
            case T_FuncCall:
                return transform_a_expr_func(pg_ptr_cast<FuncCall>(node), names, params);
            default:
                throw parser_exception_t{"Unsupported operand type in arithmetic expression", ""};
        }
    }

    void transformer::transform_select_a_expr(
        A_Expr* node, const char* alias, const name_collection_t& names,
        logical_plan::parameter_node_t* params, logical_plan::node_ptr& group) {
        auto op_str = std::string_view(strVal(node->name->lst.front().data));
        if (!is_arithmetic_operator(op_str)) {
            throw parser_exception_t{"Unsupported operator in SELECT: " + std::string(op_str), ""};
        }
        auto stype = get_arithmetic_scalar_type(op_str);

        std::string expr_name = alias ? alias : std::string(op_str);
        auto expr = make_scalar_expression(resource_, stype, expressions::key_t{resource_, std::move(expr_name)});

        if (node->lexpr) {
            expr->append_param(resolve_select_operand(node->lexpr, names, params, group));
        } else {
            // Unary minus
            auto zero_id = params->add_parameter(types::logical_value_t(resource_, int64_t(0)));
            expr->append_param(zero_id);
        }
        expr->append_param(resolve_select_operand(node->rexpr, names, params, group));

        group->append_expression(expr);
    }

    param_storage transformer::resolve_select_operand(
        Node* node, const name_collection_t& names,
        logical_plan::parameter_node_t* params, logical_plan::node_ptr& group) {
        switch (nodeTag(node)) {
            case T_ColumnRef: {
                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node), names);
                key.deduce_side(names);
                return key.field;
            }
            case T_A_Indirection: {
                auto key = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(node), names);
                key.deduce_side(names);
                return key.field;
            }
            case T_ParamRef:
            case T_A_Const:
            case T_TypeCast:
                return add_param_value(node, params);
            case T_A_Expr: {
                auto sub_expr = pg_ptr_cast<A_Expr>(node);
                if (sub_expr->kind == AEXPR_OP) {
                    auto sub_op = std::string_view(strVal(sub_expr->name->lst.front().data));
                    if (is_arithmetic_operator(sub_op)) {
                        auto sub_stype = get_arithmetic_scalar_type(sub_op);
                        auto sub_scalar = make_scalar_expression(resource_, sub_stype);
                        if (sub_expr->lexpr) {
                            sub_scalar->append_param(
                                resolve_select_operand(sub_expr->lexpr, names, params, group));
                        } else {
                            auto zero_id = params->add_parameter(types::logical_value_t(resource_, int64_t(0)));
                            sub_scalar->append_param(zero_id);
                        }
                        sub_scalar->append_param(
                            resolve_select_operand(sub_expr->rexpr, names, params, group));
                        return sub_scalar;
                    }
                }
                throw parser_exception_t{"Unsupported A_Expr in SELECT operand", ""};
            }
            case T_FuncCall: {
                // In SELECT context, FuncCall is an aggregate
                auto func = pg_ptr_cast<FuncCall>(node);
                auto funcname = std::string{strVal(linitial(func->funcname))};

                std::pmr::vector<param_storage> args(resource_);
                if (!func->agg_star) {
                    args.reserve(func->args->lst.size());
                    for (const auto& arg : func->args->lst) {
                        auto arg_node = pg_ptr_cast<Node>(arg.data);
                        if (nodeTag(arg_node) == T_ColumnRef) {
                            auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(arg_node), names);
                            key.deduce_side(names);
                            args.emplace_back(std::move(key.field));
                        } else if (nodeTag(arg_node) == T_A_Expr) {
                            auto sub = pg_ptr_cast<A_Expr>(arg_node);
                            if (sub->kind == AEXPR_OP && is_arithmetic_operator(strVal(sub->name->lst.front().data))) {
                                args.emplace_back(resolve_select_operand(arg_node, names, params, group));
                            } else {
                                args.emplace_back(add_param_value(arg_node, params));
                            }
                        } else {
                            args.emplace_back(add_param_value(arg_node, params));
                        }
                    }
                }

                // Create aggregate with auto-generated alias
                std::string auto_alias = "__agg_" + funcname + "_" + std::to_string(aggregate_counter_++);
                auto agg_expr = make_aggregate_expression(resource_, funcname,
                                                           expressions::key_t{resource_, auto_alias});
                for (auto& arg : args) {
                    agg_expr->append_param(arg);
                }
                group->append_expression(agg_expr);

                // Return key referencing the aggregate result
                return expressions::key_t{resource_, auto_alias};
            }
            default:
                throw parser_exception_t{"Unsupported operand type in SELECT arithmetic", ""};
        }
    }
    std::string transformer::get_str_value(Node* node) {
        switch (nodeTag(node)) {
            case T_TypeCast: {
                auto cast = pg_ptr_cast<TypeCast>(node);
                bool is_true = std::string(strVal(&pg_ptr_cast<A_Const>(cast->arg)->val)) == "t";
                return is_true ? "true" : "false";
            }
            case T_A_Const: {
                auto value = &(pg_ptr_cast<A_Const>(node)->val);
                switch (nodeTag(value)) {
                    case T_String:
                        return strVal(value);
                    case T_Integer:
                        return std::to_string(intVal(value));
                    case T_Float:
                        return strVal(value);
                }
            }
            case T_ColumnRef:
                assert(false);
                return strVal(pg_ptr_cast<ColumnRef>(node)->fields->lst.back().data);
            case T_ParamRef:
                return "$" + std::to_string(pg_ptr_cast<ParamRef>(node)->number);
        }
        return {};
    }

    core::parameter_id_t transformer::add_param_value(Node* node, logical_plan::parameter_node_t* params) {
        if (nodeTag(node) == T_ParamRef) {
            auto ref = pg_ptr_cast<ParamRef>(node);
            if (auto it = parameter_map_.find(ref->number); it != parameter_map_.end()) {
                return it->second;
            } else {
                auto id = params->add_parameter(
                    types::logical_value_t(resource_, types::complex_logical_type{types::logical_type::NA}));
                parameter_map_.emplace(ref->number, id);
                return id;
            }
        }

        return params->add_parameter(get_value(resource_, node));
    }

    expression_ptr transformer::transform_a_expr(A_Expr* node,
                                                 const name_collection_t& names,
                                                 logical_plan::parameter_node_t* params) {
        switch (node->kind) {
            case AEXPR_AND: // fall-through
            case AEXPR_OR: {
                auto expr = make_compare_union_expression(params->parameters().resource(),
                                                          node->kind == AEXPR_AND ? compare_type::union_and
                                                                                  : compare_type::union_or);
                auto append = [this, &params, &expr, &names](Node* node) {
                    expression_ptr child_expr;
                    if (nodeTag(node) == T_A_Expr) {
                        child_expr = transform_a_expr(pg_ptr_cast<A_Expr>(node), names, params);
                    } else if (nodeTag(node) == T_A_Indirection) {
                        child_expr = transform_a_indirection(pg_ptr_cast<A_Indirection>(node), names, params);
                    } else if (nodeTag(node) == T_FuncCall) {
                        child_expr = transform_a_expr_func(pg_ptr_cast<FuncCall>(node), names, params);
                    } else {
                        throw parser_exception_t({"Unsupported expression: unknown expr type in transform_a_expr"}, {});
                    }
                    if (expr->group() == child_expr->group()) {
                        auto comp_expr = reinterpret_cast<const compare_expression_ptr&>(child_expr);
                        if (expr->type() == comp_expr->type()) {
                            for (auto& child : comp_expr->children()) {
                                expr->append_child(child);
                            }
                            return;
                        }
                    }
                    expr->append_child(child_expr);
                };

                append(node->lexpr);
                append(node->rexpr);
                return expr;
            }
            case AEXPR_OP: {
                if (nodeTag(node) == T_A_Indirection) {
                    return transform_a_indirection(pg_ptr_cast<A_Indirection>(node), names, params);
                }
                auto op_str = std::string_view(strVal(node->name->lst.front().data));

                // Check if this is arithmetic (+, -, *, /, %)
                if (is_arithmetic_operator(op_str)) {
                    return transform_a_expr_arithmetic(node, names, params);
                }

                auto comp_type = get_compare_type(op_str);

                auto get_arg = [this, &names, &params](Node* node) -> param_storage {
                    switch (nodeTag(node)) {
                        case T_ColumnRef: {
                            auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node), names);
                            key.deduce_side(names);
                            return key.field;
                        }
                        // TODO: indirection can hide every other type besides T_ColumnRef
                        case T_A_Indirection: {
                            auto key = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(node), names);
                            key.deduce_side(names);
                            return key.field;
                        }
                        case T_ParamRef:
                        case T_A_Const:
                        case T_TypeCast:
                        case T_RowExpr:
                        case T_A_ArrayExpr:
                            return add_param_value(node, params);
                        case T_FuncCall:
                            return transform_a_expr_func(pg_ptr_cast<FuncCall>(node), names, params);
                        case T_A_Expr: {
                            auto sub = pg_ptr_cast<A_Expr>(node);
                            if (sub->kind == AEXPR_OP) {
                                auto sub_op = std::string_view(strVal(sub->name->lst.front().data));
                                if (is_arithmetic_operator(sub_op)) {
                                    return transform_a_expr_arithmetic(sub, names, params);
                                }
                            }
                            return nullptr;
                        }
                        default:
                            return nullptr;
                    }
                };

                param_storage left = get_arg(node->lexpr);
                param_storage right = get_arg(node->rexpr);
                return make_compare_expression(params->parameters().resource(), comp_type, left, right);
            }
            case AEXPR_NOT: {
                assert(nodeTag(node->rexpr) == T_A_Expr || nodeTag(node->rexpr) == T_A_Indirection);
                expression_ptr right;
                if (nodeTag(node->rexpr) == T_A_Expr) {
                    right = transform_a_expr(pg_ptr_cast<A_Expr>(node->rexpr), names, params);
                } else if (nodeTag(node->rexpr) == T_A_Indirection) {
                    right = transform_a_indirection(pg_ptr_cast<A_Indirection>(node->rexpr), names, params);
                } else if (nodeTag(node->rexpr) == T_FuncCall) {
                    right = transform_a_expr_func(pg_ptr_cast<FuncCall>(node->rexpr), names, params);
                } else {
                    throw parser_exception_t({"Unsupported expression: unknown expr type in transform_a_expr"}, {});
                }
                auto expr = make_compare_union_expression(params->parameters().resource(), compare_type::union_not);
                if (expr->group() == right->group()) {
                    auto comp_expr = reinterpret_cast<const compare_expression_ptr&>(right);
                    if (expr->type() == comp_expr->type()) {
                        for (auto& child : comp_expr->children()) {
                            expr->append_child(child);
                        }
                        return expr;
                    }
                }
                expr->append_child(right);
                return expr;
            }
            default:
                throw parser_exception_t({"Unsupported node type: " + expr_kind_to_string(node->kind)}, {});
        }
    }

    expression_ptr transformer::transform_a_expr_func(FuncCall* node,
                                                      const name_collection_t& names,
                                                      logical_plan::parameter_node_t* params) {
        std::string funcname = strVal(node->funcname->lst.front().data);
        std::pmr::vector<param_storage> args;
        args.reserve(node->args->lst.size());
        for (const auto& arg : node->args->lst) {
            if (nodeTag(arg.data) == T_ColumnRef) {
                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(arg.data), names);
                key.deduce_side(names);
                args.emplace_back(std::move(key.field));
            } else if (nodeTag(arg.data) == T_A_Indirection) {
                auto key = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(arg.data), names);
                key.deduce_side(names);
                args.emplace_back(std::move(key.field));
            } else if (nodeTag(arg.data) == T_FuncCall) {
                args.emplace_back(transform_a_expr_func(pg_ptr_cast<FuncCall>(arg.data), names, params));
            } else if (nodeTag(arg.data) == T_A_Expr) {
                auto sub = pg_ptr_cast<A_Expr>(arg.data);
                if (sub->kind == AEXPR_OP && is_arithmetic_operator(strVal(sub->name->lst.front().data))) {
                    args.emplace_back(transform_a_expr_arithmetic(sub, names, params));
                } else {
                    args.emplace_back(add_param_value(pg_ptr_cast<Node>(arg.data), params));
                }
            } else {
                args.emplace_back(add_param_value(pg_ptr_cast<Node>(arg.data), params));
            }
        }
        return make_function_expression(params->parameters().resource(), std::move(funcname), std::move(args));
    }

    expression_ptr transformer::transform_a_indirection(A_Indirection* node,
                                                        const name_collection_t& names,
                                                        logical_plan::parameter_node_t* params) {
        if (node->arg->type == T_A_Expr) {
            return transform_a_expr(pg_ptr_cast<A_Expr>(node->arg), names, params);
        } else if (node->arg->type == T_A_Indirection) {
            return transform_a_indirection(pg_ptr_cast<A_Indirection>(node->arg), names, params);
        } else if (node->arg->type == T_FuncCall) {
            return transform_a_expr_func(pg_ptr_cast<FuncCall>(node->arg), names, params);
        } else {
            throw std::runtime_error("Unsupported node type: " + node_tag_to_string(node->type));
        }
    }

    logical_plan::node_ptr transformer::transform_function(RangeFunction& node,
                                                           const name_collection_t& names,
                                                           logical_plan::parameter_node_t* params) {
        auto list = pg_ptr_cast<List>(node.functions->lst.front().data);
        auto func_call = pg_ptr_cast<FuncCall>(list->lst.front().data);
        return transform_function(*func_call, names, params);
    }

    logical_plan::node_ptr transformer::transform_function(FuncCall& node,
                                                           const name_collection_t& names,
                                                           logical_plan::parameter_node_t* params) {
        std::string funcname = strVal(node.funcname->lst.front().data);
        std::pmr::vector<param_storage> args;
        args.reserve(node.args->lst.size());
        for (const auto& arg : node.args->lst) {
            if (nodeTag(arg.data) == T_ColumnRef) {
                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(arg.data), names);
                key.deduce_side(names);
                args.emplace_back(std::move(key.field));
            } else {
                args.emplace_back(add_param_value(pg_ptr_cast<Node>(arg.data), params));
            }
        }
        return logical_plan::make_node_function(params->parameters().resource(), std::move(funcname), std::move(args));
    }

    void transformer::transform_select_case_expr(
        CaseExpr* node, const char* alias, const name_collection_t& names,
        logical_plan::parameter_node_t* params, logical_plan::node_ptr& group) {
        std::string expr_name = alias ? alias : "__case_" + std::to_string(aggregate_counter_++);
        auto expr = make_scalar_expression(
            resource_, scalar_type::case_expr, expressions::key_t{resource_, std::move(expr_name)});

        // Process WHEN clauses: params layout is [cond1, result1, cond2, result2, ..., default]
        for (auto& arg : node->args->lst) {
            auto when = pg_ptr_cast<CaseWhen>(arg.data);

            // Condition: boolean expression
            auto cond_node = pg_ptr_cast<Node>(when->expr);
            if (nodeTag(cond_node) == T_A_Expr) {
                auto condition = transform_a_expr(pg_ptr_cast<A_Expr>(cond_node), names, params);
                expr->append_param(condition);
            } else if (nodeTag(cond_node) == T_FuncCall) {
                auto condition = transform_a_expr_func(pg_ptr_cast<FuncCall>(cond_node), names, params);
                expr->append_param(condition);
            } else {
                throw parser_exception_t{"Unsupported WHEN condition type", ""};
            }

            // Result: any value expression
            auto result_node = pg_ptr_cast<Node>(when->result);
            expr->append_param(resolve_select_operand(result_node, names, params, group));
        }

        // Default (ELSE clause)
        if (node->defresult) {
            auto def_node = pg_ptr_cast<Node>(node->defresult);
            expr->append_param(resolve_select_operand(def_node, names, params, group));
        }

        group->append_expression(expr);
    }

    // Resolve a HAVING operand: FuncCall → find matching aggregate alias in group
    param_storage transformer::resolve_having_operand(Node* node,
                                                       const name_collection_t& names,
                                                       logical_plan::parameter_node_t* params,
                                                       const logical_plan::node_ptr& group) {
        switch (nodeTag(node)) {
            case T_FuncCall: {
                auto func = pg_ptr_cast<FuncCall>(node);
                auto funcname = std::string{strVal(linitial(func->funcname))};
                // Find matching aggregate in group expressions
                for (const auto& expr : group->expressions()) {
                    if (expr->group() == expression_group::aggregate) {
                        auto* agg = static_cast<const aggregate_expression_t*>(expr.get());
                        if (agg->function_name() == funcname) {
                            return agg->key();
                        }
                    }
                }
                // Not found — use function name as alias
                return expressions::key_t{resource_, funcname};
            }
            case T_ColumnRef: {
                auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node), names);
                key.deduce_side(names);
                return key.field;
            }
            case T_A_Const:
            case T_ParamRef:
            case T_TypeCast:
                return add_param_value(node, params);
            case T_A_Expr: {
                auto sub = pg_ptr_cast<A_Expr>(node);
                if (sub->kind == AEXPR_OP) {
                    auto sub_op = std::string_view(strVal(sub->name->lst.front().data));
                    if (is_arithmetic_operator(sub_op)) {
                        auto stype = get_arithmetic_scalar_type(sub_op);
                        auto expr = make_scalar_expression(resource_, stype);
                        if (sub->lexpr) {
                            expr->append_param(resolve_having_operand(sub->lexpr, names, params, group));
                        } else {
                            auto zero_id = params->add_parameter(types::logical_value_t(resource_, int64_t(0)));
                            expr->append_param(zero_id);
                        }
                        expr->append_param(resolve_having_operand(sub->rexpr, names, params, group));
                        return expr;
                    }
                }
                return add_param_value(node, params);
            }
            default:
                return add_param_value(node, params);
        }
    }

    expression_ptr transformer::transform_having_expr(Node* node,
                                                       const name_collection_t& names,
                                                       logical_plan::parameter_node_t* params,
                                                       const logical_plan::node_ptr& group) {
        if (nodeTag(node) == T_A_Expr) {
            auto a_expr = pg_ptr_cast<A_Expr>(node);
            if (a_expr->kind == AEXPR_OP) {
                auto op_str = std::string_view(strVal(a_expr->name->lst.front().data));
                if (!is_arithmetic_operator(op_str)) {
                    auto comp_type = get_compare_type(op_str);
                    auto left = resolve_having_operand(a_expr->lexpr, names, params, group);
                    auto right = resolve_having_operand(a_expr->rexpr, names, params, group);
                    return make_compare_expression(params->parameters().resource(), comp_type, left, right);
                }
            } else if (a_expr->kind == AEXPR_AND || a_expr->kind == AEXPR_OR) {
                auto expr = make_compare_union_expression(
                    params->parameters().resource(),
                    a_expr->kind == AEXPR_AND ? compare_type::union_and : compare_type::union_or);
                expr->append_child(transform_having_expr(a_expr->lexpr, names, params, group));
                expr->append_child(transform_having_expr(a_expr->rexpr, names, params, group));
                return expr;
            }
        }
        throw parser_exception_t{"Unsupported expression in HAVING clause", {}};
    }

} // namespace components::sql::transform
