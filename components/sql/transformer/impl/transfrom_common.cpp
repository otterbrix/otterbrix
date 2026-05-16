#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>


using namespace components::expressions;

namespace components::sql::transform {

    expression_ptr transformer::transform_a_expr_arithmetic(A_Expr* node,
                                                            const name_collection_t& names,
                                                            logical_plan::parameter_node_t* params) {
        auto op_str = std::string_view(strVal(node->name->lst.front().data));
        auto stype = get_arithmetic_scalar_type(op_str);
        if (stype == scalar_type::invalid) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"invalid arithmetics operator", resource_});
            return nullptr;
        }

        auto expr = make_scalar_expression(resource_, stype);

        if (node->lexpr) {
            expr->append_param(transform_a_expr_operand(node->lexpr, names, params));
            expr->append_param(transform_a_expr_operand(node->rexpr, names, params));
        } else {
            // Unary minus: proper unary operator with single operand
            expr = make_scalar_expression(resource_, scalar_type::unary_minus);
            expr->append_param(transform_a_expr_operand(node->rexpr, names, params));
        }
        return expr;
    }

    param_storage transformer::transform_a_expr_operand(Node* node,
                                                        const name_collection_t& names,
                                                        logical_plan::parameter_node_t* params) {
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
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"Unsupported A_Expr in arithmetic operand", resource_});
                return nullptr;
            }
            case T_FuncCall:
                return transform_a_expr_func(pg_ptr_cast<FuncCall>(node), names, params);
            default:
                error_ =
                    core::error_t(core::error_code_t::sql_parse_error,
                                  std::pmr::string{"Unsupported operand type in arithmetic expression", resource_});
                return nullptr;
        }
    }

    void transformer::transform_select_a_expr(A_Expr* node,
                                              const char* alias,
                                              const name_collection_t& names,
                                              logical_plan::parameter_node_t* params,
                                              logical_plan::node_ptr& group) {
        auto op_str = std::string_view(strVal(node->name->lst.front().data));
        if (!is_arithmetic_operator(op_str)) {
            error_ =
                core::error_t(core::error_code_t::sql_parse_error,
                              std::pmr::string{"Unsupported operator in SELECT: " + std::string(op_str), resource_});
            return;
        }
        std::string expr_name = alias ? alias : std::string(op_str);
        scalar_expression_ptr expr;

        if (node->lexpr) {
            auto stype = get_arithmetic_scalar_type(op_str);
            if (stype == scalar_type::invalid) {
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"invalid arithmetics operand", resource_});
                return;
            }
            expr = make_scalar_expression(resource_, stype, expressions::key_t{resource_, std::move(expr_name)});
            expr->append_param(resolve_select_operand(node->lexpr, names, params, group));
            expr->append_param(resolve_select_operand(node->rexpr, names, params, group));
        } else {
            // Unary minus: proper unary operator with single operand
            expr = make_scalar_expression(resource_,
                                          scalar_type::unary_minus,
                                          expressions::key_t{resource_, std::move(expr_name)});
            expr->append_param(resolve_select_operand(node->rexpr, names, params, group));
        }

        group->append_expression(expr);
    }

    param_storage transformer::resolve_select_operand(Node* node,
                                                      const name_collection_t& names,
                                                      logical_plan::parameter_node_t* params,
                                                      logical_plan::node_ptr& group) {
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
            case T_TypeCast: {
                auto cast = pg_ptr_cast<TypeCast>(node);
                if (cast->arg && nodeTag(cast->arg) == T_ColumnRef) {
                    auto target_type_res = get_type(resource_, cast->typeName);
                    if (target_type_res.has_error()) {
                        error_ = target_type_res.error();
                        return nullptr;
                    }
                    auto col_ref = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(cast->arg), names);
                    col_ref.deduce_side(names);
                    col_ref.field.set_cast_type(target_type_res.value());
                    return col_ref.field;
                }
                return add_param_value(node, params);
            }
            case T_ParamRef:
            case T_A_Const:
                return add_param_value(node, params);
            case T_A_Expr: {
                auto sub_expr = pg_ptr_cast<A_Expr>(node);
                if (sub_expr->kind == AEXPR_OP) {
                    auto sub_op = std::string_view(strVal(sub_expr->name->lst.front().data));
                    if (is_arithmetic_operator(sub_op)) {
                        auto sub_stype = get_arithmetic_scalar_type(sub_op);
                        if (sub_stype == scalar_type::invalid) {
                            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                                   std::pmr::string{"invalid arithmetics operand", resource_});
                            return nullptr;
                        }
                        auto sub_scalar = make_scalar_expression(resource_, sub_stype);
                        if (sub_expr->lexpr) {
                            sub_scalar->append_param(resolve_select_operand(sub_expr->lexpr, names, params, group));
                        } else {
                            auto zero_id = params->add_parameter(types::logical_value_t(resource_, int64_t(0)));
                            sub_scalar->append_param(zero_id);
                        }
                        sub_scalar->append_param(resolve_select_operand(sub_expr->rexpr, names, params, group));
                        return sub_scalar;
                    }
                }
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"Unsupported A_Expr in SELECT operand", resource_});
                return nullptr;
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
                        } else if (nodeTag(arg_node) == T_CaseExpr) {
                            // CASE WHEN ... inside aggregate arg, e.g. SUM(CASE ...)
                            args.emplace_back(
                                case_expr_to_scalar(pg_ptr_cast<CaseExpr>(arg_node), nullptr, names, params, group));
                        } else {
                            args.emplace_back(add_param_value(arg_node, params));
                        }
                    }
                }

                // Create aggregate with auto-generated alias
                // TODO: default aggregate aliases should come from function registry, not hardcoded here
                std::string auto_alias = "__agg_" + funcname + "_" + std::to_string(aggregate_counter_++);
                auto agg_expr =
                    make_aggregate_expression(resource_, funcname, expressions::key_t{resource_, auto_alias});
                for (auto& arg : args) {
                    agg_expr->append_param(arg);
                }
                pending_internal_aggs_.push_back(agg_expr);

                // Return key referencing the aggregate result
                return expressions::key_t{resource_, auto_alias};
            }
            default:
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"Unsupported operand type in SELECT arithmetic", resource_});
                return nullptr;
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
        error_ = core::error_t(core::error_code_t::sql_parse_error,
                               std::pmr::string{"incorrect string value in get_str_value", resource_});
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

        if (auto res = get_value(resource_, node); res.has_error()) {
            error_ = res.error();
            return core::parameter_id_t{};
        } else {
            return params->add_parameter(std::move(res.value()));
        }
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
                    } else if (nodeTag(node) == T_NullTest) {
                        child_expr = transform_null_test(pg_ptr_cast<NullTest>(node), names, params);
                    } else {
                        error_ = core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"Unsupported expression: unknown expr type in transform_a_expr",
                                             resource_});
                        return;
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
                if (!node->name || nodeTag(node->name->lst.front().data) != T_String) {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,
                                           std::pmr::string{"Unsupported expr in transform_a_exr", resource_});
                    return nullptr;
                }
                auto op_str = std::string_view(strVal(node->name->lst.front().data));

                // Check if this is arithmetic (+, -, *, /, %)
                if (is_arithmetic_operator(op_str)) {
                    return transform_a_expr_arithmetic(node, names, params);
                }

                // Check for LIKE / NOT LIKE
                if (op_str == "~~" || op_str == "!~~") {
                    column_ref_t key_left(resource_);
                    if (nodeTag(node->lexpr) == T_ColumnRef) {
                        key_left = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node->lexpr), names);
                    } else if (nodeTag(node->lexpr) == T_A_Indirection) {
                        key_left = indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(node->lexpr), names);
                    } else {
                        error_ =
                            core::error_t(core::error_code_t::sql_parse_error,

                                          std::pmr::string{"LIKE: left side must be a column reference", resource_});
                        return nullptr;
                    }
                    key_left.deduce_side(names);
                    auto raw_val = get_value(resource_, node->rexpr);
                    if (raw_val.has_error()) {
                        error_ = raw_val.error();
                        return nullptr;
                    }
                    auto pattern = like_to_regex(std::string(raw_val.value().value<std::string_view>()));
                    auto param_id = params->add_parameter(types::logical_value_t(resource_, pattern));
                    if (op_str == "!~~") {
                        auto inner = make_compare_expression(params->parameters().resource(),
                                                             compare_type::regex,
                                                             key_left.field,
                                                             param_id);
                        auto not_expr =
                            make_compare_union_expression(params->parameters().resource(), compare_type::union_not);
                        not_expr->append_child(inner);
                        return not_expr;
                    }
                    return make_compare_expression(params->parameters().resource(),
                                                   compare_type::regex,
                                                   key_left.field,
                                                   param_id);
                }

                auto comp_type = get_compare_type(op_str);
                if (comp_type == compare_type::invalid) {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,

                                           std::pmr::string{"invalid compare operand", resource_});
                    return nullptr;
                }

                auto get_arg = [this, &names, &params](Node* node) -> param_storage {
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
                        case T_TypeCast: {
                            auto cast = pg_ptr_cast<TypeCast>(node);
                            if (cast->arg && nodeTag(cast->arg) == T_ColumnRef) {
                                auto target_type_res = get_type(resource_, cast->typeName);
                                if (target_type_res.has_error()) {
                                    error_ = target_type_res.error();
                                    return nullptr;
                                }
                                auto col_ref =
                                    columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(cast->arg), names);
                                col_ref.deduce_side(names);
                                col_ref.field.set_cast_type(target_type_res.value());
                                return col_ref.field;
                            }
                            return add_param_value(node, params);
                        }
                        case T_ParamRef:
                        case T_A_Const:
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
                            error_ = core::error_t(
                                core::error_code_t::sql_parse_error,

                                std::pmr::string{"unrecognized expression in transform_a_expr", resource_});
                            return nullptr;
                        }
                        case T_MinMaxExpr: {
                            auto expr = pg_ptr_cast<MinMaxExpr>(node);
                            std::string funcname = expr->op == MinMaxOp::IS_GREATEST ? "greatest" : "least";
                            std::pmr::vector<param_storage> args{resource_};
                            args.reserve(expr->args->lst.size());
                            for (const auto& arg : expr->args->lst) {
                                if (nodeTag(arg.data) == T_ColumnRef) {
                                    auto key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(arg.data), names);
                                    key.deduce_side(names);
                                    args.emplace_back(std::move(key.field));
                                } else if (nodeTag(arg.data) == T_A_Indirection) {
                                    auto key =
                                        indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(arg.data), names);
                                    key.deduce_side(names);
                                    args.emplace_back(std::move(key.field));
                                } else if (nodeTag(arg.data) == T_FuncCall) {
                                    args.emplace_back(
                                        transform_a_expr_func(pg_ptr_cast<FuncCall>(arg.data), names, params));
                                } else if (nodeTag(arg.data) == T_A_Expr) {
                                    auto sub = pg_ptr_cast<A_Expr>(arg.data);
                                    if (sub->kind == AEXPR_OP &&
                                        is_arithmetic_operator(strVal(sub->name->lst.front().data))) {
                                        args.emplace_back(transform_a_expr_arithmetic(sub, names, params));
                                    } else {
                                        args.emplace_back(add_param_value(pg_ptr_cast<Node>(arg.data), params));
                                    }
                                } else {
                                    args.emplace_back(add_param_value(pg_ptr_cast<Node>(arg.data), params));
                                }
                            }
                            return make_function_expression(params->parameters().resource(),
                                                            std::move(funcname),
                                                            std::move(args));
                        }
                        default:
                            error_ = core::error_t(core::error_code_t::sql_parse_error,

                                                   std::pmr::string{"Unsupported expression", resource_});
                            return nullptr;
                    }
                };

                param_storage left = get_arg(node->lexpr);
                param_storage right = get_arg(node->rexpr);
                return make_compare_expression(params->parameters().resource(), comp_type, left, right);
            }
            case AEXPR_NOT: {
                if (nodeTag(node->rexpr) != T_A_Expr && nodeTag(node->rexpr) != T_A_Indirection) {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,

                                           std::pmr::string{"Unsupported expr type in transform_a_expr", resource_});
                    return nullptr;
                }
                expression_ptr right;
                if (nodeTag(node->rexpr) == T_A_Expr) {
                    right = transform_a_expr(pg_ptr_cast<A_Expr>(node->rexpr), names, params);
                } else if (nodeTag(node->rexpr) == T_A_Indirection) {
                    right = transform_a_indirection(pg_ptr_cast<A_Indirection>(node->rexpr), names, params);
                } else if (nodeTag(node->rexpr) == T_FuncCall) {
                    right = transform_a_expr_func(pg_ptr_cast<FuncCall>(node->rexpr), names, params);
                } else {
                    error_ = core::error_t(
                        core::error_code_t::sql_parse_error,

                        std::pmr::string{"Unsupported expression: unknown expr type in transform_a_expr", resource_});
                    return nullptr;
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
            case AEXPR_IN: {
                // col IN (1,2,3) → union_or(col=1, col=2, col=3)
                // col NOT IN (1,2,3) → union_and(col<>1, col<>2, col<>3)
                if (nodeTag(node->lexpr) != T_ColumnRef && nodeTag(node->lexpr) != T_A_Indirection) {
                    error_ = core::error_t(
                        core::error_code_t::sql_parse_error,

                        std::pmr::string{"IN expression: left side must be a column reference", resource_});
                    return nullptr;
                }
                auto key_in = nodeTag(node->lexpr) == T_ColumnRef
                                  ? columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node->lexpr), names)
                                  : indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(node->lexpr), names);
                key_in.deduce_side(names);

                auto op_str = std::string(strVal(node->name->lst.front().data));
                bool is_not_in = (op_str == "<>");
                auto union_type = is_not_in ? compare_type::union_and : compare_type::union_or;
                auto cmp_type = is_not_in ? compare_type::ne : compare_type::eq;

                auto list_node = pg_ptr_cast<List>(node->rexpr);
                auto union_expr = make_compare_union_expression(params->parameters().resource(), union_type);
                for (const auto& elem : list_node->lst) {
                    auto param_id = add_param_value(pg_ptr_cast<Node>(elem.data), params);
                    union_expr->append_child(
                        make_compare_expression(params->parameters().resource(), cmp_type, key_in.field, param_id));
                }
                return union_expr;
            }
            default:
                error_ = core::error_t(
                    core::error_code_t::sql_parse_error,

                    std::pmr::string{"Unsupported node type: " + expr_kind_to_string(node->kind), resource_});
                return nullptr;
        }
    }

    expression_ptr transformer::transform_a_expr_func(FuncCall* node,
                                                      const name_collection_t& names,
                                                      logical_plan::parameter_node_t* params) {
        std::string funcname = strVal(node->funcname->lst.front().data);
        std::pmr::vector<param_storage> args{resource_};
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
            error_ =
                core::error_t(core::error_code_t::sql_parse_error,

                              std::pmr::string{"Unsupported node type: " + node_tag_to_string(node->type), resource_});
            return nullptr;
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
        std::pmr::vector<param_storage> args{resource_};
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

    expression_ptr transformer::case_expr_to_scalar(CaseExpr* node,
                                                    const char* alias,
                                                    const name_collection_t& names,
                                                    logical_plan::parameter_node_t* params,
                                                    logical_plan::node_ptr group) {
        std::string expr_name = alias ? alias : "case_" + std::to_string(aggregate_counter_++);
        auto expr = make_scalar_expression(resource_,
                                           scalar_type::case_expr,
                                           expressions::key_t{resource_, std::move(expr_name)});

        // Process WHEN clauses: params layout is [cond1, result1, cond2, result2, ..., default]
        for (auto& arg : node->args->lst) {
            auto when = pg_ptr_cast<CaseWhen>(arg.data);

            // Condition
            if (node->arg) {
                // Simple CASE: CASE col WHEN val THEN ... → generate equality: col = val
                auto col_key = columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node->arg), names);
                col_key.deduce_side(names);
                auto param_id = add_param_value(pg_ptr_cast<Node>(when->expr), params);
                auto cond =
                    make_compare_expression(params->parameters().resource(), compare_type::eq, col_key.field, param_id);
                expr->append_param(expression_ptr(cond));
            } else {
                // Searched CASE: CASE WHEN condition THEN ... → boolean expression
                auto cond_node = pg_ptr_cast<Node>(when->expr);
                if (nodeTag(cond_node) == T_A_Expr) {
                    auto condition = transform_a_expr(pg_ptr_cast<A_Expr>(cond_node), names, params);
                    expr->append_param(condition);
                } else if (nodeTag(cond_node) == T_FuncCall) {
                    auto condition = transform_a_expr_func(pg_ptr_cast<FuncCall>(cond_node), names, params);
                    expr->append_param(condition);
                } else {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,

                                           std::pmr::string{"Unsupported WHEN condition type", resource_});
                    return nullptr;
                }
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

        return expr;
    }

    void transformer::transform_select_case_expr(CaseExpr* node,
                                                 const char* alias,
                                                 const name_collection_t& names,
                                                 logical_plan::parameter_node_t* params,
                                                 logical_plan::node_ptr& group) {
        auto expr = case_expr_to_scalar(node, alias, names, params, group);
        if (expr) {
            group->append_expression(expr);
        }
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
                        if (stype == scalar_type::invalid) {
                            error_ = core::error_t(core::error_code_t::sql_parse_error,

                                                   std::pmr::string{"invalid arithmetics operand", resource_});
                            return nullptr;
                        }
                        auto expr = make_scalar_expression(resource_, stype);
                        if (sub->lexpr) {
                            expr->append_param(resolve_having_operand(sub->lexpr, names, params, group));
                            expr->append_param(resolve_having_operand(sub->rexpr, names, params, group));
                        } else {
                            // Unary minus: proper unary operator with single operand
                            expr = make_scalar_expression(resource_, scalar_type::unary_minus);
                            expr->append_param(resolve_having_operand(sub->rexpr, names, params, group));
                        }
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
                    if (comp_type == compare_type::invalid) {
                        error_ = core::error_t(core::error_code_t::sql_parse_error,

                                               std::pmr::string{"invalid comparison operand", resource_});
                        return nullptr;
                    }
                    auto left = resolve_having_operand(a_expr->lexpr, names, params, group);
                    auto right = resolve_having_operand(a_expr->rexpr, names, params, group);
                    return make_compare_expression(params->parameters().resource(), comp_type, left, right);
                }
            } else if (a_expr->kind == AEXPR_AND || a_expr->kind == AEXPR_OR) {
                auto expr = make_compare_union_expression(params->parameters().resource(),
                                                          a_expr->kind == AEXPR_AND ? compare_type::union_and
                                                                                    : compare_type::union_or);
                expr->append_child(transform_having_expr(a_expr->lexpr, names, params, group));
                expr->append_child(transform_having_expr(a_expr->rexpr, names, params, group));
                return expr;
            }
        }
        error_ = core::error_t(core::error_code_t::sql_parse_error,

                               std::pmr::string{"Unsupported expression in HAVING clause", resource_});
        return nullptr;
    }

    expression_ptr transformer::transform_null_test(NullTest* node,
                                                    const name_collection_t& names,
                                                    logical_plan::parameter_node_t* params) {
        if (nodeTag(node->arg) != T_ColumnRef && nodeTag(node->arg) != T_A_Indirection) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,

                                   std::pmr::string{"IS NULL: argument must be a column reference", resource_});
            return nullptr;
        }
        auto key = nodeTag(node->arg) == T_ColumnRef
                       ? columnref_to_field(resource_, pg_ptr_cast<ColumnRef>(node->arg), names)
                       : indirection_to_field(resource_, pg_ptr_cast<A_Indirection>(node->arg), names);
        key.deduce_side(names);

        auto cmp = node->nulltesttype == IS_NULL ? compare_type::is_null : compare_type::is_not_null;
        // is_null/is_not_null don't need a value, use a dummy parameter
        auto param_id = params->add_parameter(
            types::logical_value_t(resource_, types::complex_logical_type{types::logical_type::NA}));
        return make_compare_expression(params->parameters().resource(), cmp, key.field, param_id);
    }

} // namespace components::sql::transform
