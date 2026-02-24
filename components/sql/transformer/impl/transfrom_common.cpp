#include <components/expressions/function_expression.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {
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
                auto comp_type = get_compare_type(strVal(node->name->lst.front().data));

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

} // namespace components::sql::transform
