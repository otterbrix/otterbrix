#include <components/logical_plan/node_function.hpp>
#include <components/sql/transformer/transformer.hpp>

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
                return strVal(pg_ptr_cast<ColumnRef>(node)->fields->lst.back().data);
            case T_ParamRef:
                return "$" + std::to_string(pg_ptr_cast<ParamRef>(node)->number);
        }
        return {};
    }

    document::value_t transformer::get_value(Node* node, document::impl::base_document* tape) {
        switch (nodeTag(node)) {
            case T_TypeCast: {
                auto cast = pg_ptr_cast<TypeCast>(node);
                bool is_true = std::string(strVal(&pg_ptr_cast<A_Const>(cast->arg)->val)) == "t";
                return document::value_t(tape, is_true);
            }
            case T_A_Const: {
                auto value = &(pg_ptr_cast<A_Const>(node)->val);
                switch (nodeTag(value)) {
                    case T_String: {
                        std::string str = strVal(value);
                        return document::value_t(tape, str);
                    }
                    case T_Integer:
                        return document::value_t(tape, intVal(value));
                    case T_Float:
                        return document::value_t(tape, static_cast<float>(floatVal(value)));
                }
            }
            case T_ColumnRef:
                return document::value_t(tape, strVal(pg_ptr_cast<ColumnRef>(node)->fields->lst.back().data));
        }
        return document::value_t(tape, nullptr);
    }

    core::parameter_id_t transformer::add_param_value(Node* node, logical_plan::parameter_node_t* params) {
        if (nodeTag(node) == T_ParamRef) {
            auto ref = pg_ptr_cast<ParamRef>(node);
            if (auto it = parameter_map_.find(ref->number); it != parameter_map_.end()) {
                return it->second;
            } else {
                auto id = params->add_parameter(document::value_t(params->parameters().tape(), nullptr));
                parameter_map_.emplace(ref->number, id);
                return id;
            }
        }

        return params->add_parameter(get_value(node, params->parameters().tape()));
    }

    compare_expression_ptr transformer::transform_a_expr(logical_plan::parameter_node_t* params,
                                                         A_Expr* node,
                                                         logical_plan::node_ptr* func_node) {
        switch (node->kind) {
            case AEXPR_AND: // fall-through
            case AEXPR_OR: {
                compare_expression_ptr left;
                compare_expression_ptr right;
                if (nodeTag(node->lexpr) == T_A_Expr) {
                    left = transform_a_expr(params, pg_ptr_cast<A_Expr>(node->lexpr));
                } else if (nodeTag(node->lexpr) == T_A_Indirection) {
                    left = transform_a_indirection(params, pg_ptr_cast<A_Indirection>(node->lexpr));
                } else {
                    auto func = pg_ptr_cast<FuncCall>(node->lexpr);
                    *func_node = transform_function(*func, params);
                    if (nodeTag(node->rexpr) == T_A_Expr) {
                        return transform_a_expr(params, pg_ptr_cast<A_Expr>(node->rexpr));
                    } else {
                        return transform_a_indirection(params, pg_ptr_cast<A_Indirection>(node->rexpr));
                    }
                }
                if (nodeTag(node->rexpr) == T_A_Expr) {
                    right = transform_a_expr(params, pg_ptr_cast<A_Expr>(node->rexpr));
                } else if (nodeTag(node->rexpr) == T_A_Indirection) {
                    right = transform_a_indirection(params, pg_ptr_cast<A_Indirection>(node->rexpr));
                } else {
                    auto func = pg_ptr_cast<FuncCall>(node->rexpr);
                    *func_node = transform_function(*func, params);
                    return left;
                }
                auto expr = make_compare_union_expression(params->parameters().resource(),
                                                          node->kind == AEXPR_AND ? compare_type::union_and
                                                                                  : compare_type::union_or);
                auto append = [&expr](compare_expression_ptr& e) {
                    if (expr->type() == e->type()) {
                        for (auto& child : e->children()) {
                            expr->append_child(child);
                        }
                    } else {
                        expr->append_child(e);
                    }
                };
                append(left);
                append(right);
                return expr;
            }
            case AEXPR_OP: {
                if (nodeTag(node) == T_A_Indirection) {
                    return transform_a_indirection(params, pg_ptr_cast<A_Indirection>(node));
                }
                if (nodeTag(node->lexpr) == T_ColumnRef) {
                    auto key_left = strVal(pg_ptr_cast<ColumnRef>(node->lexpr)->fields->lst.back().data);
                    if (nodeTag(node->rexpr) == T_ColumnRef) {
                        auto key_right = strVal(pg_ptr_cast<ColumnRef>(node->rexpr)->fields->lst.back().data);
                        return make_compare_expression(params->parameters().resource(),
                                                       get_compare_type(strVal(node->name->lst.front().data)),
                                                       components::expressions::key_t{key_left},
                                                       components::expressions::key_t{key_right});
                    }
                    return make_compare_expression(params->parameters().resource(),
                                                   get_compare_type(strVal(node->name->lst.front().data)),
                                                   // TODO: deduce expression side
                                                   side_t::undefined,
                                                   components::expressions::key_t{key_left},
                                                   add_param_value(node->rexpr, params));
                } else {
                    if (nodeTag(node->rexpr) != T_ColumnRef) {
                        throw parser_exception_t(
                            {"Unsupported expression: at least one side must be a column reference"},
                            {});
                    }

                    auto key = strVal(pg_ptr_cast<ColumnRef>(node->rexpr)->fields->lst.back().data);
                    return make_compare_expression(params->parameters().resource(),
                                                   get_compare_type(strVal(node->name->lst.back().data)),
                                                   // TODO: deduce expression side
                                                   side_t::undefined,
                                                   components::expressions::key_t{key},
                                                   add_param_value(node->rexpr, params));
                }
            }
            case AEXPR_NOT: {
                assert(nodeTag(node->rexpr) == T_A_Expr || nodeTag(node->rexpr) == T_A_Indirection);
                compare_expression_ptr right;
                if (nodeTag(node->rexpr) == T_A_Expr) {
                    right = transform_a_expr(params, pg_ptr_cast<A_Expr>(node->rexpr));
                } else if (nodeTag(node->rexpr) == T_A_Indirection) {
                    right = transform_a_indirection(params, pg_ptr_cast<A_Indirection>(node->rexpr));
                } else {
                    auto func = pg_ptr_cast<FuncCall>(node->rexpr);
                    *func_node = transform_function(*func, params);
                    return right;
                }
                auto expr = make_compare_union_expression(params->parameters().resource(), compare_type::union_not);
                if (expr->type() == right->type()) {
                    for (auto& child : right->children()) {
                        expr->append_child(child);
                    }
                } else {
                    expr->append_child(right);
                }
                return expr;
            }
            default:
                throw std::runtime_error("Unsupported node type: " + expr_kind_to_string(node->kind));
        }
    }
    components::expressions::compare_expression_ptr
    transformer::transform_a_indirection(logical_plan::parameter_node_t* params, A_Indirection* node) {
        if (node->arg->type == T_A_Expr) {
            return transform_a_expr(params, pg_ptr_cast<A_Expr>(node->arg));
        } else if (node->arg->type == T_A_Indirection) {
            return transform_a_indirection(params, pg_ptr_cast<A_Indirection>(node->arg));
        } else {
            throw std::runtime_error("Unsupported node type: " + node_tag_to_string(node->type));
        }
    }

    logical_plan::node_ptr transformer::transform_function(RangeFunction& node,
                                                           logical_plan::parameter_node_t* params) {
        auto func_call = pg_ptr_cast<FuncCall>(pg_ptr_cast<List>(node.functions->lst.front().data)->lst.front().data);
        return transform_function(*func_call, params);
    }

    logical_plan::node_ptr transformer::transform_function(FuncCall& node, logical_plan::parameter_node_t* params) {
        std::string funcname = strVal(node.funcname->lst.front().data);
        std::pmr::vector<core::parameter_id_t> args;
        args.reserve(node.args->lst.size());
        for (const auto& arg : node.args->lst) {
            args.emplace_back(add_param_value(pg_ptr_cast<Node>(arg.data), params));
        }
        return logical_plan::make_node_function(params->parameters().resource(), std::move(funcname), std::move(args));
    }

} // namespace components::sql::transform
