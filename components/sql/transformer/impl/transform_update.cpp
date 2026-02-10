#include <components/expressions/aggregate_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {
    update_expr_ptr transformer::transform_update_expr(Node* node,
                                                       const name_collection_t& names,
                                                       logical_plan::parameter_node_t* params) {
        switch (nodeTag(node)) {
            case T_TypeCast: {
                auto value = pg_ptr_cast<TypeCast>(node);
                bool is_true = std::string(strVal(&pg_ptr_cast<A_Const>(value->arg)->val)) == "t";
                core::parameter_id_t id = params->add_parameter(types::logical_value_t(is_true));
                return {new update_expr_get_const_value_t(id)};
            }
            case T_A_Const: {
                auto value = &(pg_ptr_cast<A_Const>(node)->val);
                core::parameter_id_t id;
                switch (nodeTag(value)) {
                    case T_String: {
                        std::string str = strVal(value);
                        id = params->add_parameter(types::logical_value_t(resource_, str));
                        break;
                    }
                    case T_Integer: {
                        int64_t int_value = intVal(value);
                        id = params->add_parameter(types::logical_value_t(int_value));
                        break;
                    }
                    case T_Float: {
                        float float_value = floatVal(value);
                        id = params->add_parameter(types::logical_value_t(float_value));
                        break;
                    }
                    default:
                        assert(false);
                }
                return {new update_expr_get_const_value_t(id)};
            }
            case T_A_ArrayExpr: {
                auto array = pg_ptr_cast<A_ArrayExpr>(node);
                auto id = params->add_parameter(get_array(resource_, array->elements));
                return {new update_expr_get_const_value_t(id)};
            }
            case T_ParamRef: {
                return {new update_expr_get_const_value_t(add_param_value(node, params))};
            }
            case T_A_Expr: {
                auto expr = pg_ptr_cast<A_Expr>(node);
                switch (expr->kind) {
                    case AEXPR_OP: {
                        update_expr_ptr res;
                        auto t = pg_ptr_cast<ResTarget>(expr->name->lst.front().data);
                        //sqr_root,
                        //cube_root,
                        //// bitwise:
                        //AND,
                        //OR,
                        //XOR,
                        //NOT,
                        switch (*t->name) {
                            case '+':
                                res = new update_expr_calculate_t(update_expr_type::add);
                                break;
                            case '-':
                                res = new update_expr_calculate_t(update_expr_type::sub);
                                break;
                            case '*':
                                res = new update_expr_calculate_t(update_expr_type::mult);
                                break;
                            case '/':
                                res = new update_expr_calculate_t(update_expr_type::div);
                                break;
                            case '%':
                                res = new update_expr_calculate_t(update_expr_type::mod);
                                break;
                            case '^':
                                res = new update_expr_calculate_t(update_expr_type::exp);
                                break;
                            case '!':
                                res = new update_expr_calculate_t(update_expr_type::factorial);
                                break;
                            case '@':
                                res = new update_expr_calculate_t(update_expr_type::abs);
                                break;
                            case '<':
                                res = new update_expr_calculate_t(update_expr_type::shift_left);
                                break;
                            case '>':
                                res = new update_expr_calculate_t(update_expr_type::shift_right);
                                break;
                            case '~':
                                res = new update_expr_calculate_t(update_expr_type::NOT);
                                break;
                            case '&':
                                res = new update_expr_calculate_t(update_expr_type::AND);
                                break;
                            case '#':
                                res = new update_expr_calculate_t(update_expr_type::XOR);
                                break;
                            case '|': {
                                if (*std::next(t->name) == '/') {
                                    res = new update_expr_calculate_t(update_expr_type::sqr_root);
                                } else if (*std::next(t->name) == '|') {
                                    res = new update_expr_calculate_t(update_expr_type::cube_root);
                                } else {
                                    res = new update_expr_calculate_t(update_expr_type::OR);
                                }
                                break;
                            }
                        }
                        assert(res);
                        res->left() = transform_update_expr(expr->lexpr, names, params);
                        res->right() = transform_update_expr(expr->rexpr, names, params);
                        return res;
                    }
                    default:
                        assert(false);
                }
            }
            case T_A_Indirection: {
                auto indirection = pg_ptr_cast<A_Indirection>(node);
                if (indirection->indirection->lst.empty()) {
                    return transform_update_expr(indirection->arg, names, params);
                } else {
                    auto key = indirection_to_field(resource_, indirection, names);
                    key.deduce_side(names);
                    return {new update_expr_get_value_t(std::move(key.field))};
                }
            }
            case T_ColumnRef: {
                auto ref = pg_ptr_cast<ColumnRef>(node);
                auto key = columnref_to_field(resource_, ref, names);
                key.deduce_side(names);
                return {new update_expr_get_value_t(std::move(key.field))};
            }
        }
        return nullptr;
    }

    logical_plan::node_ptr transformer::transform_update(UpdateStmt& node, logical_plan::parameter_node_t* params) {
        logical_plan::node_match_ptr match;
        std::pmr::vector<update_expr_ptr> updates(resource_);
        name_collection_t names;
        names.left_name = rangevar_to_collection(node.relation);
        names.left_alias = construct_alias(node.relation->alias);

        if (!node.fromClause->lst.empty()) {
            // has from
            auto from_first = node.fromClause->lst.front().data;
            if (nodeTag(from_first) == T_RangeVar) {
                names.right_name = rangevar_to_collection(pg_ptr_cast<RangeVar>(from_first));
                names.right_alias = construct_alias(pg_ptr_cast<RangeVar>(from_first)->alias);
            } else {
                throw parser_exception_t{"undefined token in UPDATE FROM", ""};
            }
        }
        // set
        {
            for (auto target : node.targetList->lst) {
                auto res = pg_ptr_cast<ResTarget>(target.data);
                if (res->indirection->lst.empty()) {
                    updates.emplace_back(new update_expr_set_t(expressions::key_t{resource_, res->name, side_t::left}));
                    updates.back()->left() = transform_update_expr(res->val, names, params);
                } else {
                    std::pmr::vector<std::pmr::string> path{resource_};
                    path.emplace_back(std::pmr::string{res->name, resource_});
                    for (const auto& val : res->indirection->lst) {
                        if (nodeTag(val.data) == T_A_Indices) {
                            auto indices = pg_ptr_cast<A_Indices>(val.data);
                            path.emplace_back(indices_to_str(resource_, indices));
                        } else {
                            path.emplace_back(pmrStrVal(val.data, resource_));
                        }
                    }
                    updates.emplace_back(new update_expr_set_t(expressions::key_t{std::move(path), side_t::left}));
                    updates.back()->left() = transform_update_expr(res->val, names, params);
                }
            }
        }

        // where
        if (node.whereClause) {
            match =
                logical_plan::make_node_match(resource_,
                                              names.left_name,
                                              transform_a_expr(pg_ptr_cast<A_Expr>(node.whereClause), names, params));
        } else {
            match = logical_plan::make_node_match(resource_,
                                                  names.left_name,
                                                  make_compare_expression(resource_, compare_type::all_true));
        }

        if (names.right_name.empty()) {
            return logical_plan::make_node_update_many(resource_, names.left_name, match, updates, false);
        } else {
            return logical_plan::make_node_update_many(resource_,
                                                       names.left_name,
                                                       names.right_name,
                                                       match,
                                                       updates,
                                                       false);
        }
    }
} // namespace components::sql::transform
