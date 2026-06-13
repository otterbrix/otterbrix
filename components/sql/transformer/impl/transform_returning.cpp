#include <components/expressions/scalar_expression.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {

    std::pmr::vector<expressions::expression_ptr>
    transformer::transform_returning(List* returning_list,
                                     const name_collection_t& names,
                                     logical_plan::execution_plan_t* plan) {
        std::pmr::vector<expressions::expression_ptr> out{resource_};
        if (!returning_list) {
            return out;
        }
        for (auto target : returning_list->lst) {
            auto* res = pg_ptr_cast<ResTarget>(target.data);
            switch (nodeTag(res->val)) {
                case T_ColumnRef: {
                    auto* col_ref = pg_ptr_cast<ColumnRef>(res->val);
                    // RETURNING *
                    if (col_ref->fields->lst.size() == 1 && nodeTag(col_ref->fields->lst.back().data) == T_A_Star) {
                        out.push_back(
                            make_scalar_expression(resource_, scalar_type::star_expand, expressions::key_t{resource_}));
                        break;
                    }
                    auto col = columnref_to_field(resource_, col_ref, names);
                    // RETURNING table.* — carry the table qualifier so the validator
                    // can expand it by result_alias.
                    if (nodeTag(col_ref->fields->lst.back().data) == T_A_Star && !col.table.empty()) {
                        std::pmr::vector<std::pmr::string> star_path{resource_};
                        star_path.emplace_back(std::pmr::string{col.table, resource_});
                        star_path.emplace_back(std::pmr::string{"*", resource_});
                        out.push_back(make_scalar_expression(resource_,
                                                             scalar_type::star_expand,
                                                             expressions::key_t{std::move(star_path)}));
                        break;
                    }
                    col.deduce_side(names);
                    if (res->name) {
                        // Carry the deduced side onto the output-alias key so the
                        // validator resolves the column against the right schema.
                        expressions::key_t out_key{resource_, res->name};
                        out_key.set_side(col.field.side());
                        out.push_back(
                            make_scalar_expression(resource_, scalar_type::get_field, std::move(out_key), col.field));
                    } else {
                        out.push_back(make_scalar_expression(resource_, scalar_type::get_field, col.field));
                    }
                    break;
                }
                case T_A_Expr: {
                    auto* a_expr = pg_ptr_cast<A_Expr>(res->val);
                    if (a_expr->kind == AEXPR_OP && a_expr->name && !a_expr->name->lst.empty() &&
                        is_arithmetic_operator(strVal(a_expr->name->lst.front().data))) {
                        auto expr = transform_a_expr_arithmetic(a_expr, names, plan->parameters.get());
                        if (error_.contains_error()) {
                            return out;
                        }
                        if (res->name) {
                            static_cast<scalar_expression_t*>(expr.get())->key() =
                                expressions::key_t{resource_, res->name};
                        }
                        out.push_back(std::move(expr));
                        break;
                    }
                    error_ = core::error_t(core::error_code_t::unimplemented_yet,
                                           std::pmr::string{"unsupported expression in RETURNING clause", resource_});
                    return out;
                }
                case T_A_Const:
                case T_TypeCast:
                case T_ParamRef: {
                    auto expr = make_scalar_expression(resource_,
                                                       scalar_type::constant,
                                                       res->name ? expressions::key_t{resource_, res->name}
                                                                 : expressions::key_t{resource_});
                    expr->append_param(add_param_value(res->val, plan->parameters.get()));
                    out.push_back(std::move(expr));
                    break;
                }
                default:
                    error_ = core::error_t(core::error_code_t::unimplemented_yet,
                                           std::pmr::string{"unsupported expression in RETURNING clause", resource_});
                    return out;
            }
        }
        return out;
    }

} // namespace components::sql::transform
