#include <components/expressions/scalar_expression.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

using namespace components::expressions;

namespace components::sql::transform {

    core::result_wrapper_t<std::pmr::vector<expressions::expression_ptr>>
    transformer::transform_returning(List* returning_list,
                                     const name_collection_t& names,
                                     logical_plan::execution_plan_t* plan) {
        std::pmr::vector<expressions::expression_ptr> out{resource_};
        if (!returning_list) {
            return out;
        }
        for (auto target : returning_list->lst) {
            auto* res = pg_ptr_cast<ResTarget>(target.data);
            if (nodeTag(res->val) == T_ColumnRef) {
                auto* col_ref = pg_ptr_cast<ColumnRef>(res->val);
                // RETURNING *
                if (col_ref->fields->lst.size() == 1 && nodeTag(col_ref->fields->lst.back().data) == T_A_Star) {
                    out.push_back(
                        make_scalar_expression(resource_, scalar_type::star_expand, expressions::key_t{resource_}));
                    continue;
                }
                if (nodeTag(col_ref->fields->lst.back().data) == T_A_Star) {
                    VALUE_OR_RETURN(auto col, columnref_to_field(resource_, col_ref, names));
                    // RETURNING table.* — carry the table qualifier so the validator
                    // can expand it by result_alias.
                    if (!col.table.empty()) {
                        std::pmr::vector<std::pmr::string> star_path{resource_};
                        star_path.emplace_back(std::pmr::string{col.table, resource_});
                        star_path.emplace_back(std::pmr::string{"*", resource_});
                        out.push_back(make_scalar_expression(resource_,
                                                             scalar_type::star_expand,
                                                             expressions::key_t{std::move(star_path)}));
                        continue;
                    }
                }
            }
            VALUE_OR_RETURN(auto operand, transform_expression(res->val, expression_context_t{names, plan}));
            const bool reads_column = std::holds_alternative<expressions::key_t>(operand);
            if (res->name) {
                expressions::key_t out_key{resource_, res->name};
                if (reads_column) {
                    // Carry the deduced side onto the output-alias key so the
                    // validator resolves the column against the right schema.
                    out_key.set_side(std::get<expressions::key_t>(operand).side());
                }
                auto expr = as_expression(std::move(operand));
                expr->key() = std::move(out_key);
                out.push_back(std::move(expr));
                continue;
            }
            // Unaliased, a bare column names its output after the column it reads.
            if (reads_column) {
                out.push_back(
                    make_scalar_expression(resource_, scalar_type::get_field, std::get<expressions::key_t>(operand)));
                continue;
            }
            out.push_back(as_expression(std::move(operand)));
        }
        return out;
    }

} // namespace components::sql::transform
