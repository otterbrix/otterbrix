#include "expression_equivalence.hpp"

#include "aggregate_expression.hpp"
#include "cast_expression.hpp"
#include "compare_expression.hpp"
#include "function_expression.hpp"
#include "scalar_expression.hpp"
#include "sort_expression.hpp"

#include <algorithm>
#include <cassert>

namespace components::expressions {

    namespace {

        bool same_expression(const expression_i* lhs,
                             const expression_i* rhs,
                             const types::parameter_map_t& parameters,
                             bool is_root);

        bool same_value(core::parameter_id_t lhs, core::parameter_id_t rhs, const types::parameter_map_t& parameters) {
            if (lhs == rhs) {
                return true;
            }
            const auto left = parameters.find(lhs);
            const auto right = parameters.find(rhs);
            assert(left != parameters.end() && right != parameters.end());
            return left->second.type() == right->second.type() && left->second == right->second;
        }

        bool same_param(const param_storage& lhs, const param_storage& rhs, const types::parameter_map_t& parameters) {
            if (is_expr(lhs) || is_expr(rhs)) {
                return is_expr(lhs) && is_expr(rhs) &&
                       same_expression(as_expr(lhs).get(), as_expr(rhs).get(), parameters, false);
            }
            if (is_parameter(lhs) || is_parameter(rhs)) {
                return is_parameter(lhs) && is_parameter(rhs) &&
                       same_value(as_parameter(lhs), as_parameter(rhs), parameters);
            }
            return as_key(lhs) == as_key(rhs);
        }

        bool same_params(const std::pmr::vector<param_storage>& lhs,
                         const std::pmr::vector<param_storage>& rhs,
                         const types::parameter_map_t& parameters) {
            return lhs.size() == rhs.size() &&
                   std::equal(lhs.begin(), lhs.end(), rhs.begin(), [&parameters](const auto& left, const auto& right) {
                       return same_param(left, right, parameters);
                   });
        }

        // Nodes whose key() carries identity rather than an output label
        bool key_is_identity(const expression_i* expr) {
            if (expr->group() != expression_group::scalar) {
                return false;
            }
            const auto* scalar = static_cast<const scalar_expression_t*>(expr);
            switch (scalar->type()) {
                case scalar_type::get_field:
                case scalar_type::group_field:
                    return scalar->params().empty();
                case scalar_type::jsonb_expand:
                case scalar_type::jsonb_delete:
                    return true;
                default:
                    return false;
            }
        }

        bool same_expression(const expression_i* lhs,
                             const expression_i* rhs,
                             const types::parameter_map_t& parameters,
                             bool is_root) {
            if (lhs == rhs) {
                return true;
            }
            if (lhs == nullptr || rhs == nullptr || lhs->group() != rhs->group()) {
                return false;
            }
            // Only the ROOT wears a label
            const bool compare_key = !is_root || key_is_identity(lhs) || key_is_identity(rhs);
            if (compare_key && lhs->key() != rhs->key()) {
                return false;
            }
            switch (lhs->group()) {
                case expression_group::compare: {
                    const auto* left = static_cast<const compare_expression_t*>(lhs);
                    const auto* right = static_cast<const compare_expression_t*>(rhs);
                    // The regex flags field defaults to id 0, which next_id() also hands out as a
                    // real parameter, so an unset one is indistinguishable from a bound value and
                    // must not be dereferenced. Compare it by id, as equality always has.
                    return left->type() == right->type() && left->inner_op() == right->inner_op() &&
                           left->regex_flags_param() == right->regex_flags_param() &&
                           same_param(left->left(), right->left(), parameters) &&
                           same_param(left->right(), right->right(), parameters) &&
                           left->children().size() == right->children().size() &&
                           std::equal(
                               left->children().begin(),
                               left->children().end(),
                               right->children().begin(),
                               [&parameters](const auto& left_child, const auto& right_child) {
                                   return same_expression(left_child.get(), right_child.get(), parameters, false);
                               });
                }
                case expression_group::scalar: {
                    const auto* left = static_cast<const scalar_expression_t*>(lhs);
                    const auto* right = static_cast<const scalar_expression_t*>(rhs);
                    return left->type() == right->type() && same_params(left->params(), right->params(), parameters);
                }
                case expression_group::aggregate: {
                    const auto* left = static_cast<const aggregate_expression_t*>(lhs);
                    const auto* right = static_cast<const aggregate_expression_t*>(rhs);
                    // DISTINCT is part of the value
                    return left->function_name() == right->function_name() &&
                           left->is_distinct() == right->is_distinct() &&
                           same_params(left->params(), right->params(), parameters);
                }
                case expression_group::function: {
                    const auto* left = static_cast<const function_expression_t*>(lhs);
                    const auto* right = static_cast<const function_expression_t*>(rhs);
                    return left->name() == right->name() && same_params(left->args(), right->args(), parameters);
                }
                case expression_group::sort: {
                    const auto* left = static_cast<const sort_expression_t*>(lhs);
                    const auto* right = static_cast<const sort_expression_t*>(rhs);
                    return left->order() == right->order() && left->null_order() == right->null_order() &&
                           same_param(left->operand(), right->operand(), parameters);
                }
                case expression_group::cast: {
                    const auto* left = static_cast<const cast_expression_t*>(lhs);
                    const auto* right = static_cast<const cast_expression_t*>(rhs);
                    return left->kind() == right->kind() && left->result_type() == right->result_type() &&
                           same_param(left->child(), right->child(), parameters);
                }
                case expression_group::invalid: {
                    // should be unreachable
                    assert(false);
                    return false;
                }
            }
            return false;
        }

    } // namespace

    bool
    same_computation(const expression_ptr& lhs, const expression_ptr& rhs, const types::parameter_map_t& parameters) {
        return same_expression(lhs.get(), rhs.get(), parameters, true);
    }

} // namespace components::expressions
