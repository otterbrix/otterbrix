#pragma once

#include <cstdint>
#include <memory_resource>
#include <type_traits>
#include <vector>

#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>

namespace components::planner::optimizer {

    // Split a predicate tree into its top-level AND-conjuncts. A `union_and` node
    // flattens (recursively) into its children; any other expression is a single
    // conjunct. The result vector is built on `resource`. Shared by
    // pushdown_filter and promote_cross_join.
    inline std::pmr::vector<components::expressions::expression_ptr>
    split_conjuncts(std::pmr::memory_resource* resource, const components::expressions::expression_ptr& expr) {
        using namespace components::expressions;
        std::pmr::vector<expression_ptr> result{resource};
        if (!expr) {
            return result;
        }
        if (expr->group() == expression_group::compare) {
            auto* cmp = static_cast<compare_expression_t*>(expr.get());
            if (cmp->type() == compare_type::union_and) {
                for (const auto& child : cmp->children()) {
                    auto sub = split_conjuncts(resource, child);
                    result.insert(result.end(), sub.begin(), sub.end());
                }
                return result;
            }
        }
        result.push_back(expr);
        return result;
    }

    // Recombine conjuncts into one predicate: nullptr when empty, the single element
    // when size 1, otherwise a `union_and` over all of them. Built on `resource`
    // on the caller's resource.
    inline components::expressions::expression_ptr
    rebuild_conjunction(std::pmr::memory_resource* resource,
                        const std::pmr::vector<components::expressions::expression_ptr>& conjuncts) {
        using namespace components::expressions;
        if (conjuncts.empty()) {
            return nullptr;
        }
        if (conjuncts.size() == 1) {
            return conjuncts.front();
        }
        auto conj = make_compare_union_expression(resource, compare_type::union_and);
        for (const auto& c : conjuncts) {
            conj->append_child(c);
        }
        return conj;
    }

    // The column a join key reads: one top-level column, bare or under a single cast. nullptr for anything
    // else, which the rules that rewrite join keys leave alone.
    inline const components::expressions::key_t*
    join_key_operand(const components::expressions::param_storage& operand) {
        using namespace components::expressions;
        const param_storage* column = &operand;
        if (is_expr(operand)) {
            const auto& expr = as_expr(operand);
            if (!expr || expr->group() != expression_group::cast) {
                return nullptr;
            }
            column = &static_cast<const cast_expression_t*>(expr.get())->child();
        }
        if (!is_key(*column) || as_key(*column).path().size() != 1) {
            return nullptr;
        }
        return &as_key(*column);
    }

    inline components::expressions::key_t* join_key_operand(components::expressions::param_storage& operand) {
        using namespace components::expressions;
        param_storage* column = &operand;
        if (is_expr(operand)) {
            auto& expr = as_expr(operand);
            if (!expr || expr->group() != expression_group::cast) {
                return nullptr;
            }
            column = &static_cast<cast_expression_t*>(expr.get())->child();
        }
        if (!is_key(*column) || as_key(*column).path().size() != 1) {
            return nullptr;
        }
        return &as_key(*column);
    }

    template<typename Visitor>
    bool walk_columns(const components::expressions::expression_ptr& expr, Visitor& visitor);

    template<typename Visitor>
    bool walk_columns(components::expressions::param_storage& operand, Visitor& visitor) {
        using namespace components::expressions;
        if (is_key(operand)) {
            visitor.column(as_key(operand));
            return true;
        }
        return !is_expr(operand) || walk_columns(as_expr(operand), visitor);
    }

    template<typename Visitor>
    bool walk_columns(std::pmr::vector<components::expressions::param_storage>& operands, Visitor& visitor) {
        for (auto& operand : operands) {
            if (!walk_columns(operand, visitor)) {
                return false;
            }
        }
        return true;
    }

    // Hands every column an expression reads to `visitor.column(key_t&)` and every function call to
    // `visitor.call(function_expression_t&)`. Only a plain get_field / group_field reads through its own key;
    // every other own key is an output name. False when the expression reads columns it cannot enumerate.
    // Every expression group is one case of the switch, so a new group does not compile until it is handled.
    template<typename Visitor>
    bool walk_columns(const components::expressions::expression_ptr& expr, Visitor& visitor) {
        using namespace components::expressions;
        if (!expr) {
            return true;
        }
        switch (expr->group()) {
            case expression_group::compare: {
                auto* cmp = static_cast<compare_expression_t*>(expr.get());
                if (is_union_compare_condition(cmp->type())) {
                    for (auto& child : cmp->children()) {
                        if (!walk_columns(child, visitor)) {
                            return false;
                        }
                    }
                    return true;
                }
                return walk_columns(cmp->left(), visitor) && walk_columns(cmp->right(), visitor);
            }
            case expression_group::scalar: {
                auto* scalar = static_cast<scalar_expression_t*>(expr.get());
                switch (scalar->type()) {
                    case scalar_type::get_field:
                    case scalar_type::group_field:
                        if (scalar->params().empty()) {
                            visitor.column(scalar->key());
                            return true;
                        }
                        return walk_columns(scalar->params(), visitor);
                    case scalar_type::constant:
                        return true;
                    case scalar_type::star_expand:
                    case scalar_type::jsonb_expand:
                    case scalar_type::jsonb_delete:
                        return false;
                    default:
                        return walk_columns(scalar->params(), visitor);
                }
            }
            case expression_group::aggregate:
                return walk_columns(static_cast<aggregate_expression_t*>(expr.get())->params(), visitor);
            case expression_group::function: {
                auto* call = static_cast<function_expression_t*>(expr.get());
                visitor.call(*call);
                return walk_columns(call->args(), visitor);
            }
            case expression_group::cast:
                return walk_columns(static_cast<cast_expression_t*>(expr.get())->child(), visitor);
            case expression_group::sort:
                return walk_columns(static_cast<sort_expression_t*>(expr.get())->operand(), visitor);
            case expression_group::invalid:
                return false;
        }
        return false;
    }

    inline bool is_volatile_call(const components::expressions::function_expression_t& call) noexcept {
        return call.volatility() == components::compute::volatility_t::volatile_;
    }

    // The one input a join operand reads, as PostgreSQL's check_hashjoinable asks: at least one column, every
    // column from the same input (`input_of(column key)`, `none` when it is in none), and no volatile call.
    // `none` for anything else, including an operand that reads no column at all.
    template<typename InputOf>
    size_t operand_input(components::expressions::param_storage& operand, InputOf&& input_of, size_t none) {
        struct visitor_t {
            std::remove_reference_t<InputOf>* input_of;
            size_t none;
            size_t input;
            bool valid;

            void column(components::expressions::key_t& key) {
                const bool resolved = !key.path().empty() && key.path()[0] != SIZE_MAX;
                const components::expressions::key_t& column = key;
                const size_t at = resolved ? (*input_of)(column) : none;
                if (at == none || (input != none && input != at)) {
                    valid = false;
                }
                input = at;
            }
            void call(const components::expressions::function_expression_t& call) {
                valid = valid && !is_volatile_call(call);
            }
        };
        visitor_t visitor{&input_of, none, none, true};
        if (!walk_columns(operand, visitor) || !visitor.valid) {
            return none;
        }
        return visitor.input;
    }

} // namespace components::planner::optimizer
