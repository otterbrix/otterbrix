#pragma once

#include "aggregate_expression.hpp"
#include "compare_expression.hpp"
#include "function_expression.hpp"
#include "scalar_expression.hpp"

#include <components/compute/function.hpp>

// Shared UDF-boundary rule for pushdown decisions. Used by the aggregate-pushdown
// optimizer rule (components/planner/optimizer/rules/pushdown_aggregate.cpp) and by
// the disk-filter lowering (components/physical_plan_generator/impl/create_plan_match.cpp)
// so the "what counts as a UDF" rule cannot drift between them.

namespace components::expressions {

    // A user-defined function is any resolved function_uid at or beyond the builtin
    // set. register_default_functions registers EXACTLY the DEFAULT_FUNCTIONS entries
    // (uids [0, N)); the owning/disk agent rebuilds its registry with the SAME
    // register_default_functions and NOTHING else (it has no access to the
    // coordinator's UDF registrations). So a uid >= N is a UDF the agent cannot
    // resolve — building the pushed fragment there would look up a null function
    // pointer and deref it. invalid_function_uid (unresolved) is NOT treated as a
    // UDF: it is left to the coordinator's normal resolution.
    inline bool is_udf_uid(compute::function_uid uid) noexcept {
        return uid != compute::invalid_function_uid && uid >= compute::DEFAULT_FUNCTIONS.size();
    }

    inline bool expr_references_udf(const expression_ptr& expr);

    // param_storage is variant<parameter_id_t, key_t, expression_ptr>; only the
    // nested-expression alternative can carry a further function reference.
    inline bool param_references_udf(const param_storage& param) {
        return is_expr(param) && expr_references_udf(as_expr(param));
    }

    // True iff any function_expression / aggregate_expression in the tree is a
    // UDF (or one of their nested argument expressions is). Recurses through
    // function/aggregate/scalar params and compare children/operands. R14: tag
    // via group() + static_cast (NO dynamic_cast).
    inline bool expr_references_udf(const expression_ptr& expr) {
        if (!expr) {
            return false;
        }
        switch (expr->group()) {
            case expression_group::function: {
                const auto* f = static_cast<const function_expression_t*>(expr.get());
                if (is_udf_uid(f->function_uid())) {
                    return true;
                }
                for (const auto& a : f->args()) {
                    if (param_references_udf(a)) {
                        return true;
                    }
                }
                return false;
            }
            case expression_group::aggregate: {
                const auto* a = static_cast<const aggregate_expression_t*>(expr.get());
                if (is_udf_uid(a->function_uid())) {
                    return true;
                }
                for (const auto& p : a->params()) {
                    if (param_references_udf(p)) {
                        return true;
                    }
                }
                return false;
            }
            case expression_group::scalar: {
                const auto* s = static_cast<const scalar_expression_t*>(expr.get());
                for (const auto& p : s->params()) {
                    if (param_references_udf(p)) {
                        return true;
                    }
                }
                return false;
            }
            case expression_group::compare: {
                const auto* c = static_cast<const compare_expression_t*>(expr.get());
                if (param_references_udf(c->left()) || param_references_udf(c->right())) {
                    return true;
                }
                for (const auto& child : c->children()) {
                    if (expr_references_udf(child)) {
                        return true;
                    }
                }
                return false;
            }
            default:
                return false;
        }
    }

} // namespace components::expressions
