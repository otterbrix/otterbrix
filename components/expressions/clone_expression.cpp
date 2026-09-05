#include "clone_expression.hpp"

#include "aggregate_expression.hpp"
#include "cast_expression.hpp"
#include "compare_expression.hpp"
#include "function_expression.hpp"
#include "scalar_expression.hpp"
#include "sort_expression.hpp"

#include <cassert>

namespace components::expressions {

    namespace {

        // A clone must own everything it holds ON `resource` — that is the whole reason
        // clone_expression exists: the clone is meant to outlive the arena it was cloned FROM.
        // So a key operand is PLACED on `resource` with the allocator-extended copy. A plain
        // copy would leave it on the process default (safe, but off the arena the caller named),
        // and copying it with the SOURCE's allocator would bind the clone to the very arena it
        // was cloned away from. parameter_id_t is a scalar with no arena at all, so it is copied
        // as is; a nested expression operand is cloned recursively so the copy owns its subtree.
        param_storage clone_param(std::pmr::memory_resource* resource, const param_storage& param) {
            if (is_expr(param)) {
                return param_storage{clone_expression(resource, as_expr(param))};
            }
            if (is_key(param)) {
                return param_storage{key_t{as_key(param), resource}};
            }
            return param;
        }

    } // namespace

    expression_ptr clone_expression(std::pmr::memory_resource* resource, const expression_ptr& expr) {
        if (!expr) {
            return nullptr;
        }
        expression_ptr copy;
        switch (expr->group()) {
            case expression_group::compare: {
                const auto* src = static_cast<const compare_expression_t*>(expr.get());
                auto dst = make_compare_expression(resource,
                                                   src->type(),
                                                   clone_param(resource, src->left()),
                                                   clone_param(resource, src->right()));
                dst->set_key(src->key());
                dst->set_inner_op(src->inner_op());
                if (src->do_not_fold()) {
                    dst->make_unfoldable();
                }
                dst->set_regex_flags(src->regex_flags_param());
                dst->add_function_uid(src->function_uid());
                for (const auto& child : src->children()) {
                    dst->append_child(clone_expression(resource, child));
                }
                copy = std::move(dst);
                break;
            }
            case expression_group::scalar: {
                const auto* src = static_cast<const scalar_expression_t*>(expr.get());
                auto dst = make_scalar_expression(resource, src->type(), src->key());
                for (const auto& param : src->params()) {
                    dst->append_param(clone_param(resource, param));
                }
                copy = std::move(dst);
                break;
            }
            case expression_group::aggregate: {
                const auto* src = static_cast<const aggregate_expression_t*>(expr.get());
                auto dst = make_aggregate_expression(resource, src->function_name(), src->key());
                dst->add_function_uid(src->function_uid());
                dst->set_distinct(src->is_distinct());
                dst->set_mergeable(src->is_mergeable());
                for (const auto& param : src->params()) {
                    dst->append_param(clone_param(resource, param));
                }
                copy = std::move(dst);
                break;
            }
            case expression_group::sort: {
                const auto* src = static_cast<const sort_expression_t*>(expr.get());
                copy = make_sort_expression(resource,
                                            clone_param(resource, src->operand()),
                                            src->order(),
                                            src->null_order());
                copy->key() = src->key();
                break;
            }
            case expression_group::function: {
                const auto* src = static_cast<const function_expression_t*>(expr.get());
                std::pmr::vector<param_storage> args{resource};
                args.reserve(src->args().size());
                for (const auto& arg : src->args()) {
                    args.push_back(clone_param(resource, arg));
                }
                auto dst = make_function_expression(resource, std::string{src->name()}, std::move(args));
                dst->set_key(src->key());
                dst->add_function_uid(src->function_uid());
                copy = std::move(dst);
                break;
            }
            case expression_group::cast: {
                const auto* src = static_cast<const cast_expression_t*>(expr.get());
                copy = make_cast_expression(resource,
                                            clone_param(resource, src->child()),
                                            src->result_type(),
                                            src->cast(),
                                            src->kind());
                break;
            }
            case expression_group::invalid: {
                // No expression_i subclass carries group invalid — unreachable.
                assert(false);
                return nullptr;
            }
        }
        copy->set_result_alias(expr->result_alias());
        copy->set_result_type(expr->result_type());
        return copy;
    }

} // namespace components::expressions
