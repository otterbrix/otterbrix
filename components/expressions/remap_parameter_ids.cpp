#include "remap_parameter_ids.hpp"

#include "aggregate_expression.hpp"
#include "cast_expression.hpp"
#include "compare_expression.hpp"
#include "function_expression.hpp"
#include "scalar_expression.hpp"

namespace components::expressions {

    namespace {

        core::parameter_id_t mapped(const parameter_id_map_t& id_map, core::parameter_id_t id) {
            const auto it = id_map.find(id);
            return it == id_map.end() ? id : it->second;
        }

        void remap_param(param_storage& param, const parameter_id_map_t& id_map) {
            if (is_parameter(param)) {
                auto& id = as_parameter(param);
                id = mapped(id_map, id);
                return;
            }
            if (is_expr(param)) {
                remap_parameter_ids(as_expr(param), id_map);
            }
            // is_key: a column name carries no parameter id.
        }

    } // namespace

    void remap_parameter_ids(const expression_ptr& expr, const parameter_id_map_t& id_map) {
        if (!expr || id_map.empty()) {
            return;
        }
        switch (expr->group()) {
            case expression_group::compare: {
                auto* e = static_cast<compare_expression_t*>(expr.get());
                remap_param(e->left(), id_map);
                remap_param(e->right(), id_map);
                // LIKE/regex flags are stored as a parameter id of their own.
                e->set_regex_flags(mapped(id_map, e->regex_flags_param()));
                for (const auto& child : e->children()) {
                    remap_parameter_ids(child, id_map);
                }
                break;
            }
            case expression_group::scalar: {
                auto* e = static_cast<scalar_expression_t*>(expr.get());
                for (auto& param : e->params()) {
                    remap_param(param, id_map);
                }
                break;
            }
            case expression_group::aggregate: {
                auto* e = static_cast<aggregate_expression_t*>(expr.get());
                for (auto& param : e->params()) {
                    remap_param(param, id_map);
                }
                // child() is const-only, but it hands out a non-const expression_i*
                // through the intrusive_ptr, which is all the walk needs.
                remap_parameter_ids(e->child(), id_map);
                break;
            }
            case expression_group::function: {
                auto* e = static_cast<function_expression_t*>(expr.get());
                for (auto& arg : e->args()) {
                    remap_param(arg, id_map);
                }
                break;
            }
            case expression_group::cast: {
                auto* e = static_cast<cast_expression_t*>(expr.get());
                remap_param(e->child(), id_map);
                break;
            }
            case expression_group::sort:
                // A sort expression is a bare key + order; it holds no operands.
                break;
            case expression_group::invalid:
                // No expression_i subclass carries group invalid (see clone_expression).
                break;
        }
    }

} // namespace components::expressions
