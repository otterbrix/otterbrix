#include "substrait_expression_exporter.hpp"

#include <type_traits>
#include <variant>

namespace components::logical_plan::substrait_adapter {

    int32_t resolve_field_index(const expressions::key_t& key, field_context_t& ctx) {
        auto name = key.as_string();
        if (key.side() == expressions::side_t::right && ctx.right) {
            auto idx = const_cast<field_mapping_t*>(ctx.right)->get_or_add(name);
            auto left_size = ctx.left ? static_cast<int32_t>(ctx.left->names.size()) : ctx.left_size;
            return left_size + idx;
        }
        if (key.side() == expressions::side_t::left && ctx.left) {
            return const_cast<field_mapping_t*>(ctx.left)->get_or_add(name);
        }
        return ctx.mapping->get_or_add(name);
    }

    substrait::Type resolve_field_type(const expressions::key_t& key, field_context_t& ctx) {
        auto name = key.as_string();
        if (key.side() == expressions::side_t::right && ctx.right) {
            auto idx = const_cast<field_mapping_t*>(ctx.right)->get_or_add(name);
            return ctx.right->type_or_default(idx);
        }
        if (key.side() == expressions::side_t::left && ctx.left) {
            auto idx = const_cast<field_mapping_t*>(ctx.left)->get_or_add(name);
            return ctx.left->type_or_default(idx);
        }
        auto idx = ctx.mapping->get_or_add(name);
        return ctx.mapping->type_or_default(idx);
    }

    void set_field_ref(substrait::Expression* expr, int32_t field_idx) {
        auto* ref = expr->mutable_selection()->mutable_direct_reference()->mutable_struct_field();
        ref->set_field(field_idx);
        expr->mutable_selection()->mutable_root_reference();
    }

    function_registry_t::function_registry_t(substrait::Plan* plan_)
        : plan(plan_) {}

    uint32_t function_registry_t::register_function(const std::string& name) {
        auto it = function_ids.find(name);
        if (it != function_ids.end()) {
            return it->second;
        }
        if (urn_id == 0) {
            auto* urn = plan->add_extension_urns();
            urn->set_extension_urn_anchor(kExtensionUrnAnchor);
            urn->set_urn(kExtensionUrn);
            urn_id = kExtensionUrnAnchor;
        }
        auto id = next_function_id++;
        auto* ext = plan->mutable_extensions()->Add();
        ext->mutable_extension_function()->set_function_anchor(id);
        ext->mutable_extension_function()->set_name(name);
        ext->mutable_extension_function()->set_extension_urn_reference(urn_id);
        function_ids.emplace(name, id);
        return id;
    }

    to_substrait_context_t::to_substrait_context_t(substrait::Plan* plan,
                                                   export_profile_t profile_,
                                                   const components::catalog::catalog* catalog_)
        : registry(plan)
        , profile(profile_)
        , catalog(catalog_) {}

    bool to_substrait_context_t::is_external_canonical() const {
        return profile == export_profile_t::external_canonical;
    }

    substrait::Type infer_param_type(const expressions::param_storage& param, field_context_t& fields) {
        return std::visit(
            [&](const auto& value) -> substrait::Type {
                using value_t = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<value_t, core::parameter_id_t>) {
                    return make_string_type();
                } else if constexpr (std::is_same_v<value_t, expressions::key_t>) {
                    return resolve_field_type(value, fields);
                } else if constexpr (std::is_same_v<value_t, expressions::expression_ptr>) {
                    return infer_expression_type(value, fields);
                } else {
                    return make_string_type();
                }
            },
            param);
    }

    substrait::Type infer_expression_type(const expressions::expression_ptr& expr, field_context_t& fields) {
        using namespace components::expressions;
        if (!expr) {
            return make_string_type();
        }
        switch (expr->group()) {
            case expression_group::compare:
                return make_bool_type();
            case expression_group::sort: {
                auto* sort_expr = static_cast<const sort_expression_t*>(expr.get());
                if (!sort_expr->key().is_null()) {
                    return resolve_field_type(sort_expr->key(), fields);
                }
                return make_string_type();
            }
            case expression_group::scalar: {
                auto* scalar_expr = static_cast<const scalar_expression_t*>(expr.get());
                if (scalar_expr->type() == scalar_type::get_field) {
                    if (!scalar_expr->key().is_null()) {
                        return resolve_field_type(scalar_expr->key(), fields);
                    }
                    if (!scalar_expr->params().empty()) {
                        return infer_param_type(scalar_expr->params().front(), fields);
                    }
                    return make_string_type();
                }
                if (scalar_expr->type() == scalar_type::round || scalar_expr->type() == scalar_type::ceil ||
                    scalar_expr->type() == scalar_type::floor || scalar_expr->type() == scalar_type::abs) {
                    if (!scalar_expr->params().empty()) {
                        return infer_param_type(scalar_expr->params().front(), fields);
                    }
                }
                return make_fp64_type();
            }
            case expression_group::aggregate: {
                auto* aggr = static_cast<const aggregate_expression_t*>(expr.get());
                const auto& function_name = aggr->function_name();
                if (function_name == "count") {
                    return make_i64_type();
                }
                if (function_name == "avg") {
                    return make_fp64_type();
                }
                if (!aggr->params().empty()) {
                    return infer_param_type(aggr->params().front(), fields);
                }
                return make_string_type();
            }
            default:
                return make_string_type();
        }
    }

    substrait::Expression* to_substrait_param(const expressions::param_storage& param,
                                              field_context_t& fields,
                                              to_substrait_context_t& ctx) {
        auto* out = new substrait::Expression();
        std::visit(
            [&](const auto& value) {
                using value_t = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<value_t, core::parameter_id_t>) {
                    out->mutable_dynamic_parameter()->set_parameter_reference(static_cast<uint32_t>(value));
                } else if constexpr (std::is_same_v<value_t, expressions::key_t>) {
                    auto idx = resolve_field_index(value, fields);
                    set_field_ref(out, idx);
                } else if constexpr (std::is_same_v<value_t, expressions::expression_ptr>) {
                    delete out;
                    out = to_substrait_expression(value, fields, ctx);
                }
            },
            param);
        return out;
    }

    substrait::Expression* to_substrait_expression(const expressions::expression_ptr& expr,
                                                   field_context_t& fields,
                                                   to_substrait_context_t& ctx) {
        using namespace components::expressions;
        auto* out = new substrait::Expression();
        if (!expr) {
            return out;
        }
        switch (expr->group()) {
            case expression_group::compare: {
                auto* cmp = static_cast<const compare_expression_t*>(expr.get());
                if (cmp->type() == compare_type::all_true) {
                    out->mutable_literal()->set_boolean(true);
                    return out;
                }
                if (cmp->type() == compare_type::all_false) {
                    out->mutable_literal()->set_boolean(false);
                    return out;
                }
                auto func_name = compare_type_to_function(cmp->type());
                auto* scalar = out->mutable_scalar_function();
                scalar->set_function_reference(ctx.registry.register_function(func_name));
                scalar->mutable_output_type()->CopyFrom(make_bool_type());
                if (cmp->is_union()) {
                    for (const auto& child : cmp->children()) {
                        auto* arg = scalar->mutable_arguments()->Add();
                        arg->set_allocated_value(to_substrait_expression(child, fields, ctx));
                    }
                    return out;
                }
                auto* arg1 = scalar->mutable_arguments()->Add();
                auto* arg2 = scalar->mutable_arguments()->Add();
                arg1->set_allocated_value(to_substrait_param(cmp->left(), fields, ctx));
                arg2->set_allocated_value(to_substrait_param(cmp->right(), fields, ctx));
                return out;
            }
            case expression_group::scalar: {
                auto* scalar_expr = static_cast<const scalar_expression_t*>(expr.get());
                if (scalar_expr->type() == scalar_type::get_field) {
                    if (scalar_expr->params().empty()) {
                        if (!scalar_expr->key().is_null()) {
                            set_field_ref(out, resolve_field_index(scalar_expr->key(), fields));
                        }
                        return out;
                    }
                    return to_substrait_param(scalar_expr->params().front(), fields, ctx);
                }

                auto func_name = scalar_type_to_function(scalar_expr->type());
                auto* scalar = out->mutable_scalar_function();
                scalar->set_function_reference(ctx.registry.register_function(func_name));
                scalar->mutable_output_type()->CopyFrom(infer_expression_type(expr, fields));
                for (const auto& param : scalar_expr->params()) {
                    auto* arg = scalar->mutable_arguments()->Add();
                    arg->set_allocated_value(to_substrait_param(param, fields, ctx));
                }
                return out;
            }
            case expression_group::aggregate:
            case expression_group::sort: {
                auto* sort_expr = static_cast<const sort_expression_t*>(expr.get());
                if (!sort_expr->key().is_null()) {
                    set_field_ref(out, resolve_field_index(sort_expr->key(), fields));
                }
                return out;
            }
            case expression_group::function:
            default:
                break;
        }
        return out;
    }

    std::string expr_output_name(const expressions::expression_ptr& expr) {
        using namespace components::expressions;
        if (!expr) {
            return "";
        }
        switch (expr->group()) {
            case expression_group::scalar: {
                auto* scalar = static_cast<const scalar_expression_t*>(expr.get());
                if (!scalar->key().is_null()) {
                    return scalar->key().as_string();
                }
                if (!scalar->params().empty()) {
                    if (std::holds_alternative<expressions::key_t>(scalar->params().front())) {
                        return std::get<expressions::key_t>(scalar->params().front()).as_string();
                    }
                }
                break;
            }
            case expression_group::aggregate: {
                auto* aggr = static_cast<const aggregate_expression_t*>(expr.get());
                if (!aggr->key().is_null()) {
                    return aggr->key().as_string();
                }
                if (!aggr->function_name().empty()) {
                    return aggr->function_name();
                }
                return "agg";
            }
            case expression_group::sort: {
                auto* sort = static_cast<const sort_expression_t*>(expr.get());
                return sort->key().as_string();
            }
            default:
                break;
        }
        return "";
    }

    std::string default_expr_output_name(size_t index) { return "expr_" + std::to_string(index); }

    field_mapping_t project_output_mapping(const std::pmr::vector<expressions::expression_ptr>& expressions,
                                           field_context_t& fields) {
        field_mapping_t output;
        for (size_t i = 0; i < expressions.size(); ++i) {
            auto name = expr_output_name(expressions[i]);
            if (name.empty()) {
                name = default_expr_output_name(i);
            }
            output.get_or_add(name, infer_expression_type(expressions[i], fields));
        }
        return output;
    }

    field_mapping_t aggregate_output_mapping(const std::pmr::vector<expressions::expression_ptr>& expressions,
                                             field_context_t& fields) {
        using namespace components::expressions;
        field_mapping_t output;
        size_t index = 0;
        for (const auto& expr : expressions) {
            if (expr->group() == expression_group::aggregate) {
                continue;
            }
            auto name = expr_output_name(expr);
            if (name.empty()) {
                name = default_expr_output_name(index);
            }
            output.get_or_add(name, infer_expression_type(expr, fields));
            ++index;
        }
        for (const auto& expr : expressions) {
            if (expr->group() != expression_group::aggregate) {
                continue;
            }
            auto name = expr_output_name(expr);
            if (name.empty()) {
                name = default_expr_output_name(index);
            }
            output.get_or_add(name, infer_expression_type(expr, fields));
            ++index;
        }
        return output;
    }

} // namespace components::logical_plan::substrait_adapter
