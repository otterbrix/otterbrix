#include "resolve_type.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/function_expression.hpp>
#include <components/expressions/scalar_expression.hpp>

namespace services::dispatcher {

    bool resolve_builtin(components::types::complex_logical_type& ct) {
        const auto lt = components::catalog::pg_name_to_logical_type(ct.type_name());
        if (lt == components::types::logical_type::UNKNOWN)
            return false;
        const std::string alias = ct.has_alias() ? ct.alias() : std::string{};
        ct = components::types::complex_logical_type{lt};
        if (!alias.empty())
            ct.set_alias(alias);
        return true;
    }

    // Sync — reads UDT metadata from the plan's resolved type entries exclusively.
    // transform_create_table / transform_types register a type entry per UDT, and we
    // consume what the resolve operator stamped. Misses leave the type as UNKNOWN —
    // validate_types surfaces "type not registered".
    components::catalog::oid_t resolve_one_type(components::types::complex_logical_type& ct,
                                                const catalog_resolves_t* resolves) {
        if (ct.type() != components::types::logical_type::UNKNOWN)
            return components::catalog::builtin_type_to_oid(ct.type());
        if (resolve_builtin(ct))
            return components::catalog::builtin_type_to_oid(ct.type());
        if (!resolves) {
            return components::catalog::INVALID_OID;
        }
        const auto* md = resolves->type_md("public", std::string_view(ct.type_name()));
        if (!md) {
            md = resolves->type_md("pg_catalog", std::string_view(ct.type_name()));
        }
        if (!md)
            return components::catalog::INVALID_OID;
        const std::string alias = ct.has_alias() ? ct.alias() : std::string{};
        ct = md->type;
        if (!alias.empty())
            ct.set_alias(alias);
        return md->type_oid;
    }

    void resolve_column_definitions(std::vector<components::table::column_definition_t>& cols,
                                    const catalog_resolves_t* resolves) {
        for (auto& col : cols) {
            auto& ct = col.type();
            resolve_one_type(ct, resolves);
            if (ct.type() == components::types::logical_type::STRUCT) {
                for (auto& field : ct.child_types()) {
                    resolve_one_type(field, resolves);
                }
            }
            if (ct.type() == components::types::logical_type::ARRAY) {
                const auto* arr_ext =
                    static_cast<const components::types::array_logical_type_extension*>(ct.extension());
                auto inner = arr_ext->internal_type();
                const size_t sz = arr_ext->size();
                if (inner.type() == components::types::logical_type::UNKNOWN) {
                    resolve_one_type(inner, resolves);
                    std::string alias = ct.has_alias() ? ct.alias() : std::string{};
                    ct = components::types::complex_logical_type::create_array(inner, sz);
                    if (!alias.empty())
                        ct.set_alias(alias);
                }
            }
            if (ct.type() == components::types::logical_type::LIST) {
                const auto* list_ext =
                    static_cast<const components::types::list_logical_type_extension*>(ct.extension());
                auto inner = list_ext->node();
                if (inner.type() == components::types::logical_type::UNKNOWN) {
                    resolve_one_type(inner, resolves);
                    std::string alias = ct.has_alias() ? ct.alias() : std::string{};
                    ct = components::types::complex_logical_type::create_list(inner);
                    if (!alias.empty())
                        ct.set_alias(alias);
                }
            }
        }
    }

    namespace {

        void resolve_casts_in(const components::expressions::expression_ptr& expression,
                              const catalog_resolves_t* resolves) {
            namespace ce = components::expressions;
            if (!expression) {
                return;
            }
            const auto visit = [&](const ce::param_storage& param) {
                if (ce::is_expr(param)) {
                    resolve_casts_in(ce::as_expr(param), resolves);
                }
            };
            switch (expression->group()) {
                case ce::expression_group::cast: {
                    auto* conversion = static_cast<ce::cast_expression_t*>(expression.get());
                    auto target = conversion->result_type();
                    resolve_one_type(target, resolves);
                    conversion->set_result_type(target);
                    visit(conversion->child());
                    break;
                }
                case ce::expression_group::scalar:
                    for (const auto& param : static_cast<ce::scalar_expression_t*>(expression.get())->params()) {
                        visit(param);
                    }
                    break;
                case ce::expression_group::function:
                    for (const auto& argument : static_cast<ce::function_expression_t*>(expression.get())->args()) {
                        visit(argument);
                    }
                    break;
                case ce::expression_group::aggregate:
                    for (const auto& param : static_cast<ce::aggregate_expression_t*>(expression.get())->params()) {
                        visit(param);
                    }
                    break;
                case ce::expression_group::compare: {
                    auto* comparison = static_cast<ce::compare_expression_t*>(expression.get());
                    visit(comparison->left());
                    visit(comparison->right());
                    for (const auto& child : comparison->children()) {
                        resolve_casts_in(child, resolves);
                    }
                    break;
                }
                default:
                    break;
            }
        }

    } // namespace

    void resolve_expression_types(const components::logical_plan::node_ptr& node, const catalog_resolves_t* resolves) {
        if (!node) {
            return;
        }
        for (const auto& expression : node->expressions()) {
            resolve_casts_in(expression, resolves);
        }
        for (const auto& child : node->children()) {
            resolve_expression_types(child, resolves);
        }
    }

} // namespace services::dispatcher