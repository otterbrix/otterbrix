#include "validate_logical_plan.hpp"

#include <core/date/date_parse.hpp>
#include <cstdio>

#include "expressions/function_expression.hpp"
#include "logical_plan/node_create_index.hpp"
#include "logical_plan/node_insert.hpp"
#include "logical_plan/node_update.hpp"
#include "resolve_function.hpp"
#include "validation/resolve_expression.hpp"

#include <atomic>
#include <components/casts/cast_registry.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/catalog/table_id.hpp>
#include <components/compute/function.hpp>
#include <components/compute/kernel_signature.hpp>
#include <components/index/logical_value_binary_codec.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_alter_column.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_check_constraint.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_cte_scan.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_extension.hpp>
#include <components/logical_plan/node_fk_cascade.hpp>
#include <components/logical_plan/node_fk_check.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_having.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_limit.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_recursive_cte.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <components/table/column_definition.hpp>
#include <list>
#include <optional>
#include <queue>
#include <set>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace services::dispatcher {

    using namespace components::types;
    using namespace components::expressions;
    using namespace components::logical_plan;
    using namespace components::cursor;
    using namespace components::catalog;
    using namespace validation;

    namespace impl {

        // Rewrite an is_not_null / is_null predicate on a multi-type field into
        // an OR / AND over its per-type-variant columns: the key "exists" iff ANY
        // variant is non-null, and is null only if ALL variants are null. This is
        // how jsonb '?'/'?|'/'?&' behave over multi-type fields. Other compare
        // types on such a name stay ambiguous (use '::?type' to pick a variant).
        components::expressions::expression_ptr
        rewrite_multitype_null_checks(std::pmr::memory_resource* resource,
                                      const components::expressions::expression_ptr& expr,
                                      const named_schema& schema) {
            using namespace components::expressions;
            if (!expr || expr->group() != expression_group::compare) {
                return expr;
            }
            auto* cmp = static_cast<compare_expression_t*>(expr.get());
            if (cmp->is_union()) {
                auto rebuilt = make_compare_union_expression(resource, cmp->type());
                for (const auto& ch : cmp->children()) {
                    rebuilt->append_child(rewrite_multitype_null_checks(resource, ch, schema));
                }
                return rebuilt;
            }
            const bool is_nn = cmp->type() == compare_type::is_not_null;
            const bool is_n = cmp->type() == compare_type::is_null;
            if ((!is_nn && !is_n) || !std::holds_alternative<components::expressions::key_t>(cmp->left())) {
                return expr;
            }
            const auto& key = std::get<components::expressions::key_t>(cmp->left());
            if (key.storage().empty()) {
                return expr;
            }
            const std::string name = key.as_string();
            std::vector<components::types::complex_logical_type> variants;
            for (const auto& c : schema) {
                if (c.type.has_alias() && std::string(c.type.alias()) == name) {
                    variants.push_back(c.type);
                }
            }
            if (variants.empty() && key.absent_ok()) {
                // A jsonb existence key ('?'/'?|'/'?&') that matches no column
                // exactly. Postgres 3VL, over the flattened representation:
                //   - an INTERMEDIATE object key (a prefix of one or more stored
                //     columns, e.g. 'a' with columns a/b, a/c) is PRESENT iff a
                //     child is non-null -> OR/AND of the per-child null checks;
                //   - a truly ABSENT key is present for no row -> constant false
                //     (is_not_null) / true (is_null), so one missing key can never
                //     poison a '?|' any-of that another key already satisfies.
                const std::string prefix_slash = name + "/";
                std::vector<components::expressions::key_t> children;
                for (const auto& c : schema) {
                    if (c.type.has_alias() && std::string(c.type.alias()).rfind(prefix_slash, 0) == 0) {
                        components::expressions::key_t ckey(resource, std::string(c.type.alias()));
                        ckey.set_side(key.side());
                        children.push_back(std::move(ckey));
                    }
                }
                if (children.empty()) {
                    return make_compare_expression(resource, is_nn ? compare_type::all_false : compare_type::all_true);
                }
                auto combined =
                    make_compare_union_expression(resource, is_nn ? compare_type::union_or : compare_type::union_and);
                for (auto& ckey : children) {
                    combined->append_child(make_compare_expression(resource, cmp->type(), ckey, cmp->right()));
                }
                return combined;
            }
            if (variants.size() <= 1) {
                return expr; // single-type (or unknown) — leave as-is
            }
            auto combined =
                make_compare_union_expression(resource, is_nn ? compare_type::union_or : compare_type::union_and);
            for (const auto& vt : variants) {
                components::expressions::key_t vkey = key;
                vkey.set_cast_type(vt);
                vkey.set_variant_select(true);
                combined->append_child(make_compare_expression(resource, cmp->type(), vkey, cmp->right()));
            }
            return combined;
        }

        [[nodiscard]] core::result_wrapper_t<named_schema>
        validate_schema(const validation::validation_context_t& context,
                        function_expression_t* expr,
                        const storage_parameters& parameters,
                        const named_schema* schema_left,
                        const named_schema* schema_right,
                        components::compute::function_types_mask allowed_function_types) {
            const validation::expression_context_t expression_context{context.resource,
                                                                      *schema_left,
                                                                      parameters,
                                                                      context.cast_registry,
                                                                      context.function_registry,
                                                                      context.execution_context,
                                                                      allowed_function_types,
                                                                      schema_right};
            expression_ptr expression{expr};
            if (auto error = validation::resolve_expression(expression, expression_context); error.contains_error()) {
                return error;
            }
            named_schema result(context.resource);
            result.emplace_back(type_from_t{expr->result_alias(), expression->result_type()});
            return result;
        }

        [[nodiscard]] core::result_wrapper_t<type_paths>
        resolve_key_path(std::pmr::memory_resource* resource, param_storage& param, const named_schema& schema);

        [[nodiscard]] core::result_wrapper_t<type_paths>
        resolve_key_paths_in_group(std::pmr::memory_resource* resource,
                                   std::pmr::vector<param_storage>& params,
                                   const named_schema& schema) {
            for (auto& param : params) {
                auto res = resolve_key_path(resource, param, schema);
                if (res.has_error()) {
                    return res;
                }
            }
            return type_paths{resource};
        }

        [[nodiscard]] core::result_wrapper_t<type_paths>
        resolve_key_path(std::pmr::memory_resource* resource, param_storage& param, const named_schema& schema) {
            if (std::holds_alternative<components::expressions::key_t>(param)) {
                auto& key = std::get<components::expressions::key_t>(param);
                if (key.storage().empty()) {
                    return core::error_t(core::error_code_t::schema_error,
                                         std::pmr::string{"key has empty storage: " + key.as_string(), resource});
                }
                return find_types(resource, key, schema);
            } else if (std::holds_alternative<expression_ptr>(param)) {
                auto& sub = std::get<expression_ptr>(param);
                if (!sub) {
                    // A null operand slot -- e.g. the unused right() of a unary IS NULL, or the
                    // left()/right() sentinels of a union node whose operands live in children_.
                    // Nothing to resolve, and dereferencing sub->group() below would crash.
                    return type_paths{resource};
                }
                if (sub->group() == expression_group::scalar) {
                    auto* scalar = static_cast<scalar_expression_t*>(sub.get());
                    auto res = resolve_key_paths_in_group(resource, scalar->params(), schema);
                    if (res.has_error()) {
                        return res;
                    }
                } else if (sub->group() == expression_group::compare) {
                    auto* cmp = static_cast<compare_expression_t*>(sub.get());
                    if (cmp->is_union()) {
                        // Union compares (AND/OR/NOT — e.g. from IN, or the union_and(is_not_null,
                        // union_not(regex)) that NOT LIKE expands into) carry their operands as CHILDREN
                        // and have no left/right, so recurse into each child instead of touching left/right
                        // (which are null for a union and would be dereferenced blindly).
                        for (auto& child : cmp->children()) {
                            param_storage child_param{child};
                            auto res = resolve_key_path(resource, child_param, schema);
                            if (res.has_error()) {
                                return res;
                            }
                        }
                    } else {
                        auto res = resolve_key_path(resource, cmp->left(), schema);
                        if (res.has_error()) {
                            return res;
                        }
                        res = resolve_key_path(resource, cmp->right(), schema);
                        if (res.has_error()) {
                            return res;
                        }
                    }
                }
            }
            return type_paths{resource};
        }

        // Defined below; a CASE condition is validated exactly like a WHERE predicate.
        [[nodiscard]] core::result_wrapper_t<named_schema>
        validate_schema(const validation::validation_context_t& context,
                        compare_expression_t* expr,
                        const storage_parameters& parameters,
                        const named_schema* schema_left,
                        const named_schema* schema_right = nullptr);

        core::error_t resolve_scalar_output_type(const validation::validation_context_t& context,
                                                 components::expressions::scalar_expression_t* scalar_expr,
                                                 const named_schema& schema,
                                                 const components::logical_plan::storage_parameters& parameters,
                                                 components::compute::function_types_mask allowed_functions,
                                                 const named_schema* schema_right = nullptr,
                                                 bool* saw_reduction = nullptr) {
            const validation::expression_context_t expression_context{context.resource,
                                                                      schema,
                                                                      parameters,
                                                                      context.cast_registry,
                                                                      context.function_registry,
                                                                      context.execution_context,
                                                                      allowed_functions,
                                                                      schema_right};
            expression_ptr expression{scalar_expr};
            return validation::resolve_expression(expression, expression_context, saw_reduction);
        }

        [[nodiscard]] core::result_wrapper_t<named_schema>
        validate_schema(const validation::validation_context_t& context,
                        compare_expression_t* expr,
                        const storage_parameters& parameters,
                        const named_schema* schema_left,
                        const named_schema* schema_right) {
            const validation::expression_context_t expression_context{
                context.resource,
                *schema_left,
                parameters,
                context.cast_registry,
                context.function_registry,
                context.execution_context,
                components::compute::create_mask(components::compute::function_type_t::row,
                                                 components::compute::function_type_t::vector),
                schema_right};
            expression_ptr expression{expr};
            if (auto error = validation::resolve_expression(expression, expression_context); error.contains_error()) {
                return error;
            }
            named_schema result(context.resource);
            result.emplace_back(type_from_t{"", logical_type::BOOLEAN});
            return result;
        }

        [[nodiscard]] core::result_wrapper_t<named_schema>
        validate_schema(const validation::validation_context_t& context,
                        node_match_t* node,
                        const storage_parameters& parameters,
                        const named_schema* schema_left,
                        const named_schema* schema_right = nullptr) {
            auto* resource = context.resource;
            if (node->expressions().empty()) {
                // physical plan reinterprets this as default scan
                const auto* tbl = node->table_metadata();
                if (tbl && tbl->relkind != 'g') {
                    named_schema result(resource);
                    const auto& table_alias = node->result_alias().empty() ? node->relname() : node->result_alias();
                    for (const auto& column : tbl->columns) {
                        result.emplace_back(type_from_t{table_alias, column.type});
                    }
                    return result;
                }
                if (tbl && tbl->relkind == 'g') {
                    named_schema result(resource);
                    for (const auto& column : tbl->columns) {
                        result.emplace_back(
                            type_from_t{node->result_alias().empty() ? node->relname() : node->result_alias(),
                                        column.type});
                    }
                    return result;
                } else {
                    std::pmr::string msg{"collection does not exist: ", resource};
                    msg.append(node->dbname().begin(), node->dbname().end());
                    msg += '.';
                    msg.append(node->relname().begin(), node->relname().end());
                    return core::error_t(core::error_code_t::table_not_exists, std::move(msg));
                }
            } else {
                assert(node->expressions().size() == 1);
                if (node->expressions()[0]->group() == expression_group::compare) {
                    auto* expr = reinterpret_cast<compare_expression_t*>(node->expressions()[0].get());
                    return validate_schema(context, expr, parameters, schema_left, schema_right);
                } else if (node->expressions()[0]->group() == expression_group::function) {
                    auto* expr = reinterpret_cast<function_expression_t*>(node->expressions()[0].get());
                    auto allowed_function_types =
                        components::compute::create_mask(components::compute::function_type_t::row,
                                                         components::compute::function_type_t::vector);
                    auto expr_res =
                        validate_schema(context, expr, parameters, schema_left, schema_right, allowed_function_types);
                    if (expr_res.has_error()) {
                        return expr_res;
                    }
                    if (expr_res.value().size() == 1 && expr_res.value().front().type.type() == logical_type::BOOLEAN) {
                        return expr_res;
                    } else {
                        return core::error_t(
                            core::error_code_t::incorrect_function_return_type,
                            std::pmr::string{"function: \'" + expr->name() +
                                                 "(...)\' was found but can not be used in WHERE clause, "
                                                 "because return type is not a boolean",
                                             resource});
                    }
                } else {
                    assert(false);
                    return core::error_t(core::error_code_t::schema_error,
                                         std::pmr::string{"incorrect expr type in node_group", resource});
                }
            }
        }

        core::result_wrapper_t<named_schema>
        validate_schema(const validation::validation_context_t& context,
                        node_sort_t* node,
                        const named_schema& schema,
                        const components::logical_plan::storage_parameters& parameters) {
            auto* resource = context.resource;
            for (auto& expr : node->expressions()) {
                if (expr->group() == expression_group::sort) {
                    auto* sort_expr = static_cast<sort_expression_t*>(expr.get());
                    auto res = find_types(resource, sort_expr->key(), schema);
                    if (res.has_error()) {
                        return res.convert_error<named_schema>();
                    }
                } else if (expr->group() == expression_group::scalar) {
                    auto* scalar_expr = static_cast<scalar_expression_t*>(expr.get());
                    auto resolve_error = resolve_scalar_output_type(
                        context,
                        scalar_expr,
                        schema,
                        parameters,
                        components::compute::create_mask(components::compute::function_type_t::row,
                                                         components::compute::function_type_t::vector));
                    if (resolve_error.contains_error()) {
                        return resolve_error;
                    }
                }
            }
            return named_schema{resource};
        }

        // Resolve key paths in a DML node's RETURNING projection expressions
        // against the schema of the affected rows (the target table's columns).
        // Mirrors the node_select resolution: get_field keys and arithmetic
        // operands get their column paths stamped; star_expand with a table
        // qualifier is validated to expand; bare '*' and constants need nothing.
        [[nodiscard]] core::error_t
        resolve_returning_columns(const validation::validation_context_t& context,
                                  std::pmr::vector<expression_ptr>* returning,
                                  const named_schema* schema_left,
                                  const named_schema* schema_right,
                                  const components::logical_plan::storage_parameters& parameters) {
            auto* resource = context.resource;
            auto& exprs = *returning;
            for (size_t idx = 0; idx < exprs.size();) {
                if (!exprs[idx] || exprs[idx]->group() != expression_group::scalar) {
                    idx++;
                    continue;
                }
                auto* scalar_expr = static_cast<scalar_expression_t*>(exprs[idx].get());
                switch (scalar_expr->type()) {
                    case scalar_type::get_field: {
                        auto& key = scalar_expr->params().empty()
                                        ? scalar_expr->key()
                                        : std::get<components::expressions::key_t>(scalar_expr->params().front());
                        if (key.path().empty()) {
                            // Side-aware: schema_left is the destination table, schema_right the
                            // USING/FROM table (null when there is no join)
                            auto res = validate_key(resource, key, schema_left, schema_right);
                            if (res.has_error()) {
                                return res.error();
                            }
                        }
                        idx++;
                        break;
                    }
                    case scalar_type::star_expand: {
                        // 'table.*' (qualified) expands, like SELECT, into one
                        // get_field per matching column — resolved against the
                        // destination, then (for a join) the USING/FROM table — each
                        // carrying its resolved side so it reads the correct chunk.
                        // Bare '*' keeps an empty key and stays a runtime star_expand
                        // (the destination row passthrough).
                        auto& star_key = scalar_expr->key();
                        if (star_key.storage().empty() || star_key.storage().front() == "*") {
                            idx++;
                            break;
                        }
                        side_t side = side_t::left;
                        auto field = find_types(resource, star_key, *schema_left);
                        if (field.has_error()) {
                            if (schema_right == nullptr) {
                                return field.error();
                            }
                            field = find_types(resource, star_key, *schema_right);
                            if (field.has_error()) {
                                return field.error();
                            }
                            side = side_t::right;
                        }
                        auto& field_paths = field.value();
                        exprs.erase(exprs.begin() + static_cast<ptrdiff_t>(idx));
                        for (size_t j = 0; j < field_paths.size(); j++) {
                            components::expressions::key_t new_key(resource);
                            if (field_paths[j].type.has_alias()) {
                                new_key.storage().push_back(std::pmr::string(field_paths[j].type.alias(), resource));
                            }
                            new_key.set_path(field_paths[j].path);
                            new_key.set_side(side);
                            exprs.insert(exprs.begin() + static_cast<ptrdiff_t>(idx + j),
                                         make_scalar_expression(resource, scalar_type::get_field, new_key));
                        }
                        idx += field_paths.size();
                        break;
                    }
                    case scalar_type::constant:
                        idx++;
                        break;
                    default: {
                        // RETURNING projects the affected rows one at a time.
                        auto resolve_error = resolve_scalar_output_type(
                            context,
                            scalar_expr,
                            *schema_left,
                            parameters,
                            components::compute::create_mask(components::compute::function_type_t::row,
                                                             components::compute::function_type_t::vector),
                            schema_right,
                            nullptr);
                        if (resolve_error.contains_error()) {
                            return resolve_error;
                        }
                        idx++;
                        break;
                    }
                }
            }
            return core::error_t::no_error();
        }

    } // namespace impl

    // ---- Existence checks over the plan's resolved catalog entries ----

    core::error_t check_namespace_exists(std::pmr::memory_resource* resource,
                                         const catalog_resolves_t* resolves,
                                         const components::catalog::table_id& id) {
        if (id.database().empty()) {
            return core::error_t(core::error_code_t::database_not_exists,
                                 std::pmr::string{"database does not exist", resource});
        }
        if (!resolves || resolves->namespace_oid(id.database()) == components::catalog::INVALID_OID) {
            return core::error_t(core::error_code_t::database_not_exists,
                                 std::pmr::string{"database does not exist", resource});
        }
        return core::error_t::no_error();
    }

    core::error_t check_collection_exists(std::pmr::memory_resource* resource,
                                          const catalog_resolves_t* resolves,
                                          const components::catalog::table_id& id) {
        if (auto err = check_namespace_exists(resource, resolves, id); err.contains_error()) {
            return err;
        }
        if (!resolves->table_md(id.database(), std::string_view(id.table_name()))) {
            return core::error_t(core::error_code_t::table_not_exists,
                                 std::pmr::string{"collection does not exist", resource});
        }
        return core::error_t::no_error();
    }

    core::error_t check_type_exists(std::pmr::memory_resource* resource,
                                    const catalog_resolves_t* resolves,
                                    const std::string& alias,
                                    std::span<const std::string> search_dbnames) {
        if (components::catalog::pg_name_to_logical_type(alias) != components::types::logical_type::UNKNOWN) {
            return core::error_t::no_error();
        }
        static const std::string kPublic{"public"};
        static const std::string kPgCatalog{"pg_catalog"};
        const std::string default_path[] = {kPublic, kPgCatalog};
        const auto path = search_dbnames.empty() ? std::span<const std::string>(default_path) : search_dbnames;
        for (const auto& db : path) {
            if (resolves && resolves->type_md(std::string_view(db), std::string_view(alias))) {
                return core::error_t::no_error();
            }
        }
        return core::error_t(core::error_code_t::schema_error,
                             std::pmr::string{"type: \'" + alias + "\' is not registered in catalog", resource});
    }

    namespace {
        // Reverse-lookup: namespace_oid -> dbname. Linear scan over the small
        // namespace entry list; only invoked when a node carries a valid
        // table_oid and we need to populate table_dbnames for the UDT type
        // probe in check_node. Returns empty string_view if not found.
        std::string_view dbname_for_ns_oid(const catalog_resolves_t* resolves, components::catalog::oid_t ns_oid) {
            if (!resolves || !resolves->namespaces) {
                return {};
            }
            for (const auto& entry : resolves->namespaces->entries()) {
                if (entry.namespace_oid == ns_oid) {
                    return entry.dbname;
                }
            }
            return {};
        }
    } // namespace

    core::error_t convert_column_defaults(std::pmr::memory_resource* resource,
                                          const components::casts::cast_registry_t* cast_registry,
                                          const components::graph_execution_context& execution_context,
                                          std::vector<components::table::column_definition_t>& columns) {
        for (auto& column : columns) {
            if (!column.has_default_value() || column.default_value().type() == column.type()) {
                continue;
            }
            const auto& written = column.default_value();
            auto conversion =
                cast_registry->resolve(written.type(), column.type(), components::casts::cast_type::assignment);
            if (!conversion.has_value()) {
                return core::error_t(core::error_code_t::conversion_failure,
                                     std::pmr::string{"DEFAULT for column '" + column.name() +
                                                          "' may not be converted to the column's type on assignment",
                                                      resource});
            }
            components::vector::vector_t source{resource, written, 1};
            components::vector::vector_t converted{resource, column.type(), 1};
            auto error = (*conversion)(components::casts::cast_kind::cast, source, &converted, execution_context, 1);
            if (error.contains_error()) {
                return error;
            }
            column.set_default_value(converted.value(0));
        }
        return core::error_t::no_error();
    }

    core::error_t validate_types(std::pmr::memory_resource* resource,
                                 const catalog_resolves_t* resolves,
                                 node_t* logical_plan,
                                 const components::graph_execution_context& execution_context) {
        const auto session_tz = execution_context.timezone_offset;

        std::pmr::vector<complex_logical_type> encountered_types{resource};
        std::set<std::string> table_dbnames;
        core::error_t result = core::error_t::no_error();
        // 'g' once the VALUES target is a schemaless computing table (see the
        // NA-column drop after chunk reconciliation below).
        char insert_target_relkind = 0;

        auto check_node = [&](node_t* node) {
            // Drop-nodes skip existence + type collection here.
            // Their catalog_resolve_* children verify existence at parse time;
            // CASCADE/RESTRICT is enforced by the cascade-delete operator downstream.
            switch (node->type()) {
                case node_type::drop_t:
                    return true;
                default:
                    break;
            }
            if (auto oid = node->table_oid(); oid != components::catalog::INVALID_OID) {
                const auto* tbl = resolves ? resolves->table_md(oid) : nullptr;
                if (!tbl) {
                    result = core::error_t(core::error_code_t::table_not_exists,
                                           std::pmr::string{"collection does not exist", resource});
                    return false;
                }
                insert_target_relkind = tbl->relkind;
                if (tbl->relkind != 'g') {
                    for (const auto& column : tbl->columns) {
                        encountered_types.emplace_back(column.type);
                    }
                    if (auto ns_name = dbname_for_ns_oid(resolves, tbl->namespace_oid); !ns_name.empty()) {
                        table_dbnames.emplace(ns_name);
                    }
                }
            }
            // pull/double-check check format from collection referenced by logical_plan and data stored inside node_data_t
            if (node->type() == node_type::data_t) {
                auto* data_node = reinterpret_cast<node_data_t*>(node);

                // Probe the plan's resolved type entries by dbname.
                auto type_visible = [&](std::string_view name) {
                    if (!resolves) {
                        return false;
                    }
                    for (const auto& db : table_dbnames) {
                        if (resolves->type_md(std::string_view(db), name))
                            return true;
                    }
                    return resolves->type_md(std::string_view{"public"}, name) ||
                           resolves->type_md(std::string_view{"pg_catalog"}, name);
                };

                // Raw data is a batch of ≤CAP chunks sharing one column shape; coerce each.
                for (auto& chunk : data_node->chunks()) {
                    for (auto& column : chunk.data) {
                        auto it = std::find_if(encountered_types.begin(),
                                               encountered_types.end(),
                                               [&column](const complex_logical_type& type) {
                                                   return type.alias() == column.type().alias();
                                               });
                        // if this is a registered type, then conversion is required
                        bool ty_exists =
                            it != encountered_types.end() && type_visible(std::string_view(it->type_name()));
                        if (ty_exists) {
                            if (is_duration(it->type()) && column.type().type() == logical_type::STRING_LITERAL) {
                                components::vector::vector_t new_column(resource, *it, chunk.capacity());
                                for (size_t i = 0; i < chunk.size(); i++) {
                                    auto str = column.data<std::string_view>()[i];
                                    std::optional<logical_value_t> parsed_val;
                                    switch (it->type()) {
                                        case logical_type::DATE:
                                            if (auto parsed = core::date::parse_date(str)) {
                                                parsed_val = logical_value_t(resource, *parsed);
                                            }
                                            break;
                                        case logical_type::TIME:
                                            if (auto parsed = core::date::parse_time(str)) {
                                                parsed_val = logical_value_t(resource, *parsed);
                                            }
                                            break;
                                        case logical_type::TIME_TZ:
                                            if (auto parsed = core::date::parse_timetz(str)) {
                                                parsed_val = logical_value_t(resource, *parsed);
                                            }
                                            break;
                                        case logical_type::TIMESTAMP:
                                            if (auto parsed = core::date::parse_timestamp(str)) {
                                                parsed_val = logical_value_t(resource, *parsed);
                                            }
                                            break;
                                        case logical_type::TIMESTAMP_TZ:
                                            if (auto parsed = core::date::parse_timestamptz(str)) {
                                                parsed_val = logical_value_t(resource, *parsed);
                                            }
                                            break;
                                        case logical_type::INTERVAL:
                                            if (auto parsed = core::date::parse_interval(str)) {
                                                parsed_val = logical_value_t(resource, *parsed);
                                            }
                                            break;
                                        default:
                                            break;
                                    }
                                    if (!parsed_val) {
                                        result = core::error_t(
                                            core::error_code_t::schema_error,
                                            std::pmr::string{"couldn't convert string to date/time type: \'" +
                                                                 it->alias() + "\', value: \'" + std::string(str) +
                                                                 "\'",
                                                             resource});
                                        return false;
                                    }
                                    new_column.set_value(i, *parsed_val);
                                }
                                column = std::move(new_column);
                            } else if (it->type() == logical_type::DECIMAL &&
                                       (is_numeric(column.type().type()) ||
                                        column.type().type() == logical_type::STRING_LITERAL)) {
                                components::vector::vector_t new_column(resource, *it, chunk.capacity());
                                for (size_t i = 0; i < chunk.size(); i++) {
                                    auto casted = column.value(i).cast_as(*it, session_tz);
                                    if (casted.has_error()) {
                                        result = casted.error();
                                        return false;
                                    }
                                    const auto& val = casted.value();
                                    if (val.type().type() == logical_type::NA) {
                                        result = core::error_t(
                                            core::error_code_t::schema_error,
                                            std::pmr::string{"couldn't convert value to decimal type: \'" +
                                                                 it->alias() + "\'",
                                                             resource});
                                        return false;
                                    }
                                    new_column.set_value(i, val);
                                }
                                column = std::move(new_column);
                            } else if (!check_type_exists(resource,
                                                          resolves,
                                                          it->type_name(),
                                                          std::span<const std::string>())
                                            .contains_error()) {
                                // if this is a registered type, then conversion is required
                                if (it->type() == logical_type::STRUCT) {
                                    components::vector::vector_t new_column(resource, *it, chunk.capacity());
                                    for (size_t i = 0; i < chunk.size(); i++) {
                                        auto casted = column.value(i).cast_as(*it, session_tz);
                                        if (casted.has_error()) {
                                            result = casted.error();
                                            return false;
                                        }
                                        const auto& val = casted.value();
                                        if (val.type().type() == logical_type::NA) {
                                            result = core::error_t(
                                                core::error_code_t::schema_error,
                                                std::pmr::string{"couldn't convert parsed ROW to type: \'" +
                                                                     it->alias() + "\'",
                                                                 resource});
                                            return false;
                                        } else {
                                            new_column.set_value(i, val);
                                        }
                                    }
                                    column = std::move(new_column);
                                } else if (it->type() == logical_type::ENUM) {
                                    components::vector::vector_t new_column(resource, *it, chunk.capacity());
                                    for (size_t i = 0; i < chunk.size(); i++) {
                                        auto val = column.data<std::string_view>()[i];
                                        auto enum_val = logical_value_t::create_enum(resource, *it, val);
                                        if (enum_val.type().type() == logical_type::NA) {
                                            result =
                                                core::error_t(core::error_code_t::schema_error,
                                                              std::pmr::string{"enum: \'" + it->alias() +
                                                                                   "\' does not contain value: \'" +
                                                                                   std::string(val) + "\'",
                                                                               resource});
                                            return false;
                                        } else {
                                            new_column.set_value(i, enum_val);
                                        }
                                    }
                                    column = std::move(new_column);
                                } else {
                                    assert(false &&
                                           "missing type conversion in dispatcher_t::check_collections_format_");
                                }
                            }
                        }
                        // A column still typed NA after reconciliation carries no
                        // storable type. On a schemaless computing table that is an
                        // absent key (every row null), not a real column, and handing
                        // an all-NA column to storage segfaults the append. A declared
                        // table never reaches here NA — its columns are typed by the
                        // schema — so drop such columns only for a computing target.
                        if (insert_target_relkind == 'g') {
                            auto& cols = chunk.data;
                            cols.erase(std::remove_if(cols.begin(),
                                                      cols.end(),
                                                      [](const components::vector::vector_t& c) {
                                                          return c.type().type() == logical_type::NA;
                                                      }),
                                       cols.end());
                        }
                    }
                }
            }
            return true;
        };

        std::queue<node_t*> look_up;
        look_up.emplace(logical_plan);
        while (!look_up.empty()) {
            auto plan_node = look_up.front();

            if (check_node(plan_node)) {
                for (const auto& child : plan_node->children()) {
                    look_up.emplace(child.get());
                }
                look_up.pop();
            } else {
                return result;
            }
        }

        return core::error_t::no_error();
    }

    // Renamed body of the public validate_schema. All node recursion calls the public
    // wrapper below (which stamps), so every node's output schema is recorded.
    [[nodiscard]] static core::result_wrapper_t<named_schema>
    validate_schema_impl(const validation::validation_context_t& context,
                         node_t* node,
                         const components::logical_plan::storage_parameters& parameters,
                         cte_schemas_t* cte_schemas) {
        auto* resource = context.resource;
        const auto* resolves = context.resolves;
        const auto* cast_registry = &context.cast_registry;
        named_schema result{resource};

        switch (node->type()) {
            // Host-extension: a REGISTERED CATALOG TABLE lowered by a host operator.
            //   - SINK (has a child): a federated WRITE (INSERT..SELECT into a
            //     backend). Validate the child (the rows to write) so they are
            //     typed; the statement returns an affected-count, so its output
            //     schema is empty (NoData) — like a plain DML without RETURNING.
            //   - SOURCE (leaf): typed exactly like any table — from the catalog by
            //     its (db, rel), resolved into the plan-tree idx by the standard
            //     catalog-resolve wrap. Surfacing the columns lets parents (JOIN /
            //     GROUP BY / SELECT) type normally and the wrapper stamp output_types().
            // An unregistered target/source (missing tbl_md) is a host bug.
            case node_type::extension_t: {
                const auto* ext = static_cast<const components::logical_plan::node_extension_t*>(node);
                if (!node->children().empty()) {
                    auto child = validate_schema(context, node->children().front().get(), parameters, cte_schemas);
                    if (child.has_error()) {
                        return child;
                    }
                    return result; // empty = affected-count / NoData
                }
                const auto* tbl = node->table_metadata();
                if (!tbl) {
                    return core::error_t(
                        core::error_code_t::table_not_exists,
                        std::pmr::string{"extension table is not registered in the catalog", resource});
                }
                const std::string& visible_alias = node->result_alias().empty() ? ext->relname() : node->result_alias();
                for (const auto& column : tbl->columns) {
                    type_from_t entry;
                    entry.result_alias = visible_alias;
                    entry.type = column.type;
                    result.push_back(std::move(entry));
                }
                return result;
            }
            // SQL transaction-control leaf (BEGIN/COMMIT/ROLLBACK): no table
            // schema to validate — empty schema, like an all-resolve sequence_t.
            // Defensive mirror of the executor's validate break-group; without
            // this the default arm below assert(false)s on the node type.
            case node_type::transaction_t:
                break;
            case node_type::aggregate_t: {
                auto* aggregate_node = static_cast<node_aggregate_t*>(node);
                node_group_t* node_group = nullptr;
                node_match_t* node_match = nullptr;
                node_sort_t* node_sort = nullptr;
                node_select_t* node_select = nullptr;
                node_having_t* node_having = nullptr;
                node_t* node_data = nullptr;

                named_schema table_schema(resource);
                named_schema incoming_schema(resource);
                const named_schema* source_schema = &incoming_schema;
                // Set when the aggregate's direct source is a relkind='g' (computed) table scan: its
                // transfer_scan reorders columns by chunk_position, so a validate-time schema index
                // would not match the runtime scan index — DISTINCT ON is rejected on such tables (v1).
                bool relkind_computed = false;

                for (auto& child : node->children()) {
                    switch (child->type()) {
                        case node_type::group_t:
                            node_group = reinterpret_cast<node_group_t*>(child.get());
                            break;
                        case node_type::match_t:
                            node_match = reinterpret_cast<node_match_t*>(child.get());
                            break;
                        case node_type::sort_t:
                            node_sort = reinterpret_cast<node_sort_t*>(child.get());
                            break;
                        case node_type::limit_t:
                            break;
                        case node_type::select_t:
                            node_select = reinterpret_cast<node_select_t*>(child.get());
                            break;
                        case node_type::having_t:
                            node_having = reinterpret_cast<node_having_t*>(child.get());
                            break;
                        default:
                            node_data = child.get();
                            break;
                    }
                }

                // Table-valued jsonb operators ('->'/'#>' expand, '-'/'#-' delete)
                // are lowered to per-column get_field only on the non-GROUP-BY path.
                // With a GROUP BY (or a bare aggregate, which also routes here) they
                // are never expanded, so an un-expanded jsonb_expand/jsonb_delete would
                // reach physical execution and crash. Reject them cleanly instead —
                // expanding one row into several columns has no meaning under grouping.
                if (node_group && node_select) {
                    for (const auto& expr : node_select->expressions()) {
                        if (expr->group() != expression_group::scalar) {
                            continue;
                        }
                        auto* se = reinterpret_cast<scalar_expression_t*>(expr.get());
                        if (se->type() == scalar_type::jsonb_expand || se->type() == scalar_type::jsonb_delete) {
                            return core::error_t(core::error_code_t::schema_error,
                                                 std::pmr::string{"table-valued jsonb operator ('->'/'#>'/'-'/'#-') "
                                                                  "is not supported with GROUP BY or aggregation",
                                                                  resource});
                        }
                    }
                }

                if (node_data) {
                    auto node_data_res = validate_schema(context, node_data, parameters, cte_schemas);
                    if (node_data_res.has_error()) {
                        return node_data_res;
                    } else {
                        incoming_schema = std::move(node_data_res.value());
                    }
                } else if (auto* agg_node = static_cast<node_aggregate_t*>(node);
                           !static_cast<const std::string&>(agg_node->dbname()).empty()) {
                    const auto& agg_dbname_s = static_cast<const std::string&>(agg_node->dbname());
                    const auto& agg_relname_s = static_cast<const std::string&>(agg_node->relname());
                    const auto& visible_alias = node->result_alias().empty() ? agg_relname_s : node->result_alias();
                    // there will be a scan
                    const auto* tbl = node->table_metadata();
                    if (tbl) {
                        relkind_computed = (tbl->relkind == 'g');
                        // Both relkinds ('g' and non-'g') build the schema identically
                        // here: same alias source (visible_alias) and same column loop.
                        for (const auto& column : tbl->columns) {
                            table_schema.emplace_back(type_from_t{visible_alias, column.type});
                        }
                    } else {
                        // Distinguish missing database from missing collection
                        // so callers (and tests) get the right error code.
                        if (!resolves || resolves->namespace_oid(std::string_view(agg_dbname_s)) ==
                                             components::catalog::INVALID_OID) {
                            std::pmr::string msg{"database does not exist: ", resource};
                            msg.append(agg_dbname_s.begin(), agg_dbname_s.end());
                            return core::error_t(core::error_code_t::database_not_exists, std::move(msg));
                        }
                        std::pmr::string msg{"collection does not exist: ", resource};
                        msg.append(agg_dbname_s.begin(), agg_dbname_s.end());
                        msg += '.';
                        msg.append(agg_relname_s.begin(), agg_relname_s.end());
                        return core::error_t(core::error_code_t::table_not_exists, std::move(msg));
                    }
                }
                if (table_schema.empty() && incoming_schema.empty()) {
                    // Empty computing table — still need aggregate validation for function_uid
                }
                if (incoming_schema.empty()) {
                    incoming_schema = table_schema;
                    source_schema = nullptr;
                }
                if (table_schema.empty()) {
                    table_schema = incoming_schema;
                    source_schema = nullptr;
                }
                if (node_group != nullptr) {
                    std::pmr::vector<complex_logical_type> group_input_types{node_group->resource()};
                    group_input_types.reserve(incoming_schema.size());
                    for (const auto& column : incoming_schema) {
                        group_input_types.push_back(column.type);
                    }
                    node_group->set_input_types(std::move(group_input_types));
                }
                if (node_group != nullptr && node_select != nullptr) {
                    bool grouped = node_having != nullptr;
                    for (const auto& expr : node_group->expressions()) {
                        if (grouped) {
                            break;
                        }
                        if (expr->group() == expression_group::scalar &&
                            static_cast<scalar_expression_t*>(expr.get())->type() == scalar_type::group_field) {
                            grouped = true;
                        }
                    }
                    if (!grouped) {
                        // Without an explicit GROUP BY, the query is grouped if some SELECT-list
                        // expression reduces
                        bool reduces = false;
                        const validation::expression_context_t expression_context{
                            context.resource,
                            incoming_schema,
                            parameters,
                            context.cast_registry,
                            context.function_registry,
                            context.execution_context,
                            components::compute::create_mask(components::compute::function_type_t::row,
                                                             components::compute::function_type_t::vector,
                                                             components::compute::function_type_t::aggregate)};
                        for (auto& expr : node_group->expressions()) {
                            if (expr->group() == expression_group::aggregate) {
                                reduces = true;
                                continue;
                            }
                            if (expr->group() == expression_group::scalar) {
                                const auto scalar_kind = static_cast<scalar_expression_t*>(expr.get())->type();
                                // Plain column references and '*' are grouping keys, bound where the
                                // keys are collected rather than resolved as values here.
                                if (scalar_kind == scalar_type::group_field || scalar_kind == scalar_type::get_field ||
                                    scalar_kind == scalar_type::star_expand) {
                                    continue;
                                }
                            }
                            if (auto error = validation::resolve_expression(expr, expression_context, &reduces);
                                error.contains_error()) {
                                return error;
                            }
                        }
                        grouped = reduces;
                    }
                    if (!grouped) {
                        for (auto& expr : node_group->expressions()) {
                            node_select->append_expression(expr);
                        }
                        node_group->expressions().clear();
                        auto& children = node->children();
                        children.erase(std::remove_if(children.begin(),
                                                      children.end(),
                                                      [node_group](const components::logical_plan::node_ptr& child) {
                                                          return child.get() == node_group;
                                                      }),
                                       children.end());
                        node_group = nullptr;
                    }
                }

                if (node_match) {
                    // Expand is_not_null/is_null on multi-type fields into OR/AND
                    // over their variants (jsonb '?'/'?|'/'?&' over multi-type).
                    for (auto& e : node_match->expressions()) {
                        e = impl::rewrite_multitype_null_checks(resource, e, incoming_schema);
                    }
                    auto res = impl::validate_schema(context, node_match, parameters, &table_schema, source_schema);
                    if (res.has_error()) {
                        return res;
                    }
                }

                if (!node_group) {
                    if (node_sort) {
                        auto res = impl::validate_schema(context, node_sort, incoming_schema, parameters);
                        if (res.has_error()) {
                            return res;
                        }
                    }
                    // DISTINCT ON: resolve the ON keys against the pre-projection (incoming) schema —
                    // the same column index the runtime distinct-below-select operator reads from the
                    // transfer_scan. Outside the node_sort guard so a no-ORDER-BY DISTINCT ON resolves.
                    if (!aggregate_node->distinct_on_keys().empty()) {
                        if (relkind_computed) {
                            return core::error_t(core::error_code_t::unimplemented_yet,
                                                 std::pmr::string{"DISTINCT ON is not yet supported on "
                                                                  "relkind='g' (dynamic-schema) tables",
                                                                  resource});
                        }
                        for (auto& on_key : aggregate_node->distinct_on_keys()) {
                            auto r = validation::find_types(resource, on_key, incoming_schema);
                            if (r.has_error()) {
                                return r.convert_error<named_schema>();
                            }
                        }
                    }
                    // Validate node_select expressions (no GROUP BY path)
                    if (node_select) {
                        // Pre-expand UDT .* expressions into individual child fields
                        {
                            auto& exprs = node_select->expressions();
                            for (size_t expr_index = 0; expr_index < exprs.size();) {
                                if (exprs[expr_index]->group() != expression_group::scalar) {
                                    expr_index++;
                                    continue;
                                }
                                auto* scalar_expr = reinterpret_cast<scalar_expression_t*>(exprs[expr_index].get());
                                // t.x.* — expand by result_alias against merged JOIN schema.
                                if (scalar_expr->type() == scalar_type::star_expand &&
                                    !scalar_expr->key().storage().empty() &&
                                    scalar_expr->key().storage().front() != "*") {
                                    const auto& alias = scalar_expr->key().storage().front();
                                    std::pmr::vector<size_t> matched(resource);
                                    for (size_t i = 0; i < incoming_schema.size(); i++) {
                                        if (core::pmr::operator==(incoming_schema[i].result_alias, alias)) {
                                            matched.push_back(i);
                                        }
                                    }
                                    if (matched.empty()) {
                                        return core::error_t(core::error_code_t::schema_error,
                                                             std::pmr::string{(std::string{"alias '"} + alias.c_str() +
                                                                               "' has no columns in scope")
                                                                                  .c_str(),
                                                                              resource});
                                    }
                                    exprs.erase(exprs.begin() + static_cast<ptrdiff_t>(expr_index));
                                    for (size_t j = 0; j < matched.size(); j++) {
                                        size_t schema_idx = matched[j];
                                        components::expressions::key_t new_key(resource);
                                        if (incoming_schema[schema_idx].type.has_alias()) {
                                            new_key.storage().push_back(
                                                std::pmr::string(incoming_schema[schema_idx].type.alias(), resource));
                                        }
                                        new_key.set_path(column_path{{schema_idx}, resource});
                                        exprs.insert(exprs.begin() + static_cast<ptrdiff_t>(expr_index + j),
                                                     make_scalar_expression(resource, scalar_type::get_field, new_key));
                                    }
                                    expr_index += matched.size();
                                    continue;
                                }
                                if (scalar_expr->type() == scalar_type::star_expand &&
                                    scalar_expr->key().storage().empty()) {
                                    components::expressions::key_t star_key(resource);
                                    star_key.storage().push_back(std::pmr::string("*", resource));
                                    exprs[expr_index] =
                                        make_scalar_expression(resource, scalar_type::get_field, star_key);
                                    continue;
                                }
                                if (scalar_expr->type() != scalar_type::get_field) {
                                    expr_index++;
                                    continue;
                                }
                                auto& k_ref =
                                    scalar_expr->params().empty()
                                        ? scalar_expr->key()
                                        : std::get<components::expressions::key_t>(scalar_expr->params().front());
                                if (k_ref.storage().empty() || k_ref.storage().back() != "*") {
                                    expr_index++;
                                    continue;
                                }
                                components::expressions::key_t k_copy(k_ref);
                                auto field = validation::find_types(resource, k_copy, incoming_schema);
                                if (field.has_error()) {
                                    return field.convert_error<named_schema>();
                                }
                                auto& field_paths = field.value();
                                exprs.erase(exprs.begin() + static_cast<ptrdiff_t>(expr_index));
                                for (size_t j = 0; j < field_paths.size(); j++) {
                                    components::expressions::key_t new_key(resource);
                                    for (size_t sub = 0; sub + 1 < k_copy.storage().size(); sub++) {
                                        new_key.storage().push_back(k_copy.storage()[sub]);
                                    }
                                    if (field_paths[j].type.has_alias()) {
                                        new_key.storage().push_back(
                                            std::pmr::string(field_paths[j].type.alias(), resource));
                                    }
                                    new_key.set_path(field_paths[j].path);
                                    exprs.insert(exprs.begin() + static_cast<ptrdiff_t>(expr_index + j),
                                                 make_scalar_expression(resource, scalar_type::get_field, new_key));
                                }
                                expr_index += field_paths.size();
                            }
                        }

                        // Pre-expand table-valued jsonb operators (jsonb_expand '->'/'#>'
                        // and jsonb_delete '-'/'#-') into individual get_field columns
                        // against the resolved schema. On a computing table nested fields
                        // are flattened to columns named by their slash-joined path, so:
                        //   expand prefix P -> every column == P or under "P/", rerooted
                        //                      (strip "P/"; a leaf == P keeps its last seg)
                        //   delete prefix P -> every column NOT under P (kept as-is)
                        {
                            auto& exprs = node_select->expressions();
                            for (size_t ei = 0; ei < exprs.size();) {
                                if (exprs[ei]->group() != expression_group::scalar) {
                                    ei++;
                                    continue;
                                }
                                auto* se = reinterpret_cast<scalar_expression_t*>(exprs[ei].get());
                                const bool is_expand = se->type() == scalar_type::jsonb_expand;
                                const bool is_delete = se->type() == scalar_type::jsonb_delete;
                                if (!is_expand && !is_delete) {
                                    ei++;
                                    continue;
                                }
                                const std::string prefix = se->key().as_string();
                                const std::string prefix_slash = prefix + "/";
                                // A jsonb operator 'base OP path' works on the columns of
                                // ONE table — the base. In a join the two sides share
                                // subtree names (both l and m may carry "d/e"), so the
                                // base's side is what disambiguates: without it the loop
                                // matched columns from both sides and every produced
                                // get_field became ambiguous ("path not found"). The array
                                // delete form keeps its side on the params, not key().
                                components::expressions::side_t op_side = se->key().side();
                                if (se->key().is_null()) {
                                    for (const auto& p : se->params()) {
                                        if (std::holds_alternative<components::expressions::key_t>(p)) {
                                            op_side = std::get<components::expressions::key_t>(p).side();
                                            break;
                                        }
                                    }
                                }
                                auto on_op_side = [&](const type_from_t& sc) {
                                    return op_side == side_t::undefined || sc.side == side_t::undefined ||
                                           sc.side == op_side;
                                };
                                // Delete may carry several prefixes: key() plus any
                                // key_t params (the multi-key form `jsonb - text[]`).
                                // A column survives only if it is under NONE of them.
                                std::vector<std::string> del_prefixes;
                                if (is_delete) {
                                    if (!se->key().is_null()) {
                                        del_prefixes.push_back(prefix);
                                    }
                                    for (const auto& p : se->params()) {
                                        if (std::holds_alternative<components::expressions::key_t>(p)) {
                                            del_prefixes.push_back(
                                                std::get<components::expressions::key_t>(p).as_string());
                                        }
                                    }
                                }
                                auto under_any = [&](const std::string& alias) {
                                    for (const auto& pfx : del_prefixes) {
                                        if (alias == pfx || alias.rfind(pfx + "/", 0) == 0) {
                                            return true;
                                        }
                                    }
                                    return false;
                                };
                                // (output_name, source_alias) pairs
                                std::vector<std::pair<std::string, std::string>> cols;
                                for (const auto& sc : incoming_schema) {
                                    if (!sc.type.has_alias() || !on_op_side(sc)) {
                                        continue;
                                    }
                                    std::string alias(sc.type.alias());
                                    if (is_delete) {
                                        if (!under_any(alias)) {
                                            cols.emplace_back(alias, alias);
                                        }
                                    } else if (alias == prefix || alias.rfind(prefix_slash, 0) == 0) {
                                        std::string out = alias == prefix ? prefix.substr(prefix.find_last_of('/') + 1)
                                                                          : alias.substr(prefix_slash.size());
                                        cols.emplace_back(std::move(out), std::move(alias));
                                    }
                                }
                                // Expand names a specific object: a key matching no
                                // column is a "path not found" error, the same as the
                                // scalar form ('->>'/'#>>'), never a select item that
                                // silently vanishes and hands the caller fewer columns.
                                // (Delete-to-empty is legal — it yields the empty object
                                // '{}' — so this guard is expand-only.)
                                if (is_expand && cols.empty()) {
                                    return core::error_t(core::error_code_t::schema_error,
                                                         std::pmr::string{(std::string{"jsonb expand: path '"} +
                                                                           prefix + "' matches no column")
                                                                              .c_str(),
                                                                          resource});
                                }
                                exprs.erase(exprs.begin() + static_cast<ptrdiff_t>(ei));
                                for (size_t j = 0; j < cols.size(); j++) {
                                    components::expressions::key_t out_key(resource, cols[j].first.c_str());
                                    components::expressions::key_t src_key(resource, cols[j].second.c_str());
                                    // Carry the base's side so the shared subtree name
                                    // resolves back to the side the operator named.
                                    src_key.set_side(op_side);
                                    exprs.insert(
                                        exprs.begin() + static_cast<ptrdiff_t>(ei + j),
                                        make_scalar_expression(resource, scalar_type::get_field, out_key, src_key));
                                }
                                ei += cols.size();
                            }
                        }

                        bool has_computed_column = false;
                        for (auto& expr : node_select->expressions()) {
                            if (expr->group() == expression_group::function ||
                                expr->group() == expression_group::compare || expr->group() == expression_group::cast) {
                                has_computed_column = true;
                                continue;
                            }
                            if (expr->group() != expression_group::scalar) {
                                continue;
                            }
                            auto* scalar_expr = reinterpret_cast<scalar_expression_t*>(expr.get());
                            if (scalar_expr->type() == scalar_type::get_field) {
                                auto& key =
                                    scalar_expr->params().empty()
                                        ? scalar_expr->key()
                                        : std::get<components::expressions::key_t>(scalar_expr->params().front());
                                if (key.path().empty()) {
                                    auto validated_key = validation::validate_key(resource, key, &incoming_schema);
                                    if (validated_key.has_error()) {
                                        return validated_key.convert_error<named_schema>();
                                    }
                                }
                                const auto& col_type = incoming_schema[key.path()[0]].type;
                                const components::types::complex_logical_type* res_type = &col_type;
                                for (size_t j = 1; j < key.path().size(); j++) {
                                    if (!res_type->is_nested()) {
                                        return core::error_t(
                                            core::error_code_t::schema_error,
                                            std::pmr::string{"trying to access field of non-nested type", resource});
                                    } else if (res_type->type() == logical_type::STRUCT) {
                                        res_type = &res_type->child_types()[key.path()[j]];
                                    } else {
                                        res_type = &res_type->child_type();
                                    }
                                }
                                result.emplace_back(type_from_t{node->result_alias(), *res_type});
                            } else if (scalar_expr->type() == scalar_type::star_expand) {
                                for (const auto& col : incoming_schema) {
                                    result.emplace_back(col);
                                }
                            } else {
                                if (scalar_expr->type() != scalar_type::constant) {
                                    auto res = impl::resolve_key_paths_in_group(resource,
                                                                                scalar_expr->params(),
                                                                                incoming_schema);
                                    if (res.has_error()) {
                                        return res.convert_error<named_schema>();
                                    }
                                }
                                has_computed_column = true;
                            }
                        }
                        if (!has_computed_column) {
                            return result;
                        }
                    } else {
                        // "SELECT *" / "SELECT t.*" — emit every column, including
                        // several same-name columns of different types (multi-type
                        // fields on a computing table); the wildcard simply returns
                        // them all (an EXPLICIT reference to such a name still errors
                        // as ambiguous in find_types and must use type selection).
                        // Duplicate names across JOIN'd tables are likewise legitimate
                        // (PostgreSQL semantics) — including a self-join, where the
                        // copies are distinguished by their join side. Reject only a
                        // truly-identical column: same output alias AND same source
                        // name AND same physical type AND same join side (multi-type
                        // fields of one computing table all share one side).
                        struct column_key {
                            std::string result_alias;
                            std::string name;
                            logical_type type;
                            side_t side;
                            auto operator<=>(const column_key&) const = default;
                        };
                        std::set<column_key> seen_cols;
                        for (const auto& col : incoming_schema) {
                            // A projected constant (e.g. `SELECT 1`) has no alias/extension;
                            // complex_logical_type::alias() asserts on that, so guard it. An
                            // alias-less column keys on the empty string, which is correct for
                            // this duplicate-name check.
                            std::string col_alias =
                                col.type.has_alias() ? std::string(col.type.alias()) : std::string{};
                            column_key key{col.result_alias, col_alias, col.type.type(), col.side};
                            if (!seen_cols.insert(std::move(key)).second) {
                                return core::error_t(
                                    core::error_code_t::schema_error,
                                    std::pmr::string{"column '" + col_alias +
                                                         "' has multiple types; use explicit type selection",
                                                     resource});
                            }
                        }
                    }
                    if (node_select) {
                        named_schema result_schema(resource);
                        for (auto& expr : node_select->expressions()) {
                            if (expr->group() == expression_group::function ||
                                expr->group() == expression_group::compare || expr->group() == expression_group::cast) {
                                complex_logical_type out_type = expr->result_type();
                                const components::expressions::key_t* out_key = nullptr;
                                if (expr->group() == expression_group::function) {
                                    out_key = &static_cast<function_expression_t*>(expr.get())->key();
                                } else if (expr->group() == expression_group::compare) {
                                    out_key = &static_cast<compare_expression_t*>(expr.get())->key();
                                } else {
                                    out_key =
                                        &static_cast<components::expressions::cast_expression_t*>(expr.get())->key();
                                }
                                if (!out_key->is_null()) {
                                    out_type.set_alias(out_key->as_string());
                                }
                                result_schema.push_back(type_from_t{node->result_alias(), std::move(out_type)});
                                continue;
                            }
                            if (expr->group() != expression_group::scalar) {
                                continue;
                            }
                            auto* scalar_expr = reinterpret_cast<scalar_expression_t*>(expr.get());
                            if (scalar_expr->type() == scalar_type::get_field) {
                                const auto& key =
                                    scalar_expr->params().empty()
                                        ? scalar_expr->key()
                                        : std::get<components::expressions::key_t>(scalar_expr->params().front());
                                if (!key.path().empty() && key.path().front() < incoming_schema.size()) {
                                    result_schema.push_back(incoming_schema[key.path().front()]);
                                }
                            } else if (scalar_expr->type() == scalar_type::star_expand) {
                                for (const auto& col : incoming_schema) {
                                    result_schema.push_back(col);
                                }
                            } else {
                                // Computed projection (CASE / COALESCE / arithmetic /
                                // unary_minus / constant): resolve the real output type
                                // against incoming_schema. Rule 6 — never the UNKNOWN
                                // sentinel; an unresolvable type is a bind error.
                                auto resolve_error = impl::resolve_scalar_output_type(
                                    context,
                                    scalar_expr,
                                    incoming_schema,
                                    parameters,
                                    components::compute::create_mask(components::compute::function_type_t::row,
                                                                     components::compute::function_type_t::vector,
                                                                     components::compute::function_type_t::aggregate));
                                if (resolve_error.contains_error()) {
                                    return resolve_error;
                                }
                                // A bare NULL literal is a scalar constant whose bound value is NULL.
                                // Mark the column so a UNION can reconcile it to the other branch's type.
                                bool from_null = false;
                                if (scalar_expr->type() == scalar_type::constant && !scalar_expr->params().empty() &&
                                    components::expressions::is_parameter(scalar_expr->params().front())) {
                                    auto pit = parameters.parameters.find(
                                        components::expressions::as_parameter(scalar_expr->params().front()));
                                    from_null = (pit != parameters.parameters.end() && pit->second.is_null());
                                }
                                complex_logical_type out_type = expr->result_type();
                                if (!expr->key().is_null()) {
                                    out_type.set_alias(expr->key().as_string());
                                }
                                type_from_t entry{node->result_alias(), std::move(out_type)};
                                entry.from_null_literal = from_null;
                                result_schema.push_back(std::move(entry));
                            }
                        }
                        return result_schema;
                    }
                    return incoming_schema;
                } else {
                    // Pre-expand UDT .* expressions into individual child fields
                    {
                        auto& exprs = node_group->expressions();
                        for (size_t expr_index = 0; expr_index < exprs.size();) {
                            if (exprs[expr_index]->group() != expression_group::scalar) {
                                expr_index++;
                                continue;
                            }
                            auto* scalar_expr = reinterpret_cast<scalar_expression_t*>(exprs[expr_index].get());
                            if (scalar_expr->type() != scalar_type::get_field) {
                                expr_index++;
                                continue;
                            }
                            auto& k_ref = scalar_expr->params().empty()
                                              ? scalar_expr->key()
                                              : std::get<components::expressions::key_t>(scalar_expr->params().front());
                            if (k_ref.storage().empty() || k_ref.storage().back() != "*") {
                                expr_index++;
                                continue;
                            }
                            // Copy key before find_types (which mutates it via set_path)
                            // and before erase (which invalidates the reference)
                            components::expressions::key_t k_copy(k_ref);
                            auto field = validation::find_types(resource, k_copy, incoming_schema);
                            if (field.has_error()) {
                                return field.convert_error<named_schema>();
                            }

                            auto& field_paths = field.value();
                            exprs.erase(exprs.begin() + static_cast<ptrdiff_t>(expr_index));
                            for (size_t j = 0; j < field_paths.size(); j++) {
                                components::expressions::key_t new_key(resource);
                                for (size_t sub_field_index = 0; sub_field_index + 1 < k_copy.storage().size();
                                     sub_field_index++)
                                    new_key.storage().push_back(k_copy.storage()[sub_field_index]);
                                // Append child field name so plan generator picks it up
                                if (field_paths[j].type.has_alias()) {
                                    new_key.storage().push_back(
                                        std::pmr::string(field_paths[j].type.alias(), resource));
                                }
                                new_key.set_path(field_paths[j].path);
                                exprs.insert(exprs.begin() + static_cast<ptrdiff_t>(expr_index + j),
                                             make_scalar_expression(resource, scalar_type::get_field, new_key));
                            }
                            expr_index += field_paths.size();
                        }
                    }

                    // --- Helpers ---
                    auto is_case_or_arithmetic = [](scalar_type t) -> bool {
                        switch (t) {
                            case scalar_type::case_expr:
                            case scalar_type::add:
                            case scalar_type::subtract:
                            case scalar_type::multiply:
                            case scalar_type::divide:
                            case scalar_type::mod:
                            case scalar_type::unary_minus:
                            case scalar_type::bit_and:
                            case scalar_type::bit_or:
                            case scalar_type::bit_xor:
                            case scalar_type::bit_not:
                            case scalar_type::shift_left:
                            case scalar_type::shift_right:
                                return true;
                            default:
                                return false;
                        }
                    };

                    // The GROUP BY keys, resolved first: whether a column reference is a grouping
                    // key or a bare row value decides the cardinality of every expression that
                    // reads it, so the key set is an input to the resolutions below.
                    std::pmr::vector<std::pmr::vector<size_t>> key_paths(resource);
                    for (const auto& expr : node_group->expressions()) {
                        if (expr->group() != expression_group::scalar) {
                            continue;
                        }
                        auto* scalar_expr = static_cast<scalar_expression_t*>(expr.get());
                        if (scalar_expr->type() != scalar_type::group_field) {
                            continue;
                        }
                        auto res = validation::validate_key(resource, scalar_expr->key(), &incoming_schema);
                        if (res.has_error()) {
                            return res.convert_error<named_schema>();
                        }
                        key_paths.emplace_back(scalar_expr->key().path().begin(), scalar_expr->key().path().end());
                    }

                    // resolve_type/compute_type_entry have no return-channel for
                    // errors (they return plain types); a missing parameter is
                    // surfaced through this flag, checked after each call site.
                    core::error_t compute_type_error = core::error_t::no_error();
                    auto compute_type_entry =
                        [&](scalar_expression_t* scalar_expr,
                            const named_schema& schema,
                            const std::pmr::vector<std::pmr::vector<size_t>>* group_keys) -> type_from_t {
                        const validation::expression_context_t expression_context{
                            context.resource,
                            schema,
                            parameters,
                            context.cast_registry,
                            context.function_registry,
                            context.execution_context,
                            components::compute::create_mask(components::compute::function_type_t::row,
                                                             components::compute::function_type_t::vector),
                            nullptr,
                            group_keys};
                        expression_ptr expression{scalar_expr};
                        if (auto error = validation::resolve_expression(expression, expression_context);
                            error.contains_error()) {
                            compute_type_error = error;
                            return type_from_t{node->result_alias(), complex_logical_type(logical_type::INVALID)};
                        }
                        auto result_type = scalar_expr->result_type();
                        if (!scalar_expr->key().is_null()) {
                            result_type.set_alias(scalar_expr->key().as_string());
                        }
                        return type_from_t{node->result_alias(), std::move(result_type)};
                    };

                    // --- Resolve the projected expressions and classify cardinality ---
                    {
                        // The target list of a grouped query:
                        const validation::expression_context_t projection_context{
                            context.resource,
                            incoming_schema,
                            parameters,
                            context.cast_registry,
                            context.function_registry,
                            context.execution_context,
                            components::compute::create_mask(components::compute::function_type_t::row,
                                                             components::compute::function_type_t::vector,
                                                             components::compute::function_type_t::aggregate,
                                                             components::compute::function_type_t::expand),
                            nullptr,
                            &key_paths};
                        for (auto& expr : node_group->expressions()) {
                            if (expr->group() == expression_group::scalar) {
                                const auto kind = static_cast<scalar_expression_t*>(expr.get())->type();
                                if (kind == scalar_type::group_field) {
                                    continue; // the key list itself, not a projected column
                                }
                                if (kind == scalar_type::star_expand) {
                                    continue; // a structural marker, expanded by the node-level pass
                                }
                            }
                            if (auto error = validation::resolve_expression(expr, projection_context);
                                error.contains_error()) {
                                return error;
                            }
                            const auto projected = expr->cardinality();
                            // A grouped query emits one row per group, so every projected
                            // expression has to be one value per group: a grouping key, a
                            // reduction, or a constant. Row cardinality at the top of one is a
                            // bare non-key column.
                            if (projected == cardinality_t::row) {
                                return core::error_t(core::error_code_t::sql_parse_error,
                                                     std::pmr::string{"column must appear in a GROUP BY clause or be "
                                                                      "used in an aggregate function",
                                                                      resource});
                            }
                        }
                    }

                    // --- Pass 1: classify + resolve + collect schemas ---
                    size_t select_end = node_group->expressions().size() - node_group->internal_aggregate_count;
                    named_schema key_schema(resource);
                    std::vector<size_t> post_agg_indices;
                    std::vector<size_t> agg_result_positions;

                    for (size_t i = 0; i < node_group->expressions().size(); i++) {
                        auto& expr = node_group->expressions()[i];
                        if (expr->group() == expression_group::scalar) {
                            auto* scalar_expr = reinterpret_cast<scalar_expression_t*>(expr.get());
                            if (scalar_expr->type() == scalar_type::get_field) {
                                // get_field — existing code unchanged
                                auto& key =
                                    scalar_expr->params().empty()
                                        ? scalar_expr->key()
                                        : std::get<components::expressions::key_t>(scalar_expr->params().front());
                                auto res = validation::validate_key(resource, key, &incoming_schema);
                                if (res.has_error()) {
                                    return res.convert_error<named_schema>();
                                }

                                const auto& col_type = incoming_schema[key.path()[0]].type;
                                const components::types::complex_logical_type* res_type = &col_type;
                                for (size_t j = 1; j < key.path().size(); j++) {
                                    if (!res_type->is_nested()) {
                                        return core::error_t(
                                            core::error_code_t::schema_error,

                                            std::pmr::string{"trying to access field of non-nested type", resource});
                                    } else if (res_type->type() == logical_type::STRUCT) {
                                        res_type = &res_type->child_types()[key.path()[j]];
                                    } else {
                                        res_type = &res_type->child_type();
                                    }
                                }
                                result.emplace_back(type_from_t{node->result_alias(), *res_type});
                                key_schema.emplace_back(result.back());
                            } else if (scalar_expr->type() == scalar_type::group_field) {
                                auto& key = scalar_expr->key();
                                auto res = validation::validate_key(resource, key, &incoming_schema);
                                if (res.has_error()) {
                                    return res.convert_error<named_schema>();
                                }
                            } else if (scalar_expr->type() == scalar_type::constant) {
                                if (scalar_expr->params().empty() ||
                                    !components::expressions::is_parameter(scalar_expr->params().front())) {
                                    return core::error_t(
                                        core::error_code_t::invalid_parameter,
                                        std::pmr::string{"constant in a grouped projection carries no parameter",
                                                         resource});
                                }
                                auto constant_it = parameters.parameters.find(
                                    components::expressions::as_parameter(scalar_expr->params().front()));
                                if (constant_it == parameters.parameters.end()) {
                                    return core::error_t(
                                        core::error_code_t::invalid_parameter,
                                        std::pmr::string{"unbound parameter in a grouped projection", resource});
                                }
                                complex_logical_type constant_type = constant_it->second.type();
                                if (!scalar_expr->key().is_null()) {
                                    constant_type.set_alias(scalar_expr->key().as_string());
                                }
                                result.emplace_back(type_from_t{node->result_alias(), constant_type});
                                key_schema.emplace_back(result.back());
                            } else if (is_case_or_arithmetic(scalar_expr->type())) {
                                // Try resolve against incoming_schema
                                auto res =
                                    impl::resolve_key_paths_in_group(resource, scalar_expr->params(), incoming_schema);
                                if (res.has_error()) {
                                    post_agg_indices.push_back(i); // defer to Pass 2
                                } else {
                                    auto entry = compute_type_entry(scalar_expr, incoming_schema, &key_paths);
                                    if (compute_type_error.contains_error()) {
                                        return compute_type_error;
                                    }
                                    result.emplace_back(entry);
                                    key_schema.emplace_back(entry);
                                }
                            }
                        } else if (expr->group() == expression_group::aggregate) {
                            auto* agg_expr = reinterpret_cast<aggregate_expression_t*>(expr.get());
                            bool is_internal = (i >= select_end);

                            // Resolving the marker binds its argument keys, applies the signature's
                            // argument casts and stamps uid / result / mergeable. A marker this
                            // walk already built is left alone; a HAVING marker from the
                            // transformer is resolved here for the first time.
                            const validation::expression_context_t aggregate_context{
                                context.resource,
                                incoming_schema,
                                parameters,
                                context.cast_registry,
                                context.function_registry,
                                context.execution_context,
                                components::compute::create_mask(components::compute::function_type_t::aggregate)};
                            if (auto error = validation::resolve_expression(expr, aggregate_context);
                                error.contains_error()) {
                                return error;
                            }
                            if (!is_internal) {
                                result.emplace_back(type_from_t{node->result_alias(), agg_expr->result_type()});
                                if (!agg_expr->key().is_null()) {
                                    result.back().type.set_alias(agg_expr->key().as_string());
                                }
                                agg_result_positions.push_back(result.size() - 1);
                            }
                        } else {
                            // TODO: add check to validate schema, if assert is triggered
                            assert(false);
                            return core::error_t(core::error_code_t::unimplemented_yet,
                                                 std::pmr::string{"unrecognized state in validate_schema", resource});
                        }
                    }

                    // --- Pass 2: build post_agg_schema + resolve deferred expressions ---
                    {
                        named_schema post_agg_schema(result);

                        for (size_t pa_idx : post_agg_indices) {
                            auto& expr = node_group->expressions()[pa_idx];
                            auto* scalar_expr = reinterpret_cast<scalar_expression_t*>(expr.get());

                            auto res2 =
                                impl::resolve_key_paths_in_group(resource, scalar_expr->params(), post_agg_schema);
                            if (res2.has_error()) {
                                return res2.convert_error<named_schema>();
                            }
                            scalar_expr->key().set_path({SIZE_MAX}); // Mark for planner

                            // Post-aggregation every column of post_agg_schema is already one value
                            // per group; the incoming key paths do not address that schema.
                            auto entry = compute_type_entry(scalar_expr, post_agg_schema, nullptr);
                            if (compute_type_error.contains_error()) {
                                return compute_type_error;
                            }
                            result.emplace_back(entry);
                        }
                    }

                    // Resolve node_select scalar expression key paths against the group output schema.
                    // GROUP BY key columns are real columns addressable by name (key_schema).
                    // Computed aggregate columns are internal artifacts — resolve positionally.
                    if (node_select) {
                        size_t agg_cursor = 0;
                        for (auto& expr : node_select->expressions()) {
                            if (expr->group() != expression_group::scalar) {
                                continue;
                            }
                            auto* scalar_expr = reinterpret_cast<scalar_expression_t*>(expr.get());
                            if (scalar_expr->type() == scalar_type::get_field) {
                                auto& key =
                                    scalar_expr->params().empty()
                                        ? scalar_expr->key()
                                        : std::get<components::expressions::key_t>(scalar_expr->params().front());
                                if (key.path().empty()) {
                                    auto res = validation::validate_key(resource, key, &key_schema);
                                    if (res.has_error()) {
                                        if (agg_cursor >= agg_result_positions.size()) {
                                            return res.convert_error<named_schema>();
                                        }
                                        key.set_path({agg_result_positions[agg_cursor++]});
                                    }
                                }
                            } else if (scalar_expr->type() != scalar_type::constant &&
                                       scalar_expr->type() != scalar_type::star_expand) {
                                auto res = impl::resolve_key_paths_in_group(resource, scalar_expr->params(), result);
                                if (res.has_error()) {
                                    return res.convert_error<named_schema>();
                                }
                                // Post-aggregation: reductions are already projected into columns.
                                auto resolve_error = impl::resolve_scalar_output_type(
                                    context,
                                    scalar_expr,
                                    result,
                                    parameters,
                                    components::compute::create_mask(components::compute::function_type_t::row,
                                                                     components::compute::function_type_t::vector));
                                if (resolve_error.contains_error()) {
                                    return resolve_error;
                                }
                            }
                        }
                    }

                    if (node_select && node_select->expressions().empty()) {
                        auto& children = node->children();
                        children.erase(std::remove_if(children.begin(),
                                                      children.end(),
                                                      [node_select](const components::logical_plan::node_ptr& child) {
                                                          return child.get() == node_select;
                                                      }),
                                       children.end());
                        node_select = nullptr;
                    }
                }
                if (node_sort) {
                    // Add hidden columns for sort keys not in the GROUP output
                    for (auto& sort_child : node_sort->expressions()) {
                        if (sort_child->group() != expression_group::sort) {
                            // A computed (scalar) sort key has no column of its own — its key
                            // carries the direction encoding, not a name. Its operands are
                            // resolved against the GROUP output by validate_schema below.
                            continue;
                        }
                        auto* sort_expr = static_cast<sort_expression_t*>(sort_child.get());
                        auto& skey = sort_expr->key();
                        // Try resolving in the GROUP result schema first
                        auto field_in_result = validation::find_types(resource, skey, result);
                        if (!field_in_result.has_error() && !field_in_result.value().empty()) {
                            continue; // already in result
                        }
                        // Not in result — try incoming schema and add as hidden column
                        auto field = validation::find_types(resource, skey, incoming_schema);
                        if (!field.has_error() && !field.value().empty()) {
                            auto hidden_expr = make_scalar_expression(resource, scalar_type::get_field, skey);
                            node_group->append_expression(hidden_expr);
                            result.emplace_back(type_from_t{node->result_alias(), field.value().front().type});
                        }
                    }
                    auto res = impl::validate_schema(context, node_sort, result, parameters);
                    if (res.has_error()) {
                        return res;
                    }
                }
                // DISTINCT ON (grouped): resolve the ON keys against the group output (hidden-extended
                // above), the layer the runtime distinct-below-select operator sees. Outside the
                // node_sort guard so a no-ORDER-BY DISTINCT ON still resolves.
                if (!aggregate_node->distinct_on_keys().empty()) {
                    for (auto& on_key : aggregate_node->distinct_on_keys()) {
                        auto r = validation::find_types(resource, on_key, result);
                        if (r.has_error()) {
                            return r.convert_error<named_schema>();
                        }
                    }
                }
                // HAVING is a first-class node_having_t child of the aggregate (its compare at
                // expressions()[0]), validated against the group-output `result` schema (built above:
                // GROUP keys + aggregate columns incl. hidden __having aggregates).
                if (node_having && !node_having->expressions().empty()) {
                    auto& having = node_having->expressions()[0];
                    if (having->group() == expression_group::compare) {
                        auto* cmp_expr = reinterpret_cast<compare_expression_t*>(having.get());
                        auto res = impl::validate_schema(context, cmp_expr, parameters, &result);
                        if (res.has_error()) {
                            return res;
                        }
                    }
                }
                break;
            }
            case node_type::data_t: {
                const auto* node_data = reinterpret_cast<node_data_t*>(node);
                const auto& chunk = node_data->data_chunk();
                result.reserve(chunk.column_count());
                for (const auto& column : chunk.data) {
                    result.emplace_back(type_from_t{node->result_alias(), column.type()});
                }
                break;
            }
            case node_type::function_t: {
                // FROM-clause table function (e.g. generate_series). The argument
                // input types come from the node's args: constants resolve through the
                // parameter map; a column-ref argument is a correlated (LATERAL)
                // reference, which needs an outer row and is handled by the LATERAL
                // join path (not yet supported here).
                auto* function_node = reinterpret_cast<node_function_t*>(node);
                std::pmr::vector<complex_logical_type> function_input(resource);
                function_input.reserve(function_node->args().size());
                for (const auto& arg : function_node->args()) {
                    if (!std::holds_alternative<core::parameter_id_t>(arg)) {
                        return core::error_t(
                            core::error_code_t::unimplemented_yet,
                            std::pmr::string{"table functions with correlated arguments require LATERAL "
                                             "(not yet supported)",
                                             resource});
                    }
                    auto param_it = parameters.parameters.find(std::get<core::parameter_id_t>(arg));
                    if (param_it == parameters.parameters.end()) {
                        return core::error_t(
                            core::error_code_t::create_physical_plan_error,
                            std::pmr::string{"unbound parameter referenced in table function arguments", resource});
                    }
                    function_input.emplace_back(param_it->second.type());
                }

                auto fn_resolved =
                    resolve_function(resource,
                                     *cast_registry,
                                     context.execution_context,
                                     context.function_registry,
                                     function_node->name(),
                                     function_input,
                                     components::compute::create_mask(components::compute::function_type_t::row,
                                                                      components::compute::function_type_t::vector,
                                                                      components::compute::function_type_t::expand));
                if (fn_resolved.has_error()) {
                    return fn_resolved.convert_error<named_schema>();
                }
                {
                    const std::string& alias =
                        function_node->result_alias().empty() ? function_node->name() : function_node->result_alias();
                    function_node->add_function_uid(fn_resolved.value().uid);
                    complex_logical_type out_type = fn_resolved.value().result;
                    out_type.set_alias(alias);
                    result.emplace_back(type_from_t{alias, std::move(out_type)});
                }
                break;
            }
            case node_type::join_t: {
                const auto* join_node = static_cast<const node_join_t*>(node);
                // A LATERAL reference can only appear on the right (inner) side of the
                // join, so RIGHT/FULL JOIN LATERAL is ill-defined and PostgreSQL rejects
                // it. The lateral join operator only honours LEFT-style NULL-extension;
                // RIGHT/FULL would otherwise fall through to plain inner semantics and
                // return a silently wrong answer. Reject it here instead.
                if (join_node->is_lateral() &&
                    (join_node->type() == join_type::right || join_node->type() == join_type::full)) {
                    return core::error_t(
                        core::error_code_t::unimplemented_yet,
                        std::pmr::string{"RIGHT/FULL JOIN LATERAL is not supported: a LATERAL reference cannot "
                                         "appear on the right side of a RIGHT or FULL join",
                                         resource});
                }
                auto left_schema = validate_schema(context, node->children().front().get(), parameters, cte_schemas);
                if (left_schema.has_error()) {
                    return left_schema;
                }
                // LATERAL: the inner (right) sub-plan may reference outer columns via
                // correlation parameters (a WHERE compare or a projected column). Those
                // parameters carry only a placeholder type from the transformer, so bind
                // each one's TYPE here from the matching outer (left-schema) column before
                // validating the inner plan — otherwise a projected correlated column has
                // no concrete type. The lateral join operator rebinds the real value per
                // outer row at execution; only the type matters for validation.
                const storage_parameters* inner_parameters = &parameters;
                storage_parameters lateral_parameters(resource);
                if (join_node->is_lateral() && !join_node->correlations().empty()) {
                    lateral_parameters.parameters = parameters.parameters;
                    for (const auto& correlation : join_node->correlations()) {
                        const auto& param_id = correlation.first;
                        const auto& key = correlation.second;
                        const std::string full = key.as_string();
                        const std::string last = key.storage().empty() ? full
                                                                       : std::string(key.storage().back().data(),
                                                                                     key.storage().back().size());
                        for (const auto& outer_col : left_schema.value()) {
                            if (outer_col.type.has_alias() &&
                                (outer_col.type.alias() == full || outer_col.type.alias() == last)) {
                                lateral_parameters.parameters.insert_or_assign(
                                    param_id,
                                    logical_value_t(resource, outer_col.type));
                                break;
                            }
                        }
                    }
                    inner_parameters = &lateral_parameters;
                }
                auto right_schema =
                    validate_schema(context, node->children().back().get(), *inner_parameters, cte_schemas);
                if (right_schema.has_error()) {
                    return right_schema;
                }
                auto expr_res =
                    impl::validate_schema(context,
                                          reinterpret_cast<compare_expression_t*>(node->expressions()[0].get()),
                                          parameters,
                                          &left_schema.value(),
                                          &right_schema.value());
                if (expr_res.has_error()) {
                    return expr_res;
                }

                // Semi-/anti-join output is the LEFT (outer) schema ONLY — the right side
                // contributes only existence (matched / not-matched), never columns. Every
                // other join type merges both sides.
                // TODO: merge using join type, because some join types allow duplicate names in result, while others do not
                if (join_node->type() == join_type::semi || join_node->type() == join_type::anti) {
                    result = std::move(left_schema.value());
                } else {
                    result = validation::merge_schemas(resource,
                                                       std::move(left_schema.value()),
                                                       std::move(right_schema.value()));
                }
                break;
            }
            // For now next 3 nodes do not support returning clause:
            case node_type::insert_t: {
                auto* insert_node = reinterpret_cast<node_insert_t*>(node);
                const auto* tbl_ins = insert_node->table_metadata();
                if (!tbl_ins) {
                    // node_insert_t carries only the (unresolved) table oid, no names.
                    return core::error_t(core::error_code_t::table_not_exists,
                                         std::pmr::string{"INSERT target collection does not exist", resource});
                }

                auto incoming_schema =
                    validate_schema(context, node->children().front().get(), parameters, cte_schemas);
                if (incoming_schema.has_error()) {
                    return incoming_schema;
                } else {
                    named_schema table_schema(resource);
                    bool is_computed = false;
                    // Insert node no longer carries relname; pull it
                    // from the resolved table metadata (populated by Pass 1).
                    const std::string& target_relname_ins = tbl_ins ? tbl_ins->name : std::string{};
                    if (tbl_ins && tbl_ins->relkind != 'g') {
                        for (const auto& column : tbl_ins->columns) {
                            table_schema.emplace_back(
                                type_from_t{node->result_alias().empty() ? target_relname_ins : node->result_alias(),
                                            column.type});
                        }
                    } else if (tbl_ins && tbl_ins->relkind == 'g') {
                        is_computed = true;
                        for (const auto& column : tbl_ins->columns) {
                            table_schema.emplace_back(type_from_t{target_relname_ins, column.type});
                        }
                    }
                    // RETURNING references the target table's columns; the insert
                    // operator reads the appended rows back from storage (full
                    // table-ordered schema), so resolve the projection keys here.
                    if (!insert_node->returning().empty() && !table_schema.empty()) {
                        auto ret_err = impl::resolve_returning_columns(context,
                                                                       &insert_node->returning(),
                                                                       &table_schema,
                                                                       nullptr,
                                                                       parameters);
                        if (ret_err.contains_error()) {
                            return ret_err;
                        }
                    }
                    // relkind='g' (dynamic-schema) tables accept INSERTs
                    // whose shape differs from the catalog's currently-registered columns,
                    // BUT only for simple types. Complex types (ARRAY/STRUCT/UNION/LIST)
                    // crash the storage layer's adopt_schema path — those tests stay
                    // rejected at validate to surface as a clean error instead of SIGSEGV.
                    auto is_simple_chunk = [&]() {
                        for (const auto& nt : incoming_schema.value()) {
                            const auto lt = nt.type.type();
                            if (lt == components::types::logical_type::ARRAY ||
                                lt == components::types::logical_type::LIST ||
                                lt == components::types::logical_type::STRUCT ||
                                lt == components::types::logical_type::UNION ||
                                lt == components::types::logical_type::MAP) {
                                return false;
                            }
                        }
                        return true;
                    };
                    // Even on an empty relkind='g' schema we reject
                    // complex-type INSERTs at validate, because the downstream
                    // storage layer (table_storage_t::adopt_schema → row_group →
                    // array_column_data_t) can't initialise an ARRAY/STRUCT/UNION/
                    // LIST/MAP column without crashing (assert in
                    // complex_logical_type::size() when UNKNOWN child appears,
                    // and other edge cases). atttypspec now correctly preserves
                    // the type in the catalog (catalog roundtrip works),
                    // but the storage path is a separate scope — even VALUES
                    // literal sources still SIGSEGV; lifting requires deeper
                    // storage layer work.
                    if (is_computed && !is_simple_chunk()) {
                        return core::error_t(
                            core::error_code_t::schema_error,
                            std::pmr::string{"insert_node: complex types (ARRAY/STRUCT/UNION/LIST/MAP) "
                                             "are not yet supported on relkind='g' (dynamic-schema) tables",
                                             resource});
                    }
                    if (table_schema.empty()) {
                        // Schemaless table (no columns defined) or computing table with no
                        // columns yet — accept any INSERT without column count validation.
                    } else if (is_computed && is_simple_chunk()) {
                        // Computing table with simple-typed INSERT: skip the static-shape
                        // checks. operator_computed_field_register registers new attoids
                        // for added/widened columns at execute time.
                    } else if (incoming_schema.value().size() > table_schema.size()) {
                        return core::error_t(core::error_code_t::schema_error,
                                             std::pmr::string{"insert_node: too many columns in INSERT", resource});
                    } else {
                        if (insert_node->key_translation().size() != incoming_schema.value().size() &&
                            table_schema.size() != incoming_schema.value().size()) {
                            return core::error_t(
                                core::error_code_t::schema_error,
                                std::pmr::string{"insert_node: number of columns do not match", resource});
                        } else {
                            // validate key
                            for (auto& key : insert_node->key_translation()) {
                                auto key_res = validation::validate_key(resource, key, &table_schema);
                                if (key_res.has_error()) {
                                    return key_res.convert_error<named_schema>();
                                }
                            }
                            // validate corresponding types
                            std::pmr::unordered_set<size_t> unchecked_columns(resource);
                            for (size_t i = 0; i < table_schema.size(); i++) {
                                unchecked_columns.emplace(i);
                            }

                            components::logical_plan::insert_column_bindings_t bindings(insert_node->resource());
                            bindings.reserve(incoming_schema.value().size());
                            for (size_t i = 0; i < incoming_schema.value().size(); i++) {
                                // TODO: support partial inserts into complex types
                                // for now only first order is checked
                                size_t index = insert_node->key_translation().empty()
                                                   ? i
                                                   : insert_node->key_translation()[i].path().front();
                                const auto& corresponding_table_type = table_schema[index].type;
                                unchecked_columns.erase(index);
                                const auto& incoming_type = incoming_schema.value()[i].type;

                                // The name the append routes on: the written key for an
                                // explicit column list (it may address a nested field), the
                                // catalog column name otherwise.
                                std::string target_name = insert_node->key_translation().empty()
                                                              ? tbl_ins->columns[index].attname
                                                              : insert_node->key_translation()[i].as_string();
                                components::logical_plan::insert_column_binding_t binding{
                                    .target_index = index,
                                    .target_name = std::pmr::string{target_name.c_str(), insert_node->resource()},
                                    .target_type = corresponding_table_type,
                                    .cast = {}};
                                if (incoming_type != corresponding_table_type) {
                                    auto cast = cast_registry->resolve(incoming_type,
                                                                       corresponding_table_type,
                                                                       components::casts::cast_type::assignment);
                                    if (!cast.has_value()) {
                                        return core::error_t(
                                            core::error_code_t::conversion_failure,
                                            std::pmr::string{"insert_node: column '" + tbl_ins->columns[index].attname +
                                                                 "' is of type " +
                                                                 describe_type(corresponding_table_type) +
                                                                 " but the inserted value is of type " +
                                                                 describe_type(incoming_type) +
                                                                 "; no cast between them may be applied on assignment",
                                                             resource});
                                    }
                                    binding.cast = std::move(cast.value());
                                }
                                bindings.emplace_back(std::move(binding));
                            }
                            insert_node->set_column_bindings(std::move(bindings));

                            // validate_static_nulls: for literal VALUES, reject null in NOT NULL cols
                            if (node->children().front()->type() == node_type::data_t && tbl_ins) {
                                const auto* dat = reinterpret_cast<const node_data_t*>(node->children().front().get());
                                const auto& chunk = dat->data_chunk();
                                const auto& cat_cols = tbl_ins->columns;
                                for (size_t ci = 0; ci < incoming_schema.value().size(); ++ci) {
                                    size_t tbl_idx = insert_node->key_translation().empty()
                                                         ? ci
                                                         : insert_node->key_translation()[ci].path().front();
                                    if (tbl_idx >= cat_cols.size() || !cat_cols[tbl_idx].attnotnull)
                                        continue;
                                    for (std::uint64_t row = 0; row < chunk.size(); ++row) {
                                        if (!chunk.data[ci].validity().row_is_valid(row)) {
                                            return core::error_t{
                                                core::error_code_t::schema_error,
                                                std::pmr::string{("insert_node: NULL value for NOT NULL column '" +
                                                                  cat_cols[tbl_idx].attname + "'")
                                                                     .c_str(),
                                                                 resource}};
                                        }
                                    }
                                }
                            }

                            if (!unchecked_columns.empty()) {
                                const auto& cat_columns =
                                    tbl_ins ? tbl_ins->columns
                                            : std::vector<components::logical_plan::resolved_column_metadata_t>{};
                                for (auto index : unchecked_columns) {
                                    if (!cat_columns[index].atthasdefault && cat_columns[index].attnotnull) {
                                        return core::error_t(
                                            core::error_code_t::schema_error,
                                            std::pmr::string{
                                                "insert_node: can not fill column \'" + cat_columns[index].attname +
                                                    "\', because it lacks a default value and do not except null",
                                                resource});
                                    }
                                }
                            }
                        }
                    }
                }
                return result;
            }
            case node_type::delete_t:
            case node_type::update_t: {
                node_match_t* node_match = nullptr;
                node_t* node_data = nullptr;
                for (const auto& child : node->children()) {
                    if (child->type() == node_type::match_t) {
                        node_match = reinterpret_cast<node_match_t*>(child.get());
                    } else if (child->type() != node_type::limit_t) {
                        node_data = child.get();
                    }
                }

                named_schema table_schema(resource);
                named_schema incoming_schema(resource);
                // The USING / FROM source, or null when the statement has only the target table.
                const named_schema* source_schema = &incoming_schema;
                // Update/delete nodes no longer carry relname; pull
                // the target table name from the resolved metadata (populated
                // by Pass 1 via the sibling resolve_table).
                const auto* tbl_upd = node->table_metadata();
                const std::string target_relname = tbl_upd ? tbl_upd->name : std::string{};
                if (tbl_upd && tbl_upd->relkind != 'g') {
                    for (const auto& column : tbl_upd->columns) {
                        table_schema.emplace_back(
                            type_from_t{node->result_alias().empty() ? target_relname : node->result_alias(),
                                        column.type});
                    }
                } else if (tbl_upd && tbl_upd->relkind == 'g') {
                    // task #106: on dynamic-schema (relkind='g') tables, UPDATE may
                    // only target columns that have already been registered in
                    // pg_computed_column. tbl_upd->columns reflects the set of LIVE columns
                    // for 'g' tables (resolve_table fills it from pg_computed_column). If the
                    // SET clause references a column not in that set, reject explicitly with
                    // a clear, actionable message.
                    //
                    // TODO(task #106): consider Mongo-style auto-registration of
                    // unknown SET targets on UPDATE (option (a) in the policy decision). That
                    // requires extending the UPDATE coroutine to allocate a new attnum and
                    // append a pg_computed_column row before the row-level update is applied.
                    if (node->type() == node_type::update_t) {
                        std::set<std::string> live_columns;
                        for (const auto& column : tbl_upd->columns) {
                            live_columns.insert(column.attname);
                        }
                        auto* node_update = reinterpret_cast<node_update_t*>(node);
                        for (const auto& expr : node_update->updates()) {
                            // The SET value's own key names the column it is assigned to.
                            if (!expr || expr->key().is_null()) {
                                continue;
                            }
                            const auto& storage = expr->key().storage();
                            // Top-level field is the column name; nested paths (a.b.c) still
                            // require the head 'a' to be a registered column.
                            // storage.at(0) is safe: key_t::is_null() == storage().empty(),
                            // and the is_null() check above already skipped empty-key SETs.
                            const std::string column_name(storage.at(0).data(), storage.at(0).size());
                            if (live_columns.find(column_name) == live_columns.end()) {
                                return core::error_t{
                                    core::error_code_t::schema_error,
                                    std::pmr::string{
                                        ("UPDATE on dynamic-schema (relkind='g') table '" + target_relname +
                                         "' references column '" + column_name +
                                         "' that is not registered. Insert with this field first to register it. "
                                         "(Auto-registration on UPDATE may be added in a future Phase, see task #106.)")
                                            .c_str(),
                                        resource}};
                            }
                        }
                    }
                    for (const auto& column : tbl_upd->columns) {
                        table_schema.emplace_back(
                            type_from_t{node->result_alias().empty() ? target_relname : node->result_alias(),
                                        column.type});
                    }
                } else {
                    return core::error_t(
                        core::error_code_t::table_not_exists,
                        std::pmr::string{"could not find table in update/delete validation", resource});
                }
                if (node_data) {
                    // UPDATE ... FROM / DELETE ... USING: the source is a child sub-plan
                    // (a table, a join tree, a — possibly LATERAL — derived table, a table
                    // function, or raw VALUES). Schema it and use it as the RIGHT side of
                    // the join predicate: target columns resolve LEFT (table_schema),
                    // source columns RIGHT (this schema). The whole source is the right
                    // relation, so stamp every column right regardless of any side it
                    // carries from an internal join — otherwise a source column sharing a
                    // name with a target column would resolve to the target index and read
                    // OOB on the (differently shaped) source chunk at runtime.
                    auto source_res = validate_schema(context, node_data, parameters, cte_schemas);
                    if (source_res.has_error()) {
                        return source_res;
                    }
                    incoming_schema = std::move(source_res.value());
                    for (auto& entry : incoming_schema) {
                        entry.side = components::expressions::side_t::right;
                    }
                } else {
                    incoming_schema = table_schema;
                    source_schema = nullptr;
                }
                if (node_match) {
                    auto node_match_res =
                        impl::validate_schema(context, node_match, parameters, &table_schema, source_schema);
                    if (node_match_res.has_error()) {
                        return node_match_res;
                    }
                } else {
                    return core::error_t(
                        core::error_code_t::schema_error,
                        std::pmr::string{"update_node: invalid node, node_match is not present", resource});
                }
                if (node->type() == node_type::update_t) {
                    auto* node_update = reinterpret_cast<node_update_t*>(node);
                    auto allowed_function_types =
                        components::compute::create_mask(components::compute::function_type_t::row,
                                                         components::compute::function_type_t::vector);
                    for (auto& expr : node_update->updates()) {
                        auto target_res = validation::find_types(resource, expr->key(), table_schema);
                        if (target_res.has_error()) {
                            return target_res.convert_error<named_schema>();
                        }
                        expr->key().set_side(side_t::left);
                        expr->key().set_path(target_res.value().front().path);

                        if (expr->group() == expression_group::scalar) {
                            auto* scalar = static_cast<scalar_expression_t*>(expr.get());
                            // An assigned value is computed per affected row.
                            auto resolve_error = impl::resolve_scalar_output_type(
                                context,
                                scalar,
                                table_schema,
                                parameters,
                                components::compute::create_mask(components::compute::function_type_t::row,
                                                                 components::compute::function_type_t::vector),
                                source_schema);
                            if (resolve_error.contains_error()) {
                                return resolve_error;
                            }
                        } else if (expr->group() == expression_group::function) {
                            auto function_res = impl::validate_schema(context,
                                                                      static_cast<function_expression_t*>(expr.get()),
                                                                      parameters,
                                                                      &table_schema,
                                                                      source_schema,
                                                                      allowed_function_types);
                            if (function_res.has_error()) {
                                return function_res;
                            }
                        }

                        // get type of where expression value will be placed
                        const auto& target_type = target_res.value().front().type;

                        // Storing can call assignment cast
                        const auto& value_type = expr->result_type();
                        if (value_type.type() != logical_type::INVALID && value_type != target_type) {
                            auto cast = cast_registry->resolve(value_type,
                                                               target_type,
                                                               components::casts::cast_type::assignment);
                            if (!cast.has_value()) {
                                return core::error_t(
                                    core::error_code_t::conversion_failure,
                                    std::pmr::string{("update_node: column '" + expr->key().as_string() +
                                                      "' is of type " + describe_type(target_type) +
                                                      " but the assigned value is of type " +
                                                      describe_type(value_type) +
                                                      "; no cast between them may be applied on assignment")
                                                         .c_str(),
                                                     resource});
                            }
                            auto conversion =
                                components::expressions::make_cast_expression(resource,
                                                                              param_storage{expr},
                                                                              target_type,
                                                                              cast.value(),
                                                                              components::casts::cast_kind::cast);
                            // An expression is addressed by the key, so propagating it to the top is important
                            conversion->key() = expr->key();
                            expr = components::expressions::expression_ptr{conversion.get()};
                        }
                    }
                }
                // TODO: check updates for update_t
                // RETURNING references the target table's columns (the affected
                // rows the operator projects from); resolve the projection keys.
                {
                    auto* returning = node->type() == node_type::update_t
                                          ? &reinterpret_cast<node_update_t*>(node)->returning()
                                          : &reinterpret_cast<node_delete_t*>(node)->returning();
                    if (!returning->empty() && !table_schema.empty()) {
                        // A RETURNING key naming a FROM/USING source column resolves
                        // against the source schema (built above as incoming_schema, all
                        // columns stamped right); target columns resolve against
                        // table_schema. No source child -> resolve against the target only.
                        const bool has_join = node_data != nullptr && !incoming_schema.empty();
                        auto ret_err = impl::resolve_returning_columns(context,
                                                                       returning,
                                                                       &table_schema,
                                                                       has_join ? &incoming_schema : nullptr,
                                                                       parameters);
                        if (ret_err.contains_error()) {
                            return ret_err;
                        }
                    }
                }
                return result;
            }
            case node_type::create_index_t: {
                auto* idx_node = static_cast<node_create_index_t*>(node);
                const auto* tbl_idx = idx_node->table_metadata();
                if (!tbl_idx) {
                    // node_create_index_t carries only the index name, no table names.
                    return core::error_t(core::error_code_t::table_not_exists,
                                         std::pmr::string{"CREATE INDEX target collection does not exist", resource});
                }

                named_schema table_schema{resource};
                // For relkind='g' we reject only when no columns are
                // registered yet — once at least one INSERT has populated
                // pg_computed_column, attoids are stable (register path
                // mints fresh attoids only for new / type-evolved columns).
                // Subsequent type evolution bumping attoids on an indexed
                // column is the caller's responsibility (no automatic index
                // rebuild today).
                if (tbl_idx && tbl_idx->relkind == 'g' && tbl_idx->columns.empty()) {
                    return core::error_t{core::error_code_t::index_create_fail,
                                         "CREATE INDEX requires at least one column registered on the table; "
                                         "INSERT data first to register a schema on this dynamic-schema "
                                         "(relkind='g') table."};
                } else if (tbl_idx) {
                    for (const auto& column : tbl_idx->columns) {
                        table_schema.emplace_back(type_from_t{tbl_idx->name, column.type});
                    }
                }
                auto& keys = idx_node->keys();
                // Key-type gate. The key encoders sit far below this statement, inside actor
                // coroutines with no error channel: a key type they cannot represent is an
                // abort (Debug) or a silently NA-collapsed key serving wrong rows (NDEBUG).
                // So the statement is refused HERE, before it executes — no unrepresentable
                // key can ever reach an encoder from user data, backfill included. The
                // accepted set lives in ONE place, next to the encoders it mirrors:
                // components::index::codec::is_representable_index_key_type.
                const bool ordered_index =
                    idx_node->type() != components::logical_plan::index_type::hashed;
                for (auto& key : keys) {
                    auto key_res = validation::validate_key(resource, key, &table_schema);
                    if (key_res.has_error()) {
                        return key_res.convert_error<named_schema>();
                    }
                    const auto& key_type = key_res.value().front().type;
                    if (!components::index::codec::is_representable_index_key_type(key_type.type(),
                                                                                   ordered_index)) {
                        std::string message = "CREATE INDEX: key '" + key.as_string() + "' has type " +
                                              describe_type(key_type) +
                                              ", which the index key encoders cannot represent";
                        if (ordered_index &&
                            key_type.type() == components::types::logical_type::DECIMAL) {
                            message += " in an ordered index (USING hash carries DECIMAL)";
                        }
                        return core::error_t{core::error_code_t::index_create_fail,
                                             std::pmr::string{message.c_str(), resource}};
                    }
                }
                return named_schema{resource};
            }
            case node_type::drop_t:
                // nothing to check here (only DROP INDEX reaches validate_schema;
                // the cascade drops short-circuit in validate_types' check_node)
                break;
            case node_type::create_matview_t:
            case node_type::refresh_matview_t:
                // Schema derivation happens in enrich (Step 1); the planner reads
                // the stamped inferred_columns / source metadata. No per-clause
                // schema validation needed at this layer — the body plan's source
                // table was resolved and pasted onto the node before this runs.
                break;
            case node_type::union_t: {
                if (node->children().size() < 2 || !node->children()[0] || !node->children()[1]) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"UNION requires both operands to be present", resource});
                }
                auto left_res = validate_schema(context, node->children()[0].get(), parameters, cte_schemas);
                if (left_res.has_error()) {
                    return left_res;
                }
                auto right_res = validate_schema(context, node->children()[1].get(), parameters, cte_schemas);
                if (right_res.has_error()) {
                    return right_res;
                }
                auto& left_schema = left_res.value(); // patched in place on a NULL-branch reconcile
                const auto& right_schema = right_res.value();
                if (left_schema.size() != right_schema.size()) {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"UNION operands must have the same number of columns", resource});
                }
                // A bare NULL literal branch carries from_null_literal on its schema column (set where the
                // constant's type is resolved). PostgreSQL reconciles such a column to the other branch's
                // type rather than erroring — adopt the concrete side; error only on a genuine mismatch.
                for (size_t i = 0; i < left_schema.size(); ++i) {
                    if (left_schema[i].type.type() == right_schema[i].type.type()) {
                        continue;
                    }
                    if (right_schema[i].from_null_literal) {
                        continue; // right branch is a NULL literal -> keep the left (concrete) type
                    }
                    if (left_schema[i].from_null_literal) {
                        // left branch is NULL -> adopt the right TYPE, but keep the left column's
                        // output name: PostgreSQL takes a union's column names from the FIRST
                        // SELECT. Copying the whole complex_logical_type would silently rename
                        // the column to the right branch's alias and break outer references.
                        auto adopted = right_schema[i].type;
                        adopted.set_alias(left_schema[i].type.has_alias() ? left_schema[i].type.alias()
                                                                          : std::string{});
                        left_schema[i].type = std::move(adopted);
                        continue;
                    }
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"UNION column type mismatch at position " + std::to_string(i), resource});
                }
                return left_res;
            }
            case node_type::sequence_t: {
                // The SQL transformer wraps DML/DDL in
                //   sequence_t(catalog_resolve_*…, consumer)
                // The catalog resolve children are leaves that don't carry a
                // schema, so we descend to the last non-catalog_resolve_* child
                // — the real consumer (insert_t/update_t/aggregate_t/...).
                auto is_catalog_resolve = [](node_type t) { return t == node_type::catalog_resolve_t; };
                for (auto it = node->children().rbegin(); it != node->children().rend(); ++it) {
                    if (!*it)
                        continue;
                    if (!is_catalog_resolve((*it)->type())) {
                        return validate_schema(context, it->get(), parameters, cte_schemas);
                    }
                }
                // All children are catalog_resolve_* — no consumer, empty schema.
                break;
            }
            case node_type::recursive_cte_t: {
                if (node->children().size() < 2 || !node->children()[0] || !node->children()[1]) {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"recursive CTE requires both anchor and recursive members", resource});
                }
                const auto* cte_node = static_cast<const components::logical_plan::node_recursive_cte_t*>(node);
                auto anchor_res = validate_schema(context, node->children()[0].get(), parameters, cte_schemas);
                if (anchor_res.has_error()) {
                    return anchor_res;
                }

                // Publish the CTE column schema derived from the anchor result so
                // node_cte_scan_t inside the recursive member can look it up.
                if (!cte_schemas) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"recursive CTE reached without a CTE schema map", resource});
                }
                {
                    cte_schema_t cte_cols;
                    for (const auto& entry : anchor_res.value()) {
                        cte_cols.push_back(
                            {std::pmr::string{entry.type.has_alias() ? entry.type.alias() : "", resource}, entry.type});
                    }
                    (*cte_schemas)[cte_node->cte_name()] = std::move(cte_cols);
                }
                // Validate recursive member — sets expression paths for SELECT/WHERE/JOIN ON.
                // Errors here indicate a schema mismatch between anchor and recursive member.
                auto recursive_res = validate_schema(context, node->children()[1].get(), parameters, cte_schemas);
                if (recursive_res.has_error()) {
                    return recursive_res;
                }

                // Remap result_alias to the CTE's visible alias.
                if (!node->result_alias().empty()) {
                    for (auto& entry : anchor_res.value()) {
                        entry.result_alias = node->result_alias();
                    }
                }
                return anchor_res;
            }
            case node_type::cte_scan_t: {
                if (!cte_schemas) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"cte_scan_t reached without a CTE schema map", resource});
                }
                const auto* scan_node = static_cast<const components::logical_plan::node_cte_scan_t*>(node);
                auto it = cte_schemas->find(scan_node->cte_name());
                if (it == cte_schemas->end()) {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"cte_scan_t: no schema for CTE '" + scan_node->cte_name() + "'", resource});
                }
                std::string_view alias = node->result_alias().empty() ? std::string_view(scan_node->cte_name())
                                                                      : std::string_view(node->result_alias());
                named_schema cte_result{resource};
                for (const auto& col : it->second) {
                    type_from_t entry;
                    entry.result_alias = alias;
                    entry.type = col.type;
                    cte_result.push_back(std::move(entry));
                }
                return cte_result;
            }
            default:
                // TODO: add check to validate schema, if assert is triggered
                assert(false);
                return core::error_t(core::error_code_t::unimplemented_yet,
                                     std::pmr::string{"encountered an unknown state during plan validation", resource});
        }

        return result;
    }

    // Public entry: resolve the node's output column types (data-INDEPENDENT — derived
    // from the plan + catalog types, never from row data), then STAMP them onto the node
    // so the physical-plan generator can build correctly-typed results over ZERO input
    // rows (PostgreSQL TupleDesc model). Interposing at this boundary captures every
    // return path of validate_schema_impl's switch and every recursive child call. Error
    // or empty-schema (DDL/control) results leave the node unstamped, so consumers
    // degrade to today's data-derived behavior rather than a hard failure.
    core::result_wrapper_t<named_schema> validate_schema(const validation::validation_context_t& context,
                                                         node_t* node,
                                                         const components::logical_plan::storage_parameters& parameters,
                                                         cte_schemas_t* cte_schemas) {
        // Owned here so a caller that never mentions CTEs need not carry the map;
        // recursion below always threads a non-null pointer.
        cte_schemas_t local_cte_schemas;
        if (cte_schemas == nullptr) {
            cte_schemas = &local_cte_schemas;
        }
        auto res = validate_schema_impl(context, node, parameters, cte_schemas);
        if (!res.has_error() && !res.value().empty()) {
            // Carry the resolved column types (the codebase idiom for a column-type list
            // is std::pmr::vector<complex_logical_type>). Keep each type's alias: it is the
            // output column name, which operators stamp onto the result column type (e.g.
            // a COUNT column named "count"); it matches the data-derived alias, so there is
            // no divergence.
            std::pmr::vector<complex_logical_type> types{node->resource()};
            types.reserve(res.value().size());
            for (const auto& c : res.value()) {
                types.push_back(c.type);
            }
            node->set_output_types(std::move(types));
        }
        return res;
    }

} // namespace services::dispatcher
