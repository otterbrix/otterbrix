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
#include <components/types/type_spec_codec.hpp>
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

    components::compute::function_types_mask check_expr_allowed_functions() {
        return components::compute::create_mask(components::compute::function_type_t::vector);
    }

    namespace {
        template<typename Node>
        [[nodiscard]] core::error_t bind_predicates(const validation::validation_context_t& context,
                                                    Node* node,
                                                    const storage_parameters& parameters) {
            if (node->check_predicates().empty()) {
                return core::error_t::no_error();
            }
            const auto* target = node->table_metadata();
            if (target == nullptr) {
                return core::error_t::no_error();
            }
            named_schema stored_row_schema{context.resource};
            for (const auto& column : target->columns) {
                stored_row_schema.emplace_back(type_from_t{target->name, column.type});
            }
            const auto& predicate_parameters = node->check_params() ? node->check_params()->parameters() : parameters;
            const validation::expression_context_t predicate_context{context.resource,
                                                                     stored_row_schema,
                                                                     predicate_parameters,
                                                                     context.cast_registry,
                                                                     context.function_registry,
                                                                     context.execution_context,
                                                                     check_expr_allowed_functions()};
            for (auto& [name, predicate] : node->check_predicates()) {
                if (auto error = validation::resolve_expression(predicate, predicate_context); error.contains_error()) {
                    return error;
                }
            }
            return core::error_t::no_error();
        }
    } // namespace

    core::error_t resolve_constraint_predicates(const validation::validation_context_t& context,
                                                node_t* root,
                                                const storage_parameters& parameters) {
        if (root == nullptr) {
            return core::error_t::no_error();
        }
        if (root->type() == node_type::insert_t) {
            if (auto error = bind_predicates(context, reinterpret_cast<node_insert_t*>(root), parameters);
                error.contains_error()) {
                return error;
            }
        } else if (root->type() == node_type::update_t) {
            if (auto error = bind_predicates(context, reinterpret_cast<node_update_t*>(root), parameters);
                error.contains_error()) {
                return error;
            }
        }
        for (const auto& child : root->children()) {
            if (auto error = resolve_constraint_predicates(context, child.get(), parameters); error.contains_error()) {
                return error;
            }
        }
        return core::error_t::no_error();
    }

    namespace impl {

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
                return expr;
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
                    // A null operand slot (e.g. IS NULL's unused right()) would crash sub->group() below.
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
                components::compute::create_mask(components::compute::function_type_t::vector),
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
                } else {
                    validation::expression_context_t predicate_context{
                        resource,
                        *schema_left,
                        parameters,
                        context.cast_registry,
                        context.function_registry,
                        context.execution_context,
                        components::compute::create_mask(components::compute::function_type_t::vector),
                        schema_right};
                    predicate_context.required_type = components::types::complex_logical_type{logical_type::BOOLEAN};
                    if (auto error = validation::resolve_expression(node->expressions()[0], predicate_context);
                        error.contains_error()) {
                        return error;
                    }
                    named_schema predicate_schema{resource};
                    predicate_schema.emplace_back(
                        type_from_t{node->result_alias(),
                                    components::types::complex_logical_type{logical_type::BOOLEAN}});
                    return predicate_schema;
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
                    if (components::expressions::is_key(sort_expr->operand())) {
                        auto res = find_types(resource, components::expressions::as_key(sort_expr->operand()), schema);
                        if (res.has_error()) {
                            return res.convert_error<named_schema>();
                        }
                        continue;
                    }
                    const validation::expression_context_t expression_context{
                        resource,
                        schema,
                        parameters,
                        context.cast_registry,
                        context.function_registry,
                        context.execution_context,
                        components::compute::create_mask(components::compute::function_type_t::vector)};
                    auto& operand = std::get<components::expressions::expression_ptr>(sort_expr->operand());
                    if (auto error = validation::resolve_expression(operand, expression_context);
                        error.contains_error()) {
                        return error;
                    }
                } else if (expr->group() == expression_group::scalar) {
                    auto* scalar_expr = static_cast<scalar_expression_t*>(expr.get());
                    auto resolve_error = resolve_scalar_output_type(
                        context,
                        scalar_expr,
                        schema,
                        parameters,
                        components::compute::create_mask(components::compute::function_type_t::vector));
                    if (resolve_error.contains_error()) {
                        return resolve_error;
                    }
                }
            }
            return named_schema{resource};
        }

        [[nodiscard]] core::error_t
        resolve_returning_columns(const validation::validation_context_t& context,
                                  std::pmr::vector<expression_ptr>* returning,
                                  const named_schema* schema_left,
                                  const named_schema* schema_right,
                                  const components::logical_plan::storage_parameters& parameters) {
            auto* resource = context.resource;
            auto& exprs = *returning;
            for (size_t idx = 0; idx < exprs.size();) {
                if (!exprs[idx]) {
                    idx++;
                    continue;
                }
                if (exprs[idx]->group() != expression_group::scalar) {
                    const validation::expression_context_t expression_context{
                        resource,
                        *schema_left,
                        parameters,
                        context.cast_registry,
                        context.function_registry,
                        context.execution_context,
                        components::compute::create_mask(components::compute::function_type_t::vector),
                        schema_right};
                    if (auto error = validation::resolve_expression(exprs[idx], expression_context);
                        error.contains_error()) {
                        return error;
                    }
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
                            auto res = validate_key(resource, key, schema_left, schema_right);
                            if (res.has_error()) {
                                return res.error();
                            }
                        }
                        idx++;
                        break;
                    }
                    case scalar_type::star_expand: {
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
                        auto resolve_error = resolve_scalar_output_type(
                            context,
                            scalar_expr,
                            *schema_left,
                            parameters,
                            components::compute::create_mask(components::compute::function_type_t::vector),
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

    }

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

    namespace {
        core::error_t check_dml_target_not_catalog(std::pmr::memory_resource* resource,
                                                   const components::logical_plan::node_t* node) {
            if (!components::catalog::is_catalog_table(node->table_oid())) {
                return core::error_t::no_error();
            }
            const auto* tbl = node->table_metadata();
            std::pmr::string msg{"cannot modify system catalog", resource};
            if (tbl && !tbl->name.empty()) {
                msg += " \"";
                msg.append(tbl->name.begin(), tbl->name.end());
                msg += '"';
            }
            return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
        }
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

    core::error_t convert_column_defaults(std::pmr::memory_resource* resource,
                                          const components::casts::cast_registry_t* cast_registry,
                                          const components::graph_execution_context& execution_context,
                                          std::vector<components::table::column_definition_t>& columns) {
        // Shared with ALTER TABLE ADD COLUMN (services/collection/executor.cpp).
        const auto gate_persistable = [&](const components::table::column_definition_t& column) {
            std::string encoded;
            return components::catalog::encode_default_spec(resource, column.default_value(), encoded);
        };
        for (auto& column : columns) {
            if (!column.has_default_value()) {
                continue;
            }
            if (column.default_value().type() == column.type()) {
                if (auto ec = gate_persistable(column); ec.contains_error()) {
                    return ec;
                }
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
            if (auto ec = gate_persistable(column); ec.contains_error()) {
                return ec;
            }
        }
        return core::error_t::no_error();
    }

    core::error_t gate_persistable_type(std::pmr::memory_resource* resource,
                                        const std::string& subject,
                                        const components::types::complex_logical_type& type) {
        std::pmr::vector<std::byte> spec(resource);
        auto encoded = components::types::encode_type_spec(type, spec);
        if (encoded.has_error()) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{subject + " cannot be persisted: " +
                                                      std::string(encoded.error().what.c_str()),
                                                  resource});
        }
        return core::error_t::no_error();
    }

    std::string insert_arity_disagreement(std::size_t written, std::size_t provided) {
        return "insert_node: INSERT names " + std::to_string(written) + " columns but the source provides " +
               std::to_string(provided);
    }

    core::error_t validate_types(std::pmr::memory_resource* resource,
                                 const catalog_resolves_t* resolves,
                                 node_t* logical_plan,
                                 const components::graph_execution_context& execution_context) {
        const auto session_tz = execution_context.timezone_offset;

        core::error_t result = core::error_t::no_error();
        char insert_target_relkind = 0;
        // Breadth-first walk visits INSERT before its data child, the only moment the two
        // are still positionally 1:1 (see the NA-column drop below).
        const node_insert_t* written_column_list = nullptr;

        auto check_node = [&](node_t* node) {
            switch (node->type()) {
                case node_type::drop_t:
                    return true;
                default:
                    break;
            }
            if (node->type() == node_type::insert_t) {
                written_column_list = static_cast<const node_insert_t*>(node);
            }
            if (auto oid = node->table_oid(); oid != components::catalog::INVALID_OID) {
                const auto* tbl = resolves ? resolves->table_md(oid) : nullptr;
                if (!tbl) {
                    result = core::error_t(core::error_code_t::table_not_exists,
                                           std::pmr::string{"collection does not exist", resource});
                    return false;
                }
                insert_target_relkind = tbl->relkind;
            }
            if (node->type() == node_type::data_t) {
                auto* data_node = reinterpret_cast<node_data_t*>(node);

                if (insert_target_relkind == 'g') {
                    for (auto& chunk : data_node->chunks()) {
                        auto& cols = chunk.data;
                        const bool names_readable =
                            written_column_list != nullptr &&
                            written_column_list->key_translation().size() == cols.size();
                        std::string dropped_names;
                        std::size_t dropped_count = 0;
                        for (std::size_t i = 0; i < cols.size(); ++i) {
                            if (cols[i].type().type() != logical_type::NA) {
                                continue;
                            }
                            ++dropped_count;
                            if (!names_readable) {
                                continue;
                            }
                            if (!dropped_names.empty()) {
                                dropped_names += ", ";
                            }
                            dropped_names += '"';
                            dropped_names += written_column_list->key_translation()[i].as_string();
                            dropped_names += '"';
                        }
                        cols.erase(std::remove_if(cols.begin(),
                                                  cols.end(),
                                                  [](const components::vector::vector_t& c) {
                                                      return c.type().type() == logical_type::NA;
                                                  }),
                                   cols.end());
                        if (dropped_count != 0 && names_readable) {
                            result = core::error_t(
                                core::error_code_t::schema_error,
                                std::pmr::string{
                                    insert_arity_disagreement(written_column_list->key_translation().size(),
                                                              cols.size()) +
                                        " — column" + (dropped_count > 1 ? "s " : " ") + dropped_names +
                                        (dropped_count > 1 ? " are " : " is ") +
                                        "NULL in every row, so there is no type to create the column from",
                                    resource});
                            return false;
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
            case node_type::extension_t: {
                const auto* ext = static_cast<const components::logical_plan::node_extension_t*>(node);
                if (!node->children().empty()) {
                    auto child = validate_schema(context, node->children().front().get(), parameters, cte_schemas);
                    if (child.has_error()) {
                        return child;
                    }
                    return result;
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

                // Table-valued jsonb operators expand only on the non-GROUP-BY path (else crash).
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
                           !static_cast<const std::string&>(agg_node->relname()).empty()) {
                    const auto& agg_dbname_s = static_cast<const std::string&>(agg_node->dbname());
                    const auto& agg_relname_s = static_cast<const std::string&>(agg_node->relname());
                    const auto& visible_alias = node->result_alias().empty() ? agg_relname_s : node->result_alias();
                    const auto* tbl = node->table_metadata();
                    if (tbl) {
                        relkind_computed = (tbl->relkind == 'g');
                        for (const auto& column : tbl->columns) {
                            table_schema.emplace_back(type_from_t{visible_alias, column.type});
                        }
                    } else {
                        if (!agg_dbname_s.empty() &&
                            (!resolves || resolves->namespace_oid(std::string_view(agg_dbname_s)) ==
                                              components::catalog::INVALID_OID)) {
                            std::pmr::string msg{"database does not exist: ", resource};
                            msg.append(agg_dbname_s.begin(), agg_dbname_s.end());
                            return core::error_t(core::error_code_t::database_not_exists, std::move(msg));
                        }
                        std::pmr::string msg{"collection does not exist: ", resource};
                        if (!agg_dbname_s.empty()) {
                            msg.append(agg_dbname_s.begin(), agg_dbname_s.end());
                            msg += '.';
                        }
                        msg.append(agg_relname_s.begin(), agg_relname_s.end());
                        return core::error_t(core::error_code_t::table_not_exists, std::move(msg));
                    }
                }
                if (table_schema.empty() && incoming_schema.empty()) {
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
                        bool reduces = false;
                        const validation::expression_context_t expression_context{
                            context.resource,
                            incoming_schema,
                            parameters,
                            context.cast_registry,
                            context.function_registry,
                            context.execution_context,
                            components::compute::create_mask(components::compute::function_type_t::vector,
                                                             components::compute::function_type_t::aggregate)};
                        for (auto& expr : node_group->expressions()) {
                            if (expr->group() == expression_group::aggregate) {
                                reduces = true;
                                continue;
                            }
                            if (expr->group() == expression_group::scalar) {
                                const auto scalar_kind = static_cast<scalar_expression_t*>(expr.get())->type();
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
                    if (node_select) {
                        {
                            auto& exprs = node_select->expressions();
                            for (size_t expr_index = 0; expr_index < exprs.size();) {
                                if (exprs[expr_index]->group() != expression_group::scalar) {
                                    expr_index++;
                                    continue;
                                }
                                auto* scalar_expr = reinterpret_cast<scalar_expression_t*>(exprs[expr_index].get());
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
                        struct column_key {
                            std::string result_alias;
                            std::string name;
                            logical_type type;
                            side_t side;
                            auto operator<=>(const column_key&) const = default;
                        };
                        std::set<column_key> seen_cols;
                        for (const auto& col : incoming_schema) {
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
                                auto resolve_error = impl::resolve_scalar_output_type(
                                    context,
                                    scalar_expr,
                                    incoming_schema,
                                    parameters,
                                    components::compute::create_mask(components::compute::function_type_t::vector,
                                                                     components::compute::function_type_t::aggregate));
                                if (resolve_error.contains_error()) {
                                    return resolve_error;
                                }
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
                            // Copy before find_types (mutates via set_path) and erase (invalidates it).
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

                    std::pmr::vector<validation::precomputed_column_t> group_keys(resource);
                    named_schema grouping_schema(incoming_schema.begin(), incoming_schema.end(), resource);
                    for (const auto& expr : node_group->expressions()) {
                        if (expr->group() != expression_group::scalar) {
                            continue;
                        }
                        auto* scalar_expr = static_cast<scalar_expression_t*>(expr.get());
                        if (scalar_expr->type() != scalar_type::group_field) {
                            continue;
                        }
                        validation::precomputed_column_t key{resource};
                        // Reading a grouping key yields one value per group.
                        key.cardinality = cardinality_t::group;
                        if (scalar_expr->params().empty()) {
                            auto res = validation::validate_key(resource, scalar_expr->key(), &incoming_schema);
                            if (res.has_error()) {
                                return res.convert_error<named_schema>();
                            }
                            key.reference = scalar_expr->key();
                            group_keys.push_back(std::move(key));
                            continue;
                        }
                        // A vector-only mask is what refuses GROUP BY sum(x): an aggregate has no
                        // signature this clause admits.
                        const validation::expression_context_t key_context{
                            context.resource,
                            incoming_schema,
                            parameters,
                            context.cast_registry,
                            context.function_registry,
                            context.execution_context,
                            components::compute::create_mask(components::compute::function_type_t::vector)};
                        auto& operand = std::get<expression_ptr>(scalar_expr->params().front());
                        if (auto error = validation::resolve_expression(operand, key_context); error.contains_error()) {
                            return error;
                        }
                        // Held for comparison only — the resolved operand stays in the plan, and
                        // the comparison never mutates either side.
                        key.expression = operand;
                        key.reference = components::expressions::key_t{resource, scalar_expr->key().as_string()};
                        std::pmr::vector<size_t> path(resource);
                        path.push_back(grouping_schema.size());
                        key.reference.set_path(std::move(path));
                        auto key_type = operand->result_type();
                        key_type.set_alias(scalar_expr->key().as_string());
                        grouping_schema.emplace_back(type_from_t{node->result_alias(), key_type});
                        group_keys.push_back(std::move(key));
                    }

                    core::error_t compute_type_error = core::error_t::no_error();
                    auto compute_type_entry =
                        [&](scalar_expression_t* scalar_expr,
                            const named_schema& schema,
                            const std::pmr::vector<validation::precomputed_column_t>* keys) -> type_from_t {
                        const validation::expression_context_t expression_context{
                            context.resource,
                            schema,
                            parameters,
                            context.cast_registry,
                            context.function_registry,
                            context.execution_context,
                            components::compute::create_mask(components::compute::function_type_t::vector),
                            nullptr,
                            keys};
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

                    {
                        const validation::expression_context_t projection_context{
                            context.resource,
                            grouping_schema,
                            parameters,
                            context.cast_registry,
                            context.function_registry,
                            context.execution_context,
                            components::compute::create_mask(components::compute::function_type_t::vector,
                                                             components::compute::function_type_t::aggregate,
                                                             components::compute::function_type_t::expand),
                            nullptr,
                            &group_keys};
                        for (auto& expr : node_group->expressions()) {
                            if (expr->group() == expression_group::scalar) {
                                const auto kind = static_cast<scalar_expression_t*>(expr.get())->type();
                                if (kind == scalar_type::group_field) {
                                    continue;
                                }
                                if (kind == scalar_type::star_expand) {
                                    continue;
                                }
                            }
                            if (auto error = validation::resolve_expression(expr, projection_context);
                                error.contains_error()) {
                                return error;
                            }
                            const auto projected = expr->cardinality();
                            if (projected == cardinality_t::row) {
                                return core::error_t(core::error_code_t::sql_parse_error,
                                                     std::pmr::string{"column must appear in a GROUP BY clause or be "
                                                                      "used in an aggregate function",
                                                                      resource});
                            }
                        }
                    }

                    size_t select_end = node_group->expressions().size() - node_group->internal_aggregate_count;
                    named_schema key_schema(resource);
                    std::vector<size_t> post_agg_indices;
                    std::vector<size_t> agg_result_positions;

                    for (size_t i = 0; i < node_group->expressions().size(); i++) {
                        auto& expr = node_group->expressions()[i];
                        if (expr->group() == expression_group::scalar) {
                            auto* scalar_expr = reinterpret_cast<scalar_expression_t*>(expr.get());
                            if (scalar_expr->type() == scalar_type::get_field) {
                                auto& key =
                                    scalar_expr->params().empty()
                                        ? scalar_expr->key()
                                        : std::get<components::expressions::key_t>(scalar_expr->params().front());
                                auto res = validation::validate_key(resource, key, &grouping_schema);
                                if (res.has_error()) {
                                    return res.convert_error<named_schema>();
                                }

                                const auto& col_type = grouping_schema[key.path()[0]].type;
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
                                auto field_type = *res_type;
                                if (!scalar_expr->params().empty() && !scalar_expr->key().is_null()) {
                                    field_type.set_alias(scalar_expr->key().as_string());
                                }
                                result.emplace_back(type_from_t{node->result_alias(), std::move(field_type)});
                                key_schema.emplace_back(result.back());
                            } else if (scalar_expr->type() == scalar_type::group_field) {
                                if (scalar_expr->params().empty()) {
                                    auto& key = scalar_expr->key();
                                    auto res = validation::validate_key(resource, key, &incoming_schema);
                                    if (res.has_error()) {
                                        return res.convert_error<named_schema>();
                                    }
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
                                auto res =
                                    impl::resolve_key_paths_in_group(resource, scalar_expr->params(), grouping_schema);
                                if (res.has_error()) {
                                    post_agg_indices.push_back(i);
                                } else {
                                    auto entry = compute_type_entry(scalar_expr, grouping_schema, &group_keys);
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

                            const validation::expression_context_t aggregate_context{
                                context.resource,
                                grouping_schema,
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
                            scalar_expr->key().set_path({SIZE_MAX});

                            auto entry = compute_type_entry(scalar_expr, post_agg_schema, nullptr);
                            if (compute_type_error.contains_error()) {
                                return compute_type_error;
                            }
                            result.emplace_back(entry);
                        }
                    }

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
                                auto resolve_error = impl::resolve_scalar_output_type(
                                    context,
                                    scalar_expr,
                                    result,
                                    parameters,
                                    components::compute::create_mask(components::compute::function_type_t::vector));
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
                    for (auto& sort_child : node_sort->expressions()) {
                        if (sort_child->group() != expression_group::sort) {
                            continue;
                        }
                        auto* sort_expr = static_cast<sort_expression_t*>(sort_child.get());
                        if (!components::expressions::is_key(sort_expr->operand())) {
                            continue;
                        }
                        auto& skey = components::expressions::as_key(sort_expr->operand());
                        auto field_in_result = validation::find_types(resource, skey, result);
                        if (!field_in_result.has_error() && !field_in_result.value().empty()) {
                            continue;
                        }
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
                if (!aggregate_node->distinct_on_keys().empty()) {
                    for (auto& on_key : aggregate_node->distinct_on_keys()) {
                        auto r = validation::find_types(resource, on_key, result);
                        if (r.has_error()) {
                            return r.convert_error<named_schema>();
                        }
                    }
                }
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
                                     components::compute::create_mask(components::compute::function_type_t::vector,
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
            case node_type::insert_t: {
                auto* insert_node = reinterpret_cast<node_insert_t*>(node);
                if (auto guard = check_dml_target_not_catalog(resource, node); guard.contains_error()) {
                    return guard;
                }
                const auto* tbl_ins = insert_node->table_metadata();
                if (!tbl_ins) {
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
                    // ARRAY/STRUCT/UNION/LIST/MAP crash table_storage_t::adopt_schema (SIGSEGV).
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
                    if (is_computed && !is_simple_chunk()) {
                        return core::error_t(
                            core::error_code_t::schema_error,
                            std::pmr::string{"insert_node: complex types (ARRAY/STRUCT/UNION/LIST/MAP) "
                                             "are not yet supported on relkind='g' (dynamic-schema) tables",
                                             resource});
                    }
                    // Unchecked, this crashes column_segment_t ("no segment storage for physical type 127").
                    if (is_computed) {
                        const auto& source_columns = incoming_schema.value();
                        const bool written_names_align =
                            insert_node->key_translation().size() == source_columns.size();
                        for (size_t i = 0; i < source_columns.size(); i++) {
                            if (source_columns[i].type.type() != components::types::logical_type::NA) {
                                continue;
                            }
                            std::string column_name;
                            if (written_names_align) {
                                column_name = insert_node->key_translation()[i].as_string();
                            } else if (source_columns[i].type.has_alias()) {
                                column_name = std::string(source_columns[i].type.alias());
                            }
                            std::string named = column_name.empty()
                                                    ? std::string{}
                                                    : std::string{" \""} + column_name + "\"";
                            return core::error_t(
                                core::error_code_t::schema_error,
                                std::pmr::string{"insert_node: INSERT into dynamic-schema table '" +
                                                     target_relname_ins + "': source column " +
                                                     std::to_string(i + 1) + named +
                                                     " is NULL in every row, so there is no type to create the "
                                                     "column from",
                                                 resource});
                        }
                    }
                    auto bind_computed_rename = [&]() -> core::error_t {
                        if (insert_node->key_translation().empty()) {
                            return core::error_t::no_error();
                        }
                        if (insert_node->key_translation().size() != incoming_schema.value().size()) {
                            return core::error_t(
                                core::error_code_t::schema_error,
                                std::pmr::string{insert_arity_disagreement(insert_node->key_translation().size(),
                                                                           incoming_schema.value().size()),
                                                 resource});
                        }
                        components::logical_plan::insert_column_bindings_t bindings(insert_node->resource());
                        bindings.reserve(incoming_schema.value().size());
                        for (size_t i = 0; i < incoming_schema.value().size(); i++) {
                            std::string target_name = insert_node->key_translation()[i].as_string();
                            bindings.emplace_back(components::logical_plan::insert_column_binding_t{
                                .target_index = i,
                                .target_name = std::pmr::string{target_name.c_str(), insert_node->resource()},
                                .target_type = incoming_schema.value()[i].type,
                                .cast = {}});
                        }
                        insert_node->set_column_bindings(std::move(bindings));
                        auto* source_child = node->children().front().get();
                        if (source_child->has_output_types()) {
                            auto renamed = source_child->output_types();
                            const size_t bound = std::min(renamed.size(), insert_node->key_translation().size());
                            for (size_t i = 0; i < bound; i++) {
                                renamed[i].set_alias(insert_node->key_translation()[i].as_string());
                            }
                            source_child->set_output_types(std::move(renamed));
                        }
                        return core::error_t::no_error();
                    };
                    if (table_schema.empty()) {
                        // Must stay relkind='r' (test_persistence::zero_column_regular_table_stays_regular).
                        if (!is_computed) {
                            return core::error_t(
                                core::error_code_t::schema_error,
                                std::pmr::string{"insert_node: table '" + target_relname_ins +
                                                     "' has no columns; INSERT needs at least one column",
                                                 resource});
                        }
                        if (auto rename_err = bind_computed_rename(); rename_err.contains_error()) {
                            return rename_err;
                        }
                    } else if (is_computed && is_simple_chunk()) {
                        if (auto rename_err = bind_computed_rename(); rename_err.contains_error()) {
                            return rename_err;
                        }
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
                            for (auto& key : insert_node->key_translation()) {
                                auto key_res = validation::validate_key(resource, key, &table_schema);
                                if (key_res.has_error()) {
                                    return key_res.convert_error<named_schema>();
                                }
                            }
                            std::pmr::unordered_set<size_t> unchecked_columns(resource);
                            for (size_t i = 0; i < table_schema.size(); i++) {
                                unchecked_columns.emplace(i);
                            }

                            const bool source_is_raw_values = node->children().front()->type() == node_type::data_t;
                            components::logical_plan::insert_column_bindings_t bindings(insert_node->resource());
                            bindings.reserve(incoming_schema.value().size());
                            for (size_t i = 0; i < incoming_schema.value().size(); i++) {
                                // TODO: support partial inserts into complex types
                                size_t key_pos = i;
                                if (!insert_node->key_translation().empty() && source_is_raw_values &&
                                    incoming_schema.value()[i].type.has_alias()) {
                                    const std::string written_name{incoming_schema.value()[i].type.alias()};
                                    const auto& keys = insert_node->key_translation();
                                    auto key_it =
                                        std::find_if(keys.begin(), keys.end(), [&written_name](const auto& key) {
                                            return key.as_string() == written_name;
                                        });
                                    if (key_it == keys.end()) {
                                        return core::error_t(
                                            core::error_code_t::schema_error,
                                            std::pmr::string{"insert_node: VALUES column '" + written_name +
                                                                 "' is not in the INSERT column list",
                                                             resource});
                                    }
                                    key_pos = static_cast<size_t>(key_it - keys.begin());
                                }
                                size_t index = insert_node->key_translation().empty()
                                                   ? i
                                                   : insert_node->key_translation()[key_pos].path().front();
                                const auto& corresponding_table_type = table_schema[index].type;
                                unchecked_columns.erase(index);
                                const auto& incoming_type = incoming_schema.value()[i].type;

                                std::string target_name = insert_node->key_translation().empty()
                                                              ? tbl_ins->columns[index].attname
                                                              : insert_node->key_translation()[key_pos].as_string();
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

                            if (source_is_raw_values && tbl_ins) {
                                const auto* dat = reinterpret_cast<const node_data_t*>(node->children().front().get());
                                const auto& chunk = dat->data_chunk();
                                const auto& cat_cols = tbl_ins->columns;
                                for (size_t ci = 0; ci < incoming_schema.value().size(); ++ci) {
                                    size_t tbl_idx = insert_node->column_bindings()[ci].target_index;
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
                if (auto guard = check_dml_target_not_catalog(resource, node); guard.contains_error()) {
                    return guard;
                }
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
                const named_schema* source_schema = &incoming_schema;
                const auto* tbl_upd = node->table_metadata();
                const std::string target_relname = tbl_upd ? tbl_upd->name : std::string{};
                if (tbl_upd && tbl_upd->relkind != 'g') {
                    for (const auto& column : tbl_upd->columns) {
                        table_schema.emplace_back(
                            type_from_t{node->result_alias().empty() ? target_relname : node->result_alias(),
                                        column.type});
                    }
                } else if (tbl_upd && tbl_upd->relkind == 'g') {
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
                            if (!expr || expr->key().is_null()) {
                                continue;
                            }
                            const auto& storage = expr->key().storage();
                            // storage.at(0) is safe: is_null() above already skipped empty-key SETs.
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
                    for (auto& expr : node_update->updates()) {
                        auto target_res = validation::find_types(resource, expr->key(), table_schema);
                        if (target_res.has_error()) {
                            return target_res.convert_error<named_schema>();
                        }
                        expr->key().set_side(side_t::left);
                        expr->key().set_path(target_res.value().front().path);

                        const validation::expression_context_t assignment_context{
                            resource,
                            table_schema,
                            parameters,
                            context.cast_registry,
                            context.function_registry,
                            context.execution_context,
                            components::compute::create_mask(components::compute::function_type_t::vector),
                            source_schema};
                        if (auto error = validation::resolve_expression(expr, assignment_context);
                            error.contains_error()) {
                            return error;
                        }

                        const auto& target_type = target_res.value().front().type;

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
                            conversion->key() = expr->key();
                            expr = components::expressions::expression_ptr{conversion.get()};
                        }
                    }
                }
                // TODO: check updates for update_t
                {
                    auto* returning = node->type() == node_type::update_t
                                          ? &reinterpret_cast<node_update_t*>(node)->returning()
                                          : &reinterpret_cast<node_delete_t*>(node)->returning();
                    if (!returning->empty() && !table_schema.empty()) {
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
                if (components::catalog::is_catalog_table(idx_node->table_oid())) {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"cannot create an index on a system catalog table", resource});
                }
                const auto* tbl_idx = idx_node->table_metadata();
                if (!tbl_idx) {
                    return core::error_t(core::error_code_t::table_not_exists,
                                         std::pmr::string{"CREATE INDEX target collection does not exist", resource});
                }

                named_schema table_schema{resource};
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
                // The encoders below have no error channel (abort in Debug, wrong rows under NDEBUG).
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
            case node_type::create_constraint_t: {
                auto* constraint_node = static_cast<node_create_constraint_t*>(node);
                if (constraint_node->kind() != constraint_kind::check) {
                    break;
                }
                const auto* constrained = constraint_node->table_metadata();
                if (!constrained) {
                    return core::error_t(
                        core::error_code_t::table_not_exists,
                        std::pmr::string{"CHECK constraint target collection does not exist", resource});
                }
                auto expression = constraint_node->check_expression();
                if (!expression) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"CHECK constraint carries no expression", resource});
                }

                named_schema constraint_schema{resource};
                for (const auto& column : constrained->columns) {
                    constraint_schema.emplace_back(type_from_t{constrained->name, column.type});
                }

                validation::expression_context_t constraint_context{resource,
                                                                    constraint_schema,
                                                                    parameters,
                                                                    context.cast_registry,
                                                                    context.function_registry,
                                                                    context.execution_context,
                                                                    check_expr_allowed_functions()};
                constraint_context.required_type = components::types::complex_logical_type{logical_type::BOOLEAN};
                bool saw_reduction = false;
                if (auto error = validation::resolve_expression(expression, constraint_context, &saw_reduction);
                    error.contains_error()) {
                    return error;
                }
                if (saw_reduction) {
                    return core::error_t(
                        core::error_code_t::invalid_constraint,
                        std::pmr::string{"CHECK constraint \"" + constraint_node->name() +
                                             "\" uses an aggregate; a CHECK is evaluated for one row at a time",
                                         resource});
                }
                constraint_node->set_check_expression(std::move(expression));
                return named_schema{resource};
            }
            case node_type::drop_t:
                break;
            case node_type::create_matview_t:
            case node_type::refresh_matview_t:
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
                auto& left_schema = left_res.value();
                const auto& right_schema = right_res.value();
                if (left_schema.size() != right_schema.size()) {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"UNION operands must have the same number of columns", resource});
                }
                for (size_t i = 0; i < left_schema.size(); ++i) {
                    if (left_schema[i].type.type() == right_schema[i].type.type()) {
                        continue;
                    }
                    if (right_schema[i].from_null_literal) {
                        continue;
                    }
                    if (left_schema[i].from_null_literal) {
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
                auto is_catalog_resolve = [](node_type t) { return t == node_type::catalog_resolve_t; };
                for (auto it = node->children().rbegin(); it != node->children().rend(); ++it) {
                    if (!*it)
                        continue;
                    if (!is_catalog_resolve((*it)->type())) {
                        return validate_schema(context, it->get(), parameters, cte_schemas);
                    }
                }
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
                auto recursive_res = validate_schema(context, node->children()[1].get(), parameters, cte_schemas);
                if (recursive_res.has_error()) {
                    return recursive_res;
                }

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

    // Resolves output column types and stamps them onto the node. Error/empty results leave
    // it unstamped by design; every consumer must check has_output_types() before reading it.
    core::result_wrapper_t<named_schema> validate_schema(const validation::validation_context_t& context,
                                                         node_t* node,
                                                         const components::logical_plan::storage_parameters& parameters,
                                                         cte_schemas_t* cte_schemas) {
        cte_schemas_t local_cte_schemas;
        if (cte_schemas == nullptr) {
            cte_schemas = &local_cte_schemas;
        }
        auto res = validate_schema_impl(context, node, parameters, cte_schemas);
        if (!res.has_error() && !res.value().empty()) {
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
