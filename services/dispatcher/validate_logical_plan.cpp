#include "validate_logical_plan.hpp"

#include <core/date/date_parse.hpp>
#include <cstdio>

#include "expressions/function_expression.hpp"
#include "expressions/update_expression.hpp"
#include "logical_plan/node_create_index.hpp"
#include "logical_plan/node_insert.hpp"
#include "logical_plan/node_update.hpp"
#include "plan_resolve_index.hpp"

#include <atomic>
#include <components/catalog/system_table_schemas.hpp>
#include <components/catalog/table_id.hpp>
#include <components/compute/function.hpp>
#include <components/compute/kernel_signature.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_alter_column_add.hpp>
#include <components/logical_plan/node_alter_column_drop.hpp>
#include <components/logical_plan/node_alter_column_rename.hpp>
#include <components/logical_plan/node_alter_table.hpp>
#include <components/logical_plan/node_catalog_resolve_function.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_catalog_resolve_type.hpp>
#include <components/logical_plan/node_check_constraint.hpp>
#include <components/logical_plan/node_computed_field_register.hpp>
#include <components/logical_plan/node_computed_field_unregister.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_constraint.hpp>
#include <components/logical_plan/node_create_database.hpp>
#include <components/logical_plan/node_create_macro.hpp>
#include <components/logical_plan/node_create_sequence.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_cte_scan.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop_collection.hpp>
#include <components/logical_plan/node_drop_database.hpp>
#include <components/logical_plan/node_drop_index.hpp>
#include <components/logical_plan/node_drop_macro.hpp>
#include <components/logical_plan/node_drop_sequence.hpp>
#include <components/logical_plan/node_drop_view.hpp>
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

    namespace impl {

        components::vector::arithmetic_op scalar_to_arith_op(components::expressions::scalar_type t) {
            switch (t) {
                case components::expressions::scalar_type::add:
                    return components::vector::arithmetic_op::add;
                case components::expressions::scalar_type::subtract:
                    return components::vector::arithmetic_op::subtract;
                case components::expressions::scalar_type::multiply:
                    return components::vector::arithmetic_op::multiply;
                case components::expressions::scalar_type::divide:
                    return components::vector::arithmetic_op::divide;
                case components::expressions::scalar_type::mod:
                    return components::vector::arithmetic_op::mod;
                default:
                    return components::vector::arithmetic_op::add;
            }
        }
        // plan_resolve_index_t + helpers live in
        // services/dispatcher/plan_resolve_index.hpp so
        // enrich_logical_plan.cpp can use the same probe-then-fallback
        // pattern.

        // V4 function lookup helper. Returns (uid, signature) for the matching overload, or
        // {invalid_function_uid, {{}, {}}} if no match found. name_exists set if any function
        // with this name was registered (for "unrecognized function" vs "wrong args" errors).
        struct function_lookup_t {
            components::compute::function_uid uid{components::compute::invalid_function_uid};
            components::compute::kernel_signature_t signature{components::compute::function_type_t::invalid, {}, {}};
            bool name_exists{false};
            bool match_found{false};
        };

        inline function_lookup_t
        lookup_function(const std::string& name,
                        const std::pmr::vector<components::types::complex_logical_type>& args) {
            function_lookup_t out;
            auto* reg = components::compute::function_registry_t::get_default();
            if (!reg)
                return out;
            for (auto& [n, uid] : reg->get_functions()) {
                if (n != name)
                    continue;
                out.name_exists = true;
                auto* fn = reg->get_function(uid);
                if (!fn)
                    continue;
                for (auto& sig : fn->get_signatures()) {
                    if (sig.matches_inputs(args)) {
                        out.uid = uid;
                        out.signature = sig;
                        out.match_found = true;
                        return out;
                    }
                }
            }
            return out;
        }

        struct type_match_t {
            column_path path;
            const complex_logical_type* type;
            size_t key_order;
        };

        // JOIN schema = pure concatenation of both sides. The runtime join
        // operators (join_utils.hpp: res_types = left.types() + all right
        // types) emit every column of both inputs, including same-named join
        // keys. Dropping a duplicate here desynchronizes the validator's
        // column indices from the runtime chunk layout: every key path
        // resolved after the dropped column points one column to the left
        // (wrong values for same-typed columns, kernel errors or worse for
        // mismatched ones). Notably both sides carry an empty result_alias
        // when they are raw node_data inputs, so a same-named join key used
        // to be deduplicated exactly there.
        named_schema merge_schemas(std::pmr::memory_resource* resource, named_schema lhs, named_schema rhs) {
            named_schema merged(resource);
            merged.reserve(lhs.size() + rhs.size());
            for (auto&& type : lhs) {
                if (type.side == side_t::undefined) {
                    type.side = side_t::left;
                }
                merged.emplace_back(std::move(type));
            }
            for (auto&& type : rhs) {
                if (type.side == side_t::undefined) {
                    type.side = side_t::right;
                }
                merged.emplace_back(std::move(type));
            }
            return merged;
        }

        [[nodiscard]] core::result_wrapper_t<type_paths> find_types(std::pmr::memory_resource* resource,
                                                                    components::expressions::key_t& key,
                                                                    const named_schema& schema) {
            assert(!key.storage().empty());
            type_paths result{resource};
            if (key.storage().at(0) == "*") {
                for (size_t i = 0; i < schema.size(); i++) {
                    result.emplace_back(type_path_t{column_path{{i}, resource}, schema[i].type});
                }
                return result;
            }
            // Handle table-alias wildcard: "table.*" → expand all columns with matching result_alias
            if (key.storage().size() >= 2 && key.storage().back() == "*") {
                const auto& table_part = key.storage().at(key.storage().size() - 2);
                for (size_t i = 0; i < schema.size(); i++) {
                    if (core::pmr::operator==(schema[i].result_alias, table_part)) {
                        result.emplace_back(type_path_t{column_path{{i}, resource}, schema[i].type});
                    }
                }
                if (!result.empty()) {
                    key.set_path(result.front().path);
                    return result;
                }
            }
            // removed '*' at the end, if it has one
            components::expressions::key_t truncated_key = key;
            if (truncated_key.storage().back() == "*") {
                truncated_key.storage().resize(truncated_key.storage().size() - 1);
            }
            // First key is either table name or type name
            // Also we store number of keys used to get there and path
            std::pmr::list<type_match_t> matches(resource);
            for (size_t i = 0; i < schema.size(); i++) {
                if (truncated_key.storage().size() > 2 &&
                    core::pmr::operator==(schema[i].result_alias, truncated_key.storage().at(1)) &&
                    core::pmr::operator==(schema[i].type.alias(), truncated_key.storage().at(2))) {
                    matches.emplace_back(type_match_t{column_path{{i}, resource}, &schema[i].type, 3});
                } else if (truncated_key.storage().size() > 1 &&
                           core::pmr::operator==(schema[i].result_alias, truncated_key.storage().at(0)) &&
                           core::pmr::operator==(schema[i].type.alias(), truncated_key.storage().at(1))) {
                    matches.emplace_back(type_match_t{column_path{{i}, resource}, &schema[i].type, 2});
                } else if (core::pmr::operator==(schema[i].type.alias(), truncated_key.storage().at(0))) {
                    matches.emplace_back(type_match_t{column_path{{i}, resource}, &schema[i].type, 1});
                }
            }

            // Side-aware disambiguation: only when there's ambiguity to resolve.
            // Drop schema candidates whose stamped side disagrees only when >1 match
            // (otherwise we'd drop legitimate single matches in chained-JOIN where
            // inner-merge sides don't align with outer-merge name_collection sides).
            if (matches.size() > 1 && key.side() != side_t::undefined) {
                for (auto it = matches.begin(); it != matches.end();) {
                    size_t schema_idx = it->path.empty() ? 0 : it->path[0];
                    if (schema_idx < schema.size() && schema[schema_idx].side != side_t::undefined &&
                        schema[schema_idx].side != key.side()) {
                        it = matches.erase(it);
                    } else {
                        ++it;
                    }
                }
            }

            while (!matches.empty()) {
                auto it = matches.begin();
                auto next_it = std::next(it);
                if (truncated_key.storage().size() > it->key_order) {
                    if (it->type->type() == logical_type::STRUCT) {
                        for (size_t i = 0; i < it->type->child_types().size(); i++) {
                            const auto& child = it->type->child_types()[i];
                            if (core::pmr::operator==(child.alias(), truncated_key.storage()[it->key_order])) {
                                column_path path = it->path;
                                path.emplace_back(i);
                                matches.emplace(next_it, type_match_t{std::move(path), &child, it->key_order + 1});
                            }
                        }
                    } else if (it->type->type() == logical_type::ARRAY) {
                        auto arr_type_ext = static_cast<array_logical_type_extension*>(it->type->extension());
                        // used atoll because it does not give exceptions with incorrect arguments
                        // and 0 index is invalid anyway
                        auto index = std::atoll(truncated_key.storage()[it->key_order].c_str());
                        if (index <= 0 || static_cast<size_t>(index) > arr_type_ext->size()) {
                            matches.erase(it);
                            continue;
                        }
                        column_path path = it->path;
                        // store 0 based index
                        path.emplace_back(index - 1);
                        matches.emplace(next_it,
                                        type_match_t{std::move(path), &it->type->child_type(), it->key_order + 1});
                    } else if (it->type->type() == logical_type::LIST) {
                        // used atoll because it does not give exceptions with incorrect arguments
                        // and 0 index is invalid anyway
                        auto index = std::atoll(truncated_key.storage()[it->key_order].c_str());
                        // list does not have a fixed size, so we can not check upper bounds here
                        if (index <= 0) {
                            matches.erase(it);
                            continue;
                        }
                        column_path path = it->path;
                        // store 0 based index
                        path.emplace_back(index - 1);
                        matches.emplace(next_it,
                                        type_match_t{std::move(path), &it->type->child_type(), it->key_order + 1});
                    }
                } else {
                    // this is an exact match
                    result.emplace_back(type_path_t{std::move(it->path), *it->type});
                }
                matches.erase(it);
            }

            // '::?' type-variant selection: among several same-name columns
            // (computing multi-type fields), keep only the one whose physical type
            // matches the requested type. Disambiguates what would otherwise be an
            // ambiguous name; an empty result falls through to "not found" below.
            if (key.is_variant_select() && key.has_cast_type()) {
                const auto want = key.cast_type().type();
                type_paths filtered{resource};
                for (auto& tp : result) {
                    if (tp.type.type() == want) {
                        filtered.push_back(std::move(tp));
                    }
                }
                result = std::move(filtered);
            }

            // if result still contains multiple types, try to disambiguate via the
            // cast_type_ hint; if it remains ambiguous, that is an error
            if (result.size() > 1) {
                if (truncated_key.has_cast_type()) {
                    auto cast_lt = truncated_key.cast_type().type();
                    type_paths filtered{resource};
                    for (auto& tp : result) {
                        if (tp.type.type() == cast_lt) {
                            filtered.emplace_back(std::move(tp));
                            break;
                        }
                    }
                    result = std::move(filtered);
                }
                if (result.size() > 1) {
                    return core::error_t(core::error_code_t::ambiguous_name,
                                         std::pmr::string{"path: \'" + truncated_key.as_string() +
                                                              "\' is ambiguous. Use aliases or full path",
                                                          resource});
                }
            }
            if (!result.empty()) {
                if (key.storage().back() == "*") {
                    if (result.empty()) {
                        return core::error_t(
                            core::error_code_t::schema_error,
                            std::pmr::string{"path: \'" + key.as_string() + "\' was not found", resource});
                    }
                    if (!result.front().type.is_nested()) {
                        return core::error_t(core::error_code_t::schema_error,
                                             std::pmr::string{"path: \'" + truncated_key.as_string() +
                                                                  "\' is not nested, and \'*\' can not be applied",
                                                              resource});
                    }
                    if (result.front().type.type() == logical_type::LIST) {
                        return core::error_t(core::error_code_t::schema_error,
                                             std::pmr::string{"path: \'" + truncated_key.as_string() +
                                                                  "\' is a list type, and \'*\' can not be applied",
                                                              resource});
                    }
                    if (result.front().type.type() == logical_type::STRUCT) {
                        auto parent_type = std::move(result[0]);
                        result.clear();
                        result.reserve(parent_type.type.child_types().size());
                        for (size_t i = 0; i < parent_type.type.child_types().size(); i++) {
                            column_path path = parent_type.path;
                            path.emplace_back(i);
                            result.emplace_back(type_path_t{std::move(path), parent_type.type.child_types()[i]});
                        }
                    } else {
                        auto parent_type = std::move(result[0]);
                        auto arr_type_ext = static_cast<array_logical_type_extension*>(parent_type.type.extension());
                        result.clear();
                        result.reserve(arr_type_ext->size());
                        for (size_t i = 0; i < arr_type_ext->size(); i++) {
                            column_path path = parent_type.path;
                            path.emplace_back(i);
                            result.emplace_back(type_path_t{std::move(path), arr_type_ext->internal_type()});
                        }
                    }
                }
            }

            if (result.empty()) {
                return core::error_t(core::error_code_t::schema_error,
                                     std::pmr::string{"path: \'" + key.as_string() + "\' was not found", resource});
            }
            // Store path inside a key, since we will need it later
            key.set_path(result.front().path);
            return result;
        }

        [[nodiscard]] core::result_wrapper_t<type_paths> validate_key(std::pmr::memory_resource* resource,
                                                                      components::expressions::key_t& key,
                                                                      const named_schema& schema_left,
                                                                      const named_schema& schema_right,
                                                                      bool same_schema) {
            if (key.side() == side_t::left) {
                return find_types(resource, key, schema_left);
            } else if (key.side() == side_t::right) {
                return find_types(resource, key, schema_right);
            } else {
                // find_types sets a path, but if both left and right are valid, this will be an error and won't matter
                auto column_path_left = find_types(resource, key, schema_left);
                auto column_path_right = find_types(resource, key, schema_right);
                // TODO Stop erasing errors from right and left
                if (column_path_left.has_error() && column_path_right.has_error()) {
                    return core::error_t(core::error_code_t::field_not_exists,
                                         std::pmr::string{"path: \'" + key.as_string() + "\' was not found", resource});
                }
                if (!same_schema && !column_path_left.has_error() && !column_path_right.has_error()) {
                    return core::error_t(
                        core::error_code_t::ambiguous_name,
                        std::pmr::string{"path: \'" + key.as_string() + "\' is ambiguous. Use aliases or full path",
                                         resource});
                }
                if (column_path_left.has_error()) {
                    key.set_side(side_t::right);
                    return column_path_right;
                } else {
                    key.set_side(side_t::left);
                    return column_path_left;
                }
            }
        }

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
        validate_schema(std::pmr::memory_resource* resource,
                        function_expression_t* expr,
                        const storage_parameters& parameters,
                        const named_schema& schema_left,
                        const named_schema& schema_right,
                        bool same_schema,
                        components::compute::function_types_mask allowed_function_types) {
            named_schema result(resource);
            std::pmr::vector<complex_logical_type> function_input_types(resource);
            function_input_types.reserve(expr->args().size());
            for (auto& field : expr->args()) {
                if (std::holds_alternative<components::expressions::key_t>(field)) {
                    auto& key = std::get<components::expressions::key_t>(field);
                    auto field_res = validate_key(resource, key, schema_left, schema_right, same_schema);
                    if (field_res.has_error()) {
                        return field_res.convert_error<named_schema>();
                    } else {
                        for (const auto& sub_field : field_res.value()) {
                            function_input_types.emplace_back(sub_field.type);
                        }
                    }
                } else if (std::holds_alternative<components::expressions::expression_ptr>(field)) {
                    if (std::get<components::expressions::expression_ptr>(field)->group() !=
                        expression_group::function) {
                        return core::error_t(
                            core::error_code_t::incorrect_function_argument,

                            std::pmr::string{"otterbrix functions do not support nesting; except other functions",
                                             resource});
                    }
                    auto& sub_expr = reinterpret_cast<components::expressions::function_expression_ptr&>(
                        std::get<components::expressions::expression_ptr>(field));
                    auto sub_expr_res = validate_schema(resource,
                                                        sub_expr.get(),
                                                        parameters,
                                                        schema_left,
                                                        schema_right,
                                                        same_schema,
                                                        allowed_function_types);
                    if (sub_expr_res.has_error()) {
                        return sub_expr_res;
                    } else {
                        for (const auto& sub_field : sub_expr_res.value()) {
                            function_input_types.emplace_back(sub_field.type);
                        }
                    }
                } else {
                    auto id = std::get<core::parameter_id_t>(field);
                    function_input_types.emplace_back(parameters.parameters.at(id).type());
                }
            }
            auto fn_lk = lookup_function(expr->name(), function_input_types);
            if (!fn_lk.name_exists) {
                return core::error_t(
                    core::error_code_t::unrecognized_function,
                    std::pmr::string{"function: \'" + expr->name() + "(...)\' was not found by the name", resource});
            } else if (fn_lk.match_found) {
                std::pmr::vector<complex_logical_type> function_output_types(resource);
                function_output_types.reserve(fn_lk.signature.output_types.size());
                for (const auto& output_type : fn_lk.signature.output_types) {
                    auto res = output_type.resolve(resource, function_input_types);
                    if (res.has_error()) {
                        return res.convert_error<named_schema>();
                    }
                    function_output_types.emplace_back(res.value());
                }
                if (function_output_types.size() == 1) {
                    result.emplace_back(type_from_t{expr->result_alias(), function_output_types.front()});
                } else {
                    result.emplace_back(type_from_t{expr->result_alias(),
                                                    complex_logical_type::create_struct("", function_output_types)});
                }
                expr->add_function_uid(fn_lk.uid);
            } else {
                // function does exist, but can not take this set of arguments
                // TODO: given arg number and types to error
                return core::error_t(core::error_code_t::incorrect_function_argument,
                                     std::pmr::string{"function: \'" + expr->name() +
                                                          "(...)\' was found but do not except given set of arguments",
                                                      resource});
            }

            return result;
        }

        // TODO: validate parameters
        // TODO: validate algebra
        [[nodiscard]] core::result_wrapper_t<named_schema> validate_schema(std::pmr::memory_resource* resource,
                                                                           update_expr_t* expr,
                                                                           const named_schema& schema_left,
                                                                           const named_schema& schema_right,
                                                                           bool same_schema) {
            switch (expr->type()) {
                case update_expr_type::set: {
                    auto* set_expr = reinterpret_cast<update_expr_set_t*>(expr);
                    auto type_res = find_types(resource, set_expr->key(), schema_left);
                    if (type_res.has_error()) {
                        return type_res.convert_error<named_schema>();
                    }
                    set_expr->key().set_side(side_t::left);
                    return validate_schema(resource, set_expr->left().get(), schema_left, schema_right, same_schema);
                }
                case update_expr_type::add:
                case update_expr_type::sub:
                case update_expr_type::mult:
                case update_expr_type::div:
                case update_expr_type::mod:
                case update_expr_type::exp:
                case update_expr_type::AND:
                case update_expr_type::OR:
                case update_expr_type::XOR:
                case update_expr_type::NOT:
                case update_expr_type::shift_left:
                case update_expr_type::shift_right: {
                    auto left_res =
                        validate_schema(resource, expr->left().get(), schema_left, schema_right, same_schema);
                    if (left_res.has_error()) {
                        return left_res;
                    }
                    auto right_res =
                        validate_schema(resource, expr->right().get(), schema_left, schema_right, same_schema);
                    if (right_res.has_error()) {
                        return right_res;
                    }
                    break;
                }
                case update_expr_type::sqr_root:
                case update_expr_type::cube_root:
                case update_expr_type::factorial:
                case update_expr_type::abs: {
                    auto left_res =
                        validate_schema(resource, expr->left().get(), schema_left, schema_right, same_schema);
                    if (left_res.has_error()) {
                        return left_res;
                    }
                    break;
                }
                case update_expr_type::get_value: {
                    auto* get_expr = reinterpret_cast<update_expr_get_value_t*>(expr);
                    auto key_res = validate_key(resource, get_expr->key(), schema_left, schema_right, same_schema);
                    if (key_res.has_error()) {
                        return key_res.convert_error<named_schema>();
                    }
                    break;
                }
                case update_expr_type::get_value_params:
                default:
                    break;
            }
            return named_schema(resource);
        }

        // Recursively validate keys inside scalar expression params
        // TODO: validate non-scalar sub-expressions
        [[nodiscard]] core::result_wrapper_t<named_schema>
        validate_scalar_params(std::pmr::memory_resource* resource,
                               std::pmr::vector<param_storage>& params,
                               const named_schema& schema_left,
                               const named_schema& schema_right,
                               bool same_schema,
                               const storage_parameters* parameters = nullptr) {
            for (auto& param : params) {
                if (std::holds_alternative<components::expressions::key_t>(param)) {
                    auto key_res = validate_key(resource,
                                                std::get<components::expressions::key_t>(param),
                                                schema_left,
                                                schema_right,
                                                same_schema);
                    if (key_res.has_error()) {
                        return key_res.convert_error<named_schema>();
                    }
                } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                    if (parameters) {
                        auto id = std::get<core::parameter_id_t>(param);
                        if (parameters->parameters.find(id) == parameters->parameters.end()) {
                            return core::error_t(core::error_code_t::invalid_parameter,
                                                 std::pmr::string{"Unknown parameter id in expression", resource});
                        }
                    }
                } else if (std::holds_alternative<expression_ptr>(param)) {
                    auto& sub = std::get<expression_ptr>(param);
                    if (sub->group() == expression_group::scalar) {
                        auto* scalar = static_cast<scalar_expression_t*>(sub.get());
                        auto sub_res = validate_scalar_params(resource,
                                                              scalar->params(),
                                                              schema_left,
                                                              schema_right,
                                                              same_schema,
                                                              parameters);
                        if (sub_res.has_error()) {
                            return sub_res;
                        }
                    }
                }
            }
            return named_schema(resource);
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
                if (sub->group() == expression_group::scalar) {
                    auto* scalar = static_cast<scalar_expression_t*>(sub.get());
                    auto res = resolve_key_paths_in_group(resource, scalar->params(), schema);
                    if (res.has_error()) {
                        return res;
                    }
                } else if (sub->group() == expression_group::compare) {
                    auto* cmp = static_cast<compare_expression_t*>(sub.get());
                    auto res = resolve_key_path(resource, cmp->left(), schema);
                    if (res.has_error()) {
                        return res;
                    }
                    res = resolve_key_path(resource, cmp->right(), schema);
                    if (res.has_error()) {
                        return res;
                    }
                    for (auto& child : cmp->children()) {
                        if (child->group() == expression_group::compare) {
                            auto* child_cmp = static_cast<compare_expression_t*>(child.get());
                            res = resolve_key_path(resource, child_cmp->left(), schema);
                            if (res.has_error()) {
                                return res;
                            }
                            res = resolve_key_path(resource, child_cmp->right(), schema);
                            if (res.has_error()) {
                                return res;
                            }
                        }
                    }
                }
            }
            return type_paths{resource};
        }

        [[nodiscard]] core::result_wrapper_t<named_schema> validate_schema(std::pmr::memory_resource* resource,
                                                                           compare_expression_t* expr,
                                                                           const storage_parameters& parameters,
                                                                           const named_schema& schema_left,
                                                                           const named_schema& schema_right,
                                                                           bool same_schema) {
            named_schema result(resource);
            result.emplace_back(type_from_t{"", logical_type::BOOLEAN});
            auto allowed_function_types =
                components::compute::create_mask(components::compute::function_type_t::row,
                                                 components::compute::function_type_t::vector);

            switch (expr->type()) {
                case compare_type::union_and:
                case compare_type::union_or:
                case compare_type::union_not: {
                    for (auto& nested_expr : expr->children()) {
                        core::error_t expr_error(
                            core::error_code_t::schema_error,
                            std::pmr::string{"unsupported expression type in compound predicate", resource});

                        switch (nested_expr->group()) {
                            case expression_group::function:
                                expr_error =
                                    validate_schema(resource,
                                                    reinterpret_cast<function_expression_t*>(nested_expr.get()),
                                                    parameters,
                                                    schema_left,
                                                    schema_right,
                                                    same_schema,
                                                    allowed_function_types)
                                        .error();
                                break;
                            case expression_group::compare:
                                expr_error = validate_schema(resource,
                                                             reinterpret_cast<compare_expression_t*>(nested_expr.get()),
                                                             parameters,
                                                             schema_left,
                                                             schema_right,
                                                             same_schema)
                                                 .error();
                                break;
                            default: // do noting & return unsupported error
                                break;
                        }
                        if (expr_error.contains_error()) {
                            return expr_error;
                        }
                    }
                    break;
                }
                // TODO: check if type have required comp operators
                case compare_type::eq:
                case compare_type::ne:
                case compare_type::gt:
                case compare_type::gte:
                case compare_type::lt:
                case compare_type::lte:
                    // TODO: check type for regex
                case compare_type::regex: {
                    // Validate left operand
                    if (std::holds_alternative<components::expressions::key_t>(expr->left())) {
                        auto key_res = validate_key(resource,
                                                    std::get<components::expressions::key_t>(expr->left()),
                                                    schema_left,
                                                    schema_right,
                                                    same_schema);
                        if (key_res.has_error()) {
                            return key_res.error();
                        }
                    } else if (std::holds_alternative<components::expressions::expression_ptr>(expr->left())) {
                        auto& sub_expr = std::get<components::expressions::expression_ptr>(expr->left());
                        if (sub_expr->group() == expression_group::function) {
                            auto& func_expr =
                                reinterpret_cast<components::expressions::function_expression_ptr&>(sub_expr);
                            auto expr_res = validate_schema(resource,
                                                            func_expr.get(),
                                                            parameters,
                                                            schema_left,
                                                            schema_right,
                                                            same_schema,
                                                            allowed_function_types);
                            if (expr_res.has_error()) {
                                return expr_res;
                            }
                        } else if (sub_expr->group() == expression_group::scalar) {
                            auto* scalar = static_cast<scalar_expression_t*>(sub_expr.get());
                            auto val_res = validate_scalar_params(resource,
                                                                  scalar->params(),
                                                                  schema_left,
                                                                  schema_right,
                                                                  same_schema,
                                                                  &parameters);
                            if (val_res.has_error()) {
                                return val_res;
                            }
                        }
                    }
                    // Validate right operand
                    if (std::holds_alternative<components::expressions::key_t>(expr->right())) {
                        auto key_res = validate_key(resource,
                                                    std::get<components::expressions::key_t>(expr->right()),
                                                    schema_left,
                                                    schema_right,
                                                    same_schema);
                        if (key_res.has_error()) {
                            return key_res.convert_error<named_schema>();
                        }
                    } else if (std::holds_alternative<components::expressions::expression_ptr>(expr->right())) {
                        auto& sub_expr = std::get<components::expressions::expression_ptr>(expr->right());
                        if (sub_expr->group() == expression_group::function) {
                            auto& func_expr =
                                reinterpret_cast<components::expressions::function_expression_ptr&>(sub_expr);
                            auto expr_res = validate_schema(resource,
                                                            func_expr.get(),
                                                            parameters,
                                                            schema_left,
                                                            schema_right,
                                                            same_schema,
                                                            allowed_function_types);
                            if (expr_res.has_error()) {
                                return expr_res;
                            }
                        } else if (sub_expr->group() == expression_group::scalar) {
                            auto* scalar = static_cast<scalar_expression_t*>(sub_expr.get());
                            auto val_res = validate_scalar_params(resource,
                                                                  scalar->params(),
                                                                  schema_left,
                                                                  schema_right,
                                                                  same_schema,
                                                                  &parameters);
                            if (val_res.has_error()) {
                                return val_res;
                            }
                        }
                    }
                    break;
                }
                case compare_type::any:
                case compare_type::all:
                case compare_type::is_null:
                case compare_type::is_not_null: {
                    if (std::holds_alternative<components::expressions::key_t>(expr->left())) {
                        auto key_res = validate_key(resource,
                                                    std::get<components::expressions::key_t>(expr->left()),
                                                    schema_left,
                                                    schema_right,
                                                    same_schema);
                        if (key_res.has_error()) {
                            return key_res.convert_error<named_schema>();
                        }
                    }
                    break;
                }
                default:
                    break;
            }
            return result;
        }

        [[nodiscard]] core::result_wrapper_t<named_schema> validate_schema(std::pmr::memory_resource* resource,
                                                                           const impl::plan_resolve_index_t* idx,
                                                                           node_match_t* node,
                                                                           const storage_parameters& parameters,
                                                                           const named_schema& schema_left,
                                                                           const named_schema& schema_right,
                                                                           bool same_schema) {
            if (node->expressions().empty()) {
                // physical plan reinterprets this as default scan
                const auto* tbl = impl::tbl_md_for(idx, node->dbname(), node->relname());
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
                    return core::error_t(core::error_code_t::table_not_exists, std::pmr::string{"", resource});
                }
            } else {
                assert(node->expressions().size() == 1);
                if (node->expressions()[0]->group() == expression_group::compare) {
                    auto* expr = reinterpret_cast<compare_expression_t*>(node->expressions()[0].get());
                    return validate_schema(resource, expr, parameters, schema_left, schema_right, same_schema);
                } else if (node->expressions()[0]->group() == expression_group::function) {
                    auto* expr = reinterpret_cast<function_expression_t*>(node->expressions()[0].get());
                    auto allowed_function_types =
                        components::compute::create_mask(components::compute::function_type_t::row,
                                                         components::compute::function_type_t::vector);
                    auto expr_res = validate_schema(resource,
                                                    expr,
                                                    parameters,
                                                    schema_left,
                                                    schema_right,
                                                    same_schema,
                                                    allowed_function_types);
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
        validate_schema(std::pmr::memory_resource* resource, node_sort_t* node, const named_schema& schema) {
            for (auto& expr : node->expressions()) {
                if (expr->group() == expression_group::sort) {
                    auto* sort_expr = static_cast<sort_expression_t*>(expr.get());
                    auto res = find_types(resource, sort_expr->key(), schema);
                    if (res.has_error()) {
                        return res.convert_error<named_schema>();
                    }
                } else if (expr->group() == expression_group::scalar) {
                    // Computed arithmetic sort key: resolve column params against schema.
                    auto* scalar_expr = static_cast<scalar_expression_t*>(expr.get());
                    auto res = resolve_key_paths_in_group(resource, scalar_expr->params(), schema);
                    if (res.has_error()) {
                        return res.convert_error<named_schema>();
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
        [[nodiscard]] core::error_t resolve_returning_columns(std::pmr::memory_resource* resource,
                                                              std::pmr::vector<expression_ptr>* returning,
                                                              const named_schema& schema_left,
                                                              const named_schema& schema_right,
                                                              bool same_schema) {
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
                            // Side-aware: schema_left is the destination table,
                            // schema_right the USING/FROM table (same as left when
                            // there is no join). validate_key stamps the key's side
                            // and sets its path into the matching side's schema.
                            auto res = validate_key(resource, key, schema_left, schema_right, same_schema);
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
                        auto field = find_types(resource, star_key, schema_left);
                        if (field.has_error()) {
                            if (same_schema) {
                                return field.error();
                            }
                            field = find_types(resource, star_key, schema_right);
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
                        auto res = resolve_key_paths_in_group(resource, scalar_expr->params(), schema_left);
                        if (res.has_error()) {
                            return res.error();
                        }
                        idx++;
                        break;
                    }
                }
            }
            return core::error_t::no_error();
        }

    } // namespace impl

    // ---- V4 plan-tree idx-based check_*_exists ----

    core::error_t check_namespace_exists(std::pmr::memory_resource* resource,
                                         const impl::plan_resolve_index_t* idx,
                                         const components::catalog::table_id& id) {
        const auto& ns = id.get_namespace();
        if (ns.empty()) {
            return core::error_t(core::error_code_t::database_not_exists,
                                 std::pmr::string{"database does not exist", resource});
        }
        if (impl::ns_oid_for_dbname(idx, std::string_view(ns.front())) == components::catalog::INVALID_OID) {
            return core::error_t(core::error_code_t::database_not_exists,
                                 std::pmr::string{"database does not exist", resource});
        }
        return core::error_t::no_error();
    }

    core::error_t check_collection_exists(std::pmr::memory_resource* resource,
                                          const impl::plan_resolve_index_t* idx,
                                          const components::catalog::table_id& id) {
        if (auto err = check_namespace_exists(resource, idx, id); err.contains_error()) {
            return err;
        }
        const auto* tbl =
            impl::tbl_md_for(idx, std::string_view(id.get_namespace().front()), std::string_view(id.table_name()));
        if (!tbl) {
            return core::error_t(core::error_code_t::table_not_exists,
                                 std::pmr::string{"collection does not exist", resource});
        }
        return core::error_t::no_error();
    }

    core::error_t check_type_exists(std::pmr::memory_resource* resource,
                                    const impl::plan_resolve_index_t* idx,
                                    const std::string& alias,
                                    std::span<const std::string> search_dbnames) {
        if (components::catalog::pg_name_to_logical_type(alias) != components::types::logical_type::UNKNOWN) {
            return core::error_t::no_error();
        }
        static const std::string kPublic{"public"};
        static const std::string kPgCatalog{"pg_catalog"};
        if (search_dbnames.empty()) {
            const std::string default_path[] = {kPublic, kPgCatalog};
            for (const auto& db : default_path) {
                if (impl::type_md_for(idx, std::string_view(db), std::string_view(alias))) {
                    return core::error_t::no_error();
                }
            }
        } else {
            for (const auto& db : search_dbnames) {
                if (impl::type_md_for(idx, std::string_view(db), std::string_view(alias))) {
                    return core::error_t::no_error();
                }
            }
        }
        return core::error_t(core::error_code_t::schema_error,
                             std::pmr::string{"type: \'" + alias + "\' is not registered in catalog", resource});
    }

    namespace {
        // Reverse-lookup: namespace_oid -> dbname. Linear scan over the small
        // plan-resolve index; only invoked when a node carries a valid
        // table_oid and we need to populate table_dbnames for the UDT type
        // probe in check_node. Returns empty string_view if not found.
        std::string_view dbname_for_ns_oid(const impl::plan_resolve_index_t* idx, components::catalog::oid_t ns_oid) {
            if (!idx)
                return {};
            for (const auto& [name, oid] : idx->ns_by_dbname) {
                if (oid == ns_oid)
                    return name;
            }
            return {};
        }
    } // namespace

    core::error_t validate_types(std::pmr::memory_resource* resource,
                                 const impl::plan_resolve_index_t* idx,
                                 node_t* logical_plan,
                                 core::date::timezone_offset_t session_tz) {
        impl::plan_resolve_index_t local_idx;
        if (idx == nullptr) {
            impl::gather_plan_resolve_index(logical_plan, &local_idx);
            if (!local_idx.empty()) {
                idx = &local_idx;
            }
        }

        std::pmr::vector<complex_logical_type> encountered_types{resource};
        std::set<std::string> table_dbnames;
        core::error_t result = core::error_t::no_error();

        auto check_node = [&](node_t* node) {
            // Drop-nodes skip existence + type collection here.
            // Their catalog_resolve_* children verify existence at parse time;
            // CASCADE/RESTRICT is enforced by the cascade-delete operator downstream.
            switch (node->type()) {
                case node_type::drop_collection_t:
                case node_type::drop_database_t:
                case node_type::drop_index_t:
                case node_type::drop_macro_t:
                case node_type::drop_sequence_t:
                case node_type::drop_type_t:
                case node_type::drop_view_t:
                    return true;
                default:
                    break;
            }
            if (auto oid = node->table_oid(); oid != components::catalog::INVALID_OID) {
                const auto* tbl = impl::tbl_md_for_oid(idx, oid);
                if (!tbl) {
                    result = core::error_t(core::error_code_t::table_not_exists,
                                           std::pmr::string{"collection does not exist", resource});
                    return false;
                }
                if (tbl->relkind != 'g') {
                    for (const auto& column : tbl->columns) {
                        encountered_types.emplace_back(column.type);
                    }
                    if (auto ns_name = dbname_for_ns_oid(idx, tbl->namespace_oid); !ns_name.empty()) {
                        table_dbnames.emplace(ns_name);
                    }
                }
            }
            // pull/double-check check format from collection referenced by logical_plan and data stored inside node_data_t
            if (node->type() == node_type::data_t) {
                auto* data_node = reinterpret_cast<node_data_t*>(node);

                // Probe plan-tree idx by dbname strings.
                auto type_visible = [&](std::string_view name) {
                    for (const auto& db : table_dbnames) {
                        if (impl::type_md_for(idx, std::string_view(db), name))
                            return true;
                    }
                    return impl::type_md_for(idx, std::string_view{"public"}, name) ||
                           impl::type_md_for(idx, std::string_view{"pg_catalog"}, name);
                };

                for (auto& column : data_node->data_chunk().data) {
                    auto it = std::find_if(
                        encountered_types.begin(),
                        encountered_types.end(),
                        [&column](const complex_logical_type& type) { return type.alias() == column.type().alias(); });
                    // if this is a registered type, then conversion is required
                    bool ty_exists = it != encountered_types.end() && type_visible(std::string_view(it->type_name()));
                    if (ty_exists) {
                        if (is_duration(it->type()) && column.type().type() == logical_type::STRING_LITERAL) {
                            components::vector::vector_t new_column(resource, *it, data_node->data_chunk().capacity());
                            for (size_t i = 0; i < data_node->data_chunk().size(); i++) {
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
                                        std::pmr::string{"couldn't convert string to date/time type: \'" + it->alias() +
                                                             "\', value: \'" + std::string(str) + "\'",
                                                         resource});
                                    return false;
                                }
                                new_column.set_value(i, *parsed_val);
                            }
                            column = std::move(new_column);
                        } else if (it->type() == logical_type::DECIMAL &&
                                   (is_numeric(column.type().type()) ||
                                    column.type().type() == logical_type::STRING_LITERAL)) {
                            components::vector::vector_t new_column(resource, *it, data_node->data_chunk().capacity());
                            for (size_t i = 0; i < data_node->data_chunk().size(); i++) {
                                auto val = column.value(i).cast_as(*it, session_tz);
                                if (val.type().type() == logical_type::NA) {
                                    result =
                                        core::error_t(core::error_code_t::schema_error,
                                                      std::pmr::string{"couldn't convert value to decimal type: \'" +
                                                                           it->alias() + "\'",
                                                                       resource});
                                    return false;
                                }
                                new_column.set_value(i, val);
                            }
                            column = std::move(new_column);
                        } else if (!check_type_exists(resource, idx, it->type_name(), std::span<const std::string>())
                                        .contains_error()) {
                            // if this is a registered type, then conversion is required
                            if (it->type() == logical_type::STRUCT) {
                                components::vector::vector_t new_column(resource,
                                                                        *it,
                                                                        data_node->data_chunk().capacity());
                                for (size_t i = 0; i < data_node->data_chunk().size(); i++) {
                                    auto val = column.value(i).cast_as(*it, session_tz);
                                    if (val.type().type() == logical_type::NA) {
                                        result =
                                            core::error_t(core::error_code_t::schema_error,
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
                                components::vector::vector_t new_column(resource,
                                                                        *it,
                                                                        data_node->data_chunk().capacity());
                                for (size_t i = 0; i < data_node->data_chunk().size(); i++) {
                                    auto val = column.data<std::string_view>()[i];
                                    auto enum_val = logical_value_t::create_enum(resource, *it, val);
                                    if (enum_val.type().type() == logical_type::NA) {
                                        result = core::error_t(core::error_code_t::schema_error,
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
                                assert(false && "missing type conversion in dispatcher_t::check_collections_format_");
                            }
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

    [[nodiscard]] core::result_wrapper_t<named_schema>
    validate_schema(std::pmr::memory_resource* resource,
                    const impl::plan_resolve_index_t* idx,
                    node_t* node,
                    const components::logical_plan::storage_parameters& parameters) {
        // `idx` is supplied by the dispatcher.
        // Legacy harnesses may pass nullptr — fall back to a locally-gathered
        // index so internal callers still get plan-tree lookups.
        impl::plan_resolve_index_t local_idx;
        if (idx == nullptr) {
            impl::gather_plan_resolve_index(node, &local_idx);
            if (!local_idx.empty()) {
                idx = &local_idx;
            }
        }

        named_schema result{resource};

        switch (node->type()) {
            // SQL transaction-control leaves (BEGIN/COMMIT/ROLLBACK): no table
            // schema to validate — empty schema, like an all-resolve sequence_t.
            // Defensive mirror of the executor's validate break-group; without
            // these the default arm below assert(false)s on the node type.
            case node_type::begin_transaction_t:
            case node_type::commit_transaction_t:
            case node_type::abort_transaction_t:
                break;
            case node_type::aggregate_t: {
                node_group_t* node_group = nullptr;
                node_match_t* node_match = nullptr;
                node_sort_t* node_sort = nullptr;
                node_select_t* node_select = nullptr;
                node_t* node_data = nullptr;

                named_schema table_schema(resource);
                named_schema incoming_schema(resource);
                bool same_schema = false;

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
                        case node_type::having_t:
                            break;
                        case node_type::select_t:
                            node_select = reinterpret_cast<node_select_t*>(child.get());
                            break;
                        default:
                            node_data = child.get();
                            break;
                    }
                }

                if (node_data) {
                    auto node_data_res = validate_schema(resource, idx, node_data, parameters);
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
                    const auto* tbl = impl::tbl_md_for(idx, agg_dbname_s, agg_relname_s);
                    if (tbl && tbl->relkind != 'g') {
                        for (const auto& column : tbl->columns) {
                            table_schema.emplace_back(type_from_t{visible_alias, column.type});
                        }
                    } else if (tbl && tbl->relkind == 'g') {
                        for (const auto& column : tbl->columns) {
                            table_schema.emplace_back(type_from_t{visible_alias, column.type});
                        }
                    } else {
                        // Distinguish missing database from missing collection
                        // so callers (and tests) get the right error code.
                        if (impl::ns_oid_for_dbname(idx, std::string_view(agg_dbname_s)) ==
                            components::catalog::INVALID_OID) {
                            return core::error_t(core::error_code_t::database_not_exists,
                                                 std::pmr::string{"", resource});
                        }
                        return core::error_t(core::error_code_t::table_not_exists, std::pmr::string{"", resource});
                    }
                }
                if (table_schema.empty() && incoming_schema.empty()) {
                    // Empty computing table — still need aggregate validation for function_uid
                }
                if (incoming_schema.empty()) {
                    incoming_schema = table_schema;
                    same_schema = true;
                }
                if (table_schema.empty()) {
                    table_schema = incoming_schema;
                    same_schema = true;
                }
                if (node_match) {
                    // Expand is_not_null/is_null on multi-type fields into OR/AND
                    // over their variants (jsonb '?'/'?|'/'?&' over multi-type).
                    for (auto& e : node_match->expressions()) {
                        e = impl::rewrite_multitype_null_checks(resource, e, incoming_schema);
                    }
                    auto res = impl::validate_schema(resource,
                                                     idx,
                                                     node_match,
                                                     parameters,
                                                     table_schema,
                                                     incoming_schema,
                                                     same_schema);
                    if (res.has_error()) {
                        return res;
                    }
                }

                if (!node_group) {
                    if (node_sort) {
                        auto res = impl::validate_schema(resource, node_sort, incoming_schema);
                        if (res.has_error()) {
                            return res;
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
                                auto field = impl::find_types(resource, k_copy, incoming_schema);
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
                                // (output_name, source_alias) pairs
                                std::vector<std::pair<std::string, std::string>> cols;
                                for (const auto& sc : incoming_schema) {
                                    if (!sc.type.has_alias()) {
                                        continue;
                                    }
                                    std::string alias(sc.type.alias());
                                    const bool under = alias == prefix || alias.rfind(prefix_slash, 0) == 0;
                                    if (is_delete) {
                                        if (!under) {
                                            cols.emplace_back(alias, alias);
                                        }
                                    } else if (under) {
                                        std::string out = alias == prefix ? prefix.substr(prefix.find_last_of('/') + 1)
                                                                          : alias.substr(prefix_slash.size());
                                        cols.emplace_back(std::move(out), std::move(alias));
                                    }
                                }
                                exprs.erase(exprs.begin() + static_cast<ptrdiff_t>(ei));
                                for (size_t j = 0; j < cols.size(); j++) {
                                    components::expressions::key_t out_key(resource, cols[j].first.c_str());
                                    components::expressions::key_t src_key(resource, cols[j].second.c_str());
                                    exprs.insert(
                                        exprs.begin() + static_cast<ptrdiff_t>(ei + j),
                                        make_scalar_expression(resource, scalar_type::get_field, out_key, src_key));
                                }
                                ei += cols.size();
                            }
                        }

                        bool has_computed_column = false;
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
                                    auto res =
                                        impl::validate_key(resource, key, incoming_schema, incoming_schema, true);
                                    if (res.has_error()) {
                                        return res.convert_error<named_schema>();
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
                            column_key key{col.result_alias, std::string(col.type.alias()), col.type.type(), col.side};
                            if (!seen_cols.insert(std::move(key)).second) {
                                return core::error_t(
                                    core::error_code_t::schema_error,
                                    std::pmr::string{"column '" + col.type.alias() +
                                                         "' has multiple types; use explicit type selection",
                                                     resource});
                            }
                        }
                    }
                    if (node_select) {
                        named_schema result_schema(resource);
                        for (const auto& expr : node_select->expressions()) {
                            if (expr->group() != expression_group::scalar) {
                                continue;
                            }
                            const auto* scalar_expr = reinterpret_cast<const scalar_expression_t*>(expr.get());
                            if (scalar_expr->type() == scalar_type::get_field) {
                                const auto& key =
                                    scalar_expr->params().empty()
                                        ? scalar_expr->key()
                                        : std::get<components::expressions::key_t>(scalar_expr->params().front());
                                if (!key.path().empty() && key.path().front() < incoming_schema.size()) {
                                    result_schema.push_back(incoming_schema[key.path().front()]);
                                }
                            } else {
                                complex_logical_type unknown_type(components::types::logical_type::UNKNOWN);
                                if (!scalar_expr->key().is_null()) {
                                    unknown_type.set_alias(scalar_expr->key().as_string());
                                }
                                result_schema.push_back(type_from_t{node->result_alias(), std::move(unknown_type)});
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
                            auto field = impl::find_types(resource, k_copy, incoming_schema);
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
                        return t == scalar_type::case_expr || t == scalar_type::add || t == scalar_type::subtract ||
                               t == scalar_type::multiply || t == scalar_type::divide || t == scalar_type::mod ||
                               t == scalar_type::unary_minus;
                    };

                    auto compute_type_entry = [&](scalar_expression_t* scalar_expr,
                                                  const named_schema& schema) -> type_from_t {
                        auto resolve_type = [&](param_storage& param, auto& self) -> complex_logical_type {
                            if (std::holds_alternative<components::expressions::key_t>(param)) {
                                auto& key = std::get<components::expressions::key_t>(param);
                                assert(!key.path().empty());
                                return schema[key.path()[0]].type;
                            } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                                return parameters.parameters.at(std::get<core::parameter_id_t>(param)).type();
                            } else {
                                auto& sub = std::get<expression_ptr>(param);
                                if (sub->group() == expression_group::scalar) {
                                    auto* sub_s = static_cast<scalar_expression_t*>(sub.get());
                                    if (!sub_s->params().empty()) {
                                        auto lt = self(sub_s->params()[0], self);
                                        auto rt = sub_s->params().size() > 1 ? self(sub_s->params()[1], self) : lt;
                                        return complex_logical_type(
                                            arithmetic_result_type(lt.type(),
                                                                   rt.type(),
                                                                   impl::scalar_to_arith_op(sub_s->type())));
                                    }
                                }
                                assert(false);
                                return complex_logical_type(logical_type::BIGINT);
                            }
                        };
                        complex_logical_type result_type;
                        if (scalar_expr->type() == scalar_type::case_expr) {
                            result_type = (scalar_expr->params().size() >= 2)
                                              ? resolve_type(scalar_expr->params()[1], resolve_type)
                                              : complex_logical_type(logical_type::BIGINT);
                        } else {
                            auto lt = resolve_type(scalar_expr->params()[0], resolve_type);
                            auto rt = scalar_expr->params().size() > 1
                                          ? resolve_type(scalar_expr->params()[1], resolve_type)
                                          : lt;
                            result_type = complex_logical_type(
                                arithmetic_result_type(lt.type(),
                                                       rt.type(),
                                                       impl::scalar_to_arith_op(scalar_expr->type())));
                        }
                        if (!scalar_expr->key().is_null()) {
                            result_type.set_alias(scalar_expr->key().as_string());
                        }
                        return type_from_t{node->result_alias(), std::move(result_type)};
                    };

                    // --- Pass 1: classify + resolve + collect schemas ---
                    size_t select_end = node_group->expressions().size() - node_group->internal_aggregate_count;
                    named_schema key_schema(resource);
                    named_schema agg_schema(resource);
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
                                auto res = impl::validate_key(resource, key, incoming_schema, incoming_schema, true);
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
                                // GROUP BY field: resolve key path and expose in output schema
                                auto& key = scalar_expr->key();
                                auto res = impl::validate_key(resource, key, incoming_schema, incoming_schema, true);
                                if (res.has_error()) {
                                    return res.convert_error<named_schema>();
                                }
                                const auto& col_type = incoming_schema[key.path()[0]].type;
                                complex_logical_type out_type = col_type;
                                if (!key.storage().empty()) {
                                    out_type.set_alias(std::string(key.storage().back()));
                                }
                                result.emplace_back(type_from_t{node->result_alias(), out_type});
                                key_schema.emplace_back(result.back());
                            } else if (is_case_or_arithmetic(scalar_expr->type())) {
                                // Try resolve against incoming_schema
                                auto res =
                                    impl::resolve_key_paths_in_group(resource, scalar_expr->params(), incoming_schema);
                                if (res.has_error()) {
                                    post_agg_indices.push_back(i); // defer to Pass 2
                                } else {
                                    auto entry = compute_type_entry(scalar_expr, incoming_schema);
                                    result.emplace_back(entry);
                                    key_schema.emplace_back(entry);
                                }
                            }
                        } else if (expr->group() == expression_group::aggregate) {
                            auto* agg_expr = reinterpret_cast<aggregate_expression_t*>(expr.get());
                            bool is_internal = (i >= select_end);

                            // Resolve aggregate params against incoming schema
                            auto res_paths =
                                impl::resolve_key_paths_in_group(resource, agg_expr->params(), incoming_schema);
                            if (res_paths.has_error()) {
                                return res_paths.convert_error<named_schema>();
                            }

                            std::pmr::vector<complex_logical_type> function_input_types(resource);
                            function_input_types.reserve(agg_expr->params().size());
                            for (auto& param : agg_expr->params()) {
                                if (std::holds_alternative<components::expressions::key_t>(param)) {
                                    auto& key = std::get<components::expressions::key_t>(param);
                                    auto field = impl::find_types(resource, key, incoming_schema);
                                    if (field.has_error()) {
                                        return field.convert_error<named_schema>();
                                    }
                                    for (const auto& sub_field : field.value()) {
                                        function_input_types.emplace_back(sub_field.type);
                                    }
                                } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                                    auto id = std::get<core::parameter_id_t>(param);
                                    function_input_types.emplace_back(parameters.parameters.at(id).type());
                                } else {
                                    auto& sub_expr = std::get<expression_ptr>(param);
                                    if (sub_expr->group() == expression_group::scalar) {
                                        auto* sub_scalar = reinterpret_cast<scalar_expression_t*>(sub_expr.get());
                                        core::error_t resolve_error = core::error_t::no_error();
                                        // Recursive arithmetic-type resolver. A plain lambda cannot name
                                        // itself for the recursion, so this is a local functor struct
                                        // (rule 14 forbids std::function): operator() recurses via (*this).
                                        // References to the surrounding context are held as members.
                                        struct arith_type_resolver {
                                            std::pmr::memory_resource* resource;
                                            const named_schema& incoming_schema;
                                            // Named params (not `parameters`): reusing the enclosing
                                            // function parameter's name inside the class changes its
                                            // meaning mid-scope — ill-formed under GCC.
                                            const components::logical_plan::storage_parameters& params;
                                            core::error_t& resolve_error;

                                            complex_logical_type operator()(param_storage& p) const {
                                                if (std::holds_alternative<components::expressions::key_t>(p)) {
                                                    auto& k = std::get<components::expressions::key_t>(p);
                                                    auto f = impl::find_types(resource, k, incoming_schema);
                                                    if (!f.has_error()) {
                                                        return f.value().front().type;
                                                    }
                                                    if (f.has_error()) {
                                                        resolve_error = f.error();
                                                    }
                                                    assert(false);
                                                    return complex_logical_type(logical_type::INVALID);
                                                } else if (std::holds_alternative<core::parameter_id_t>(p)) {
                                                    return params.parameters.at(std::get<core::parameter_id_t>(p))
                                                        .type();
                                                } else {
                                                    auto& inner = std::get<expression_ptr>(p);
                                                    if (inner->group() == expression_group::scalar) {
                                                        auto* s = reinterpret_cast<scalar_expression_t*>(inner.get());
                                                        if (s->params().size() >= 2) {
                                                            auto lt = (*this)(s->params()[0]);
                                                            auto rt = (*this)(s->params()[1]);
                                                            return complex_logical_type(
                                                                promote_type(lt.type(), rt.type()));
                                                        }
                                                    }
                                                    assert(false);
                                                    return complex_logical_type(logical_type::INVALID);
                                                }
                                            }
                                        };
                                        arith_type_resolver resolve_arith_type{resource,
                                                                               incoming_schema,
                                                                               parameters,
                                                                               resolve_error};
                                        // CASE WHEN inside an aggregate (e.g. SUM(CASE WHEN cond THEN a ELSE b END)).
                                        // case_expr params layout (per transform_select_case_expr): pairs of
                                        // [cond, result, cond, result, ..., default]. We take the type of the
                                        // first THEN result (params[1]) as the CASE return type. Mixed branch
                                        // result types would need a wider promote — deferred.
                                        if (sub_scalar->type() == scalar_type::case_expr) {
                                            if (sub_scalar->params().size() < 2) {
                                                return core::error_t{
                                                    core::error_code_t::invalid_parameter,
                                                    std::pmr::string{"CASE expression with no THEN branch", resource}};
                                            }
                                            auto rt = resolve_arith_type(sub_scalar->params()[1]);
                                            if (resolve_error.type != core::error_code_t::none) {
                                                return resolve_error;
                                            }
                                            function_input_types.emplace_back(rt);
                                        } else if (sub_scalar->params().size() >= 2) {
                                            auto lt = resolve_arith_type(sub_scalar->params()[0]);
                                            auto rt = resolve_arith_type(sub_scalar->params()[1]);
                                            if (resolve_error.type != core::error_code_t::none) {
                                                return resolve_error;
                                            }
                                            function_input_types.emplace_back(
                                                arithmetic_result_type(lt.type(),
                                                                       rt.type(),
                                                                       impl::scalar_to_arith_op(sub_scalar->type())));
                                        } else {
                                            return core::error_t(
                                                core::error_code_t::invalid_parameter,
                                                std::pmr::string{"single-operand scalar is not supported", resource});
                                        }
                                    } else {
                                        return core::error_t(
                                            core::error_code_t::invalid_parameter,
                                            std::pmr::string{"non-scalar expression param is not supported", resource});
                                    }
                                }
                            }
                            auto agg_lk = impl::lookup_function(agg_expr->function_name(), function_input_types);
                            if (!agg_lk.name_exists) {
                                return core::error_t(core::error_code_t::unrecognized_function,
                                                     std::pmr::string{"function: \'" + agg_expr->function_name() +
                                                                          "(...)\' was not found by the name",
                                                                      resource});
                            } else if (agg_lk.match_found) {
                                std::pmr::vector<complex_logical_type> function_output_types(resource);
                                function_output_types.reserve(agg_lk.signature.output_types.size());
                                for (const auto& output_type : agg_lk.signature.output_types) {
                                    auto res = output_type.resolve(resource, function_input_types);
                                    if (res.has_error()) {
                                        return res.convert_error<named_schema>();
                                    }
                                    function_output_types.emplace_back(res.value());
                                }
                                auto agg_alias = agg_expr->key().as_string();
                                if (!is_internal) {
                                    if (function_output_types.size() == 1) {
                                        result.emplace_back(
                                            type_from_t{node->result_alias(), function_output_types.front()});
                                        if (!agg_expr->key().is_null()) {
                                            result.back().type.set_alias(agg_alias);
                                        }
                                    } else {
                                        result.emplace_back(type_from_t{
                                            node->result_alias(),
                                            complex_logical_type::create_struct("", function_output_types, agg_alias)});
                                    }
                                }
                                agg_expr->add_function_uid(agg_lk.uid);
                                // Collect for post_agg_schema
                                if (!is_internal && !function_output_types.empty()) {
                                    agg_result_positions.push_back(result.size() - 1);
                                    agg_schema.emplace_back(result.back());
                                } else if (is_internal && !function_output_types.empty()) {
                                    type_from_t entry{node->result_alias(), function_output_types.front()};
                                    if (!agg_expr->key().is_null()) {
                                        entry.type.set_alias(agg_alias);
                                    }
                                    agg_schema.emplace_back(std::move(entry));
                                }
                            } else {
                                return core::error_t(
                                    core::error_code_t::incorrect_function_argument,
                                    std::pmr::string{"function: \'" + agg_expr->function_name() +
                                                         "(...)\' was found but do not except given set of arguments",
                                                     resource});
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
                        named_schema post_agg_schema(resource);
                        post_agg_schema.insert(post_agg_schema.end(), key_schema.begin(), key_schema.end());
                        post_agg_schema.insert(post_agg_schema.end(), agg_schema.begin(), agg_schema.end());

                        for (size_t pa_idx : post_agg_indices) {
                            auto& expr = node_group->expressions()[pa_idx];
                            auto* scalar_expr = reinterpret_cast<scalar_expression_t*>(expr.get());

                            auto res2 =
                                impl::resolve_key_paths_in_group(resource, scalar_expr->params(), post_agg_schema);
                            if (res2.has_error()) {
                                return res2.convert_error<named_schema>();
                            }
                            scalar_expr->key().set_path({SIZE_MAX}); // Mark for planner

                            auto entry = compute_type_entry(scalar_expr, post_agg_schema);
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
                                    auto res = impl::validate_key(resource, key, key_schema, key_schema, true);
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
                            }
                        }
                    }
                }
                if (node_sort) {
                    // Add hidden columns for sort keys not in the GROUP output
                    for (auto& sort_child : node_sort->expressions()) {
                        auto* sort_expr = static_cast<sort_expression_t*>(sort_child.get());
                        auto& skey = sort_expr->key();
                        // Try resolving in the GROUP result schema first
                        auto field_in_result = impl::find_types(resource, skey, result);
                        if (!field_in_result.has_error() && !field_in_result.value().empty()) {
                            continue; // already in result
                        }
                        // Not in result — try incoming schema and add as hidden column
                        auto field = impl::find_types(resource, skey, incoming_schema);
                        if (!field.has_error() && !field.value().empty()) {
                            auto hidden_expr = make_scalar_expression(resource, scalar_type::get_field, skey);
                            node_group->append_expression(hidden_expr);
                            result.emplace_back(type_from_t{node->result_alias(), field.value().front().type});
                        }
                    }
                    auto res = impl::validate_schema(resource, node_sort, result);
                    if (res.has_error()) {
                        return res;
                    }
                }
                if (node_group->having()) {
                    auto& having = node_group->having();
                    if (having->group() == expression_group::compare) {
                        auto* cmp_expr = reinterpret_cast<compare_expression_t*>(having.get());
                        auto res = impl::validate_schema(resource, cmp_expr, parameters, result, result, true);
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
                if (node->children().empty()) {
                    return core::error_t(
                        core::error_code_t::unimplemented_yet,
                        std::pmr::string{"otterbrix does not support constants as function arguments in this context",
                                         resource});
                }
                auto* function_node = reinterpret_cast<node_function_t*>(node);
                auto input_schema = validate_schema(resource, idx, node->children().front().get(), parameters);
                if (input_schema.has_error()) {
                    return input_schema;
                }
                std::pmr::vector<complex_logical_type> function_input(resource);
                function_input.reserve(input_schema.value().size());
                for (const auto& pair : input_schema.value()) {
                    function_input.emplace_back(pair.type);
                }
                // TODO: check for errors between function_node->args() and input_schema (amount of args and name correctness)
                // Also order could be different
                auto fn_lk = impl::lookup_function(function_node->name(), function_input);
                if (!fn_lk.name_exists) {
                    return core::error_t(
                        core::error_code_t::unrecognized_function,
                        std::pmr::string{
                            ("function: \'" + function_node->name() + "(...)\' was not found by the name").c_str(),
                            resource});
                } else if (fn_lk.match_found) {
                    result.reserve(fn_lk.signature.output_types.size());
                    for (const auto& output_type : fn_lk.signature.output_types) {
                        auto res = output_type.resolve(resource, function_input);
                        if (res.has_error()) {
                            return res.convert_error<named_schema>();
                        }
                        result.emplace_back(type_from_t{node->result_alias(), res.value()});
                        function_node->add_function_uid(fn_lk.uid);
                    }
                } else {
                    return core::error_t(
                        core::error_code_t::incorrect_function_argument,
                        std::pmr::string{"function: \'" + function_node->name() +
                                             "(...)\' was found but do not except given set of arguments",
                                         resource});
                }
                break;
            }
            case node_type::join_t: {
                auto left_schema = validate_schema(resource, idx, node->children().front().get(), parameters);
                if (left_schema.has_error()) {
                    return left_schema;
                }
                auto right_schema = validate_schema(resource, idx, node->children().back().get(), parameters);
                if (right_schema.has_error()) {
                    return right_schema;
                }
                auto expr_res =
                    impl::validate_schema(resource,
                                          reinterpret_cast<compare_expression_t*>(node->expressions()[0].get()),
                                          parameters,
                                          left_schema.value(),
                                          right_schema.value(),
                                          false);
                if (expr_res.has_error()) {
                    return expr_res;
                }

                // TODO: merge using join type, because some join types allow duplicate names in result, while others do not
                result = impl::merge_schemas(resource, std::move(left_schema.value()), std::move(right_schema.value()));
                break;
            }
            // For now next 3 nodes do not support returning clause:
            case node_type::insert_t: {
                auto* insert_node = reinterpret_cast<node_insert_t*>(node);
                const auto* tbl_ins = impl::tbl_md_for_oid(idx, insert_node->table_oid());
                if (!tbl_ins) {
                    return core::error_t(core::error_code_t::table_not_exists, std::pmr::string{"", resource});
                }

                auto incoming_schema = validate_schema(resource, idx, node->children().front().get(), parameters);
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
                        auto ret_err = impl::resolve_returning_columns(resource,
                                                                       &insert_node->returning(),
                                                                       table_schema,
                                                                       table_schema,
                                                                       true);
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
                                auto key_res = impl::validate_key(resource, key, table_schema, table_schema, true);
                                if (key_res.has_error()) {
                                    return key_res.convert_error<named_schema>();
                                }
                            }
                            // validate corresponding types
                            std::pmr::unordered_set<size_t> unchecked_columns(resource);
                            for (size_t i = 0; i < table_schema.size(); i++) {
                                unchecked_columns.emplace(i);
                            }

                            for (size_t i = 0; i < incoming_schema.value().size(); i++) {
                                // TODO: support partial inserts into complex types
                                // for now only first order is checked
                                size_t index = insert_node->key_translation().empty()
                                                   ? i
                                                   : insert_node->key_translation()[i].path().front();
                                const auto& corresponding_table_type = table_schema[index].type;
                                unchecked_columns.erase(index);
                                if (incoming_schema.value()[i].type.type() != components::types::logical_type::NA &&
                                    incoming_schema.value()[i].type.type() !=
                                        components::types::logical_type::UNKNOWN &&
                                    !incoming_schema.value()[i].type.is_convertable_to(corresponding_table_type)) {
                                    return core::error_t(core::error_code_t::schema_error,
                                                         std::pmr::string{"insert_node: can not convert data column[" +
                                                                              std::to_string(i) +
                                                                              "] type to table type",
                                                                          resource});
                                }
                            }

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
                bool same_schema = false;
                // Update/delete nodes no longer carry relname; pull
                // the target table name from the resolved metadata (populated
                // by Pass 1 via the sibling resolve_table).
                const auto* tbl_upd = impl::tbl_md_for_oid(idx, node->table_oid());
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
                            if (!expr || expr->type() != update_expr_type::set) {
                                continue;
                            }
                            auto* set_expr = reinterpret_cast<update_expr_set_t*>(expr.get());
                            if (set_expr->key().is_null()) {
                                continue;
                            }
                            const auto& storage = set_expr->key().storage();
                            // Top-level field is the column name; nested paths (a.b.c) still
                            // require the head 'a' to be a registered column.
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
                    auto node_data_res = validate_schema(resource, idx, node_data, parameters);
                    if (node_data_res.has_error()) {
                        return node_data_res;
                    }
                    incoming_schema = std::move(node_data_res.value());
                    if (incoming_schema.size() != table_schema.size()) {
                        return core::error_t(
                            core::error_code_t::schema_error,
                            std::pmr::string{"update_node: computed schema and table schema missmatch", resource});
                    }
                    for (size_t i = 0; i < table_schema.size(); i++) {
                        // ignore aliases, since they do not matter here
                        if (incoming_schema[i].type != table_schema[i].type) {
                            return core::error_t(
                                core::error_code_t::schema_error,
                                std::pmr::string{"update_node: computed schema and table schema name missmatch",
                                                 resource});
                        }
                    }
                } else {
                    incoming_schema = table_schema;
                    same_schema = true;
                }
                if (node_match) {
                    auto node_match_res = impl::validate_schema(resource,
                                                                idx,
                                                                node_match,
                                                                parameters,
                                                                table_schema,
                                                                incoming_schema,
                                                                same_schema);
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
                        auto expr_res =
                            impl::validate_schema(resource, expr.get(), table_schema, incoming_schema, same_schema);
                        if (expr_res.has_error()) {
                            return expr_res;
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
                        // The USING/FROM table is a sibling resolve node, not a
                        // child, so it never reaches incoming_schema. Build its
                        // schema from table_oid_from() (the catalog columns) and use
                        // it as the right side: a right-stamped RETURNING key (a
                        // joined column) resolves against it, while target columns
                        // resolve against table_schema as before.
                        const auto from_oid = node->type() == node_type::update_t
                                                  ? reinterpret_cast<node_update_t*>(node)->table_oid_from()
                                                  : reinterpret_cast<node_delete_t*>(node)->table_oid_from();
                        named_schema from_schema(resource);
                        if (from_oid != components::catalog::INVALID_OID) {
                            if (const auto* tbl_from = impl::tbl_md_for_oid(idx, from_oid)) {
                                for (const auto& column : tbl_from->columns) {
                                    from_schema.emplace_back(type_from_t{tbl_from->name,
                                                                         column.type,
                                                                         components::expressions::side_t::right});
                                }
                            }
                        }
                        const bool has_join = !from_schema.empty();
                        auto ret_err = impl::resolve_returning_columns(resource,
                                                                       returning,
                                                                       table_schema,
                                                                       has_join ? from_schema : table_schema,
                                                                       /*same_schema=*/!has_join);
                        if (ret_err.contains_error()) {
                            return ret_err;
                        }
                    }
                }
                return result;
            }
            case node_type::create_index_t: {
                auto* idx_node = static_cast<node_create_index_t*>(node);
                const auto* tbl_idx = impl::tbl_md_for_oid(idx, idx_node->table_oid());
                if (!tbl_idx) {
                    return core::error_t(core::error_code_t::table_not_exists, std::pmr::string{"", resource});
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
                for (auto& key : keys) {
                    auto key_res = impl::validate_key(resource, key, table_schema, table_schema, true);
                    if (key_res.has_error()) {
                        return key_res.convert_error<named_schema>();
                    }
                }
                return named_schema{resource};
            }
            case node_type::drop_index_t:
                // nothing to check here
                break;
            case node_type::create_matview_t:
            case node_type::refresh_matview_t:
                // Schema derivation happens in enrich's stamp_oids_from_resolves;
                // planner reads stamped inferred_columns / source metadata. No
                // per-clause schema validation needed at this layer (the body
                // plan is validated via its own catalog_resolve_table sibling
                // which Pass 1 has stamped before this validate runs).
                break;
            case node_type::union_t: {
                if (node->children().size() < 2 || !node->children()[0] || !node->children()[1]) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"UNION requires both operands to be present", resource});
                }
                auto left_res = validate_schema(resource, idx, node->children()[0].get(), parameters);
                if (left_res.has_error()) {
                    return left_res;
                }
                auto right_res = validate_schema(resource, idx, node->children()[1].get(), parameters);
                if (right_res.has_error()) {
                    return right_res;
                }
                const auto& left_schema = left_res.value();
                const auto& right_schema = right_res.value();
                if (left_schema.size() != right_schema.size()) {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"UNION operands must have the same number of columns", resource});
                }
                for (size_t i = 0; i < left_schema.size(); ++i) {
                    if (left_schema[i].type.type() != right_schema[i].type.type()) {
                        return core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"UNION column type mismatch at position " + std::to_string(i), resource});
                    }
                }
                return left_res;
            }
            case node_type::sequence_t: {
                // The SQL transformer wraps DML/DDL in
                //   sequence_t(catalog_resolve_*…, consumer)
                // The catalog resolve children are leaves that don't carry a
                // schema, so we descend to the last non-catalog_resolve_* child
                // — the real consumer (insert_t/update_t/aggregate_t/...).
                auto is_catalog_resolve = [](node_type t) {
                    return t == node_type::catalog_resolve_namespace_t || t == node_type::catalog_resolve_table_t ||
                           t == node_type::catalog_resolve_type_t || t == node_type::catalog_resolve_function_t ||
                           t == node_type::catalog_resolve_constraint_t;
                };
                for (auto it = node->children().rbegin(); it != node->children().rend(); ++it) {
                    if (!*it)
                        continue;
                    if (!is_catalog_resolve((*it)->type())) {
                        return validate_schema(resource, idx, it->get(), parameters);
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
                auto anchor_res = validate_schema(resource, idx, node->children()[0].get(), parameters);
                if (anchor_res.has_error()) {
                    return anchor_res;
                }

                // Build the CTE column schema from the anchor result and store in a
                // modified idx so that node_cte_scan_t inside the recursive member can
                // look it up without any parameter threading.
                impl::plan_resolve_index_t idx_with_cte = idx ? *idx : impl::plan_resolve_index_t{};
                {
                    catalog_resolve::cte_schema_t cte_cols;
                    for (const auto& entry : anchor_res.value()) {
                        cte_cols.push_back(
                            {std::pmr::string{entry.type.has_alias() ? entry.type.alias() : "", resource}, entry.type});
                    }
                    idx_with_cte.cte_schemas[cte_node->cte_name()] = std::move(cte_cols);
                }
                // Validate recursive member — sets expression paths for SELECT/WHERE/JOIN ON.
                // Errors here indicate a schema mismatch between anchor and recursive member.
                auto recursive_res = validate_schema(resource, &idx_with_cte, node->children()[1].get(), parameters);
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
                if (!idx) {
                    return core::error_t(core::error_code_t::sql_parse_error,
                                         std::pmr::string{"cte_scan_t reached without resolve index", resource});
                }
                const auto* scan_node = static_cast<const components::logical_plan::node_cte_scan_t*>(node);
                auto it = idx->cte_schemas.find(scan_node->cte_name());
                if (it == idx->cte_schemas.end()) {
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

} // namespace services::dispatcher
