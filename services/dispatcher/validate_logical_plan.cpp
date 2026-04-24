#include "validate_logical_plan.hpp"

#include <core/date/date_parse.hpp>

#include "expressions/function_expression.hpp"
#include "expressions/update_expression.hpp"
#include "logical_plan/node_create_index.hpp"
#include "logical_plan/node_insert.hpp"
#include "logical_plan/node_update.hpp"

#include <components/catalog/table_id.hpp>
#include <components/expressions/aggregate_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/expressions/sort_expression.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_data.hpp>
#include <components/logical_plan/node_function.hpp>
#include <components/logical_plan/node_group.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_match.hpp>
#include <components/logical_plan/node_select.hpp>
#include <components/logical_plan/node_sort.hpp>
#include <list>
#include <queue>

namespace services::dispatcher {

    using namespace components::types;
    using namespace components::expressions;
    using namespace components::logical_plan;
    using namespace components::cursor;
    using namespace components::catalog;

    namespace impl {
        struct type_match_t {
            column_path path;
            const complex_logical_type* type;
            size_t key_order;
        };

        // If type does exist in both, merged will have only first instance
        named_schema merge_schemas(std::pmr::memory_resource* resource, named_schema lhs, named_schema rhs) {
            // We do not check table alias here, because we only care about fields
            named_schema merged(resource);
            for (auto&& type : lhs) {
                auto it = std::find_if(merged.begin(), merged.end(), [&type](const auto& t) {
                    return t.type.alias() == type.type.alias();
                });
                if (it == merged.end()) {
                    merged.emplace_back(std::move(type));
                }
            }
            for (auto&& type : rhs) {
                auto it = std::find_if(merged.begin(), merged.end(), [&type](const auto& t) {
                    return t.type.alias() == type.type.alias();
                });
                if (it == merged.end()) {
                    merged.emplace_back(std::move(type));
                }
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

            // if result contains multiple types, then we have an ambiguous key, which is an error
            if (result.size() > 1) {
                return core::error_t(core::error_code_t::ambiguous_name,

                                     std::pmr::string{"path: \'" + truncated_key.as_string() +
                                                          "\' is ambiguous. Use aliases or full path",
                                                      resource});
            } else {
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

        [[nodiscard]] core::result_wrapper_t<named_schema>
        validate_schema(std::pmr::memory_resource* resource,
                        const catalog& catalog,
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
                                                        catalog,
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
            if (!catalog.function_name_exists(expr->name())) {
                return core::error_t(
                    core::error_code_t::unrecognized_function,

                    std::pmr::string{"function: \'" + expr->name() + "(...)\' was not found by the name", resource});
            } else if (catalog.function_exists(expr->name(), function_input_types)) {
                auto func = catalog.get_function(expr->name(), function_input_types);
                if (!components::compute::check_mask(allowed_function_types, func.second.function_type)) {
                    return core::error_t(core::error_code_t::unrecognized_function,

                                         std::pmr::string{"function: \'" + expr->name() +
                                                              "(...)\' was found can not be used in current context",
                                                          resource});
                }
                std::vector<complex_logical_type> function_output_types;
                function_output_types.reserve(func.second.output_types.size());
                for (const auto& output_type : func.second.output_types) {
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
                expr->add_function_uid(func.first);
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
                                                                           const catalog& catalog,
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
                                                    catalog,
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
                                                             catalog,
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
                            return key_res.convert_error<named_schema>();
                        }
                    } else if (std::holds_alternative<components::expressions::expression_ptr>(expr->left())) {
                        auto& sub_expr = std::get<components::expressions::expression_ptr>(expr->left());
                        if (sub_expr->group() == expression_group::function) {
                            auto& func_expr =
                                reinterpret_cast<components::expressions::function_expression_ptr&>(sub_expr);
                            auto expr_res = validate_schema(resource,
                                                            catalog,
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
                                                            catalog,
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
                                                                           const catalog& catalog,
                                                                           node_match_t* node,
                                                                           const storage_parameters& parameters,
                                                                           const named_schema& schema_left,
                                                                           const named_schema& schema_right,
                                                                           bool same_schema) {
            if (node->expressions().empty()) {
                // physical plan reinterprets this as default scan
                table_id id(resource, node->collection_full_name());
                if (catalog.table_exists(id)) {
                    named_schema result(resource);
                    for (const auto& column : catalog.get_table_schema(id).columns()) {
                        result.emplace_back(type_from_t{node->collection_name(), column.type()});
                    }
                    return result;
                }
                if (catalog.table_computes(id)) {
                    named_schema result(resource);
                    auto sch = catalog.get_computing_table_schema(id).latest_types_struct();
                    for (const auto& column : sch.child_types()) {
                        result.emplace_back(
                            type_from_t{node->result_alias().empty() ? node->collection_name() : node->result_alias(),
                                        column});
                    }
                    return result;
                } else {
                    return core::error_t(core::error_code_t::table_not_exists,

                                         std::pmr::string{"", resource});
                }
            } else {
                assert(node->expressions().size() == 1);
                if (node->expressions()[0]->group() == expression_group::compare) {
                    auto* expr = reinterpret_cast<compare_expression_t*>(node->expressions()[0].get());
                    return validate_schema(resource, catalog, expr, parameters, schema_left, schema_right, same_schema);
                } else if (node->expressions()[0]->group() == expression_group::function) {
                    auto* expr = reinterpret_cast<function_expression_t*>(node->expressions()[0].get());
                    auto allowed_function_types =
                        components::compute::create_mask(components::compute::function_type_t::row,
                                                         components::compute::function_type_t::vector);
                    auto expr_res = validate_schema(resource,
                                                    catalog,
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

    } // namespace impl

    core::error_t
    check_namespace_exists(std::pmr::memory_resource* resource, const catalog& catalog, const table_id& id) {
        if (!catalog.namespace_exists(id.get_namespace())) {
            return core::error_t(core::error_code_t::database_not_exists,

                                 std::pmr::string{"database does not exist", resource});
        }
        return core::error_t::no_error();
    }

    core::error_t
    check_collection_exists(std::pmr::memory_resource* resource, const catalog& catalog, const table_id& id) {
        if (auto res = check_namespace_exists(resource, catalog, id); !res.contains_error()) {
            bool exists = catalog.table_exists(id);
            bool computes = catalog.table_computes(id);
            // table can either compute or exist with schema - not both
            if (exists == computes) {
                return core::error_t(core::error_code_t::table_not_exists,

                                     std::pmr::string{exists ? "collection exists and computes schema at the same time"
                                                             : "collection does not exist",
                                                      resource});
            }
            return core::error_t::no_error();
        } else {
            return res;
        }
    }

    core::error_t
    check_type_exists(std::pmr::memory_resource* resource, const catalog& catalog, const std::string& alias) {
        if (!catalog.type_exists(alias)) {
            return core::error_t(core::error_code_t::type_not_exists,

                                 std::pmr::string{"type: \'" + alias + "\' is not registered in catalog", resource});
        }
        return core::error_t::no_error();
    }

    core::error_t validate_types(std::pmr::memory_resource* resource, const catalog& catalog, node_t* logical_plan) {
        std::pmr::vector<complex_logical_type> encountered_types{resource};
        core::error_t result = core::error_t::no_error();

        auto check_node = [&](node_t* node) {
            if (!node->collection_full_name().empty()) {
                table_id id(resource, node->collection_full_name());
                if (auto res = check_collection_exists(resource, catalog, id); !res.contains_error()) {
                    if (!catalog.table_computes(id)) {
                        for (const auto& column : catalog.get_table_schema(id).columns()) {
                            encountered_types.emplace_back(column.type());
                        }
                    }
                } else {
                    result = res;
                    return false;
                }
            }
            // pull/double-check check format from collection referenced by logical_plan and data stored inside node_data_t
            if (node->type() == node_type::data_t) {
                auto* data_node = reinterpret_cast<node_data_t*>(node);

                for (auto& column : data_node->data_chunk().data) {
                    auto it = std::find_if(
                        encountered_types.begin(),
                        encountered_types.end(),
                        [&column](const complex_logical_type& type) { return type.alias() == column.type().alias(); });
                    if (it != encountered_types.end()) {
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
                        } else if (catalog.type_exists(it->type_name())) {
                            // if this is a registered type, then conversion is required
                            if (it->type() == logical_type::STRUCT) {
                                components::vector::vector_t new_column(resource,
                                                                        *it,
                                                                        data_node->data_chunk().capacity());
                                for (size_t i = 0; i < data_node->data_chunk().size(); i++) {
                                    auto val = column.value(i).cast_as(*it);
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
                    const catalog& catalog,
                    node_t* node,
                    const components::logical_plan::storage_parameters& parameters) {
        named_schema result{resource};

        switch (node->type()) {
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
                    auto node_data_res = validate_schema(resource, catalog, node_data, parameters);
                    if (node_data_res.has_error()) {
                        return node_data_res;
                    } else {
                        incoming_schema = std::move(node_data_res.value());
                    }
                } else if (!node->collection_full_name().database.empty()) {
                    // there will be a scan
                    table_id id(resource, node->collection_full_name());
                    if (catalog.table_exists(id)) {
                        for (const auto& column : catalog.get_table_schema(id).columns()) {
                            table_schema.emplace_back(type_from_t{node->collection_name(), column.type()});
                        }
                    } else if (catalog.table_computes(id)) {
                        auto sch = catalog.get_computing_table_schema(id).latest_types_struct();
                        for (const auto& column : sch.child_types()) {
                            table_schema.emplace_back(type_from_t{node->result_alias().empty() ? node->collection_name()
                                                                                               : node->result_alias(),
                                                                  column});
                        }
                    } else {
                        return core::error_t(core::error_code_t::table_not_exists,

                                             std::pmr::string{"", resource});
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
                    auto res = impl::validate_schema(resource,
                                                     catalog,
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

                        // Resolve key paths in node_select scalar expressions against incoming schema.
                        // Aggregates are always in node_group_t now, so only scalar expressions appear here.
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
                            } else if (scalar_expr->type() != scalar_type::constant &&
                                       scalar_expr->type() != scalar_type::star_expand) {
                                auto res =
                                    impl::resolve_key_paths_in_group(resource, scalar_expr->params(), incoming_schema);
                                if (res.has_error()) {
                                    return res.convert_error<named_schema>();
                                }
                            }
                        }
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
                                        return complex_logical_type(promote_type(lt.type(), rt.type()));
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
                            result_type = complex_logical_type(promote_type(lt.type(), rt.type()));
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
                                        std::function<complex_logical_type(param_storage&)> resolve_arith_type;
                                        resolve_arith_type = [&](param_storage& p) -> complex_logical_type {
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
                                                return parameters.parameters.at(std::get<core::parameter_id_t>(p))
                                                    .type();
                                            } else {
                                                auto& inner = std::get<expression_ptr>(p);
                                                if (inner->group() == expression_group::scalar) {
                                                    auto* s = reinterpret_cast<scalar_expression_t*>(inner.get());
                                                    if (s->params().size() >= 2) {
                                                        auto lt = resolve_arith_type(s->params()[0]);
                                                        auto rt = resolve_arith_type(s->params()[1]);
                                                        return complex_logical_type(promote_type(lt.type(), rt.type()));
                                                    }
                                                }
                                                assert(false);
                                                return complex_logical_type(logical_type::INVALID);
                                            }
                                        };
                                        if (sub_scalar->params().size() >= 2) {
                                            auto lt = resolve_arith_type(sub_scalar->params()[0]);
                                            auto rt = resolve_arith_type(sub_scalar->params()[1]);
                                            if (resolve_error.contains_error()) {
                                                return resolve_error;
                                            }
                                            function_input_types.emplace_back(promote_type(lt.type(), rt.type()));
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
                            if (!catalog.function_name_exists(agg_expr->function_name())) {
                                return core::error_t(core::error_code_t::unrecognized_function,

                                                     std::pmr::string{"function: \'" + agg_expr->function_name() +
                                                                          "(...)\' was not found by the name",
                                                                      resource});
                            } else if (catalog.function_exists(agg_expr->function_name(), function_input_types)) {
                                auto func = catalog.get_function(agg_expr->function_name(), function_input_types);
                                std::vector<complex_logical_type> function_output_types;
                                function_output_types.reserve(func.second.output_types.size());
                                for (const auto& output_type : func.second.output_types) {
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
                                agg_expr->add_function_uid(func.first);
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

                        for (size_t idx : post_agg_indices) {
                            auto& expr = node_group->expressions()[idx];
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
                        auto res = impl::validate_schema(resource, catalog, cmp_expr, parameters, result, result, true);
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
                auto input_schema = validate_schema(resource, catalog, node->children().front().get(), parameters);
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
                if (!catalog.function_name_exists(function_node->name())) {
                    return core::error_t(
                        core::error_code_t::unrecognized_function,

                        std::pmr::string{"function: \'" + function_node->name() + "(...)\' was not found by the name",
                                         resource});
                } else if (catalog.function_exists(function_node->name(), function_input)) {
                    auto func = catalog.get_function(function_node->name(), function_input);
                    result.reserve(func.second.output_types.size());
                    for (const auto& output_type : func.second.output_types) {
                        auto res = output_type.resolve(resource, function_input);
                        if (res.has_error()) {
                            return res.convert_error<named_schema>();
                        }
                        result.emplace_back(type_from_t{node->result_alias(), res.value()});
                        function_node->add_function_uid(func.first);
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
                auto left_schema = validate_schema(resource, catalog, node->children().front().get(), parameters);
                if (left_schema.has_error()) {
                    return left_schema;
                }
                auto right_schema = validate_schema(resource, catalog, node->children().back().get(), parameters);
                if (right_schema.has_error()) {
                    return right_schema;
                }
                auto expr_res =
                    impl::validate_schema(resource,
                                          catalog,
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
                table_id id(resource, node->collection_full_name());
                if (auto err = check_collection_exists(resource, catalog, id); err.contains_error()) {
                    return err;
                }

                auto incoming_schema = validate_schema(resource, catalog, node->children().front().get(), parameters);
                if (incoming_schema.has_error()) {
                    return incoming_schema;
                } else {
                    named_schema table_schema(resource);
                    bool is_computed = false;
                    if (catalog.table_exists(id)) {
                        for (const auto& column : catalog.get_table_schema(id).columns()) {
                            table_schema.emplace_back(type_from_t{node->result_alias().empty() ? node->collection_name()
                                                                                               : node->result_alias(),
                                                                  column.type()});
                        }
                    } else {
                        is_computed = true;
                        auto sch = catalog.get_computing_table_schema(id).latest_types_struct();
                        for (const auto& column : sch.child_types()) {
                            table_schema.emplace_back(type_from_t{node->collection_name(), column});
                        }
                    }
                    if (table_schema.empty() && is_computed) {
                        // Computing table — accept any INSERT
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
                                if (!incoming_schema.value()[i].type.is_convertable_to(corresponding_table_type)) {
                                    return core::error_t(core::error_code_t::schema_error,

                                                         std::pmr::string{"insert_node: can not convert data column[" +
                                                                              std::to_string(i) +
                                                                              "] type to table type",
                                                                          resource});
                                }
                            }

                            if (!unchecked_columns.empty()) {
                                const auto& catalog_schema = catalog.get_table_schema(id).columns();
                                for (auto index : unchecked_columns) {
                                    if (!catalog_schema[index].has_default_value() &&
                                        catalog_schema[index].is_not_null()) {
                                        return core::error_t(
                                            core::error_code_t::schema_error,

                                            std::pmr::string{
                                                "insert_node: can not fill column \'" + catalog_schema[index].name() +
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
                table_id id(resource, node->collection_full_name());
                if (catalog.table_exists(id)) {
                    for (const auto& column : catalog.get_table_schema(id).columns()) {
                        table_schema.emplace_back(
                            type_from_t{node->result_alias().empty() ? node->collection_name() : node->result_alias(),
                                        column.type()});
                    }
                } else if (catalog.table_computes(id)) {
                    auto sch = catalog.get_computing_table_schema(id).latest_types_struct();
                    for (const auto& column : sch.child_types()) {
                        table_schema.emplace_back(
                            type_from_t{node->result_alias().empty() ? node->collection_name() : node->result_alias(),
                                        column});
                    }
                } else {
                    return core::error_t(
                        core::error_code_t::table_not_exists,

                        std::pmr::string{"could not find table in update/delete validation", resource});
                }
                if (node_data) {
                    auto node_data_res = validate_schema(resource, catalog, node_data, parameters);
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
                                                                catalog,
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
                return result;
            }
            case node_type::create_index_t: {
                table_id id(resource, node->collection_full_name());
                if (auto err = check_collection_exists(resource, catalog, id); err.contains_error()) {
                    return err;
                }

                named_schema table_schema{resource};
                if (catalog.table_computes(id)) {
                    auto sch = catalog.get_computing_table_schema(id).latest_types_struct();
                    for (const auto& column : sch.child_types()) {
                        table_schema.emplace_back(type_from_t{node->collection_name(), column});
                    }
                } else {
                    const auto& sch = catalog.get_table_schema(id).columns();
                    for (const auto& column : sch) {
                        table_schema.emplace_back(type_from_t{node->collection_name(), column.type()});
                    }
                }
                auto& keys = reinterpret_cast<node_create_index_t*>(node)->keys();
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
            default:
                // TODO: add check to validate schema, if assert is triggered
                assert(false);
                return core::error_t(core::error_code_t::unimplemented_yet,

                                     std::pmr::string{"encountered an unknown state during plan validation", resource});
        }

        return result;
    }

} // namespace services::dispatcher