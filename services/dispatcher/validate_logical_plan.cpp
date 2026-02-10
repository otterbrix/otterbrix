#include "validate_logical_plan.hpp"

#include "expressions/function_expression.hpp"
#include "expressions/update_expression.hpp"
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
#include <components/logical_plan/node_sort.hpp>
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

        schema_result<type_paths> find_types(std::pmr::memory_resource* resource,
                                             components::expressions::key_t& key,
                                             const named_schema& schema) {
            assert(!key.storage().empty());
            type_paths result{resource};
            if (schema.empty()) {
                return schema_result{std::move(result)};
            }
            if (key.storage().at(0) == "*") {
                for (size_t i = 0; i < schema.size(); i++) {
                    result.emplace_back(type_path_t{column_path{{i}, resource}, schema[i].type});
                }
                return schema_result{std::move(result)};
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
                if (truncated_key.storage().size() > 1 &&
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
                    if (it->type->is_nested()) {
                        for (size_t i = 0; i < it->type->child_types().size(); i++) {
                            const auto& child = it->type->child_types()[i];
                            if (core::pmr::operator==(child.alias(), truncated_key.storage()[it->key_order])) {
                                column_path path = it->path;
                                path.emplace_back(i);
                                matches.emplace(next_it, type_match_t{std::move(path), &child, it->key_order + 1});
                            }
                        }
                    }
                } else {
                    // this is an exact match
                    result.emplace_back(type_path_t{std::move(it->path), *it->type});
                }
                matches.erase(it);
            }

            // if result contains multiple types, then we have an ambiguous key, which is an error
            if (result.size() > 1) {
                return schema_result<type_paths>(
                    resource,
                    components::cursor::error_t(error_code_t::ambiguous_name,
                                                "path: \'" + truncated_key.as_string() +
                                                    "\' is ambiguous. Use aliases or full path"));
            } else {
                if (key.storage().back() == "*") {
                    if (!result.front().type.is_nested()) {
                        return schema_result<type_paths>(
                            resource,
                            components::cursor::error_t(error_code_t::schema_error,
                                                        "path: \'" + truncated_key.as_string() +
                                                            "\' is not nested, and \'*\' can not be applied"));
                    }

                    auto parent_type = std::move(result[0]);
                    result.clear();
                    result.reserve(parent_type.type.child_types().size());
                    for (size_t i = 0; i < parent_type.type.child_types().size(); i++) {
                        column_path path = parent_type.path;
                        path.emplace_back(i);
                        result.emplace_back(type_path_t{std::move(path), parent_type.type.child_types()[i]});
                    }
                }
            }

            if (result.empty()) {
                return schema_result<type_paths>(
                    resource,
                    components::cursor::error_t(error_code_t::schema_error,
                                                "path: \'" + key.as_string() + "\' was not found"));
            }
            // Store path inside a key, since we will need it later
            key.set_path(result.front().path);
            return schema_result{std::move(result)};
        }

        schema_result<type_paths> validate_key(std::pmr::memory_resource* resource,
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
                if (column_path_left.is_error() && column_path_right.is_error()) {
                    return schema_result<type_paths>{
                        resource,
                        components::cursor::error_t{error_code_t::field_not_exists,
                                                    "path: \'" + key.as_string() + "\' was not found"}};
                }
                if (!same_schema && !column_path_left.is_error() && !column_path_right.is_error()) {
                    return schema_result<type_paths>{
                        resource,
                        components::cursor::error_t{error_code_t::ambiguous_name,
                                                    "path: \'" + key.as_string() +
                                                        "\' is ambiguous. Use aliases or full path"}};
                }
                if (column_path_left.is_error()) {
                    key.set_side(side_t::right);
                    return column_path_right;
                } else {
                    key.set_side(side_t::left);
                    return column_path_left;
                }
            }
        }

        schema_result<named_schema> validate_schema(std::pmr::memory_resource* resource,
                                                    const catalog& catalog,
                                                    function_expression_t* expr,
                                                    const storage_parameters& parameters,
                                                    const named_schema& schema_left,
                                                    const named_schema& schema_right,
                                                    bool same_schema) {
            named_schema result(resource);
            std::pmr::vector<complex_logical_type> function_input_types(resource);
            function_input_types.reserve(expr->args().size());
            for (auto& field : expr->args()) {
                if (std::holds_alternative<components::expressions::key_t>(field)) {
                    auto& key = std::get<components::expressions::key_t>(field);
                    auto field_res = validate_key(resource, key, schema_left, schema_right, same_schema);
                    if (field_res.is_error()) {
                        return schema_result<named_schema>{resource, field_res.error()};
                    } else {
                        for (const auto& sub_field : field_res.value()) {
                            function_input_types.emplace_back(sub_field.type);
                        }
                    }
                } else if (std::holds_alternative<components::expressions::expression_ptr>(field)) {
                    auto& sub_expr = reinterpret_cast<components::expressions::function_expression_ptr&>(
                        std::get<components::expressions::expression_ptr>(field));
                    auto sub_expr_res = validate_schema(resource,
                                                        catalog,
                                                        sub_expr.get(),
                                                        parameters,
                                                        schema_left,
                                                        schema_right,
                                                        same_schema);
                    if (sub_expr_res.is_error()) {
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
                return schema_result<named_schema>{
                    resource,
                    components::cursor::error_t{error_code_t::unrecognized_function,
                                                "function: \'" + expr->name() + "(...)\' was not found by the name"}};
            } else if (catalog.function_exists(expr->name(), function_input_types)) {
                auto func = catalog.get_function(expr->name(), function_input_types);
                std::vector<complex_logical_type> function_output_types;
                function_output_types.reserve(func.second.output_types.size());
                for (const auto& output_type : func.second.output_types) {
                    auto res = output_type.resolve(function_input_types);
                    if (res.status() != components::compute::compute_status::ok()) {
                        return schema_result<named_schema>{
                            resource,
                            components::cursor::error_t{
                                error_code_t::incorrect_function_argument,
                                "function: \'" + expr->name() +
                                    "(...)\' was found but there is an error, resolving output types"}};
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
                return schema_result<named_schema>{
                    resource,
                    components::cursor::error_t{error_code_t::incorrect_function_argument,
                                                "function: \'" + expr->name() +
                                                    "(...)\' was found but do not except given set of arguments"}};
            }

            return schema_result{std::move(result)};
        }

        // TODO: validate parameters
        // TODO: validate algebra
        schema_result<named_schema> validate_schema(std::pmr::memory_resource* resource,
                                                    update_expr_t* expr,
                                                    const named_schema& schema_left,
                                                    const named_schema& schema_right,
                                                    bool same_schema) {
            switch (expr->type()) {
                case update_expr_type::set: {
                    auto* set_expr = reinterpret_cast<update_expr_set_t*>(expr);
                    auto type_res = find_types(resource, set_expr->key(), schema_left);
                    if (type_res.is_error()) {
                        return schema_result<named_schema>{resource, type_res.error()};
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
                    if (left_res.is_error()) {
                        return schema_result<named_schema>{resource, left_res.error()};
                    }
                    auto right_res =
                        validate_schema(resource, expr->right().get(), schema_left, schema_right, same_schema);
                    if (right_res.is_error()) {
                        return schema_result<named_schema>{resource, right_res.error()};
                    }
                    break;
                }
                case update_expr_type::sqr_root:
                case update_expr_type::cube_root:
                case update_expr_type::factorial:
                case update_expr_type::abs: {
                    auto left_res =
                        validate_schema(resource, expr->left().get(), schema_left, schema_right, same_schema);
                    if (left_res.is_error()) {
                        return schema_result<named_schema>{resource, left_res.error()};
                    }
                    break;
                }
                case update_expr_type::get_value: {
                    auto* get_expr = reinterpret_cast<update_expr_get_value_t*>(expr);
                    auto key_res = validate_key(resource, get_expr->key(), schema_left, schema_right, same_schema);
                    if (key_res.is_error()) {
                        return schema_result<named_schema>{resource, key_res.error()};
                    }
                    break;
                }
                case update_expr_type::get_value_params:
                default:
                    break;
            }
            return schema_result<named_schema>{named_schema(resource)};
        }

        // Since we use some casts in physical plan, we can validate only expr key and side
        // TODO: validate parameters
        schema_result<named_schema> validate_schema(std::pmr::memory_resource* resource,
                                                    const catalog& catalog,
                                                    compare_expression_t* expr,
                                                    const storage_parameters& parameters,
                                                    const named_schema& schema_left,
                                                    const named_schema& schema_right,
                                                    bool same_schema) {
            named_schema result(resource);
            result.emplace_back(type_from_t{"", logical_type::BOOLEAN});

            switch (expr->type()) {
                case compare_type::union_and:
                case compare_type::union_or:
                case compare_type::union_not: {
                    for (auto& nested_expr : expr->children()) {
                        auto nested_res = validate_schema(resource,
                                                          catalog,
                                                          reinterpret_cast<compare_expression_t*>(nested_expr.get()),
                                                          parameters,
                                                          schema_left,
                                                          schema_right,
                                                          same_schema);
                        if (nested_res.is_error()) {
                            return nested_res;
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
                case compare_type::lte: {
                    if (std::holds_alternative<components::expressions::key_t>(expr->left())) {
                        auto key_res = validate_key(resource,
                                                    std::get<components::expressions::key_t>(expr->left()),
                                                    schema_left,
                                                    schema_right,
                                                    same_schema);
                        if (key_res.is_error()) {
                            return schema_result<named_schema>(resource, key_res.error());
                        }
                    } else if (std::holds_alternative<components::expressions::expression_ptr>(expr->left())) {
                        auto& func_expr = reinterpret_cast<components::expressions::function_expression_ptr&>(
                            std::get<components::expressions::expression_ptr>(expr->left()));
                        auto expr_res = validate_schema(resource,
                                                        catalog,
                                                        func_expr.get(),
                                                        parameters,
                                                        schema_left,
                                                        schema_right,
                                                        same_schema);
                        if (expr_res.is_error()) {
                            return schema_result<named_schema>(resource, expr_res.error());
                        }
                    }
                    if (std::holds_alternative<components::expressions::key_t>(expr->right())) {
                        auto key_res = validate_key(resource,
                                                    std::get<components::expressions::key_t>(expr->right()),
                                                    schema_left,
                                                    schema_right,
                                                    same_schema);
                        if (key_res.is_error()) {
                            return schema_result<named_schema>(resource, key_res.error());
                        }
                    } else if (std::holds_alternative<components::expressions::expression_ptr>(expr->right())) {
                        auto& func_expr = reinterpret_cast<components::expressions::function_expression_ptr&>(
                            std::get<components::expressions::expression_ptr>(expr->right()));
                        auto expr_res = validate_schema(resource,
                                                        catalog,
                                                        func_expr.get(),
                                                        parameters,
                                                        schema_left,
                                                        schema_right,
                                                        same_schema);
                        if (expr_res.is_error()) {
                            return schema_result<named_schema>(resource, expr_res.error());
                        }
                    }
                    break;
                }
                case compare_type::regex: {
                    // TODO:
                }
                default:
                    break;
            }
            return schema_result{std::move(result)};
        }

        schema_result<named_schema> validate_schema(std::pmr::memory_resource* resource,
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
                        result.emplace_back(type_from_t{node->collection_name(), column});
                    }
                    return schema_result<named_schema>{std::move(result)};
                } else {
                    return schema_result<named_schema>{
                        resource,
                        components::cursor::error_t{error_code_t::collection_not_exists, ""}};
                }
            } else {
                assert(node->expressions().size() == 1);
                if (node->expressions()[0]->group() == expression_group::compare) {
                    auto* expr = reinterpret_cast<compare_expression_t*>(node->expressions()[0].get());
                    return validate_schema(resource, catalog, expr, parameters, schema_left, schema_right, same_schema);
                } else if (node->expressions()[0]->group() == expression_group::function) {
                    auto* expr = reinterpret_cast<function_expression_t*>(node->expressions()[0].get());
                    auto expr_res =
                        validate_schema(resource, catalog, expr, parameters, schema_left, schema_right, same_schema);
                    if (expr_res.is_error()) {
                        return expr_res;
                    }
                    if (expr_res.value().size() == 1 && expr_res.value().front().type.type() == logical_type::BOOLEAN) {
                        return expr_res;
                    } else {
                        return schema_result<named_schema>{
                            resource,
                            components::cursor::error_t{error_code_t::incorrect_function_return_type,
                                                        "function: \'" + expr->name() +
                                                            "(...)\' was found but can not be used in WHERE clause, "
                                                            "because return type is not a boolean"}};
                    }
                } else {
                    assert(false);
                    return schema_result<named_schema>{
                        resource,
                        components::cursor::error_t{error_code_t::schema_error, "incorrect expr type in node_group"}};
                }
            }
        }

        schema_result<named_schema>
        validate_schema(std::pmr::memory_resource* resource, node_sort_t* node, const named_schema& schema) {
            for (auto& expr : node->expressions()) {
                auto* sort_expr = static_cast<sort_expression_t*>(expr.get());
                auto res = find_types(resource, sort_expr->key(), schema);
                if (res.is_error()) {
                    return schema_result<named_schema>{resource, res.error()};
                }
            }
            return schema_result{named_schema{resource}};
        }

    } // namespace impl

    cursor_t_ptr
    check_namespace_exists(std::pmr::memory_resource* resource, const catalog& catalog, const table_id& id) {
        cursor_t_ptr error;
        if (!catalog.namespace_exists(id.get_namespace())) {
            error = make_cursor(resource, error_code_t::database_not_exists, "database does not exist");
        }
        return error;
    }

    cursor_t_ptr
    check_collection_exists(std::pmr::memory_resource* resource, const catalog& catalog, const table_id& id) {
        cursor_t_ptr error = check_namespace_exists(resource, catalog, id);
        if (!error) {
            bool exists = catalog.table_exists(id);
            bool computes = catalog.table_computes(id);
            // table can either compute or exist with schema - not both
            if (exists == computes) {
                error = make_cursor(resource,
                                    error_code_t::collection_not_exists,
                                    exists ? "collection exists and computes schema at the same time"
                                           : "collection does not exist");
            }
        }

        return error;
    }

    cursor_t_ptr
    check_type_exists(std::pmr::memory_resource* resource, const catalog& catalog, const std::string& alias) {
        cursor_t_ptr error;
        if (!catalog.type_exists(alias)) {
            error = make_cursor(resource,
                                error_code_t::schema_error,
                                "type: \'" + alias + "\' is not registered in catalog");
        }
        return error;
    }

    cursor_t_ptr
    validate_format_and_types(std::pmr::memory_resource* resource, const catalog& catalog, node_t* logical_plan) {
        used_format_t used_format = used_format_t::undefined;
        std::pmr::vector<complex_logical_type> encountered_types{resource};
        cursor_t_ptr result = make_cursor(resource, operation_status_t::success);

        auto check_format = [&](node_t* node) {
            // incoming raw data might not know about type used and only have a field name
            // which is different from the type
            // so we have to collect all known fields that may be used
            used_format_t check = used_format_t::undefined;
            // pull check format from collection referenced by logical_plan
            if (!node->collection_full_name().empty()) {
                table_id id(resource, node->collection_full_name());
                if (auto res = check_collection_exists(resource, catalog, id); !res) {
                    check = catalog.get_table_format(id);
                    if (!catalog.table_computes(id)) {
                        for (const auto& type : catalog.get_table_schema(id).columns()) {
                            encountered_types.emplace_back(type);
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
                if (check == used_format_t::undefined) {
                    check = static_cast<used_format_t>(data_node->uses_data_chunk());
                } else if (check != static_cast<used_format_t>(data_node->uses_data_chunk())) {
                    result =
                        make_cursor(resource,
                                    error_code_t::incompatible_storage_types,
                                    "logical plan data format is not the same as referenced collection data format");
                    return false;
                }

                // convert data_chunk to documents
                if (used_format == used_format_t::documents && check == used_format_t::columns) {
                    data_node->convert_to_documents();
                    check = used_format_t::documents;
                }

                if (data_node->uses_data_chunk()) {
                    for (auto& column : data_node->data_chunk().data) {
                        auto it = std::find_if(encountered_types.begin(),
                                               encountered_types.end(),
                                               [&column](const complex_logical_type& type) {
                                                   return type.alias() == column.type().alias();
                                               });
                        // if this is a registered type, then conversion is required
                        if (it != encountered_types.end() && catalog.type_exists(it->type_name())) {
                            // try to cast to it
                            if (it->type() == logical_type::STRUCT) {
                                components::vector::vector_t new_column(resource,
                                                                        *it,
                                                                        data_node->data_chunk().capacity());
                                for (size_t i = 0; i < data_node->data_chunk().size(); i++) {
                                    auto val = column.value(i).cast_as(*it);
                                    if (val.type().type() == logical_type::NA) {
                                        result =
                                            make_cursor(resource,
                                                        error_code_t::schema_error,
                                                        "couldn't convert parsed ROW to type: \'" + it->alias() + "\'");
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
                                    auto enum_val = logical_value_t::create_enum(*it, val);
                                    if (enum_val.type().type() == logical_type::NA) {
                                        result =
                                            make_cursor(resource,
                                                        error_code_t::schema_error,
                                                        "enum: \'" + it->alias() + "\' does not contain value: \'" +
                                                            std::string(val) + "\'");
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

            // compare check to previously gathered
            if (used_format == check) {
                return true;
            } else if (used_format == used_format_t::undefined) {
                used_format = check;
                return true;
            } else if (check == used_format_t::undefined) {
                return true;
            }
            result = make_cursor(resource,
                                 error_code_t::incompatible_storage_types,
                                 "logical plan data format is not the same as referenced collection data format");
            return false;
        };

        std::queue<node_t*> look_up;
        look_up.emplace(logical_plan);
        while (!look_up.empty()) {
            auto plan_node = look_up.front();

            if (check_format(plan_node)) {
                for (const auto& child : plan_node->children()) {
                    look_up.emplace(child.get());
                }
                look_up.pop();
            } else {
                return result;
            }
        }

        switch (used_format) {
            case used_format_t::documents:
                return make_cursor(resource, std::pmr::vector<components::document::document_ptr>{resource});
            case used_format_t::columns:
                return make_cursor(resource, components::vector::data_chunk_t{resource, {}, 0});
            default:
                return make_cursor(resource, error_code_t::incompatible_storage_types, "undefined storage format");
        }
    }

    schema_result<named_schema> validate_schema(std::pmr::memory_resource* resource,
                                                const catalog& catalog,
                                                node_t* node,
                                                const components::logical_plan::storage_parameters& parameters) {
        named_schema result{resource};

        switch (node->type()) {
            case node_type::aggregate_t: {
                node_group_t* node_group = nullptr;
                node_match_t* node_match = nullptr;
                node_sort_t* node_sort = nullptr;
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
                        default:
                            node_data = child.get();
                            break;
                    }
                }

                if (node_data) {
                    auto node_data_res = validate_schema(resource, catalog, node_data, parameters);
                    if (node_data_res.is_error()) {
                        return node_data_res;
                    } else {
                        incoming_schema = std::move(node_data_res.value());
                    }
                } else {
                    // there will be a scan
                    table_id id(resource, node->collection_full_name());
                    if (catalog.table_exists(id)) {
                        for (const auto& column : catalog.get_table_schema(id).columns()) {
                            table_schema.emplace_back(type_from_t{node->collection_name(), column});
                        }
                    } else {
                        return schema_result<named_schema>{
                            resource,
                            components::cursor::error_t{error_code_t::collection_not_exists, ""}};
                    }
                }
                if (incoming_schema.empty()) {
                    incoming_schema = table_schema;
                    same_schema = true;
                }
                if (table_schema.empty()) {
                    table_schema = incoming_schema;
                    same_schema = true;
                }
                if (table_schema.empty() && incoming_schema.empty()) {
                    return schema_result<named_schema>{
                        resource,
                        components::cursor::error_t{error_code_t::schema_error,
                                                    "invalid aggregate node, that contains no fields"}};
                }
                if (node_match) {
                    auto res = impl::validate_schema(resource,
                                                     catalog,
                                                     node_match,
                                                     parameters,
                                                     table_schema,
                                                     incoming_schema,
                                                     same_schema);
                    if (res.is_error()) {
                        return res;
                    }
                }

                if (!node_group) {
                    result = incoming_schema;
                } else {
                    for (auto& expr : node_group->expressions()) {
                        if (expr->group() == expression_group::scalar) {
                            auto* scalar_expr = reinterpret_cast<scalar_expression_t*>(expr.get());
                            if (scalar_expr->type() == scalar_type::get_field) {
                                auto& key =
                                    scalar_expr->params().empty()
                                        ? scalar_expr->key()
                                        : std::get<components::expressions::key_t>(scalar_expr->params().front());
                                auto field = impl::find_types(resource, key, incoming_schema);
                                if (field.is_error()) {
                                    return schema_result<named_schema>{resource, field.error()};
                                } else {
                                    for (auto& pair : field.value()) {
                                        result.emplace_back(type_from_t{node->result_alias(), std::move(pair.type)});
                                    }
                                }
                            }
                        } else if (expr->group() == expression_group::aggregate) {
                            auto* agg_expr = reinterpret_cast<aggregate_expression_t*>(expr.get());
                            std::pmr::vector<complex_logical_type> function_input_types(resource);
                            function_input_types.reserve(agg_expr->params().size());
                            for (auto& param : agg_expr->params()) {
                                if (std::holds_alternative<components::expressions::key_t>(param)) {
                                    auto& key = std::get<components::expressions::key_t>(param);
                                    auto field = impl::find_types(resource, key, incoming_schema);
                                    if (field.is_error()) {
                                        return schema_result<named_schema>{resource, field.error()};
                                    } else {
                                        for (const auto& sub_field : field.value()) {
                                            function_input_types.emplace_back(sub_field.type);
                                        }
                                    }
                                } else {
                                    auto id = std::get<core::parameter_id_t>(param);
                                    function_input_types.emplace_back(parameters.parameters.at(id).type());
                                }
                            }
                            if (!catalog.function_name_exists(agg_expr->function_name())) {
                                return schema_result<named_schema>{
                                    resource,
                                    components::cursor::error_t{error_code_t::unrecognized_function,
                                                                "function: \'" + agg_expr->function_name() +
                                                                    "(...)\' was not found by the name"}};
                            } else if (catalog.function_exists(agg_expr->function_name(), function_input_types)) {
                                auto func = catalog.get_function(agg_expr->function_name(), function_input_types);
                                std::vector<complex_logical_type> function_output_types;
                                function_output_types.reserve(func.second.output_types.size());
                                for (const auto& output_type : func.second.output_types) {
                                    auto res = output_type.resolve(function_input_types);
                                    if (res.status() != components::compute::compute_status::ok()) {
                                        return schema_result<named_schema>{
                                            resource,
                                            components::cursor::error_t{
                                                error_code_t::incorrect_function_argument,
                                                "function: \'" + agg_expr->function_name() +
                                                    "(...)\' was found but there is an error, resolving output types"}};
                                    }
                                    function_output_types.emplace_back(res.value());
                                }
                                if (function_output_types.size() == 1) {
                                    result.emplace_back(
                                        type_from_t{node->result_alias(), function_output_types.front()});
                                    if (!agg_expr->key().is_null()) {
                                        result.back().type.set_alias(agg_expr->key().as_string());
                                    }
                                } else {
                                    result.emplace_back(
                                        type_from_t{node->result_alias(),
                                                    complex_logical_type::create_struct("",
                                                                                        function_output_types,
                                                                                        agg_expr->key().as_string())});
                                }
                                agg_expr->add_function_uid(func.first);
                            } else {
                                return schema_result<named_schema>{
                                    resource,
                                    components::cursor::error_t{
                                        error_code_t::incorrect_function_argument,
                                        "function: \'" + agg_expr->function_name() +
                                            "(...)\' was found but do not except given set of arguments"}};
                            }
                        } else {
                            // TODO: add check to validate schema, if assert is triggered
                            assert(false);
                        }
                    }
                }
                if (node_sort) {
                    auto res = impl::validate_schema(resource, node_sort, result);
                    if (res.is_error()) {
                        return res;
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
                auto input_schema = validate_schema(resource, catalog, node->children().front().get(), parameters);
                if (input_schema.is_error()) {
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
                    return schema_result<named_schema>{
                        resource,
                        components::cursor::error_t{error_code_t::unrecognized_function,
                                                    "function: \'" + function_node->name() +
                                                        "(...)\' was not found by the name"}};
                } else if (catalog.function_exists(function_node->name(), function_input)) {
                    auto func = catalog.get_function(function_node->name(), function_input);
                    result.reserve(func.second.output_types.size());
                    for (const auto& output_type : func.second.output_types) {
                        auto res = output_type.resolve(function_input);
                        if (res.status() != components::compute::compute_status::ok()) {
                            return schema_result<named_schema>{
                                resource,
                                components::cursor::error_t{
                                    error_code_t::incorrect_function_argument,
                                    "function: \'" + function_node->name() +
                                        "(...)\' was found but there is an error, resolving output types"}};
                        }
                        result.emplace_back(type_from_t{node->result_alias(), res.value()});
                        function_node->add_function_uid(func.first);
                    }
                } else {
                    return schema_result<named_schema>{
                        resource,
                        components::cursor::error_t{error_code_t::incorrect_function_argument,
                                                    "function: \'" + function_node->name() +
                                                        "(...)\' was found but do not except given set of arguments"}};
                }
                break;
            }
            case node_type::join_t: {
                auto left_schema = validate_schema(resource, catalog, node->children().front().get(), parameters);
                if (left_schema.is_error()) {
                    return left_schema;
                }
                auto right_schema = validate_schema(resource, catalog, node->children().back().get(), parameters);
                if (right_schema.is_error()) {
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
                if (expr_res.is_error()) {
                    return expr_res;
                }

                // TODO: merge using join type, because some join types allow duplicate names in result, while others do not
                result = impl::merge_schemas(resource, std::move(left_schema.value()), std::move(right_schema.value()));
                break;
            }
            // For now next 3 nodes do not support returning clause:
            case node_type::insert_t: {
                auto incoming_schema = validate_schema(resource, catalog, node->children().front().get(), parameters);
                if (incoming_schema.is_error()) {
                    return incoming_schema;
                } else {
                    table_id id(resource, node->collection_full_name());
                    if (catalog.table_exists(id)) {
                        const auto& table_schema = catalog.get_table_schema(id).columns();
                        if (table_schema.size() == incoming_schema.value().size()) {
                            for (size_t i = 0; i < incoming_schema.value().size(); i++) {
                                if (table_schema[i].alias() != incoming_schema.value()[i].type.alias()) {
                                    return schema_result<named_schema>{
                                        resource,
                                        components::cursor::error_t{error_code_t::schema_error,
                                                                    "insert_node: field name missmatch"}};
                                }
                            }
                        } else {
                            return schema_result<named_schema>{
                                resource,
                                components::cursor::error_t{
                                    error_code_t::schema_error,
                                    "insert_node: number of data columns does not match the table one"}};
                        }
                    } else {
                        return schema_result<named_schema>{
                            resource,
                            components::cursor::error_t{error_code_t::collection_not_exists, ""}};
                    }
                }
                return schema_result{std::move(result)};
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
                                        column});
                    }
                } else {
                    return schema_result<named_schema>{
                        resource,
                        components::cursor::error_t{error_code_t::collection_not_exists, ""}};
                }
                if (node_data) {
                    auto node_data_res = validate_schema(resource, catalog, node_data, parameters);
                    if (node_data_res.is_error()) {
                        return schema_result<named_schema>{resource, node_data_res.error()};
                    }
                    incoming_schema = std::move(node_data_res.value());
                    if (incoming_schema.size() != table_schema.size()) {
                        return schema_result<named_schema>{
                            resource,
                            components::cursor::error_t{error_code_t::schema_error,
                                                        "update_node: computed schema and table schema missmatch"}};
                    }
                    for (size_t i = 0; i < table_schema.size(); i++) {
                        // ignore aliases, since they do not matter here
                        if (incoming_schema[i].type != table_schema[i].type) {
                            return schema_result<named_schema>{
                                resource,
                                components::cursor::error_t{
                                    error_code_t::schema_error,
                                    "update_node: computed schema and table schema name missmatch"}};
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
                    if (node_match_res.is_error()) {
                        return schema_result<named_schema>{resource, node_match_res.error()};
                    }
                } else {
                    return schema_result<named_schema>{
                        resource,
                        components::cursor::error_t{error_code_t::schema_error,
                                                    "update_node: invalid node, node_match is not present"}};
                }
                if (node->type() == node_type::update_t) {
                    auto* node_update = reinterpret_cast<node_update_t*>(node);
                    for (auto& expr : node_update->updates()) {
                        auto expr_res =
                            impl::validate_schema(resource, expr.get(), table_schema, incoming_schema, same_schema);
                        if (expr_res.is_error()) {
                            return expr_res;
                        }
                    }
                }
                // TODO: check updates for update_t
                return schema_result{std::move(result)};
            }
            default:
                // TODO: add check to validate schema, if assert is triggered
                assert(false);
        }

        return schema_result{std::move(result)};
    }

} // namespace services::dispatcher