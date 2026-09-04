#include "schema.hpp"

#include <components/catalog/system_table_schemas.hpp>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <list>
#include <source_location>
#include <string>
#include <utility>

namespace services::dispatcher::validation {

    using namespace components::types;
    using namespace components::expressions;

    namespace {

        struct type_match_t {
            column_path path;
            const complex_logical_type* type;
            size_t key_order;
        };

        [[nodiscard]] core::error_t ambiguous_key(std::pmr::memory_resource* resource,
                                                  const components::expressions::key_t& key,
                                                  std::source_location location = std::source_location::current()) {
            return core::error_t(
                core::error_code_t::ambiguous_name,
                std::pmr::string{"path: \'" + key.as_string() + "\' is ambiguous. Use aliases or full path", resource},
                location);
        }

    } // namespace

    std::string describe_type(const complex_logical_type& type) {
        switch (type.type()) {
            case logical_type::ENUM:
            case logical_type::STRUCT:
            case logical_type::UNKNOWN:
                if (type.extension() != nullptr && !type.type_name().empty()) {
                    return type.type_name();
                }
                break;
            case logical_type::LIST:
            case logical_type::ARRAY:
                return describe_type(type.child_type()) + "[]";
            default:
                break;
        }
        return std::string{components::catalog::logical_type_to_pg_name(type.type())};
    }

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

    core::result_wrapper_t<type_paths>
    find_types(std::pmr::memory_resource* resource, components::expressions::key_t& key, const named_schema& schema) {
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
        // A qualified reference (m.v) names one table, so resolve it against that
        // table's columns. side cannot do this: it is binary, and a chained JOIN puts
        // three tables on two sides, so the qualifier is the only thing that tells
        // the middle table from the leftmost one.
        if (truncated_key.has_qualifier()) {
            for (size_t i = 0; i < schema.size(); i++) {
                if (core::pmr::operator==(schema[i].result_alias, truncated_key.qualifier()) &&
                    core::pmr::operator==(schema[i].type.alias(), truncated_key.storage().at(0))) {
                    matches.emplace_back(type_match_t{column_path{{i}, resource}, &schema[i].type, 1});
                }
            }
        }
        // Either unqualified, or the qualifier names nothing this schema knows: a
        // sub-plan's schema is labelled with the derived table's alias, so a reference
        // written inside it against the inner relation's own name ('inner_t.k' against
        // 'sub'.k) matches nothing here, and raw node_data inputs carry no alias at all.
        // Both are resolved by the by-name rules, which is what every key used to get.
        // A qualifier the schema does know is the opposite case: the relation is right
        // here and simply has no such column, and answering with a same-named column of
        // another relation is a wrong answer, not a fallback. Decided once, before the
        // loop: testing matches.empty() per iteration would stop at the first hit and
        // hide the ambiguity that collecting every match detects.
        const bool schema_knows_qualifier =
            truncated_key.has_qualifier() &&
            std::any_of(schema.begin(), schema.end(), [&truncated_key](const type_from_t& entry) {
                return core::pmr::operator==(entry.result_alias, truncated_key.qualifier());
            });
        const bool match_by_name = matches.empty() && !schema_knows_qualifier;
        for (size_t i = 0; match_by_name && i < schema.size(); i++) {
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
                    matches.emplace(next_it, type_match_t{std::move(path), &it->type->child_type(), it->key_order + 1});
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
                    matches.emplace(next_it, type_match_t{std::move(path), &it->type->child_type(), it->key_order + 1});
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
                return ambiguous_key(resource, truncated_key);
            }
        }
        if (!result.empty()) {
            if (key.storage().back() == "*") {
                if (result.empty()) {
                    return core::error_t(core::error_code_t::schema_error,
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

    core::result_wrapper_t<type_paths> validate_key(std::pmr::memory_resource* resource,
                                                    components::expressions::key_t& key,
                                                    const named_schema* schema_left,
                                                    const named_schema* schema_right) {
        if (schema_right == nullptr) {
            auto resolved = find_types(resource, key, *schema_left);
            if (!resolved.has_error() && key.side() == side_t::undefined) {
                key.set_side(side_t::left);
            }
            return resolved;
        }
        if (key.side() == side_t::left) {
            return find_types(resource, key, *schema_left);
        }
        if (key.side() == side_t::right) {
            return find_types(resource, key, *schema_right);
        }
        // find_types sets a path, but if both left and right are valid, this will be an error and won't matter
        auto column_path_left = find_types(resource, key, *schema_left);
        auto column_path_right = find_types(resource, key, *schema_right);
        // TODO Stop erasing errors from right and left
        if (column_path_left.has_error() && column_path_right.has_error()) {
            if (column_path_left.error().type == core::error_code_t::ambiguous_name ||
                column_path_right.error().type == core::error_code_t::ambiguous_name) {
                return ambiguous_key(resource, key);
            }
            return core::error_t(core::error_code_t::field_not_exists,
                                 std::pmr::string{"path: \'" + key.as_string() + "\' was not found", resource});
        }
        if (!column_path_left.has_error() && !column_path_right.has_error()) {
            return ambiguous_key(resource, key);
        }
        if (column_path_left.has_error()) {
            key.set_side(side_t::right);
            return column_path_right;
        }
        key.set_side(side_t::left);
        return column_path_left;
    }

} // namespace services::dispatcher::validation
