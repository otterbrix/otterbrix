#include "utils.hpp"

#include <components/logical_plan/identifier_types.hpp>
#include <components/logical_plan/node_catalog_resolve_constraint.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
#include <components/logical_plan/node_catalog_resolve_table.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/types/logical_value.hpp>
#include <core/date/date_parse.hpp>

#include <atomic>
#include <cstdlib>

namespace components::sql::transform {
    bool string_to_double(const char* buf, size_t len, double& result /*, char decimal_separator = '.'*/) {
        // Skip leading spaces
        while (len > 0 && std::isspace(*buf)) {
            buf++;
            len--;
        }
        if (len == 0) {
            return false;
        }
        if (*buf == '+') {
            buf++;
            len--;
        }

        std::string str(buf, len);
        const char* start = str.c_str();
        char* endptr = nullptr;

        result = std::strtod(start, &endptr);

        if (start == endptr) {
            return false;
        }
        while (*endptr != '\0' && std::isspace(*endptr)) {
            endptr++;
        }

        return *endptr == '\0';
    }

    std::pmr::string indices_to_str(std::pmr::memory_resource* resource, A_Indices* indices) {
        return core::pmr::to_pmr_string(resource, pg_ptr_cast<A_Const>(indices->uidx)->val.val.ival);
    }

    bool name_collection_t::is_left_table(const std::string& name) const {
        if (name == left_name.relname || name == left_alias) {
            return true;
        }
        for (const auto& alias : extra_left_aliases) {
            if (alias == name) {
                return true;
            }
        }
        for (const auto& nm : extra_left_names) {
            if (nm.relname == name) {
                return true;
            }
        }
        return false;
    }

    bool name_collection_t::is_right_table(const std::string& name) const {
        return name == right_name.relname || name == right_alias;
    }

    expressions::side_t deduce_side(const name_collection_t& names, const std::string& target_name) {
        if (target_name.empty()) {
            return expressions::side_t::undefined;
        }
        if (names.is_left_table(target_name)) {
            return expressions::side_t::left;
        } else if (names.right_name.relname == target_name || names.right_alias == target_name) {
            return expressions::side_t::right;
        } else {
            return expressions::side_t::undefined;
        }
    }

    void column_ref_t::deduce_side(const name_collection_t& names) {
        field.set_side(transform::deduce_side(names, table));
    }

    column_ref_t
    columnref_to_field(std::pmr::memory_resource* resource, ColumnRef* ref, const name_collection_t& names) {
        auto lst = ref->fields->lst;
        if (lst.empty()) {
            return column_ref_t(resource);
        } else if (lst.size() == 1) {
            return column_ref_t{{}, expressions::key_t(resource, strVal(lst.back().data))};
        } else {
            auto it = lst.begin();
            std::string table_name;
            std::pmr::vector<std::pmr::string> field_path(resource);
            expressions::side_t side = expressions::side_t::undefined;

            if (names.is_left_table(strVal(lst.begin()->data))) {
                table_name = strVal(it->data);
                ++it;
                side = expressions::side_t::left;
            } else if (names.is_right_table(strVal(lst.begin()->data))) {
                table_name = strVal(it->data);
                ++it;
                side = expressions::side_t::right;
            } else if (lst.size() >= 3) {
                // db.table.x style: first elem is database; check if second elem matches a known table.
                auto second_it = std::next(it);
                const char* second_name = strVal(second_it->data);
                if (names.is_left_table(second_name)) {
                    ++it;
                    table_name = strVal(it->data);
                    ++it;
                    side = expressions::side_t::left;
                } else if (names.is_right_table(second_name)) {
                    ++it;
                    table_name = strVal(it->data);
                    ++it;
                    side = expressions::side_t::right;
                }
            }
            for (; it != lst.end(); ++it) {
                if (nodeTag(it->data) == T_A_Star) {
                    field_path.emplace_back(std::pmr::string{"*", resource});
                } else {
                    field_path.emplace_back(pmrStrVal(it->data, resource));
                }
            }
            return {std::move(table_name), expressions::key_t{std::move(field_path), side}};
        }
    }

    column_ref_t indirection_to_field(std::pmr::memory_resource* resource,
                                      A_Indirection* indirection,
                                      const name_collection_t& names) {
        column_ref_t ref(resource);
        if (nodeTag(indirection->arg) == T_ColumnRef) {
            ref = columnref_to_field(resource, pg_ptr_cast<ColumnRef>(indirection->arg), names);
        } else {
            ref = indirection_to_field(resource, pg_ptr_cast<A_Indirection>(indirection->arg), names);
        }
        auto key = indirection->indirection->lst.back().data;
        if (nodeTag(key) == T_A_Indices) {
            ref.field.storage().emplace_back(indices_to_str(resource, pg_ptr_cast<A_Indices>(key)));
        } else {
            ref.field.storage().emplace_back(pmrStrVal(key, resource));
        }
        return ref;
    }

    bool is_jsonb_nav_operator(std::string_view op) { return op == "->" || op == "->>" || op == "#>" || op == "#>>"; }

    bool jsonb_nav_returns_scalar(std::string_view op) { return op == "->>" || op == "#>>"; }

    bool jsonb_op_takes_path(std::string_view op) { return op == "#>" || op == "#>>" || op == "#-"; }

    std::string node_tag_to_string(NodeTag type) {
        switch (type) {
            case T_A_Expr:
                return "T_A_Expr";
            case T_ColumnRef:
                return "T_ColumnRef";
            case T_ParamRef:
                return "T_ParamRef";
            case T_A_Const:
                return "T_A_Const";
            case T_FuncCall:
                return "T_FuncCall";
            case T_A_Star:
                return "T_A_Star";
            case T_A_Indices:
                return "T_A_Indices";
            case T_A_Indirection:
                return "T_A_Indirection";
            case T_A_ArrayExpr:
                return "T_A_ArrayExpr";
            case T_ResTarget:
                return "T_ResTarget";
            case T_TypeCast:
                return "T_TypeCast";
            case T_CollateClause:
                return "T_CollateClause";
            case T_SortBy:
                return "T_SortBy";
            case T_WindowDef:
                return "T_WindowDef";
            case T_RangeSubselect:
                return "T_RangeSubselect";
            case T_RangeFunction:
                return "T_RangeFunction";
            case T_TypeName:
                return "T_TypeName";
            case T_ColumnDef:
                return "T_ColumnDef";
            case T_IndexElem:
                return "T_IndexElem";
            case T_Constraint:
                return "T_Constraint";
            case T_DefElem:
                return "T_DefElem";
            case T_RangeTblEntry:
                return "T_RangeTblEntry";
            case T_RangeTblFunction:
                return "T_RangeTblFunction";
            case T_WithCheckOption:
                return "T_WithCheckOption";
            case T_GroupingClause:
                return "T_GroupingClause";
            case T_GroupingFunc:
                return "T_GroupingFunc";
            case T_SortGroupClause:
                return "T_SortGroupClause";
            case T_WindowClause:
                return "T_WindowClause";
            case T_PrivGrantee:
                return "T_PrivGrantee";
            case T_FuncWithArgs:
                return "T_FuncWithArgs";
            case T_AccessPriv:
                return "T_AccessPriv";
            case T_CreateOpClassItem:
                return "T_CreateOpClassItem";
            case T_TableLikeClause:
                return "T_TableLikeClause";
            case T_FunctionParameter:
                return "T_FunctionParameter";
            case T_LockingClause:
                return "T_LockingClause";
            case T_RowMarkClause:
                return "T_RowMarkClause";
            case T_XmlSerialize:
                return "T_XmlSerialize";
            case T_WithClause:
                return "T_WithClause";
            case T_CommonTableExpr:
                return "T_CommonTableExpr";
            case T_ColumnReferenceStorageDirective:
                return "T_ColumnReferenceStorageDirective";
            default:
                return "unknown";
        }
    }

    std::string expr_kind_to_string(A_Expr_Kind type) {
        switch (type) {
            case AEXPR_OP:
                return "AEXPR_OP";
            case AEXPR_AND:
                return "AEXPR_AND";
            case AEXPR_OR:
                return "AEXPR_OR";
            case AEXPR_NOT:
                return "AEXPR_NOT";
            case AEXPR_OP_ANY:
                return "AEXPR_OP_ANY";
            case AEXPR_OP_ALL:
                return "AEXPR_OP_ALL";
            case AEXPR_DISTINCT:
                return "AEXPR_DISTINCT";
            case AEXPR_NULLIF:
                return "AEXPR_NULLIF";
            case AEXPR_OF:
                return "AEXPR_OF";
            case AEXPR_IN:
                return "AEXPR_IN";
            default:
                return "unknown";
        }
    }

    core::result_wrapper_t<types::complex_logical_type> get_type(std::pmr::memory_resource* resource, TypeName* type) {
        types::complex_logical_type column;
        if (!type || !type->names) {
            return column;
        }
        if (auto linint_name = strVal(linitial(type->names)); !std::strcmp(linint_name, "pg_catalog")) {
            if (auto col = get_logical_type(strVal(lsecond(type->names))); col != types::logical_type::DECIMAL) {
                column = col;
            } else {
                if (list_length(type->typmods) != 2) {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"Incorrect modifiers for DECIMAL, width and scale required", resource});
                } else if (nodeTag(linitial(type->typmods)) != T_A_Const ||
                           nodeTag(lsecond(type->typmods)) != T_A_Const) {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"Incorrect width or scale for DECIMAL, must be integer", resource});
                }

                auto width = pg_ptr_cast<A_Const>(linitial(type->typmods));
                auto scale = pg_ptr_cast<A_Const>(lsecond(type->typmods));

                if (width->val.type != scale->val.type || width->val.type != T_Integer) {
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"Incorrect width or scale for DECIMAL, must be integer", resource});
                }
                column = types::complex_logical_type::create_decimal(static_cast<uint8_t>(intVal(&width->val)),
                                                                     static_cast<uint8_t>(intVal(&scale->val)));
            }
        } else {
            types::logical_type t = get_logical_type(linint_name);
            if (t == types::logical_type::UNKNOWN) {
                column = types::complex_logical_type::create_unknown(linint_name);
            } else {
                column = t;
            }
        }

        if (list_length(type->arrayBounds)) {
            auto size = pg_ptr_assert_cast<Value>(linitial(type->arrayBounds), T_Integer);
            column = types::complex_logical_type::create_array(column, intVal(size));
        }

        return std::move(column);
    }

    template<typename Container>
    void fill_with_types(Container& container, PGList& list) {}

    core::result_wrapper_t<std::pmr::vector<types::complex_logical_type>> get_types(std::pmr::memory_resource* resource,
                                                                                    PGList& list) {
        std::pmr::vector<types::complex_logical_type> types(resource);
        types.reserve(list.lst.size());
        for (auto data : list.lst) {
            if (nodeTag(data.data) != T_ColumnDef) {
                continue;
            }
            auto coldef = pg_ptr_cast<ColumnDef>(data.data);
            if (auto type_res = get_type(resource, coldef->typeName); type_res.has_error()) {
                return type_res.convert_error<std::pmr::vector<types::complex_logical_type>>();
            } else {
                type_res.value().set_alias(coldef->colname);
                types.emplace_back(std::move(type_res.value()));
            }
        }
        return types;
    }

    core::result_wrapper_t<types::logical_value_t> get_value(std::pmr::memory_resource* resource, Node* node) {
        switch (nodeTag(node)) {
            case T_TypeCast: {
                auto cast = pg_ptr_cast<TypeCast>(node);
                auto constant = pg_ptr_cast<A_Const>(cast->arg);
                if (constant->val.type != T_String) {
                    return types::logical_value_t(resource, constant->val.val.ival);
                }
                std::string_view str = strVal(&constant->val);
                auto type_res = get_type(resource, cast->typeName);
                if (!type_res.has_error() && types::is_duration(type_res.value().type())) {
                    switch (type_res.value().type()) {
                        case types::logical_type::DATE:
                            if (auto parsed = core::date::parse_date(str)) {
                                return types::logical_value_t(resource, *parsed);
                            }
                            return core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"invalid DATE literal: " + std::string(str), resource});
                        case types::logical_type::TIME:
                            if (auto parsed = core::date::parse_time(str)) {
                                return types::logical_value_t(resource, *parsed);
                            }
                            return core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"invalid TIME literal: " + std::string(str), resource});
                        case types::logical_type::TIME_TZ:
                            if (auto parsed = core::date::parse_timetz(str)) {
                                return types::logical_value_t(resource, *parsed);
                            }
                            return core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"invalid TIMETZ literal: " + std::string(str), resource});
                        case types::logical_type::TIMESTAMP:
                            if (auto parsed = core::date::parse_timestamp(str)) {
                                return types::logical_value_t(resource, *parsed);
                            }
                            return core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"invalid TIMESTAMP literal: " + std::string(str), resource});
                        case types::logical_type::TIMESTAMP_TZ:
                            if (auto parsed = core::date::parse_timestamptz(str)) {
                                return types::logical_value_t(resource, *parsed);
                            }
                            return core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"invalid TIMESTAMPTZ literal: " + std::string(str), resource});
                        case types::logical_type::INTERVAL:
                            if (auto parsed = core::date::parse_interval(str)) {
                                return types::logical_value_t(resource, *parsed);
                            }
                            return core::error_t(
                                core::error_code_t::sql_parse_error,
                                std::pmr::string{"invalid INTERVAL literal: " + std::string(str), resource});
                        default:
                            break;
                    }
                }
                if (!type_res.has_error() && type_res.value().type() == types::logical_type::BOOLEAN) {
                    return types::logical_value_t(resource, str == "t");
                }
                return types::logical_value_t(resource, std::string(str));
            }
            case T_A_Const: {
                auto* value = &(pg_ptr_cast<A_Const>(node)->val);
                switch (nodeTag(value)) {
                    case T_String: {
                        std::string str = strVal(value);
                        return types::logical_value_t(resource, str);
                    }
                    case T_Integer:
                        return types::logical_value_t(resource, intVal(value));
                    case T_Float:
                        return types::logical_value_t(resource, floatVal(value));
                    case T_Null:
                        return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
                    default:
                        break;
                }
                break;
            }
            case T_A_ArrayExpr: {
                auto array = pg_ptr_cast<A_ArrayExpr>(node);
                return get_array(resource, array->elements);
            }
            case T_RowExpr: {
                auto row = pg_ptr_cast<RowExpr>(node);
                std::vector<types::logical_value_t> fields;
                fields.reserve(row->args->lst.size());
                for (auto& field : row->args->lst) {
                    if (auto res = get_value(resource, pg_ptr_cast<Node>(field.data)); res.has_error()) {
                        return res;
                    } else {
                        fields.emplace_back(std::move(res.value()));
                    }
                }
                return types::logical_value_t::create_struct(resource, "", fields);
            }
            default:
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"unable to parse value", resource});
        }
    }

    core::result_wrapper_t<types::logical_value_t> get_array(std::pmr::memory_resource* resource, PGList* list) {
        std::vector<types::logical_value_t> values;
        values.reserve(list->lst.size());
        for (auto& elem : list->lst) {
            if (auto res = get_value(resource, pg_ptr_cast<Node>(elem.data)); res.has_error()) {
                return res;
            } else {
                values.emplace_back(std::move(res.value()));
            }
        }
        assert(!values.empty());
        auto fist_type = values.front().type();
        for (auto it = ++values.begin(); it != values.end(); ++it) {
            if (fist_type != it->type()) {
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"array has inconsistent element types", resource});
            }
        }
        return types::logical_value_t::create_array(resource, fist_type, std::move(values));
    }

    core::result_wrapper_t<types::logical_value_t> evaluate_const_a_expr(std::pmr::memory_resource* resource,
                                                                         A_Expr* node) {
        if (node->kind != AEXPR_OP) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"Only AEXPR_OP supported in constant arithmetic", resource});
        }
        auto op_str = std::string_view(strVal(node->name->lst.front().data));

        auto resolve = [resource](Node* n) -> core::result_wrapper_t<types::logical_value_t> {
            if (nodeTag(n) == T_A_Expr) {
                return evaluate_const_a_expr(resource, pg_ptr_cast<A_Expr>(n));
            }
            return get_value(resource, n);
        };

        auto left = node->lexpr
                        ? resolve(node->lexpr)
                        : core::result_wrapper_t<types::logical_value_t>{types::logical_value_t(resource, int64_t(0))};
        auto right = resolve(node->rexpr);
        if (left.has_error()) {
            return left;
        }
        if (right.has_error()) {
            return right;
        }

        if (op_str == "+")
            return types::logical_value_t::sum(left.value(), right.value());
        if (op_str == "-")
            return types::logical_value_t::subtract(left.value(), right.value());
        if (op_str == "*")
            return types::logical_value_t::mult(left.value(), right.value());
        if (op_str == "/")
            return types::logical_value_t::divide(left.value(), right.value());
        if (op_str == "%")
            return types::logical_value_t::modulus(left.value(), right.value());
        return core::error_t(
            core::error_code_t::sql_parse_error,
            std::pmr::string{"Unknown arithmetic operator in constant expression: " + std::string(op_str), resource});
    }

    core::result_wrapper_t<std::vector<table::column_definition_t>>
    get_column_definitions(std::pmr::memory_resource* resource, PGList& table_elts) {
        std::vector<table::column_definition_t> out;
        out.reserve(table_elts.lst.size());
        for (auto data : table_elts.lst) {
            if (nodeTag(data.data) != T_ColumnDef) {
                continue;
            }
            auto coldef = pg_ptr_cast<ColumnDef>(data.data);
            auto type = get_type(resource, coldef->typeName);
            if (type.has_error()) {
                return type.convert_error<std::vector<table::column_definition_t>>();
            }
            type.value().set_alias(coldef->colname);
            bool not_null = coldef->is_not_null;
            std::optional<types::logical_value_t> default_val;

            if (coldef->constraints) {
                for (auto cdata : coldef->constraints->lst) {
                    auto constraint = pg_ptr_cast<Constraint>(cdata.data);
                    switch (constraint->contype) {
                        case CONSTR_NOTNULL:
                            not_null = true;
                            break;
                        case CONSTR_DEFAULT:
                            if (constraint->raw_expr) {
                                if (auto val = get_value(resource, constraint->raw_expr); val.has_error()) {
                                    return val.convert_error<std::vector<table::column_definition_t>>();
                                } else {
                                    default_val = std::move(val.value());
                                }
                            }
                            break;
                        case CONSTR_PRIMARY:
                            not_null = true;
                            break;
                        default:
                            break;
                    }
                }
            }

            if (coldef->raw_default && !default_val) {
                if (auto val = get_value(resource, coldef->raw_default); val.has_error()) {
                    return val.convert_error<std::vector<table::column_definition_t>>();
                } else {
                    default_val = std::move(val.value());
                }
            }

            out.emplace_back(coldef->colname, std::move(type.value()), not_null, std::move(default_val));
        }
        return std::move(out);
    }

    core::result_wrapper_t<std::vector<table::table_constraint_t>>
    extract_table_constraints(std::pmr::memory_resource* resource, PGList& table_elts) {
        std::vector<table::table_constraint_t> result;
        for (auto data : table_elts.lst) {
            if (nodeTag(data.data) != T_Constraint) {
                continue;
            }
            auto constraint = pg_ptr_cast<Constraint>(data.data);
            table::table_constraint_t tc;
            switch (constraint->contype) {
                case CONSTR_PRIMARY:
                    tc.type = table::table_constraint_type::PRIMARY_KEY;
                    break;
                case CONSTR_UNIQUE:
                    tc.type = table::table_constraint_type::UNIQUE;
                    break;
                case CONSTR_FOREIGN:
                    tc.type = table::table_constraint_type::FOREIGN_KEY;
                    if (constraint->fk_attrs) {
                        for (auto col : constraint->fk_attrs->lst) {
                            tc.columns.emplace_back(strVal(col.data));
                        }
                    }
                    if (constraint->pk_attrs) {
                        for (auto col : constraint->pk_attrs->lst) {
                            tc.ref_columns.emplace_back(strVal(col.data));
                        }
                    }
                    if (constraint->pktable) {
                        if (constraint->pktable->catalogname) {
                            tc.ref_database = constraint->pktable->catalogname;
                        } else if (constraint->pktable->schemaname) {
                            tc.ref_database = constraint->pktable->schemaname;
                        }
                        if (constraint->pktable->relname) {
                            tc.ref_collection = constraint->pktable->relname;
                        }
                    }
                    if (constraint->conname) {
                        tc.name = constraint->conname;
                    }
                    // PostgreSQL stores ' ' / '\0' for unspecified MATCH/action; normalize to
                    // SQL-standard defaults ('s' SIMPLE, 'a' NO ACTION) so downstream code never
                    // sees an unexpected sentinel.
                    if (constraint->fk_matchtype == 'f' || constraint->fk_matchtype == 'p' ||
                        constraint->fk_matchtype == 's') {
                        tc.fk_matchtype = constraint->fk_matchtype;
                    }
                    {
                        auto da = constraint->fk_del_action;
                        if (da == 'a' || da == 'r' || da == 'c' || da == 'n' || da == 'd') {
                            tc.fk_del_action = da;
                        }
                        auto ua = constraint->fk_upd_action;
                        if (ua == 'a' || ua == 'r' || ua == 'c' || ua == 'n' || ua == 'd') {
                            tc.fk_upd_action = ua;
                        }
                    }
                    result.push_back(std::move(tc));
                    continue; // skip the unique-keys-based code below
                case CONSTR_CHECK:
                    tc.type = table::table_constraint_type::CHECK;
                    if (constraint->conname) {
                        tc.name = constraint->conname;
                    }
                    if (constraint->raw_expr) {
                        if (auto expr_res = deparse_check_expr(resource, constraint->raw_expr); expr_res.has_error()) {
                            return expr_res.convert_error<std::vector<table::table_constraint_t>>();
                        } else {
                            tc.check_expression = std::move(expr_res.value());
                        }
                    }
                    result.push_back(std::move(tc));
                    continue;
                default:
                    continue;
            }
            if (constraint->keys) {
                for (auto key : constraint->keys->lst) {
                    tc.columns.emplace_back(strVal(key.data));
                }
            }
            result.push_back(std::move(tc));
        }
        return result;
    }

    std::string like_to_regex(const std::string& pattern) {
        std::string result = "^";
        for (size_t i = 0; i < pattern.size(); ++i) {
            char c = pattern[i];
            if (c == '%') {
                result += ".*";
            } else if (c == '_') {
                result += '.';
            } else if (c == '\\' && i + 1 < pattern.size()) {
                ++i;
                // escape the next character literally
                char next = pattern[i];
                if (next == '.' || next == '*' || next == '+' || next == '?' || next == '(' || next == ')' ||
                    next == '[' || next == ']' || next == '{' || next == '}' || next == '|' || next == '^' ||
                    next == '$' || next == '\\') {
                    result += '\\';
                }
                result += next;
            } else if (c == '.' || c == '*' || c == '+' || c == '?' || c == '(' || c == ')' || c == '[' || c == ']' ||
                       c == '{' || c == '}' || c == '|' || c == '^' || c == '$' || c == '\\') {
                result += '\\';
                result += c;
            } else {
                result += c;
            }
        }
        result += '$';
        return result;
    }

    core::result_wrapper_t<std::string> deparse_check_expr(std::pmr::memory_resource* resource, Node* node) {
        if (!node) {
            return "";
        }
        switch (nodeTag(node)) {
            case T_ColumnRef: {
                auto* cr = pg_ptr_cast<ColumnRef>(node);
                if (!cr->fields || cr->fields->lst.empty()) {
                    return "";
                }
                // Use only the last field (unqualified column name)
                return std::string(strVal(cr->fields->lst.back().data));
            }
            case T_A_Const: {
                auto* ac = pg_ptr_cast<A_Const>(node);
                switch (nodeTag(&ac->val)) {
                    case T_Integer:
                        return std::to_string(intVal(&ac->val));
                    case T_Float:
                        return std::string(strVal(&ac->val));
                    case T_String:
                        return "'" + std::string(strVal(&ac->val)) + "'";
                    default:
                        return "";
                }
            }
            case T_A_Expr: {
                auto* e = pg_ptr_cast<A_Expr>(node);
                if (e->kind == AEXPR_OP && e->name && !e->name->lst.empty()) {
                    std::string op = std::string(strVal(e->name->lst.front().data));
                    auto left = deparse_check_expr(resource, e->lexpr);
                    if (left.has_error() || left.value().empty()) {
                        return left;
                    }
                    auto right = deparse_check_expr(resource, e->rexpr);
                    if (right.has_error() || right.value().empty()) {
                        return right;
                    }
                    return std::move(left.value()) + " " + op + " " + std::move(right.value());
                }
                if (e->kind == AEXPR_AND || e->kind == AEXPR_OR) {
                    std::string sep = (e->kind == AEXPR_AND) ? " AND " : " OR ";
                    auto left = deparse_check_expr(resource, e->lexpr);
                    if (left.has_error() || left.value().empty()) {
                        return left;
                    }
                    auto right = deparse_check_expr(resource, e->rexpr);
                    if (right.has_error() || right.value().empty()) {
                        return right;
                    }
                    return "(" + std::move(left.value()) + ")" + sep + "(" + std::move(right.value()) + ")";
                }
                return "";
            }
            case T_BoolExpr: {
                auto* b = pg_ptr_cast<BoolExpr>(node);
                if (!b->args || b->args->lst.empty()) {
                    return "";
                }
                if (b->boolop == NOT_EXPR) {
                    auto inner = deparse_check_expr(resource, pg_ptr_cast<Node>(b->args->lst.front().data));
                    if (inner.has_error()) {
                        return inner;
                    }
                    return inner.value().empty() ? "" : "NOT (" + std::move(inner.value()) + ")";
                }
                std::string sep = (b->boolop == AND_EXPR) ? " AND " : " OR ";
                std::string result;
                for (auto& cell : b->args->lst) {
                    auto part = deparse_check_expr(resource, pg_ptr_cast<Node>(cell.data));
                    if (part.has_error() || part.value().empty()) {
                        return part;
                    }
                    // TODO?
                    // result here is always empty?
                    // and separator is not required?
                    if (!result.empty()) {
                        result += sep;
                    }
                    result += "(" + std::move(part.value()) + ")";
                }
                return result;
            }
            case T_NullTest: {
                auto* nt = pg_ptr_cast<NullTest>(node);
                auto arg = deparse_check_expr(resource, pg_ptr_cast<Node>(nt->arg));
                if (arg.has_error() || arg.value().empty()) {
                    return arg;
                }
                return std::move(arg.value()) + (nt->nulltesttype == IS_NULL ? " IS NULL" : " IS NOT NULL");
            }
            default:
                return core::error_t(
                    core::error_code_t::sql_parse_error,
                    std::pmr::string{"CHECK constraint contains unsupported expression type " +
                                         node_tag_to_string(nodeTag(node)) +
                                         "; allowed: column references, constants, comparison/arithmetic operators, "
                                         "AND/OR/NOT, IS NULL/IS NOT NULL",
                                     resource});
        }
    }

    logical_plan::node_ptr maybe_wrap_with_catalog_resolve_table(std::pmr::memory_resource* resource,
                                                                 const std::string& dbname,
                                                                 const std::string& relname,
                                                                 logical_plan::node_ptr main_node,
                                                                 constraint_resolve_kind with_constraints) {
        if (!main_node) {
            return main_node;
        }
        // Build sequence_t([resolve_namespace?], [resolve_table?],
        //                  [resolve_constraint?], main_node).
        // Empty dbname/relname means the caller doesn't have a target identity
        // (e.g. parameter-only statements, schemaless DDL) — skip the resolve
        // node so the wrapped plan still carries useful structure.
        auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(resource));
        if (!dbname.empty()) {
            seq->append_child(logical_plan::make_node_catalog_resolve_namespace(resource, core::dbname_t{dbname}));
        }
        logical_plan::node_catalog_resolve_table_t* table_node_ptr = nullptr;
        if (!relname.empty()) {
            auto table_node = logical_plan::make_node_catalog_resolve_table(resource,
                                                                            core::dbname_t{dbname},
                                                                            core::relname_t{relname});
            table_node_ptr = table_node.get();
            seq->append_child(std::move(table_node));
        }
        if (with_constraints != constraint_resolve_kind::none && table_node_ptr) {
            const auto dir = (with_constraints == constraint_resolve_kind::outgoing)
                                 ? logical_plan::node_catalog_resolve_constraint_t::direction_t::outgoing
                                 : logical_plan::node_catalog_resolve_constraint_t::direction_t::referencing;
            seq->append_child(logical_plan::make_node_catalog_resolve_constraint(resource, table_node_ptr, dir));
        }
        seq->append_child(std::move(main_node));
        return seq;
    }

    logical_plan::node_ptr maybe_wrap_with_catalog_resolve_namespace(std::pmr::memory_resource* resource,
                                                                     const std::string& dbname,
                                                                     logical_plan::node_ptr main_node) {
        if (!main_node) {
            return main_node;
        }
        if (dbname.empty()) {
            return main_node;
        }
        auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(resource));
        seq->append_child(logical_plan::make_node_catalog_resolve_namespace(resource, core::dbname_t{dbname}));
        seq->append_child(std::move(main_node));
        return seq;
    }

    logical_plan::node_ptr
    maybe_wrap_with_catalog_resolve_tables(std::pmr::memory_resource* resource,
                                           std::vector<std::pair<std::string, std::string>> targets,
                                           logical_plan::node_ptr main_node) {
        if (!main_node || targets.empty()) {
            return main_node;
        }
        auto seq = boost::intrusive_ptr(new logical_plan::node_sequence_t(resource));
        // Dedupe dbname for namespace resolves; preserve table order.
        std::vector<std::string> seen_dbs;
        for (const auto& [db, rel] : targets) {
            if (db.empty())
                continue;
            bool already = false;
            for (const auto& s : seen_dbs) {
                if (s == db) {
                    already = true;
                    break;
                }
            }
            if (already)
                continue;
            seen_dbs.push_back(db);
            seq->append_child(logical_plan::make_node_catalog_resolve_namespace(resource, core::dbname_t{db}));
        }
        for (auto& [db, rel] : targets) {
            if (rel.empty())
                continue;
            seq->append_child(
                logical_plan::make_node_catalog_resolve_table(resource, core::dbname_t{db}, core::relname_t{rel}));
        }
        seq->append_child(std::move(main_node));
        return seq;
    }

} // namespace components::sql::transform
