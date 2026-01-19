#include "utils.hpp"

#include <components/types/logical_value.hpp>

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
        return name == left_name.collection || name == left_alias;
    }

    bool name_collection_t::is_right_table(const std::string& name) const {
        return name == right_name.collection || name == right_alias;
    }

    expressions::side_t deduce_side(const name_collection_t& names, const std::string& target_name) {
        if (target_name.empty()) {
            return expressions::side_t::undefined;
        }
        if (names.left_name.collection == target_name || names.left_alias == target_name) {
            return expressions::side_t::left;
        } else if (names.right_name.collection == target_name || names.right_alias == target_name) {
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

    types::complex_logical_type get_type(TypeName* type) {
        types::complex_logical_type column;
        if (auto linint_name = strVal(linitial(type->names)); !std::strcmp(linint_name, "pg_catalog")) {
            if (auto col = get_logical_type(strVal(lsecond(type->names))); col != types::logical_type::DECIMAL) {
                column = col;
            } else {
                if (list_length(type->typmods) != 2) {
                    throw parser_exception_t{"Incorrect modifiers for DECIMAL, width and scale required", ""};
                } else if (nodeTag(linitial(type->typmods)) != T_A_Const ||
                           nodeTag(lsecond(type->typmods)) != T_A_Const) {
                    throw parser_exception_t{"Incorrect width or scale for DECIMAL, must be integer", ""};
                }

                auto width = pg_ptr_cast<A_Const>(linitial(type->typmods));
                auto scale = pg_ptr_cast<A_Const>(lsecond(type->typmods));

                if (width->val.type != scale->val.type || width->val.type != T_Integer) {
                    throw parser_exception_t{"Incorrect width or scale for DECIMAL, must be integer", ""};
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
        return column;
    }

    template<typename Container>
    void fill_with_types(Container& container, PGList& list) {
        container.reserve(list.lst.size());
        for (auto data : list.lst) {
            auto coldef = pg_ptr_assert_cast<ColumnDef>(data.data, T_ColumnDef);
            types::complex_logical_type type = get_type(coldef->typeName);
            type.set_alias(coldef->colname);
            container.emplace_back(std::move(type));
        }
    }

    std::vector<types::complex_logical_type> get_types(PGList& list) {
        std::vector<types::complex_logical_type> types;
        fill_with_types(types, list);
        return types;
    }

    std::pmr::vector<types::complex_logical_type> get_types(std::pmr::memory_resource* resource, PGList& list) {
        std::pmr::vector<types::complex_logical_type> types(resource);
        fill_with_types(types, list);
        return types;
    }

    types::logical_value_t get_value(Node* node) {
        switch (nodeTag(node)) {
            case T_TypeCast: {
                auto cast = pg_ptr_cast<TypeCast>(node);
                bool is_true = std::string(strVal(&pg_ptr_cast<A_Const>(cast->arg)->val)) == "t";
                return types::logical_value_t(is_true);
            }
            case T_A_Const: {
                auto* value = &(pg_ptr_cast<A_Const>(node)->val);
                switch (nodeTag(value)) {
                    case T_String: {
                        std::string str = strVal(value);
                        return types::logical_value_t(str);
                    }
                    case T_Integer:
                        return types::logical_value_t(intVal(value));
                    case T_Float:
                        return types::logical_value_t(static_cast<float>(floatVal(value)));
                }
            }
            case T_A_ArrayExpr: {
                auto array = pg_ptr_cast<A_ArrayExpr>(node);
                return get_array(array->elements);
            }
            case T_RowExpr: {
                auto row = pg_ptr_cast<RowExpr>(node);
                std::vector<types::logical_value_t> fields;
                fields.reserve(row->args->lst.size());
                for (auto& field : row->args->lst) {
                    fields.emplace_back(get_value(pg_ptr_cast<Node>(field.data)));
                }
                return types::logical_value_t::create_struct("", fields);
            }
            case T_ColumnRef:
                assert(false);
                return types::logical_value_t(strVal(pg_ptr_cast<ColumnRef>(node)->fields->lst.back().data));
        }
        return types::logical_value_t(nullptr);
    }

    types::logical_value_t get_array(PGList* list) {
        std::vector<types::logical_value_t> values;
        values.reserve(list->lst.size());
        for (auto& elem : list->lst) {
            values.emplace_back(get_value(pg_ptr_cast<Node>(elem.data)));
        }
        assert(!values.empty());
        auto fist_type = values.front().type();
        for (auto it = ++values.begin(); it != values.end(); ++it) {
            if (fist_type != it->type()) {
                throw parser_exception_t{"array has inconsistent element types", {}};
            }
        }
        return types::logical_value_t::create_array(fist_type, std::move(values));
    }

} // namespace components::sql::transform
