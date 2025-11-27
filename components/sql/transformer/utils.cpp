#include "utils.hpp"
#include <cstdlib>
#include <stdexcept>

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

    column_ref_t columnref_to_fied(ColumnRef* ref) {
        auto lst = ref->fields->lst;
        if (lst.empty()) {
            return column_ref_t();
        } else if (lst.size() == 1) {
            return column_ref_t{{}, expressions::key_t(strVal(lst.back().data))};
        } else {
            return column_ref_t{strVal(std::next(lst.rbegin())->data), expressions::key_t(strVal(lst.rbegin()->data))};
        }
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
                }

                auto width = pg_ptr_assert_cast<A_Const>(linitial(type->typmods), T_A_Const);
                auto scale = pg_ptr_assert_cast<A_Const>(lsecond(type->typmods), T_A_Const);

                if (width->val.type != scale->val.type || width->val.type != T_Integer) {
                    throw parser_exception_t{"Incorrect width or scale for DECIMAL, must be integer", ""};
                }
                column = types::complex_logical_type::create_decimal(static_cast<uint8_t>(intVal(&width->val)),
                                                                     static_cast<uint8_t>(intVal(&scale->val)));
            }
        } else {
            column = get_logical_type(linint_name);
        }

        if (list_length(type->arrayBounds)) {
            auto size = pg_ptr_assert_cast<Value>(linitial(type->arrayBounds), T_Value);
            assert(size->type == T_Integer);
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

} // namespace components::sql::transform
