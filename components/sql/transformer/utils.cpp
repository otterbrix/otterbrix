#include "utils.hpp"

#include <components/logical_plan/identifier_types.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/logical_plan/node_delete.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/types/logical_value.hpp>
#include <core/date/date_parse.hpp>

#include <absl/strings/numbers.h>

#include <atomic>
#include <cstdlib>

namespace components::sql::transform {
    namespace {
        struct element_view_t {
            const qualified_name* name;
            const std::string* alias;
            expressions::side_t side;

            bool exists() const noexcept { return !name->relname.empty() || !alias->empty(); }
            const std::string& visible_name() const noexcept { return transform::visible_name(*name, *alias); }
        };

        std::vector<element_view_t> elements_of(const name_collection_t& names) {
            std::vector<element_view_t> elements;
            elements.reserve(names.extra_left.size() + 2);
            elements.push_back({&names.left_name, &names.left_alias, expressions::side_t::left});
            for (const auto& element : names.extra_left) {
                elements.push_back({&element.name, &element.alias, expressions::side_t::left});
            }
            elements.push_back({&names.right_name, &names.right_alias, expressions::side_t::right});
            return elements;
        }

        bool answers(const element_view_t& element, const column_ref_t& ref) {
            auto slot_answers = [](const std::string& reference, const std::string& element) {
                return reference.empty() || reference == element;
            };

            if (!element.alias->empty()) {
                // An alias replaces the relation name and takes no qualification
                // of its own, so only a bare `alias.col` reaches it.
                return ref.uid.empty() && ref.db.empty() && ref.schema.empty() && ref.table == *element.alias;
            }
            return slot_answers(ref.uid, element.name->uuid) && slot_answers(ref.db, element.name->dbname) &&
                   slot_answers(ref.schema, element.name->schemaname) && slot_answers(ref.table, element.name->relname);
        }

        std::string dotted(std::initializer_list<const std::string*> slots) {
            std::string text;
            for (const std::string* slot : slots) {
                if (slot->empty()) {
                    continue;
                }
                if (!text.empty()) {
                    text += '.';
                }
                text += *slot;
            }
            return text;
        }

        std::string spell(const qualified_name& name) {
            return dotted({&name.uuid, &name.dbname, &name.schemaname, &name.relname});
        }

        std::string spell(const element_view_t& element) {
            return element.alias->empty() ? spell(*element.name) : *element.alias;
        }

        std::string spell(const column_ref_t& ref) {
            const std::string qualification = dotted({&ref.uid, &ref.db, &ref.schema, &ref.table});
            const std::string column = ref.field.as_string();
            return qualification.empty() ? column : qualification + "." + column;
        }

        enum class refusal_reason
        {
            ambiguous,                // more than one element answered
            no_such_name,             // no element carries that name at all
            qualification_differs,    // an element has the relation name, written another way
            hidden_by_alias,          // an element has the relation name but wears an alias
            alias_is_not_qualifiable, // the name is an alias, and the reference qualified it
        };

        core::error_t refusal(std::pmr::memory_resource* resource,
                              const column_ref_t& ref,
                              refusal_reason reason,
                              const std::string& detail) {
            const std::string reference = spell(ref);
            const std::string column = ref.field.as_string();
            std::string message;
            switch (reason) {
                case refusal_reason::ambiguous:
                    message = "column reference \"" + reference + "\" is ambiguous: " + detail + " all answer to it";
                    return core::error_t{core::error_code_t::ambiguous_name, std::pmr::string{message, resource}};
                case refusal_reason::hidden_by_alias:
                    message = "invalid reference to FROM-clause entry for \"" + reference +
                              "\": the table is there under the alias \"" + detail + "\" — write \"" + detail + "." +
                              column + "\"";
                    break;
                case refusal_reason::alias_is_not_qualifiable:
                    message = "invalid reference to FROM-clause entry for \"" + reference + "\": \"" + detail +
                              "\" is an alias and takes no qualification — write \"" + detail + "." + column + "\"";
                    break;
                case refusal_reason::qualification_differs:
                    message = "invalid reference to FROM-clause entry for \"" + reference +
                              "\": no element of FROM is written that way, the nearest is \"" + detail + "\"";
                    break;
                default:
                    message = "invalid reference to FROM-clause entry for \"" + reference +
                              "\": no element of FROM carries that name";
                    break;
            }
            return core::error_t{core::error_code_t::table_not_exists, std::pmr::string{message, resource}};
        }

        // float/double and int64/128 are stored as literals
        core::result_wrapper_t<types::logical_value_t> numeric_token_value(std::pmr::memory_resource* resource,
                                                                           const char* token) {
            const std::string_view text{token};
            if (text.find_first_of(".eE") == std::string_view::npos) {
                int64_t as_bigint = 0;
                if (absl::SimpleAtoi(text, &as_bigint)) {
                    return types::logical_value_t(resource, as_bigint);
                }
                types::int128_t as_hugeint = 0;
                if (absl::SimpleAtoi(text, &as_hugeint)) {
                    return types::logical_value_t(resource, as_hugeint);
                }
                // Above the signed 128-bit range only an unsigned one is left; without it
                // the top half of a uhugeint column could not be written at all.
                types::uint128_t as_uhugeint = 0;
                if (absl::SimpleAtoi(text, &as_uhugeint)) {
                    return types::logical_value_t(resource, as_uhugeint);
                }
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"integer literal out of range: " + std::string(text), resource});
            }
            double as_double = 0.0;
            if (!string_to_double(token, text.size(), as_double)) {
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"invalid numeric literal: " + std::string(text), resource});
            }
            return types::logical_value_t(resource, as_double);
        }
    } // namespace

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
        if (name.empty()) {
            return false;
        }
        if (name == visible_name(left_name, left_alias)) {
            return true;
        }
        return std::any_of(extra_left.begin(), extra_left.end(), [&name](const from_element_t& element) {
            return element.visible_name() == name;
        });
    }

    bool name_collection_t::is_right_table(const std::string& name) const {
        return !name.empty() && name == visible_name(right_name, right_alias);
    }

    core::result_wrapper_t<expressions::side_t> name_collection_t::resolve(std::pmr::memory_resource* resource,
                                                                           const column_ref_t& ref) const {
        const auto elements = elements_of(*this);

        const element_view_t* answered = nullptr;
        std::string answering;
        size_t count = 0;
        for (const auto& element : elements) {
            if (!element.exists() || !answers(element, ref)) {
                continue;
            }
            ++count;
            answered = &element;
            if (!answering.empty()) {
                answering += ", ";
            }
            answering += spell(element);
        }
        if (count == 1) {
            return answered->side;
        }
        if (count > 1) {
            return refusal(resource, ref, refusal_reason::ambiguous, answering);
        }

        for (const auto& element : elements) {
            if (element.exists() && !element.alias->empty() && *element.alias == ref.table) {
                return refusal(resource, ref, refusal_reason::alias_is_not_qualifiable, *element.alias);
            }
        }
        for (const auto& element : elements) {
            if (!element.exists() || element.name->relname != ref.table) {
                continue;
            }
            if (!element.alias->empty()) {
                return refusal(resource, ref, refusal_reason::hidden_by_alias, *element.alias);
            }
            return refusal(resource, ref, refusal_reason::qualification_differs, spell(*element.name));
        }
        return refusal(resource, ref, refusal_reason::no_such_name, {});
    }

    core::error_t name_collection_t::refuse_indistinguishable_elements(std::pmr::memory_resource* resource) const {
        const auto elements = elements_of(*this);
        for (auto outer = elements.begin(); outer != elements.end(); ++outer) {
            if (!outer->exists()) {
                continue;
            }
            for (auto inner = std::next(outer); inner != elements.end(); ++inner) {
                if (!inner->exists() || inner->visible_name() != outer->visible_name()) {
                    continue;
                }
                const bool same_qualification = outer->name->uuid == inner->name->uuid &&
                                                outer->name->dbname == inner->name->dbname &&
                                                outer->name->schemaname == inner->name->schemaname;
                if (!outer->alias->empty() || !inner->alias->empty() || same_qualification) {
                    return core::error_t{core::error_code_t::ambiguous_name,
                                         std::pmr::string{"table name \"" + outer->visible_name() +
                                                              "\" specified more than once — give one of them an alias",
                                                          resource}};
                }
            }
        }
        return core::error_t::no_error();
    }

    const name_collection_t::using_column_t* name_collection_t::using_column(std::string_view name) const {
        const auto found = std::find_if(using_columns.begin(), using_columns.end(), [name](const auto& column) {
            return column.name == name;
        });
        return found == using_columns.end() ? nullptr : &*found;
    }

    core::result_wrapper_t<column_ref_t>
    columnref_to_field(std::pmr::memory_resource* resource, ColumnRef* ref, const name_collection_t& names) {
        const auto& segments = ref->fields->lst;
        if (segments.empty()) {
            return column_ref_t(resource);
        }
        if (segments.size() > MAX_COLUMN_REF_SEGMENTS) {
            return core::error_t{core::error_code_t::sql_parse_error,
                                 std::pmr::string{"improper column reference (too many dotted names)", resource}};
        }

        column_ref_t out(resource);
        auto it = segments.begin();
        auto take = [&](std::string& slot) {
            slot = strVal(it->data);
            ++it;
        };
        switch (segments.size()) {
            case 5:
                take(out.uid);
                take(out.db);
                take(out.schema);
                take(out.table);
                break;
            case 4:
                take(out.db);
                take(out.schema);
                take(out.table);
                break;
            case 3:
                take(out.db);
                take(out.table);
                break;
            case 2:
                take(out.table);
                break;
            default:
                break;
        }

        std::pmr::vector<std::pmr::string> field_path(resource);
        field_path.emplace_back(nodeTag(it->data) == T_A_Star ? std::pmr::string{"*", resource}
                                                              : pmrStrVal(it->data, resource));
        out.field = expressions::key_t{std::move(field_path)};

        if (!out.is_qualified()) {
            if (segments.size() == 1) {
                if (const auto* column = names.using_column(out.field.storage().front())) {
                    if (column->join == logical_plan::join_type::full) {
                        return core::error_t{
                            core::error_code_t::unimplemented_yet,
                            std::pmr::string{"column \'" + std::string(out.field.storage().front()) +
                                                 "\' is merged by FULL JOIN ... USING, where its value is COALESCE of "
                                                 "both sides — write that COALESCE explicitly",
                                             resource}};
                    }
                    // The other side is the padded one on an outer join, so the
                    // value lives here; on an inner join the two are equal.
                    out.field.set_side(column->join == logical_plan::join_type::right ? expressions::side_t::right
                                                                                      : expressions::side_t::left);
                }
            }
            return out;
        }
        out.field.set_qualifier(out.table);
        VALUE_OR_RETURN(auto side, names.resolve(resource, out));
        out.field.set_side(side);
        return out;
    }

    core::result_wrapper_t<column_ref_t> indirection_to_field(std::pmr::memory_resource* resource,
                                                              A_Indirection* indirection,
                                                              const name_collection_t& names) {
        column_ref_t ref(resource);
        if (nodeTag(indirection->arg) == T_ColumnRef) {
            VALUE_OR_RETURN(ref, columnref_to_field(resource, pg_ptr_cast<ColumnRef>(indirection->arg), names));
        } else if (nodeTag(indirection->arg) == T_A_Indirection) {
            VALUE_OR_RETURN(ref, indirection_to_field(resource, pg_ptr_cast<A_Indirection>(indirection->arg), names));
        } else {
            return core::error_t{core::error_code_t::sql_parse_error,
                                 std::pmr::string{"field selection is supported only on a column reference", resource}};
        }
        for (const auto& step : indirection->indirection->lst) {
            if (nodeTag(step.data) == T_A_Indices) {
                ref.field.storage().emplace_back(indices_to_str(resource, pg_ptr_cast<A_Indices>(step.data)));
            } else if (nodeTag(step.data) == T_A_Star) {
                ref.field.storage().emplace_back(std::pmr::string{"*", resource});
            } else {
                ref.field.storage().emplace_back(pmrStrVal(step.data, resource));
            }
        }
        return ref;
    }

    core::result_wrapper_t<column_ref_t>
    node_to_field(std::pmr::memory_resource* resource, Node* node, const name_collection_t& names) {
        if (nodeTag(node) == T_ColumnRef) {
            return columnref_to_field(resource, pg_ptr_cast<ColumnRef>(node), names);
        }
        if (nodeTag(node) == T_A_Indirection) {
            return indirection_to_field(resource, pg_ptr_cast<A_Indirection>(node), names);
        }
        return core::error_t{core::error_code_t::sql_parse_error,
                             std::pmr::string{"expected a column reference", resource}};
    }

    bool is_jsonb_nav_operator(std::string_view op) { return op == "->" || op == "->>" || op == "#>" || op == "#>>"; }

    bool jsonb_nav_returns_scalar(std::string_view op) { return op == "->>" || op == "#>>"; }

    bool jsonb_op_takes_path(std::string_view op) { return op == "#>" || op == "#>>" || op == "#-"; }

    operator_function_t operator_function(std::string_view op) {
        if (op == "^") {
            return {"pow", operator_fixity_t::infix};
        }
        if (op == "|/") {
            return {"sqrt", operator_fixity_t::prefix};
        }
        if (op == "||/") {
            return {"cbrt", operator_fixity_t::prefix};
        }
        if (op == "!") {
            return {"factorial", operator_fixity_t::postfix};
        }
        if (op == "!!") {
            return {"factorial", operator_fixity_t::prefix};
        }
        if (op == "@") {
            return {"abs", operator_fixity_t::prefix};
        }
        return {};
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
            case T_SubLink:
                return "T_SubLink";
            default:
                return "NodeTag(" + std::to_string(static_cast<int>(type)) + ")";
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
            const char* builtin_name = strVal(lsecond(type->names));
            auto col = get_logical_type(builtin_name);
            if (col == types::logical_type::UNKNOWN) {
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"type '", resource} + builtin_name + "' is not supported");
            } else if (col == types::logical_type::STRING_LITERAL && list_length(type->typmods) > 0) {
                // Engine has only unbounded strings, reject char/varchar(n) length rather than silently dropping it
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"string length modifier is not supported", resource});
            } else if (col != types::logical_type::DECIMAL) {
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
            // Variadic array, encoded as -1 by the grammar
            if (intVal(size) < 0) {
                column = types::complex_logical_type::create_list(column);
            } else {
                column = types::complex_logical_type::create_array(column, intVal(size));
            }
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
                    // A NULL literal under a CAST (`NULL::T`) is a typed NULL. Reading ival/fval of a T_Null
                    // node yields a garbage non-null value — return an untyped NA null instead; the value
                    // stays NULL via the vector validity mask and the projection resolves a concrete column
                    // type (PG unknown->text) downstream.
                    if (constant->val.type == T_Null) {
                        return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
                    }
                    // anything outside int32 range parses as T_Float and stored as lineral
                    if (constant->val.type == T_Float) {
                        return numeric_token_value(resource, strVal(&constant->val));
                    }
                    return types::logical_value_t(resource, intVal(&constant->val));
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
                    // anything outside int32 range parses as T_Float and stored as lineral
                    case T_Float:
                        return numeric_token_value(resource, strVal(value));
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
        if (values.empty()) {
            // Empty array literal (ARRAY[]): the element type is indeterminate at parse
            // time. Use NA  as a placeholder; it is resolved against the target column's
            // element type when the value is cast/reconciled on the INSERT path.
            return types::logical_value_t::create_array(resource,
                                                        types::complex_logical_type{types::logical_type::NA},
                                                        std::move(values));
        }
        // The element type comes from the first NON-NULL element. A NULL element (logical_type NA)
        // is a valid null slot compatible with any element type, so it is skipped both when inferring
        // the element type and when checking element-type consistency. An all-NULL array leaves the
        // element type indeterminate (UNKNOWN), resolved against the target column's element type
        // when the value is cast/reconciled on the INSERT path.
        types::complex_logical_type element_type{types::logical_type::UNKNOWN};
        bool element_type_found = false;
        for (const auto& value : values) {
            if (value.type().type() == types::logical_type::NA) {
                continue;
            }
            if (!element_type_found) {
                element_type = value.type();
                element_type_found = true;
            } else if (element_type != value.type()) {
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"array has inconsistent element types", resource});
            }
        }
        return types::logical_value_t::create_array(resource, element_type, std::move(values));
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
    extract_table_constraints(std::pmr::memory_resource* resource, PGList& table_elts, const char* raw_sql) {
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
                        if (auto expr_res = slice_check_expression(resource, raw_sql, constraint->location);
                            expr_res.has_error()) {
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

    namespace {
        // How far a lexical region starting at `text[at]` runs, or 0 when none starts there. A
        // paren inside a string or a comment is not punctuation, so the scan below has to step
        // over these whole rather than read them character by character.
        std::size_t skip_region(std::string_view text, std::size_t at) {
            const auto rest = text.size() - at;
            // -- to end of line
            if (rest >= 2 && text[at] == '-' && text[at + 1] == '-') {
                const auto line_end = text.find('\n', at);
                return (line_end == std::string_view::npos ? text.size() : line_end) - at;
            }
            // /* ... */, which nests
            if (rest >= 2 && text[at] == '/' && text[at + 1] == '*') {
                std::size_t cursor = at + 2;
                int depth = 1;
                while (cursor + 1 < text.size() && depth > 0) {
                    if (text[cursor] == '/' && text[cursor + 1] == '*') {
                        ++depth;
                        cursor += 2;
                    } else if (text[cursor] == '*' && text[cursor + 1] == '/') {
                        --depth;
                        cursor += 2;
                    } else {
                        ++cursor;
                    }
                }
                return (depth == 0 ? cursor : text.size()) - at;
            }
            // $tag$ ... $tag$
            if (text[at] == '$') {
                const auto tag_end = text.find('$', at + 1);
                if (tag_end != std::string_view::npos) {
                    const auto tag = text.substr(at, tag_end - at + 1);
                    const auto closing = text.find(tag, tag_end + 1);
                    if (closing != std::string_view::npos) {
                        return closing + tag.size() - at;
                    }
                }
                return 0;
            }
            // '...' and "...", where the quote is doubled to escape itself, and E'...' where a
            // backslash escapes the next character.
            std::size_t start = at;
            bool backslash_escapes = false;
            if ((text[at] == 'E' || text[at] == 'e') && at + 1 < text.size() && text[at + 1] == '\'') {
                backslash_escapes = true;
                start = at + 1;
            } else if (text[at] != '\'' && text[at] != '"') {
                return 0;
            }
            const char quote = text[start];
            std::size_t cursor = start + 1;
            while (cursor < text.size()) {
                if (backslash_escapes && text[cursor] == '\\' && cursor + 1 < text.size()) {
                    cursor += 2;
                    continue;
                }
                if (text[cursor] == quote) {
                    if (cursor + 1 < text.size() && text[cursor + 1] == quote) {
                        cursor += 2;
                        continue;
                    }
                    return cursor + 1 - at;
                }
                ++cursor;
            }
            return text.size() - at;
        }

        std::string_view trim_view(std::string_view text) {
            const auto first = text.find_first_not_of(" \t\r\n");
            if (first == std::string_view::npos) {
                return {};
            }
            return text.substr(first, text.find_last_not_of(" \t\r\n") - first + 1);
        }
    } // namespace

    core::result_wrapper_t<std::string>
    slice_check_expression(std::pmr::memory_resource* resource, const char* raw_sql, int check_location) {
        if (!raw_sql) {
            return core::error_t{core::error_code_t::invalid_constraint,
                                 std::pmr::string{"CHECK constraint cannot be stored: the statement text is not "
                                                  "available to read the expression from",
                                                  resource}};
        }
        const std::string_view sql{raw_sql};
        if (check_location < 0 || static_cast<std::size_t>(check_location) >= sql.size()) {
            return core::error_t{
                core::error_code_t::invalid_constraint,
                std::pmr::string{"CHECK constraint cannot be stored: its position in the statement is unknown",
                                 resource}};
        }

        // From the CHECK keyword to the '(' that opens it; only whitespace or a comment may sit
        // between the two.
        std::size_t cursor = static_cast<std::size_t>(check_location);
        while (cursor < sql.size() && sql[cursor] != '(') {
            const auto region = skip_region(sql, cursor);
            cursor += region > 0 ? region : 1;
        }
        if (cursor >= sql.size()) {
            return core::error_t{
                core::error_code_t::invalid_constraint,
                std::pmr::string{"CHECK constraint cannot be stored: no '(' follows the CHECK keyword", resource}};
        }

        // The expression is everything up to the ')' that closes that '('.
        const std::size_t begin = cursor + 1;
        int depth = 1;
        cursor = begin;
        while (cursor < sql.size() && depth > 0) {
            const auto region = skip_region(sql, cursor);
            if (region > 0) {
                cursor += region;
                continue;
            }
            if (sql[cursor] == '(') {
                ++depth;
            } else if (sql[cursor] == ')') {
                --depth;
                if (depth == 0) {
                    break;
                }
            }
            ++cursor;
        }
        if (depth != 0) {
            return core::error_t{
                core::error_code_t::invalid_constraint,
                std::pmr::string{"CHECK constraint cannot be stored: its parentheses are unbalanced", resource}};
        }

        const auto expression = trim_view(sql.substr(begin, cursor - begin));
        if (expression.empty()) {
            return core::error_t{core::error_code_t::invalid_constraint,
                                 std::pmr::string{"CHECK constraint carries no expression", resource}};
        }
        return std::string{expression};
    }

    logical_plan::node_ptr
    name_catalog_target(const std::string& dbname, const std::string& relname, logical_plan::node_ptr node) {
        if (!node) {
            return node;
        }
        switch (node->type()) {
            case logical_plan::node_type::insert_t: {
                auto* n = static_cast<logical_plan::node_insert_t*>(node.get());
                n->set_dbname(dbname);
                n->set_relname(relname);
                break;
            }
            case logical_plan::node_type::update_t: {
                auto* n = static_cast<logical_plan::node_update_t*>(node.get());
                n->set_dbname(dbname);
                n->set_relname(relname);
                break;
            }
            case logical_plan::node_type::delete_t: {
                auto* n = static_cast<logical_plan::node_delete_t*>(node.get());
                n->set_dbname(dbname);
                n->set_relname(relname);
                break;
            }
            case logical_plan::node_type::drop_t: {
                auto* n = static_cast<logical_plan::node_drop_t*>(node.get());
                n->set_dbname(dbname);
                n->set_relname(relname);
                break;
            }
            case logical_plan::node_type::create_collection_t: {
                auto* n = static_cast<logical_plan::node_create_collection_t*>(node.get());
                n->set_dbname(dbname);
                break;
            }
            case logical_plan::node_type::create_index_t: {
                auto* n = static_cast<logical_plan::node_create_index_t*>(node.get());
                n->set_dbname(dbname);
                n->set_relname(relname);
                break;
            }
            default:
                // already carry their own names.
                break;
        }
        return node;
    }

    void register_catalog_resolve_types(std::pmr::memory_resource* resource,
                                        logical_plan::catalog_resolves_t* resolves,
                                        const std::vector<std::string>& type_names) {
        if (type_names.empty()) {
            return;
        }
        auto& node = resolves->ensure(resource, logical_plan::resolve_kind::type);
        for (const auto& name : type_names) {
            logical_plan::resolve_entry_t entry;
            entry.dbname = "public";
            entry.type_name = name;
            node.add(std::move(entry));
        }
    }

    void register_catalog_resolve_namespace(std::pmr::memory_resource* resource,
                                            logical_plan::catalog_resolves_t* resolves,
                                            const std::string& dbname) {
        if (dbname.empty()) {
            return;
        }
        logical_plan::resolve_entry_t entry;
        entry.dbname = dbname;
        resolves->ensure(resource, logical_plan::resolve_kind::namespace_).add(std::move(entry));
    }

    void register_catalog_resolve_table(std::pmr::memory_resource* resource,
                                        logical_plan::catalog_resolves_t* resolves,
                                        const std::string& dbname,
                                        const std::string& relname,
                                        constraint_resolve_kind with_constraints) {
        // An empty dbname/relname means the caller has no target identity (e.g.
        // parameter-only statements, schemaless DDL) — nothing to resolve.
        register_catalog_resolve_namespace(resource, resolves, dbname);
        if (relname.empty()) {
            return;
        }
        logical_plan::resolve_entry_t table_entry;
        table_entry.dbname = dbname;
        table_entry.relname = relname;
        const auto table_index =
            resolves->ensure(resource, logical_plan::resolve_kind::table).add(std::move(table_entry));

        if (with_constraints == constraint_resolve_kind::none) {
            return;
        }
        logical_plan::resolve_entry_t constraint_entry;
        constraint_entry.target = table_index;
        constraint_entry.direction = (with_constraints == constraint_resolve_kind::outgoing)
                                         ? logical_plan::resolve_direction::outgoing
                                         : logical_plan::resolve_direction::referencing;
        resolves->ensure(resource, logical_plan::resolve_kind::constraint).add(std::move(constraint_entry));
    }

    void register_catalog_resolve_tables(std::pmr::memory_resource* resource,
                                         logical_plan::catalog_resolves_t* resolves,
                                         const std::vector<std::pair<std::string, std::string>>& targets) {
        for (const auto& [dbname, relname] : targets) {
            register_catalog_resolve_table(resource, resolves, dbname, relname, constraint_resolve_kind::none);
        }
    }

} // namespace components::sql::transform
