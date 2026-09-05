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

#include <algorithm>
#include <atomic>
#include <cctype>
#include <cstdlib>
#include <limits>
#include <string_view>

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

    core::result_wrapper_t<std::pmr::string> indices_to_str(std::pmr::memory_resource* resource, A_Indices* indices) {
        if (indices->lidx) {
            // arr[a:b] — a slice names a RANGE of elements, not one path segment.
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"an array slice subscript is not supported here", resource});
        }
        if (!indices->uidx || nodeTag(indices->uidx) != T_A_Const) {
            // arr[x], arr[i + 1] — a computed subscript has no digits to render.
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"an array subscript must be an integer literal, not a computed expression",
                                 resource});
        }
        Value* val = &pg_ptr_cast<A_Const>(indices->uidx)->val;
        // `ival` and `str` share one union slot, and the scanner stores an integer literal
        // in `ival` only when it fits int32 — arr[3000000000] arrives as a T_Float carrying
        // its ORIGINAL DIGITS in `str`. Reading `ival` regardless of the tag renders the
        // char*'s bit pattern as the segment name, a different "index" on every run.
        switch (nodeTag(val)) {
            case T_Integer:
                return core::pmr::to_pmr_string(resource, intVal(val));
            case T_Float: {
                // Digits-only text is a wide integer and names its segment exactly;
                // anything with a '.' or an exponent is not an index at all.
                const std::string_view text{strVal(val)};
                const bool digits_only =
                    !text.empty() && text.find_first_not_of("0123456789") == std::string_view::npos;
                if (digits_only) {
                    return std::pmr::string{text, resource};
                }
                std::pmr::string msg{"an array subscript must be an integer literal, got: ", resource};
                msg += text;
                return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
            }
            default: {
                std::pmr::string msg{"an array subscript must be an integer literal, got ", resource};
                msg += node_tag_to_string(nodeTag(val));
                return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
            }
        }
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
                VALUE_OR_RETURN(auto segment, indices_to_str(resource, pg_ptr_cast<A_Indices>(step.data)));
                ref.field.storage().emplace_back(std::move(segment));
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

    core::error_t refuse_dropped_call_decorations(std::pmr::memory_resource* resource, const FuncCall& call) {
        if (call.over) {
            // FuncCall::over is read by NOBODY downstream: the call would lower as a
            // plain aggregate — one value per group instead of one per row — and the
            // statement would report success. Name the call that was refused.
            std::string name = "?";
            if (call.funcname && !call.funcname->lst.empty() &&
                nodeTag(call.funcname->lst.back().data) == T_String) {
                name = strVal(call.funcname->lst.back().data);
            }
            return core::error_t(core::error_code_t::unimplemented_yet,
                                 std::pmr::string{"window function OVER is not supported yet: " + name +
                                                      "(...) would have been computed as a plain aggregate",
                                                  resource});
        }
        if (call.func_variadic) {
            // func_variadic is read by nobody: f(VARIADIC arr) would run as f(arr) —
            // the array handed over as ONE argument instead of being spread.
            return core::error_t(
                core::error_code_t::unimplemented_yet,
                std::pmr::string{"VARIADIC is not supported yet: the argument would have been passed unexpanded",
                                 resource});
        }
        if (call.agg_within_group || (call.agg_order && !call.agg_order->lst.empty())) {
            // agg_order shares the same seam: `array_agg(x ORDER BY y)` parses, the
            // ordering is read by nobody, and the aggregate would answer in storage order.
            return core::error_t(
                core::error_code_t::unimplemented_yet,
                std::pmr::string{"aggregate ORDER BY / WITHIN GROUP is not supported yet: the ordering would "
                                 "have been dropped",
                                 resource});
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<types::complex_logical_type> get_type(std::pmr::memory_resource* resource, TypeName* type) {
        types::complex_logical_type column;
        if (!type || !type->names || list_length(type->names) == 0) {
            // No TypeName, or one with an empty name list, is a FAILURE, not a
            // default-constructed NA type: answered as a value, the caller stores it as a
            // column's type.
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"cannot determine a type: the TypeName is absent", resource});
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
                // Range-check BEFORE narrowing: a bare static_cast<uint8_t> wraps silently —
                // NUMERIC(256,0) becomes DECIMAL(0,0) and NUMERIC(-1,0) becomes
                // DECIMAL(255,0), both of them types the persistence codec writes and then
                // refuses to read back. This is the EARLIEST point that owns an error
                // channel, and refusing here costs one failed statement instead of a catalog
                // row that makes the database unopenable.
                const auto raw_width = intVal(&width->val);
                const auto raw_scale = intVal(&scale->val);
                if (raw_width < 0 || raw_scale < 0 || raw_width > types::DECIMAL_MAX_WIDTH ||
                    raw_scale > types::DECIMAL_MAX_WIDTH) {
                    return core::error_t(core::error_code_t::invalid_parameter,
                                         std::pmr::string{"DECIMAL width must be between 1 and " +
                                                              std::to_string(types::DECIMAL_MAX_WIDTH) +
                                                              " and scale must not exceed width",
                                                          resource});
                }
                // In uint8 range now, so create_decimal owns the window decision and its
                // message — one authority, not a second copy that can drift from it.
                VALUE_OR_RETURN(column,
                                types::complex_logical_type::create_decimal(static_cast<uint8_t>(raw_width),
                                                                            static_cast<uint8_t>(raw_scale)));
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

    integer_text_t parse_exact_integer(std::string_view text, types::int128_t& out) {
        size_t i = 0;
        bool negative = false;
        if (i < text.size() && (text[i] == '+' || text[i] == '-')) {
            negative = (text[i] == '-');
            ++i;
        }
        if (i == text.size()) {
            return integer_text_t::not_an_integer;
        }
        // Accumulate in the UNSIGNED domain. The signed range is asymmetric — -(2^127) is
        // representable and +(2^127) is not — so the negative floor cannot be reached by
        // building a positive int128 first and negating it; that intermediate does not
        // exist. The unsigned accumulator holds both bounds, and the two's-complement
        // negation below turns it back into the signed value without ever overflowing.
        const types::uint128_t limit =
            negative ? (types::uint128_t{1} << 127) : ((types::uint128_t{1} << 127) - types::uint128_t{1});
        const types::uint128_t limit_div10 = limit / 10;
        const types::uint128_t limit_mod10 = limit % 10;
        types::uint128_t acc{0};
        for (; i < text.size(); ++i) {
            const char c = text[i];
            if (c < '0' || c > '9') {
                // A '.', an 'e'/'E', anything else: this literal really is a float.
                return integer_text_t::not_an_integer;
            }
            const types::uint128_t digit{static_cast<uint64_t>(c - '0')};
            if (acc > limit_div10 || (acc == limit_div10 && digit > limit_mod10)) {
                return integer_text_t::out_of_range;
            }
            acc = acc * 10 + digit;
        }
        out = static_cast<types::int128_t>(negative ? (~acc + types::uint128_t{1}) : acc);
        return integer_text_t::exact;
    }

    core::result_wrapper_t<types::int128_t>
    parse_exact_decimal(std::pmr::memory_resource* resource, std::string_view text, uint8_t width, uint8_t scale) {
        auto malformed = [&]() {
            std::pmr::string msg{"not a decimal number: ", resource};
            msg.append(text.data(), text.size());
            return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
        };
        auto overflow = [&]() {
            std::pmr::string msg{"numeric field overflow: ", resource};
            msg.append(text.data(), text.size());
            msg += " does not fit NUMERIC(";
            msg += std::to_string(width);
            msg += ", ";
            msg += std::to_string(scale);
            msg += ")";
            return core::error_t(core::error_code_t::invalid_parameter, std::move(msg));
        };
        // PostgreSQL trims surrounding whitespace of a numeric input; nothing else is
        // forgiven — an exponent or a stray character is a refusal, never a partial read.
        size_t begin = 0;
        size_t end = text.size();
        while (begin < end && std::isspace(static_cast<unsigned char>(text[begin]))) {
            ++begin;
        }
        while (end > begin && std::isspace(static_cast<unsigned char>(text[end - 1]))) {
            --end;
        }
        if (begin == end) {
            return malformed();
        }
        bool negative = false;
        if (text[begin] == '+' || text[begin] == '-') {
            negative = (text[begin] == '-');
            ++begin;
        }
        std::string_view digits = text.substr(begin, end - begin);
        const size_t dot = digits.find('.');
        const std::string_view int_part = (dot == std::string_view::npos) ? digits : digits.substr(0, dot);
        const std::string_view frac_part = (dot == std::string_view::npos) ? std::string_view{}
                                                                           : digits.substr(dot + 1);
        if (int_part.empty() && frac_part.empty()) {
            return malformed(); // ".", "-", "+" — no digits at all
        }
        auto all_digits = [](std::string_view s) {
            return s.find_first_not_of("0123456789") == std::string_view::npos;
        };
        if (!all_digits(int_part) || !all_digits(frac_part)) {
            return malformed(); // a second '.', an exponent, anything non-digit
        }
        // 10^38 fits int128 (2^127 ≈ 1.7e38) and DECIMAL_MAX_WIDTH == 38, so one ceiling
        // guards the accumulation for every declarable width.
        types::uint128_t ceiling{1};
        for (int i = 0; i < types::DECIMAL_MAX_WIDTH; ++i) {
            ceiling *= 10;
        }
        types::uint128_t acc{0};
        auto push_digit = [&](char c) -> bool {
            const types::uint128_t digit{static_cast<uint64_t>(c - '0')};
            if (acc > (ceiling - digit) / 10) {
                return false;
            }
            acc = acc * 10 + digit;
            return true;
        };
        for (char c : int_part) {
            if (!push_digit(c)) {
                return overflow();
            }
        }
        // Exactly `scale` fractional digits contribute; the first digit past them decides
        // the rounding (half away from zero — PostgreSQL's rule for numeric), and rounding
        // may carry into a wider value, so the width check comes AFTER it.
        for (size_t i = 0; i < scale; ++i) {
            const char c = i < frac_part.size() ? frac_part[i] : '0';
            if (!push_digit(c)) {
                return overflow();
            }
        }
        if (frac_part.size() > scale && frac_part[scale] >= '5') {
            acc += 1;
        }
        types::uint128_t limit{1};
        for (uint8_t i = 0; i < width; ++i) {
            limit *= 10;
        }
        if (acc >= limit) {
            return overflow();
        }
        // Same two's-complement negation as parse_exact_integer: the value is well under
        // 2^127, so the unsigned round-trip is exact.
        return static_cast<types::int128_t>(negative ? (~acc + types::uint128_t{1}) : acc);
    }

    core::result_wrapper_t<types::logical_value_t> numeric_literal_value(std::pmr::memory_resource* resource,
                                                                         Value* value) {
        if (nodeTag(value) == T_Integer) {
            // Already inside int32 — the scanner only stores an integer literal in `ival`
            // when it fits there. BIGINT (not INTEGER) is what the rest of the pipeline
            // expects from this arm.
            return types::logical_value_t(resource, static_cast<int64_t>(intVal(value)));
        }
        if (nodeTag(value) != T_Float) {
            // T_BitString / T_Null / anything else keeps a char* in the same union slot as
            // `ival`, so reading it as an integer answers with a pointer value. A wrong node
            // kind here is a parser bug, and it reports as one.
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"not a numeric literal: " + node_tag_to_string(nodeTag(value)), resource});
        }
        const char* text = strVal(value);
        types::int128_t exact{0};
        switch (parse_exact_integer(text, exact)) {
            case integer_text_t::exact:
                // int64 first: HUGEINT is a type most of the pipeline (numeric_widen,
                // promote_type, the comparison kernels) reaches only through promotion, so
                // it is spent only on literals that genuinely need 128 bits.
                if (exact >= types::int128_t{std::numeric_limits<int64_t>::min()} &&
                    exact <= types::int128_t{std::numeric_limits<int64_t>::max()}) {
                    return types::logical_value_t(resource, static_cast<int64_t>(exact));
                }
                return types::logical_value_t(resource, exact);
            case integer_text_t::out_of_range:
                // int128 is the widest exact integer with any storage behind it. PostgreSQL
                // would widen once more, to arbitrary-precision numeric; we have no such
                // type, and answering with the nearest double would be the silent wrong
                // answer this whole path exists to remove. So it is a refusal.
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{"integer literal out of range: " + std::string(text), resource});
            case integer_text_t::not_an_integer:
                break;
        }
        return types::logical_value_t(resource, floatVal(value));
    }

    namespace {

        // SQL-facing names, used only to address the DECLARED target in a refusal.
        std::string cast_target_name(types::logical_type t) {
            using LT = types::logical_type;
            switch (t) {
                case LT::TINYINT:
                    return "TINYINT";
                case LT::SMALLINT:
                    return "SMALLINT";
                case LT::INTEGER:
                    return "INTEGER";
                case LT::BIGINT:
                    return "BIGINT";
                case LT::HUGEINT:
                    return "HUGEINT";
                case LT::UTINYINT:
                    return "UTINYINT";
                case LT::USMALLINT:
                    return "USMALLINT";
                case LT::UINTEGER:
                    return "UINTEGER";
                case LT::UBIGINT:
                    return "UBIGINT";
                case LT::UHUGEINT:
                    return "UHUGEINT";
                case LT::FLOAT:
                    return "REAL";
                case LT::DOUBLE:
                    return "DOUBLE PRECISION";
                case LT::DECIMAL:
                    return "NUMERIC";
                case LT::BOOLEAN:
                    return "BOOLEAN";
                case LT::UUID:
                    return "UUID";
                case LT::BLOB:
                    return "BLOB";
                case LT::BIT:
                    return "BIT";
                case LT::POINTER:
                    return "POINTER";
                case LT::DATE:
                    return "DATE";
                case LT::TIME:
                    return "TIME";
                case LT::TIME_TZ:
                    return "TIMETZ";
                case LT::TIMESTAMP:
                    return "TIMESTAMP";
                case LT::TIMESTAMP_TZ:
                    return "TIMESTAMPTZ";
                case LT::INTERVAL:
                    return "INTERVAL";
                case LT::STRING_LITERAL:
                    return "TEXT";
                default:
                    return "type#" + std::to_string(static_cast<int>(t));
            }
        }

        core::error_t
        invalid_cast_input(std::pmr::memory_resource* resource, types::logical_type target, const std::string& text) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"invalid input for a cast to " + cast_target_name(target) + ": " +
                                                      text,
                                                  resource});
        }

        core::error_t
        cast_out_of_range(std::pmr::memory_resource* resource, types::logical_type target, const std::string& text) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"value out of range for a cast to " + cast_target_name(target) +
                                                      ": " + text,
                                                  resource});
        }

        // The value space of every integer cast target, as int128 bounds. Answers false
        // for a non-integer type so the caller's dispatch stays honest.
        bool integer_target_bounds(types::logical_type t, types::int128_t& lo, types::int128_t& hi) {
            using LT = types::logical_type;
            switch (t) {
                case LT::TINYINT:
                    lo = std::numeric_limits<int8_t>::min();
                    hi = std::numeric_limits<int8_t>::max();
                    return true;
                case LT::SMALLINT:
                    lo = std::numeric_limits<int16_t>::min();
                    hi = std::numeric_limits<int16_t>::max();
                    return true;
                case LT::INTEGER:
                    lo = std::numeric_limits<int32_t>::min();
                    hi = std::numeric_limits<int32_t>::max();
                    return true;
                case LT::BIGINT:
                    lo = std::numeric_limits<int64_t>::min();
                    hi = std::numeric_limits<int64_t>::max();
                    return true;
                case LT::HUGEINT: {
                    // numeric_limits may not be specialised for the custom int128; build
                    // the bounds from the bit pattern instead.
                    const types::uint128_t max_u = (types::uint128_t{1} << 127) - types::uint128_t{1};
                    hi = static_cast<types::int128_t>(max_u);
                    lo = static_cast<types::int128_t>(~max_u);
                    return true;
                }
                case LT::UTINYINT:
                    lo = 0;
                    hi = std::numeric_limits<uint8_t>::max();
                    return true;
                case LT::USMALLINT:
                    lo = 0;
                    hi = std::numeric_limits<uint16_t>::max();
                    return true;
                case LT::UINTEGER:
                    lo = 0;
                    hi = std::numeric_limits<uint32_t>::max();
                    return true;
                case LT::UBIGINT:
                    lo = 0;
                    hi = static_cast<types::int128_t>(std::numeric_limits<uint64_t>::max());
                    return true;
                default:
                    return false;
            }
        }

        types::logical_value_t
        make_integer_value(std::pmr::memory_resource* resource, types::logical_type t, types::int128_t v) {
            using LT = types::logical_type;
            switch (t) {
                case LT::TINYINT:
                    return types::logical_value_t(resource, static_cast<int8_t>(v));
                case LT::SMALLINT:
                    return types::logical_value_t(resource, static_cast<int16_t>(v));
                case LT::INTEGER:
                    return types::logical_value_t(resource, static_cast<int32_t>(v));
                case LT::BIGINT:
                    return types::logical_value_t(resource, static_cast<int64_t>(v));
                case LT::UTINYINT:
                    return types::logical_value_t(resource, static_cast<uint8_t>(v));
                case LT::USMALLINT:
                    return types::logical_value_t(resource, static_cast<uint16_t>(v));
                case LT::UINTEGER:
                    return types::logical_value_t(resource, static_cast<uint32_t>(v));
                case LT::UBIGINT:
                    return types::logical_value_t(resource, static_cast<uint64_t>(v));
                case LT::HUGEINT:
                default:
                    return types::logical_value_t(resource, v);
            }
        }

        // The digits of a numeric literal, whichever union slot the scanner used: the
        // caller has already established the tag is T_Integer or T_Float.
        std::string numeric_literal_text(Value* value) {
            if (nodeTag(value) == T_Integer) {
                return std::to_string(intVal(value));
            }
            return std::string{strVal(value)};
        }

        // A literal under a declared cast target, honoured EXACTLY or refused. `text`
        // carries the literal's digits/characters; `is_string_literal` distinguishes
        // '1.5'::int (PostgreSQL refuses) from 1.5::int (PostgreSQL rounds).
        core::result_wrapper_t<types::logical_value_t>
        cast_literal_text(std::pmr::memory_resource* resource,
                          const types::complex_logical_type& target,
                          const std::string& text,
                          bool is_string_literal) {
            using LT = types::logical_type;
            const LT t = target.type();
            types::int128_t lo{0};
            types::int128_t hi{0};
            if (integer_target_bounds(t, lo, hi)) {
                types::int128_t exact{0};
                switch (parse_exact_integer(text, exact)) {
                    case integer_text_t::exact:
                        break;
                    case integer_text_t::out_of_range:
                        return cast_out_of_range(resource, t, text);
                    case integer_text_t::not_an_integer: {
                        if (is_string_literal) {
                            // PostgreSQL: '1.5'::int is invalid input, not a rounding.
                            return invalid_cast_input(resource, t, "'" + text + "'");
                        }
                        // A NUMERIC literal rounds into an integer target (half away from
                        // zero) — parse_exact_decimal at scale 0 is exactly that rule, and
                        // it refuses exponents and garbage rather than guessing.
                        auto rounded = parse_exact_decimal(resource, text, types::DECIMAL_MAX_WIDTH, 0);
                        if (rounded.has_error()) {
                            return invalid_cast_input(resource, t, text);
                        }
                        exact = rounded.value();
                        break;
                    }
                }
                if (exact < lo || exact > hi) {
                    return cast_out_of_range(resource, t, text);
                }
                return make_integer_value(resource, t, exact);
            }
            switch (t) {
                case LT::DOUBLE:
                case LT::FLOAT: {
                    double parsed = 0.0;
                    if (!string_to_double(text.c_str(), text.size(), parsed)) {
                        return invalid_cast_input(resource,
                                                  t,
                                                  is_string_literal ? "'" + text + "'" : text);
                    }
                    if (t == LT::DOUBLE) {
                        return types::logical_value_t(resource, parsed);
                    }
                    return types::logical_value_t(resource, static_cast<float>(parsed));
                }
                case LT::DECIMAL: {
                    const auto* ext =
                        static_cast<const types::decimal_logical_type_extension*>(target.extension());
                    VALUE_OR_RETURN(auto scaled, parse_exact_decimal(resource, text, ext->width(), ext->scale()));
                    if (target.to_physical_type() == types::physical_type::INT64) {
                        return types::logical_value_t::create_decimal(resource,
                                                                      target,
                                                                      static_cast<int64_t>(scaled));
                    }
                    return types::logical_value_t::create_decimal(resource, target, scaled);
                }
                case LT::BOOLEAN: {
                    if (is_string_literal) {
                        // PostgreSQL's boolean literal words (the full spellings; 't' is
                        // what the grammar itself emits for TRUE).
                        std::string lowered(text);
                        std::transform(lowered.begin(), lowered.end(), lowered.begin(), [](unsigned char c) {
                            return static_cast<char>(std::tolower(c));
                        });
                        if (lowered == "t" || lowered == "true" || lowered == "y" || lowered == "yes" ||
                            lowered == "on" || lowered == "1") {
                            return types::logical_value_t(resource, true);
                        }
                        if (lowered == "f" || lowered == "false" || lowered == "n" || lowered == "no" ||
                            lowered == "off" || lowered == "0") {
                            return types::logical_value_t(resource, false);
                        }
                        return invalid_cast_input(resource, t, "'" + text + "'");
                    }
                    // An integer is a boolean by PostgreSQL's int -> bool rule (0 is false,
                    // anything else true); a fractional literal has no boolean cast at all.
                    types::int128_t exact{0};
                    switch (parse_exact_integer(text, exact)) {
                        case integer_text_t::exact:
                            return types::logical_value_t(resource, exact != types::int128_t{0});
                        case integer_text_t::out_of_range:
                            return types::logical_value_t(resource, true);
                        case integer_text_t::not_an_integer:
                            return invalid_cast_input(resource, t, text);
                    }
                    return invalid_cast_input(resource, t, text);
                }
                case LT::STRING_LITERAL:
                    return types::logical_value_t(resource, text);
                default:
                    return core::error_t(core::error_code_t::unimplemented_yet,
                                         std::pmr::string{"a literal cast to " + cast_target_name(t) +
                                                              " is not supported yet",
                                                          resource});
            }
        }

    } // namespace

    core::result_wrapper_t<types::logical_value_t> get_value(std::pmr::memory_resource* resource, Node* node) {
        switch (nodeTag(node)) {
            case T_TypeCast: {
                auto cast = pg_ptr_cast<TypeCast>(node);
                if (!cast->arg || nodeTag(cast->arg) != T_A_Const) {
                    // A cast collapses to a VALUE only over a literal. `CAST(x + 1 AS BIGINT)`
                    // carries an A_Expr, and reading that through an A_Const* lands on the
                    // operator node's `lexpr` POINTER: the answer was that pointer's bit
                    // pattern, identical on every row and different on every run. The operand
                    // has to be LOWERED (resolve_select_operand / the SELECT list), not folded,
                    // so anything that still asks this function for a constant gets a refusal
                    // rather than a number that means nothing.
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"a cast over a non-constant operand is not a constant value", resource});
                }
                auto constant = pg_ptr_cast<A_Const>(cast->arg);
                // A NULL literal under a CAST (`NULL::T`) is a typed NULL. Reading ival/fval of a T_Null
                // node yields a garbage non-null value — return an untyped NA null instead; the value
                // stays NULL via the vector validity mask and the projection resolves a concrete column
                // type (PG unknown->text) downstream.
                if (constant->val.type == T_Null) {
                    return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
                }
                // The DECLARED target decides the value's type, and two narrowings must stay
                // closed here: a refusal from get_type (NUMERIC without (width, scale), an
                // unsupported builtin) must not be swallowed into a plain string, and a
                // numeric literal must not ignore the target — otherwise CAST(1.5 AS INT)
                // answers a DOUBLE, CAST(1 AS DOUBLE PRECISION) an int64, and every
                // downstream type-matched comparison and storage decision is made against
                // the type the user did NOT write.
                VALUE_OR_RETURN(auto target_type, get_type(resource, cast->typeName));
                if (constant->val.type != T_String) {
                    if (constant->val.type != T_Integer && constant->val.type != T_Float) {
                        // T_BitString and friends keep a char* in the same union slot as
                        // `ival`; nothing below can read them honestly.
                        return core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"a literal cast over " + node_tag_to_string(constant->val.type) +
                                                 " is not supported",
                                             resource});
                    }
                    if (target_type.type() == types::logical_type::UNKNOWN) {
                        // A user-defined type: the literal travels in its parse-time shape
                        // and is reconciled against the type's definition downstream —
                        // the one legitimate passthrough (CREATE TYPE literals).
                        return numeric_literal_value(resource, &constant->val);
                    }
                    if (types::is_duration(target_type.type())) {
                        return core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"a numeric literal cannot be cast to " +
                                                 cast_target_name(target_type.type()),
                                             resource});
                    }
                    return cast_literal_text(resource,
                                             target_type,
                                             numeric_literal_text(&constant->val),
                                             /*is_string_literal=*/false);
                }
                std::string_view str = strVal(&constant->val);
                if (types::is_duration(target_type.type())) {
                    switch (target_type.type()) {
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
                            // is_duration covers exactly the six cases above; an is_duration
                            // type that is none of them would be a new enum member.
                            return core::error_t(core::error_code_t::sql_parse_error,
                                                 std::pmr::string{"unsupported duration cast target", resource});
                    }
                }
                if (target_type.type() == types::logical_type::UNKNOWN) {
                    // A user-defined type ('even'::oddness_t): the literal travels as its
                    // text and is reconciled against the type's definition downstream.
                    return types::logical_value_t(resource, std::string(str));
                }
                // Every remaining target is honoured exactly or refused: no handing
                // '123'::BIGINT back as the STRING "123", and a boolean accepts every word
                // PostgreSQL accepts, not just the grammar's own 't' spelling.
                return cast_literal_text(resource, target_type, std::string(str), /*is_string_literal=*/true);
            }
            case T_A_Const: {
                auto* value = &(pg_ptr_cast<A_Const>(node)->val);
                switch (nodeTag(value)) {
                    case T_String: {
                        std::string str = strVal(value);
                        return types::logical_value_t(resource, str);
                    }
                    case T_Integer: // fall-through
                    case T_Float:
                        return numeric_literal_value(resource, value);
                    case T_Null:
                        return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
                    default:
                        // T_BitString (`SELECT B'1010'`) and any future Value kind are refused
                        // BY NAME: a `break` here leaves BOTH switches and falls off the end
                        // of the function — no return value at all, UB.
                        return core::error_t(
                            core::error_code_t::sql_parse_error,
                            std::pmr::string{"unsupported constant kind: " + node_tag_to_string(nodeTag(value)),
                                             resource});
                }
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

        // A DECIMAL constant (a cast literal) has no parse-time arithmetic kernel;
        // letting it into sum/mult would hand back whatever the generic kernel
        // improvises. Refuse the expression, keep the exact-literal channel honest.
        if (left.value().type().type() == types::logical_type::DECIMAL ||
            right.value().type().type() == types::logical_type::DECIMAL) {
            return core::error_t(
                core::error_code_t::unimplemented_yet,
                std::pmr::string{"constant arithmetic over a DECIMAL literal is not supported yet", resource});
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

    namespace {
        // The referenced half of a FOREIGN KEY, identical whichever syntax spelled it:
        // `FOREIGN KEY (a) REFERENCES p (b)` names its referencing columns in fk_attrs,
        // `a bigint REFERENCES p (b)` names exactly the column it decorates. Everything
        // to the right of REFERENCES is the same Constraint node in both cases.
        void decode_fk_reference(const Constraint* constraint, table::table_constraint_t& tc) {
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
            // PostgreSQL stores ' ' / '\0' for unspecified MATCH/action; normalize to
            // SQL-standard defaults ('s' SIMPLE, 'a' NO ACTION) so downstream code never
            // sees an unexpected sentinel.
            if (constraint->fk_matchtype == 'f' || constraint->fk_matchtype == 'p' ||
                constraint->fk_matchtype == 's') {
                tc.fk_matchtype = constraint->fk_matchtype;
            }
            const auto da = constraint->fk_del_action;
            if (da == 'a' || da == 'r' || da == 'c' || da == 'n' || da == 'd') {
                tc.fk_del_action = da;
            }
            const auto ua = constraint->fk_upd_action;
            if (ua == 'a' || ua == 'r' || ua == 'c' || ua == 'n' || ua == 'd') {
                tc.fk_upd_action = ua;
            }
        }
    } // namespace

    namespace {

        // The refusals both constraint extractors share. A dropped constraint KIND and a
        // dropped constraint ATTRIBUTE are the same defect: the CREATE TABLE reports
        // success and the catalog holds less than what was written.
        core::error_t refuse_exclusion_constraint(std::pmr::memory_resource* resource) {
            return core::error_t(
                core::error_code_t::unimplemented_yet,
                std::pmr::string{"EXCLUDE constraints are not supported yet: the constraint would have been "
                                 "silently dropped",
                                 resource});
        }

        core::error_t refuse_constraint_attribute(std::pmr::memory_resource* resource, std::string_view spelling) {
            std::pmr::string msg{"the ", resource};
            msg.append(spelling.data(), spelling.size());
            msg += " constraint attribute is not supported yet: it would have been silently dropped";
            return core::error_t(core::error_code_t::unimplemented_yet, std::move(msg));
        }

        // Table-level DEFERRABLE / INITIALLY DEFERRED arrive as FIELDS on the constraint
        // node (processCASbits in gram.y), not as separate ATTR nodes the way a column's
        // do — so the kind switch alone cannot see them.
        core::error_t refuse_deferrability_fields(std::pmr::memory_resource* resource, const Constraint& constraint) {
            if (constraint.initdeferred) {
                return refuse_constraint_attribute(resource, "INITIALLY DEFERRED");
            }
            if (constraint.deferrable) {
                return refuse_constraint_attribute(resource, "DEFERRABLE");
            }
            return core::error_t::no_error();
        }

    } // namespace

    core::result_wrapper_t<std::vector<table::table_constraint_t>>
    extract_table_constraints(std::pmr::memory_resource* resource, PGList& table_elts) {
        std::vector<table::table_constraint_t> result;
        for (auto data : table_elts.lst) {
            if (nodeTag(data.data) != T_Constraint) {
                continue;
            }
            auto constraint = pg_ptr_cast<Constraint>(data.data);
            RETURN_IF_ERROR(refuse_deferrability_fields(resource, *constraint));
            table::table_constraint_t tc;
            if (constraint->conname) {
                tc.name = constraint->conname;
            }
            // Every ConstrType by name, no default: — under a `default: continue` a kind
            // this switch does not decide (`EXCLUDE (a WITH =)`) CREATEs the table with the
            // constraint absent and no diagnostic. A new enum member breaks the build here
            // instead.
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
                    decode_fk_reference(constraint, tc);
                    result.push_back(std::move(tc));
                    continue; // skip the unique-keys-based code below
                case CONSTR_CHECK:
                    tc.type = table::table_constraint_type::CHECK;
                    if (constraint->raw_expr) {
                        if (auto expr_res = deparse_check_expr(resource, constraint->raw_expr); expr_res.has_error()) {
                            return expr_res.convert_error<std::vector<table::table_constraint_t>>();
                        } else {
                            tc.check_expression = std::move(expr_res.value());
                        }
                    }
                    result.push_back(std::move(tc));
                    continue;
                case CONSTR_EXCLUSION:
                    return refuse_exclusion_constraint(resource);
                case CONSTR_NULL:
                case CONSTR_NOTNULL:
                case CONSTR_DEFAULT:
                    // Column properties; the grammar attaches them to a ColumnDef, never
                    // as a free-standing table element. Reaching here is a parser
                    // invariant break, and it must not silently drop the element.
                    return core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"a column property reached the table-constraint list", resource});
                case CONSTR_ATTR_DEFERRABLE:
                    return refuse_constraint_attribute(resource, "DEFERRABLE");
                case CONSTR_ATTR_DEFERRED:
                    return refuse_constraint_attribute(resource, "INITIALLY DEFERRED");
                case CONSTR_ATTR_NOT_DEFERRABLE:
                case CONSTR_ATTR_IMMEDIATE:
                    // These two RESTATE the default; nothing is narrowed by accepting them.
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

    core::result_wrapper_t<std::vector<table::table_constraint_t>>
    extract_column_constraints(std::pmr::memory_resource* resource, PGList& table_elts) {
        std::vector<table::table_constraint_t> result;
        for (auto data : table_elts.lst) {
            if (nodeTag(data.data) != T_ColumnDef) {
                continue;
            }
            auto coldef = pg_ptr_cast<ColumnDef>(data.data);
            if (!coldef->constraints || !coldef->colname) {
                continue;
            }
            const std::string colname{coldef->colname};
            for (auto cdata : coldef->constraints->lst) {
                auto constraint = pg_ptr_cast<Constraint>(cdata.data);
                RETURN_IF_ERROR(refuse_deferrability_fields(resource, *constraint));
                table::table_constraint_t tc;
                if (constraint->conname) {
                    tc.name = constraint->conname;
                }
                // Every ConstrType by name, no default: — of the eight kinds a
                // `default: continue` catches here only THREE really are column properties
                // get_column_definitions owns; the other five (EXCLUDE and the four
                // constraint attributes) would be dropped silently. A new enum member breaks
                // the build here instead.
                switch (constraint->contype) {
                    case CONSTR_PRIMARY:
                        tc.type = table::table_constraint_type::PRIMARY_KEY;
                        tc.columns.emplace_back(colname);
                        break;
                    case CONSTR_UNIQUE:
                        tc.type = table::table_constraint_type::UNIQUE;
                        tc.columns.emplace_back(colname);
                        break;
                    case CONSTR_FOREIGN:
                        tc.type = table::table_constraint_type::FOREIGN_KEY;
                        // The decorated column IS the referencing column list; the grammar
                        // leaves fk_attrs empty for this form.
                        tc.columns.emplace_back(colname);
                        decode_fk_reference(constraint, tc);
                        break;
                    case CONSTR_CHECK:
                        tc.type = table::table_constraint_type::CHECK;
                        if (constraint->raw_expr) {
                            if (auto expr_res = deparse_check_expr(resource, constraint->raw_expr);
                                expr_res.has_error()) {
                                return expr_res.convert_error<std::vector<table::table_constraint_t>>();
                            } else {
                                tc.check_expression = std::move(expr_res.value());
                            }
                        }
                        break;
                    case CONSTR_NULL:
                    case CONSTR_NOTNULL:
                    case CONSTR_DEFAULT:
                        // Column properties, not pg_constraint rows: NOT NULL and DEFAULT
                        // are read by get_column_definitions over this same list, and an
                        // explicit NULL restates the default. Skipping them here drops
                        // nothing.
                        continue;
                    case CONSTR_EXCLUSION:
                        return refuse_exclusion_constraint(resource);
                    case CONSTR_ATTR_DEFERRABLE:
                        // A column's attributes are separate list entries (ConstraintAttr
                        // in gram.y), not fields on the constraint they follow.
                        return refuse_constraint_attribute(resource, "DEFERRABLE");
                    case CONSTR_ATTR_DEFERRED:
                        return refuse_constraint_attribute(resource, "INITIALLY DEFERRED");
                    case CONSTR_ATTR_NOT_DEFERRABLE:
                    case CONSTR_ATTR_IMMEDIATE:
                        // These two RESTATE the default; nothing is narrowed by accepting
                        // them.
                        continue;
                }
                result.push_back(std::move(tc));
            }
        }
        return result;
    }

    namespace {

        // The refusal every shape below shares. `what` names the construct that was
        // written; the tail names what CAN be written instead.
        core::error_t unevaluable_check(std::pmr::memory_resource* resource, const std::string& what) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"CHECK constraint contains " + what +
                                                      "; allowed: a column compared (= <> < > <= >=) with a "
                                                      "constant, IS NULL / IS NOT NULL, and AND/OR/NOT of those",
                                                  resource});
        }

        // OPERAND position: exactly a column reference or a literal constant — the two
        // things the DML-time recogniser can address. `is_column` reports which of the
        // two was found, because a comparison it can evaluate needs one of each.
        core::result_wrapper_t<std::string>
        deparse_check_operand(std::pmr::memory_resource* resource, Node* node, bool* is_column) {
            *is_column = false;
            if (!node) {
                return unevaluable_check(resource, "a comparison with a missing operand");
            }
            switch (nodeTag(node)) {
                case T_ColumnRef: {
                    auto* cr = pg_ptr_cast<ColumnRef>(node);
                    if (!cr->fields || cr->fields->lst.empty()) {
                        return unevaluable_check(resource, "a column reference with no name");
                    }
                    // Use only the last field (unqualified column name)
                    *is_column = true;
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
                            return unevaluable_check(resource,
                                                     "a constant of unsupported type " +
                                                         node_tag_to_string(nodeTag(&ac->val)));
                    }
                }
                default:
                    // An arithmetic sub-expression lands here (`a + b > 0`), and it is the
                    // reason this position is restricted at all: the recogniser reads the
                    // whole left side as ONE column name, finds no such column, and compiles
                    // the constraint to TRUE.
                    return unevaluable_check(resource,
                                             "unsupported expression type " + node_tag_to_string(nodeTag(node)) +
                                                 " where a column or a constant is required");
            }
        }

        bool is_comparison_operator(std::string_view op) {
            return op == "=" || op == "<>" || op == "<" || op == ">" || op == "<=" || op == ">=";
        }

    } // namespace

    core::result_wrapper_t<std::string> deparse_check_expr(std::pmr::memory_resource* resource, Node* node) {
        if (!node) {
            return "";
        }
        switch (nodeTag(node)) {
            case T_ColumnRef:
                // A bare column in a CONDITION position (`CHECK (flag)`). The recogniser has
                // no comparison to build from it and would pass every row.
                return unevaluable_check(resource, "a bare column reference where a condition is required");
            case T_A_Const:
                // Likewise a bare constant (`CHECK (1)`): it judges no column.
                return unevaluable_check(resource, "a bare constant where a condition is required");
            case T_A_Expr: {
                auto* e = pg_ptr_cast<A_Expr>(node);
                if (e->kind == AEXPR_OP && e->name && !e->name->lst.empty()) {
                    std::string op = std::string(strVal(e->name->lst.front().data));
                    if (!is_comparison_operator(op)) {
                        return unevaluable_check(resource, "the operator \"" + op + "\" in a condition position");
                    }
                    bool left_is_column = false;
                    bool right_is_column = false;
                    auto left = deparse_check_operand(resource, e->lexpr, &left_is_column);
                    if (left.has_error()) {
                        return left;
                    }
                    auto right = deparse_check_operand(resource, e->rexpr, &right_is_column);
                    if (right.has_error()) {
                        return right;
                    }
                    if (left_is_column == right_is_column) {
                        // Column against column, or constant against constant. The first
                        // cannot be evaluated (the recogniser reads the second column NAME
                        // as the constant 0 and enforces something else entirely); the
                        // second judges no column at all.
                        return unevaluable_check(resource,
                                                 left_is_column ? "a comparison of two columns"
                                                                : "a comparison of two constants");
                    }
                    return std::move(left.value()) + " " + op + " " + std::move(right.value());
                }
                if (e->kind == AEXPR_AND || e->kind == AEXPR_OR) {
                    std::string sep = (e->kind == AEXPR_AND) ? " AND " : " OR ";
                    auto left = deparse_check_expr(resource, e->lexpr);
                    if (left.has_error()) {
                        return left;
                    }
                    auto right = deparse_check_expr(resource, e->rexpr);
                    if (right.has_error()) {
                        return right;
                    }
                    if (left.value().empty() || right.value().empty()) {
                        return unevaluable_check(resource, "an AND/OR with a missing side");
                    }
                    return "(" + std::move(left.value()) + ")" + sep + "(" + std::move(right.value()) + ")";
                }
                if (e->kind == AEXPR_NOT) {
                    // Unary NOT in A_Expr form: the operand is rexpr (lexpr is null). Emit the same
                    // "NOT (...)" shape that the BoolExpr NOT branch produces so the DML-time
                    // recogniser reads it back.
                    auto inner = deparse_check_expr(resource, e->rexpr);
                    if (inner.has_error()) {
                        return inner;
                    }
                    if (inner.value().empty()) {
                        return unevaluable_check(resource, "a NOT with no operand");
                    }
                    return "NOT (" + std::move(inner.value()) + ")";
                }
                // IN / BETWEEN / LIKE / DISTINCT FROM and the rest of the A_Expr kinds. Name
                // the kind: deparsing them to "" leaves every caller to guess "unsupported".
                return unevaluable_check(resource, "the expression kind " + expr_kind_to_string(e->kind));
            }
            case T_BoolExpr: {
                auto* b = pg_ptr_cast<BoolExpr>(node);
                if (!b->args || b->args->lst.empty()) {
                    return unevaluable_check(resource, "an AND/OR/NOT with no operands");
                }
                if (b->boolop == NOT_EXPR) {
                    auto inner = deparse_check_expr(resource, pg_ptr_cast<Node>(b->args->lst.front().data));
                    if (inner.has_error()) {
                        return inner;
                    }
                    if (inner.value().empty()) {
                        return unevaluable_check(resource, "a NOT with no operand");
                    }
                    return "NOT (" + std::move(inner.value()) + ")";
                }
                std::string sep = (b->boolop == AND_EXPR) ? " AND " : " OR ";
                std::string result;
                for (auto& cell : b->args->lst) {
                    auto part = deparse_check_expr(resource, pg_ptr_cast<Node>(cell.data));
                    if (part.has_error()) {
                        return part;
                    }
                    if (part.value().empty()) {
                        return unevaluable_check(resource, "an AND/OR with a missing side");
                    }
                    // Every operand is parenthesised and separated: the DML-time recogniser
                    // splits on a top-level ") AND (" / ") OR (", so a flat n-ary BoolExpr
                    // (`a AND b AND c` arrives as ONE node with three args) has to come back
                    // as that shape, not as a bare concatenation.
                    if (!result.empty()) {
                        result += sep;
                    }
                    result += "(" + std::move(part.value()) + ")";
                }
                return result;
            }
            case T_NullTest: {
                auto* nt = pg_ptr_cast<NullTest>(node);
                bool arg_is_column = false;
                auto arg = deparse_check_operand(resource, pg_ptr_cast<Node>(nt->arg), &arg_is_column);
                if (arg.has_error()) {
                    return arg;
                }
                if (!arg_is_column) {
                    // `5 IS NULL` names no column, so the recogniser finds none and passes
                    // every row. There is no row-dependent question here to enforce.
                    return unevaluable_check(resource, "an IS NULL / IS NOT NULL on something other than a column");
                }
                return std::move(arg.value()) + (nt->nulltesttype == IS_NULL ? " IS NULL" : " IS NOT NULL");
            }
            default:
                return unevaluable_check(resource,
                                         "unsupported expression type " + node_tag_to_string(nodeTag(node)));
        }
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
