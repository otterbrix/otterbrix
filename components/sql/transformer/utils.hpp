#pragma once

#include <core/result_wrapper.hpp>

#include <components/catalog/results/ddl_result.hpp>
#include <components/expressions/forward.hpp>
#include <components/expressions/key.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/sql/parser/nodes/parsenodes.h>
#include <components/sql/parser/pg_functions.h>
#include <components/table/column_definition.hpp>
#include <components/table/constraint.hpp>
#include <components/types/types.hpp>
#include <string>
#include <utility>
#include <vector>

namespace components::sql::transform {
    inline constexpr size_t MAX_COLUMN_REF_SEGMENTS = 5;

#ifdef DEV_MODE
    void note_promoted_rows(uint64_t rows) noexcept;
    uint64_t insert_promote_rows() noexcept;
    void reset_insert_promote_rows() noexcept;
#endif
    template<class T>
    static T& pg_cast(Node& node) {
        return reinterpret_cast<T&>(node);
    }

    template<class T>
    static T* pg_ptr_cast(void* ptr) {
        return reinterpret_cast<T*>(ptr);
    }

    template<class T>
    static T* pg_ptr_assert_cast(void* ptr, [[maybe_unused]] NodeTag tag) {
        assert(nodeTag(ptr) == tag);
        return pg_ptr_cast<T>(ptr);
    }

    inline Node& pg_cell_to_node_cast(void* node) { return pg_cast<Node&>(*reinterpret_cast<Node*>(node)); }

    bool string_to_double(const char* buf, size_t len, double& result /*, char decimal_separator*/);

    inline std::string construct(const char* ptr) { return ptr ? ptr : std::string(); }

    inline std::string construct_alias(Alias* alias) { return alias ? construct(alias->aliasname) : std::string(); }

    core::result_wrapper_t<std::pmr::string> indices_to_str(std::pmr::memory_resource* resource, A_Indices* indices);

    // Refuses FuncCall decorations nothing downstream reads: OVER, VARIADIC, aggregate ORDER BY/WITHIN GROUP.
    core::error_t refuse_dropped_call_decorations(std::pmr::memory_resource* resource, const FuncCall& call);

    // The four name components a qualified table reference may carry; name resolution compares slots by position.
    struct qualified_name {
        std::string dbname;
        std::string relname;
        std::string schemaname;
        std::string uuid;

        bool empty() const noexcept { return dbname.empty() && relname.empty() && schemaname.empty() && uuid.empty(); }
    };

    inline qualified_name rangevar_to_qualified_name(RangeVar* table) {
        std::string dbname = construct(table->catalogname);
        std::string schema = construct(table->schemaname);
        std::string rel = construct(table->relname);
        std::string uuid = construct(table->uid);
        return {std::move(dbname), std::move(rel), std::move(schema), std::move(uuid)};
    }

    inline const std::string& visible_name(const qualified_name& name, const std::string& alias) noexcept {
        return alias.empty() ? name.relname : alias;
    }

    struct from_element_t {
        qualified_name name;
        std::string alias;

        const std::string& visible_name() const noexcept { return transform::visible_name(name, alias); }
    };

    struct column_ref_t;

    struct name_collection_t {
        qualified_name left_name;
        std::string left_alias;
        qualified_name right_name;
        std::string right_alias;
        std::vector<from_element_t> extra_left; // FROM elements that belong to left but came in through a nested join

        struct using_column_t {
            std::string name;
            logical_plan::join_type join;
        };
        std::vector<using_column_t> using_columns;
        const using_column_t* using_column(std::string_view name) const;

        bool is_left_table(const std::string& name) const;
        bool is_right_table(const std::string& name) const;

        core::result_wrapper_t<expressions::side_t> resolve(std::pmr::memory_resource* resource,
                                                            const column_ref_t& ref) const;

        core::error_t refuse_indistinguishable_elements(std::pmr::memory_resource* resource) const;
    };

    struct column_ref_t {
        std::string uid;
        std::string db;
        std::string schema;
        std::string table;
        expressions::key_t field;

        explicit column_ref_t(std::pmr::memory_resource* resource)
            : field(resource) {}
        explicit column_ref_t(expressions::key_t field)
            : field(std::move(field)) {}

        bool is_qualified() const noexcept { return !table.empty(); }
    };

    core::result_wrapper_t<column_ref_t>
    columnref_to_field(std::pmr::memory_resource* resource, ColumnRef* ref, const name_collection_t& names);
    core::result_wrapper_t<column_ref_t> indirection_to_field(std::pmr::memory_resource* resource,
                                                              A_Indirection* indirection,
                                                              const name_collection_t& names);
    core::result_wrapper_t<column_ref_t>
    node_to_field(std::pmr::memory_resource* resource, Node* node, const name_collection_t& names);

    inline logical_plan::join_type jointype_to_ql(JoinExpr* join) {
        switch (join->jointype) {
            case JOIN_FULL:
                return logical_plan::join_type::full;
            case JOIN_INNER:
                return (join->quals || (join->usingClause && !join->usingClause->lst.empty()))
                           ? logical_plan::join_type::inner
                           : logical_plan::join_type::cross;
            case JOIN_LEFT:
                return logical_plan::join_type::left;
            case JOIN_RIGHT:
                return logical_plan::join_type::right;
            default:
                return logical_plan::join_type::invalid;
        }
    }

    inline expressions::compare_type get_compare_type(std::string_view str) {
        static const std::unordered_map<std::string_view, expressions::compare_type> lookup = {
            {"==", expressions::compare_type::eq},
            {"=", expressions::compare_type::eq},
            {"!=", expressions::compare_type::ne},
            {"<>", expressions::compare_type::ne},
            {"<", expressions::compare_type::lt},
            {"<=", expressions::compare_type::lte},
            {">", expressions::compare_type::gt},
            {">=", expressions::compare_type::gte},
            {"regexp", expressions::compare_type::regex},
            {"~~", expressions::compare_type::regex}};

        if (auto it = lookup.find(str); it != lookup.end()) {
            return it->second;
        }

        return expressions::compare_type::invalid;
    }

    inline types::logical_type get_logical_type(std::string_view str) {
        static const std::unordered_map<std::string_view, types::logical_type> lookup = {
            {"int2", types::logical_type::SMALLINT},
            {"int4", types::logical_type::INTEGER},
            {"int8_t", types::logical_type::BIGINT},
            {"bool", types::logical_type::BOOLEAN},
            {"float4", types::logical_type::FLOAT},
            {"float8", types::logical_type::DOUBLE},
            {"bit", types::logical_type::BIT},
            {"numeric", types::logical_type::DECIMAL},

            {"double", types::logical_type::DOUBLE},
            {"tinyint", types::logical_type::TINYINT},
            {"hugeint", types::logical_type::HUGEINT},
            {"date", types::logical_type::DATE},
            {"time", types::logical_type::TIME},
            {"timetz", types::logical_type::TIME_TZ},
            {"timestamp", types::logical_type::TIMESTAMP},
            {"timestamptz", types::logical_type::TIMESTAMP_TZ},
            {"interval", types::logical_type::INTERVAL},
            {"blob", types::logical_type::BLOB},
            {"utinyint", types::logical_type::UTINYINT},
            {"usmallint", types::logical_type::USMALLINT},
            {"uinteger", types::logical_type::UINTEGER},
            {"uint", types::logical_type::UINTEGER},
            {"ubigint", types::logical_type::UBIGINT},
            {"uhugeint", types::logical_type::UHUGEINT},
            {"pointer", types::logical_type::POINTER},
            {"uuid", types::logical_type::UUID},
            {"string", types::logical_type::STRING_LITERAL},
            {"varchar", types::logical_type::STRING_LITERAL},
            {"text", types::logical_type::STRING_LITERAL},
            {"bpchar", types::logical_type::STRING_LITERAL},
        };

        if (auto it = lookup.find(str); it != lookup.end()) {
            return it->second;
        }

        return types::logical_type::UNKNOWN;
    }

    inline bool is_arithmetic_operator(std::string_view op) {
        return op == "+" || op == "-" || op == "*" || op == "/" || op == "%" || op == "&" || op == "|" || op == "#" ||
               op == "~" || op == "<<" || op == ">>";
    }

    // Peels every `+`-layer off an expression; returns nullptr for a malformed `+` with no operand.
    inline Node* strip_unary_plus(Node* node) {
        while (node && nodeTag(node) == T_A_Expr) {
            auto* plus = pg_ptr_cast<A_Expr>(node);
            if (plus->lexpr != nullptr || !plus->name || plus->name->lst.empty() ||
                std::string_view(strVal(plus->name->lst.front().data)) != "+") {
                break;
            }
            node = plus->rexpr;
        }
        return node;
    }

    inline expressions::scalar_type get_arithmetic_scalar_type(std::string_view op) {
        if (op == "+")
            return expressions::scalar_type::add;
        if (op == "-")
            return expressions::scalar_type::subtract;
        if (op == "*")
            return expressions::scalar_type::multiply;
        if (op == "/")
            return expressions::scalar_type::divide;
        if (op == "%")
            return expressions::scalar_type::mod;
        if (op == "&")
            return expressions::scalar_type::bit_and;
        if (op == "|")
            return expressions::scalar_type::bit_or;
        if (op == "#")
            return expressions::scalar_type::bit_xor;
        if (op == "~")
            return expressions::scalar_type::bit_not;
        if (op == "<<")
            return expressions::scalar_type::shift_left;
        if (op == ">>")
            return expressions::scalar_type::shift_right;
        return expressions::scalar_type::invalid;
    }

    enum class operator_fixity_t
    {
        infix,
        prefix,
        postfix
    };

    struct operator_function_t {
        std::string_view name;
        operator_fixity_t fixity;
    };

    operator_function_t operator_function(std::string_view op);

    // On a computing table (relkind='g'), nested fields flatten into one column named by the '/'-joined path.
    bool is_jsonb_nav_operator(std::string_view op);

    bool jsonb_nav_returns_scalar(std::string_view op);

    // True for operators whose right operand is a whole path, not a single key.
    bool jsonb_op_takes_path(std::string_view op);

    std::string node_tag_to_string(NodeTag type);
    std::string expr_kind_to_string(A_Expr_Kind type);

    // Slices the CHECK expression out of the raw SQL text rather than rebuilding it, so the round trip is exact.
    core::result_wrapper_t<std::string>
    slice_check_expression(std::pmr::memory_resource* resource, const char* raw_sql, int check_location);

    core::result_wrapper_t<types::complex_logical_type> get_type(std::pmr::memory_resource* resource, TypeName* type);
    core::result_wrapper_t<std::pmr::vector<types::complex_logical_type>> get_types(std::pmr::memory_resource* resource,
                                                                                    PGList& list);

    // A literal wider than the scanner's 32-bit `ival` arrives as T_Float digits, since atof would round it.
    enum class integer_text_t
    {
        not_an_integer, // a fraction or an exponent — a real float, atof is correct for it
        out_of_range,   // plain digits, but more magnitude than the widest integer we store
        exact           // the out-parameter holds it, digit for digit
    };

    // Sign + decimal digits only; int128 ceiling matches DECIMAL_MAX_WIDTH == 38.
    integer_text_t parse_exact_integer(std::string_view text, types::int128_t& out);

    // Unreachable by the signed reader (UHUGEINT's top half); a '-' sign reports out_of_range, not wrapping.
    integer_text_t parse_exact_unsigned_integer(std::string_view text, types::uint128_t& out);

    // value * 10^scale, rounding half-away-from-zero past `scale`; too many integer-part digits is a refusal.
    core::result_wrapper_t<types::int128_t>
    parse_exact_decimal(std::pmr::memory_resource* resource, std::string_view text, uint8_t width, uint8_t scale);

    // BIGINT/HUGEINT/UHUGEINT by magnitude, DOUBLE only if fractional; out-of-range is refused, not rounded.
    core::result_wrapper_t<types::logical_value_t> numeric_literal_value(std::pmr::memory_resource* resource,
                                                                         Value* value);

    // The digits of a fractional numeric literal; a DECIMAL-typed caller re-reads them with parse_exact_decimal.
    std::string_view fractional_literal_text(Node* node);

    core::result_wrapper_t<types::logical_value_t> get_value(std::pmr::memory_resource* resource, Node* node);
    core::result_wrapper_t<types::logical_value_t> get_array(std::pmr::memory_resource* resource, PGList* list);

    core::result_wrapper_t<types::logical_value_t> evaluate_const_a_expr(std::pmr::memory_resource* resource,
                                                                         A_Expr* node);

    // gram.y maps empty and RESTRICT both to DROP_RESTRICT -> restrict_ (PostgreSQL parity, #638).
    components::catalog::drop_behavior_t drop_behavior_of(DropBehavior written) noexcept;

    core::result_wrapper_t<std::vector<table::column_definition_t>>
    get_column_definitions(std::pmr::memory_resource* resource, PGList& table_elts);
    core::result_wrapper_t<std::vector<table::table_constraint_t>>
    extract_table_constraints(std::pmr::memory_resource* resource, PGList& table_elts, const char* raw_sql);

    core::result_wrapper_t<std::vector<table::table_constraint_t>>
    extract_column_constraints(std::pmr::memory_resource* resource, PGList& table_elts, const char* raw_sql);

    // catalog_resolves lives OUTSIDE the plan trees; entries dedupe across sub-queries naming the same table.
    enum class constraint_resolve_kind
    {
        none,
        outgoing,
        referencing,
        // No enforcement decode: DROP CONSTRAINT must not refuse on the catalog state it exists to repair.
        names_only
    };

    // Names a hand-built plan node's catalog target — what transform_* does for SQL-built plans.
    logical_plan::node_ptr
    name_catalog_target(const std::string& dbname, const std::string& relname, logical_plan::node_ptr node);

    // with_constraints gathers INSERT/UPDATE's outgoing or DELETE's referencing constraints.
    void register_catalog_resolve_table(std::pmr::memory_resource* resource,
                                        logical_plan::catalog_resolves_t* resolves,
                                        const std::string& dbname,
                                        const std::string& relname,
                                        constraint_resolve_kind with_constraints = constraint_resolve_kind::none);

    void register_catalog_resolve_types(std::pmr::memory_resource* resource,
                                        logical_plan::catalog_resolves_t* resolves,
                                        const std::vector<std::string>& type_names);

    void register_catalog_resolve_namespace(std::pmr::memory_resource* resource,
                                            logical_plan::catalog_resolves_t* resolves,
                                            const std::string& dbname);

    void register_catalog_resolve_tables(std::pmr::memory_resource* resource,
                                         logical_plan::catalog_resolves_t* resolves,
                                         const std::vector<std::pair<std::string, std::string>>& targets);

} // namespace components::sql::transform