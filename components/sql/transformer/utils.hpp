#pragma once

#include <core/result_wrapper.hpp>

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
    // Test-observable count of ROWS rewritten by promote_column while parsing an INSERT.
    // Widening a column's type rebuilds every row already filled in that chunk, cell by cell
    // through logical_value_t. Widenings per column are bounded by the type lattice, so this
    // measures whether the cost really is quadratic or linear with a large constant.
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

    std::pmr::string indices_to_str(std::pmr::memory_resource* resource, A_Indices* indices);

    // Role-named DTO produced by the transformer when reading a RangeVar
    // (table reference) out of the parser AST. Carries each of the four name
    // components a SQL parser may attach to a qualified table reference:
    // optional uid, optional catalog (database), optional schema, and the
    // relation (table) name. Each slot holds what the grammar put there and
    // nothing else — name resolution compares slots by position, so a value
    // moved between slots is a wrong answer, not a convenience.
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
        // The parser produces one RangeVar shape per arity, and both productions
        // that build one (qualified_name and makeRangeVarFromAnyName) agree:
        //   `tbl`               -> catalogname="",  schemaname=""
        //   `db.tbl`            -> catalogname=db,  schemaname=""
        //   `db.schema.tbl`     -> catalogname=db,  schemaname=schema
        //   `uid.db.schema.tbl` -> uid=uid, catalogname=db, schemaname=schema
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
            // postgres built-ins
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

    // Unary plus is the identity (SQLite semantics: "+X is equivalent to X"): peel every
    // `+`-layer off an expression and return what remains. A unary operator parses as
    // A_Expr{op, lexpr = NULL, rexpr = operand}. Returns nullptr for a malformed `+` with
    // no operand — callers must reject that.
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

    // Prefix/postfix operators that are spelled as function calls internally:
    //   ^ -> pow   |/ -> sqrt   ||/ -> cbrt   ! -> factorial   @ -> abs
    inline std::string_view operator_function_name(std::string_view op) {
        if (op == "^")
            return "pow";
        if (op == "|/")
            return "sqrt";
        if (op == "||/")
            return "cbrt";
        if (op == "!")
            return "factorial";
        if (op == "@")
            return "abs";
        return {};
    }

    // --- JSONB operators -------------------------------------------------
    // Path-navigation jsonb operators. On a computing table (relkind='g')
    // nested fields are flattened into a single column whose name is the
    // path joined by '/', so navigation reduces to building that joined key.
    //   ->  /  #>   return jsonb  -> a (sub)table (relation position only)
    //   ->> / #>>   return text   -> a typed scalar value (SELECT/WHERE)
    // '#>'/'#>>' take a whole path on the right ('{a,b}' or dotted 'a.b').
    bool is_jsonb_nav_operator(std::string_view op);

    // True for the scalar (text-returning) variants usable in SELECT/WHERE.
    bool jsonb_nav_returns_scalar(std::string_view op);

    // True for operators whose right operand is a whole path ('{a,b}' / 'a.b'),
    // not a single key — '#>', '#>>' (navigation) and '#-' (delete by path).
    bool jsonb_op_takes_path(std::string_view op);

    std::string node_tag_to_string(NodeTag type);
    std::string expr_kind_to_string(A_Expr_Kind type);

    // Deparse a CHECK constraint raw expression node back to SQL text.
    // Handles: column refs, integer/float/string constants, comparison operators,
    // AND/OR/NOT, IS NULL / IS NOT NULL. Returns "" for unsupported node types.
    core::result_wrapper_t<std::string> deparse_check_expr(std::pmr::memory_resource* resource, Node* node);

    core::result_wrapper_t<types::complex_logical_type> get_type(std::pmr::memory_resource* resource, TypeName* type);
    core::result_wrapper_t<std::pmr::vector<types::complex_logical_type>> get_types(std::pmr::memory_resource* resource,
                                                                                    PGList& list);

    core::result_wrapper_t<types::logical_value_t> get_value(std::pmr::memory_resource* resource, Node* node);
    core::result_wrapper_t<types::logical_value_t> get_array(std::pmr::memory_resource* resource, PGList* list);

    // Evaluate constant arithmetic expression at parse time (e.g., 10 * 5 in INSERT VALUES)
    core::result_wrapper_t<types::logical_value_t> evaluate_const_a_expr(std::pmr::memory_resource* resource,
                                                                         A_Expr* node);

    core::result_wrapper_t<std::vector<table::column_definition_t>>
    get_column_definitions(std::pmr::memory_resource* resource, PGList& table_elts);
    core::result_wrapper_t<std::vector<table::table_constraint_t>>
    extract_table_constraints(std::pmr::memory_resource* resource, PGList& table_elts);

    // Transformer catalog-resolve emission.
    //
    // The transformer records every catalog lookup the statement depends on into
    // the plan's `catalog_resolves`, which lives OUTSIDE the plan trees. Nothing
    // is wrapped: the consumer node stays the sub-query root. Entries dedupe, so
    // the same table named by several sub-queries is resolved once.

    // Register (dbname, relname) — plus its namespace, and, when `with_constraints`
    // is set, the constraint gather for that table (INSERT/UPDATE → outgoing,
    // DELETE → referencing). An empty dbname/relname skips the corresponding entry.
    enum class constraint_resolve_kind
    {
        none,
        outgoing,
        referencing
    };

    // Name a hand-built plan node's catalog target — what the transform_* functions
    // do for SQL-built plans. Plans assembled directly through the C++/C API never
    // went through the transformer, and the executor registers a catalog lookup for
    // every target the tree NAMES, so naming is all such a plan has to do.
    // Returns the node, so it drops straight into an execution_plan_t.
    logical_plan::node_ptr
    name_catalog_target(const std::string& dbname, const std::string& relname, logical_plan::node_ptr node);

    void register_catalog_resolve_table(std::pmr::memory_resource* resource,
                                        logical_plan::catalog_resolves_t* resolves,
                                        const std::string& dbname,
                                        const std::string& relname,
                                        constraint_resolve_kind with_constraints = constraint_resolve_kind::none);

    void register_catalog_resolve_types(std::pmr::memory_resource* resource,
                                        logical_plan::catalog_resolves_t* resolves,
                                        const std::vector<std::string>& type_names);

    // Register a database-scoped DDL target (CREATE DATABASE, DROP DATABASE,
    // CREATE TYPE, ...): its namespace only.
    void register_catalog_resolve_namespace(std::pmr::memory_resource* resource,
                                            logical_plan::catalog_resolves_t* resolves,
                                            const std::string& dbname);

    // Multi-target form for DDL that touches several tables in one statement
    // (CREATE CONSTRAINT FK with ref_table, DROP INDEX with parent table + index).
    void register_catalog_resolve_tables(std::pmr::memory_resource* resource,
                                         logical_plan::catalog_resolves_t* resolves,
                                         const std::vector<std::pair<std::string, std::string>>& targets);

} // namespace components::sql::transform