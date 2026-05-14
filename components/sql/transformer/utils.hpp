#pragma once

#include <components/expressions/forward.hpp>
#include <components/expressions/key.hpp>
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

    // Phase 10.E: role-named DTO produced by the transformer when reading a RangeVar
    // (table reference) out of the parser AST. Replaces the historical
    // `rangevar_to_collection` -> cfn output and the `rangevar_to_dbname_relname` /
    // `cfn_dbname` interim helpers added in 10.D. Carries each of the four name
    // components a SQL parser may attach to a qualified table reference: optional
    // catalog (database), optional schema, the relation (table) name, and an optional
    // uid. dbname picker logic (catalog with schema as fallback) is applied at
    // construction so callers see a single role-named field.
    struct qualified_name {
        std::string dbname;
        std::string relname;
        std::string schemaname;
        std::string uuid;

        bool empty() const noexcept {
            return dbname.empty() && relname.empty() && schemaname.empty() && uuid.empty();
        }
    };

    inline qualified_name rangevar_to_qualified_name(RangeVar* table) {
        std::string dbname = construct(table->catalogname);
        std::string schema = construct(table->schemaname);
        std::string rel = construct(table->relname);
        std::string uuid = construct(table->uid);
        // The parser produces several RangeVar shapes:
        //   `tbl`            -> catalogname="", schemaname=""
        //   `db.tbl`         -> catalogname=db, schemaname=""
        //   `schema.tbl`     -> catalogname="", schemaname=schema
        //                       (from makeRangeVarFromAnyName: ALTER/RENAME paths)
        //   `db.schema.tbl`  -> catalogname=db, schemaname=schema
        //   `uid.db.schema.tbl` -> uid=uid, catalogname=db, schemaname=schema
        // Pre-10.E callers ran the picker `cfn.database.empty() ? cfn.schema :
        // cfn.database` at every use site while keeping cfn.schema unchanged.
        // Mirror that here: pick dbname once, but leave schemaname intact so
        // factories that consume both fields (create_collection, create_index,
        // drop_collection, drop_index) keep the original parser-display schema.
        if (dbname.empty()) {
            dbname = schema;
        }
        return {std::move(dbname), std::move(rel), std::move(schema), std::move(uuid)};
    }

    struct name_collection_t {
        qualified_name left_name;
        std::string left_alias;
        qualified_name right_name;
        std::string right_alias;

        bool is_left_table(const std::string& name) const;
        bool is_right_table(const std::string& name) const;
    };

    expressions::side_t deduce_side(const name_collection_t& names, const std::string& target_name);

    struct column_ref_t {
        std::string table;
        expressions::key_t field;

        explicit column_ref_t(std::pmr::memory_resource* resource)
            : field(resource) {}
        column_ref_t(std::string table, expressions::key_t field)
            : table(std::move(table))
            , field(std::move(field)) {}
        void deduce_side(const name_collection_t& names);
    };

    column_ref_t
    columnref_to_field(std::pmr::memory_resource* resource, ColumnRef* ref, const name_collection_t& names);
    column_ref_t indirection_to_field(std::pmr::memory_resource* resource,
                                      A_Indirection* indirection,
                                      const name_collection_t& names);

    inline logical_plan::join_type jointype_to_ql(JoinExpr* join) {
        switch (join->jointype) {
            case JOIN_FULL:
                return logical_plan::join_type::full;
            case JOIN_INNER:
                return join->quals ? logical_plan::join_type::inner : logical_plan::join_type::cross;
            case JOIN_LEFT:
                return logical_plan::join_type::left;
            case JOIN_RIGHT:
                return logical_plan::join_type::right;
            default:
                throw parser_exception_t{"unsupported join type", ""};
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

        throw parser_exception_t{"Unknown comparison operator: " + std::string(str), ""};
    }

    inline bool is_arithmetic_operator(std::string_view op) {
        return op == "+" || op == "-" || op == "*" || op == "/" || op == "%";
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
        throw parser_exception_t{"Unknown arithmetic operator: " + std::string(op), ""};
    }

    std::string node_tag_to_string(NodeTag type);
    std::string expr_kind_to_string(A_Expr_Kind type);
    std::string like_to_regex(const std::string& pattern);

    // Deparse a CHECK constraint raw expression node back to SQL text.
    // Handles: column refs, integer/float/string constants, comparison operators,
    // AND/OR/NOT, IS NULL / IS NOT NULL. Returns "" for unsupported node types.
    std::string deparse_check_expr(Node* node);

    types::complex_logical_type get_type(TypeName* type);
    std::vector<types::complex_logical_type> get_types(PGList& list);
    std::pmr::vector<types::complex_logical_type> get_types(std::pmr::memory_resource* resource, PGList& list);

    types::logical_value_t get_value(std::pmr::memory_resource* resource, Node* node);
    types::logical_value_t get_array(std::pmr::memory_resource* resource, PGList* list);

    // Evaluate constant arithmetic expression at parse time (e.g., 10 * 5 in INSERT VALUES)
    types::logical_value_t evaluate_const_a_expr(std::pmr::memory_resource* resource, A_Expr* node);

    void fill_column_definitions(std::vector<table::column_definition_t>& out,
                                 std::pmr::memory_resource* resource,
                                 PGList& table_elts);
    std::vector<table::table_constraint_t> extract_table_constraints(PGList& table_elts);

    // Phase 13 T13 — transformer catalog-resolve emission.
    //
    // The transformer wraps a main DML/DDL `node_ptr` in
    // `sequence_t(catalog_resolve_*_t..., main_node)` so the planner can
    // treat catalog resolution as a first-class pipeline dependency.
    // Phase 13 (2026-05-13): wrap is unconditional — the previous
    // `transformer_emit_catalog_resolve_enabled()` toggle was removed
    // after the dispatcher/executor/planner cascade learned to descend
    // through the sequence_t wrapper.

    // Wrap `main_node` (an INSERT/SELECT/UPDATE/DELETE-style consumer that
    // targets a specific (dbname, relname)) in
    //   sequence_t(catalog_resolve_namespace_t, catalog_resolve_table_t,
    //              [catalog_resolve_constraint_t,] main_node)
    // Empty dbname/relname skips the corresponding resolve node. When
    // `with_constraints` is set, a catalog_resolve_constraint_t with the
    // matching direction is appended right after the resolve_table (used for
    // INSERT/UPDATE → outgoing, DELETE → referencing).
    enum class constraint_resolve_kind { none, outgoing, referencing };

    logical_plan::node_ptr maybe_wrap_with_catalog_resolve_table(
        std::pmr::memory_resource* resource,
        const std::string& dbname,
        const std::string& relname,
        logical_plan::node_ptr main_node,
        constraint_resolve_kind with_constraints = constraint_resolve_kind::none);

    // Wrap `main_node` (a database-scoped DDL — CREATE DATABASE, DROP DATABASE,
    // CREATE TYPE, etc.) in
    //   sequence_t(catalog_resolve_namespace_t, main_node)
    // when the toggle is enabled.
    logical_plan::node_ptr maybe_wrap_with_catalog_resolve_namespace(
        std::pmr::memory_resource* resource,
        const std::string& dbname,
        logical_plan::node_ptr main_node);

    // Multi-target wrap: prepends a catalog_resolve_namespace for every distinct
    // dbname in `targets`, then a catalog_resolve_table for each (dbname,
    // relname) pair. Used by DDL transformers that touch multiple tables in a
    // single statement (CREATE CONSTRAINT FK with ref_table, DROP INDEX with
    // parent table + index name).
    logical_plan::node_ptr maybe_wrap_with_catalog_resolve_tables(
        std::pmr::memory_resource* resource,
        std::vector<std::pair<std::string, std::string>> targets,
        logical_plan::node_ptr main_node);

} // namespace components::sql::transform