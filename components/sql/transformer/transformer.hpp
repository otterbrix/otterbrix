#pragma once

#include "transform_result.hpp"
#include "utils.hpp"

#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/nodes/parsenodes.h>

namespace components::sql::parser {
    class parser_extension_registry_t;
} // namespace components::sql::parser

namespace components::sql::transform {
    class transformer {
    public:
        explicit transformer(std::pmr::memory_resource* resource,
                             const char* raw_sql = nullptr,
                             const parser::parser_extension_registry_t* extensions = nullptr)
            : resource_(resource)
            , raw_sql_(raw_sql)
            , extensions_(extensions)
            , parameter_map_(resource_)
            , parameter_insert_map_(resource_)
            , parameter_insert_rows_(resource_, {})
            , error_(core::error_t::no_error()) {}

        transform_result transform(Node& node);
        // Lower a single statement node to a plan node (the dispatch switch);
        // transform() wraps the result with parameter bookkeeping. Sets the
        // internal error and returns nullptr on failure. Subqueries are collected
        // into plan->sub_queries; extension nodes route through plan->parameters.
        logical_plan::node_ptr transform(Node& node, logical_plan::execution_plan_t* plan);

        // Parse a bare SQL expression string (e.g. "age > 0") as if it were a WHERE clause.
        // Used to compile stored CHECK constraint expressions for runtime evaluation.
        // Returns nullptr expr if unparseable. params holds constants referenced by parameter_id_t
        // inside the expression — caller must keep it alive for the lifetime of the predicate.
        struct check_expr_result {
            expressions::expression_ptr expr;
            logical_plan::parameter_node_ptr params;
        };
        check_expr_result parse_where_expr(const std::string& expr_text);

    private:
        bool has_error() const noexcept;

        logical_plan::node_ptr transform_create_database(CreatedbStmt& node);
        logical_plan::node_ptr transform_drop_database(DropdbStmt& node);
        logical_plan::node_ptr transform_checkpoint(CheckPointStmt& node);
        logical_plan::node_ptr transform_vacuum(VacuumStmt& node);
        logical_plan::node_ptr transform_create_table(CreateStmt& node);
        logical_plan::node_ptr transform_drop(DropStmt& node);
        logical_plan::node_ptr transform_select(SelectStmt& node, logical_plan::execution_plan_t* plan);
        logical_plan::node_ptr transform_update(UpdateStmt& node, logical_plan::execution_plan_t* plan);
        logical_plan::node_ptr transform_insert(InsertStmt& node, logical_plan::execution_plan_t* plan);
        logical_plan::node_ptr transform_delete(DeleteStmt& node, logical_plan::execution_plan_t* plan);
        logical_plan::node_ptr transform_create_index(IndexStmt& node);
        logical_plan::node_ptr transform_create_type(CompositeTypeStmt& node);
        logical_plan::node_ptr transform_create_enum_type(CreateEnumStmt& node);
        logical_plan::node_ptr transform_create_sequence(CreateSeqStmt& node);
        logical_plan::node_ptr transform_create_view(ViewStmt& node);
        // CREATE MATERIALIZED VIEW … AS SELECT … (PostgreSQL-canonical, relkind='m').
        // Body is transformed via transform_select; source's catalog_resolve_table
        // is hoisted to the outer sequence_t front so Pass 1 stamps source's
        // pg_attribute. The planner reads body_plan + stamped source metadata to
        // derive output schema before lowering to physical operators.
        logical_plan::node_ptr transform_create_matview(CreateTableAsStmt& cs, logical_plan::execution_plan_t* plan);
        // REFRESH MATERIALIZED VIEW [CONCURRENTLY] mv [WITH NO DATA].
        // Wrapped with catalog_resolve_table(mv) so Pass 1 stamps view_sql from
        // pg_rewrite.ev_action (already supported for relkind='m' by Phase A.A2).
        logical_plan::node_ptr transform_refresh_matview(RefreshMatViewStmt& rs);
        logical_plan::node_ptr transform_create_function(CreateFunctionStmt& node);
        // ALTER TABLE → node_alter_table_t. Multi-clause ALTER TABLE (multiple AT_AddColumn
        // etc) emits a sequence — currently only first command supported. RENAME TABLE not
        // here (T_RenameStmt routes separately).
        logical_plan::node_ptr transform_alter_table(AlterTableStmt& node);
        // RENAME COLUMN comes through T_RenameStmt with renameType=OBJECT_COLUMN.
        // Routes here from the top-level transform() switch.
        logical_plan::node_ptr transform_rename(RenameStmt& node);
        // BEGIN / COMMIT / ROLLBACK. Lowers to node_commit_transaction_t /
        // node_abort_transaction_t respectively; BEGIN returns nullptr (see
        // impl for rationale).
        logical_plan::node_ptr transform_transaction(TransactionStmt& node);
        logical_plan::node_ptr transform_set_timezone(VariableSetStmt& node);

    private:
        using insert_location_t = std::pair<size_t, std::string>; // position in vector + string key

        expressions::expression_ptr
        transform_a_expr(A_Expr* node, const name_collection_t& names, logical_plan::execution_plan_t* plan);

        expressions::expression_ptr
        transform_sublink_expr(SubLink* node, const name_collection_t& names, logical_plan::execution_plan_t* plan);

        // Arithmetic expression: returns scalar_expression_t
        expressions::expression_ptr transform_a_expr_arithmetic(A_Expr* node,
                                                                const name_collection_t& names,
                                                                logical_plan::parameter_node_t* params);

        // Resolve any node to param_storage for arithmetic operand
        expressions::param_storage
        transform_a_expr_operand(Node* node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        // Handle T_A_Expr in SELECT target list (may contain aggregates)
        void transform_select_a_expr(A_Expr* node,
                                     const char* alias,
                                     const name_collection_t& names,
                                     logical_plan::execution_plan_t* plan,
                                     logical_plan::node_ptr& group);

        // Parse a RETURNING target list (List* of ResTarget) into scalar
        // projection expressions. Supports column references (including * and
        // table.*), constants/parameters, and arithmetic, each with an optional
        // AS alias. On an unsupported construct sets error_ and returns the
        // partial result — callers must check error_, not the return value.
        std::pmr::vector<expressions::expression_ptr>
        transform_returning(List* returning_list, const name_collection_t& names, logical_plan::execution_plan_t* plan);

        // Resolve SELECT operand — aggregates become separate group expressions
        expressions::param_storage resolve_select_operand(Node* node,
                                                          const name_collection_t& names,
                                                          logical_plan::execution_plan_t* plan,
                                                          logical_plan::node_ptr& group);

        expressions::expression_ptr
        transform_a_expr_func(FuncCall* node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        // HAVING clause: resolve aggregate references to aliases from group node
        expressions::expression_ptr transform_having_expr(Node* node,
                                                          const name_collection_t& names,
                                                          logical_plan::execution_plan_t* plan,
                                                          const logical_plan::node_ptr& group);

        // Handle T_CaseExpr in SELECT target list
        void transform_select_case_expr(CaseExpr* node,
                                        const char* alias,
                                        const name_collection_t& names,
                                        logical_plan::execution_plan_t* plan,
                                        logical_plan::node_ptr& group);

        // Build a scalar_expression_ptr (type=case_expr) from a CaseExpr
        expressions::expression_ptr case_expr_to_scalar(CaseExpr* node,
                                                        const char* alias,
                                                        const name_collection_t& names,
                                                        logical_plan::execution_plan_t* plan,
                                                        logical_plan::node_ptr group);

        // Resolve a HAVING operand: FuncCall → aggregate alias key
        expressions::param_storage resolve_having_operand(Node* node,
                                                          const name_collection_t& names,
                                                          logical_plan::execution_plan_t* plan,
                                                          const logical_plan::node_ptr& group);

        expressions::expression_ptr transform_a_indirection(A_Indirection* node,
                                                            const name_collection_t& names,
                                                            logical_plan::execution_plan_t* plan);

        // --- JSONB navigation (-> ->> #> #>>) ----------------------------
        // Resolve a scalar (text-returning, ->> / #>>) jsonb navigation chain
        // into the single slash-joined column key it addresses (e.g.
        // `t -> 'a' ->> 'b'` -> key "a/b"). The chain collapses to one path:
        // the base operand (a bare table name contributes nothing/root, a
        // column contributes its name) followed by every operator's key(s).
        // On a table-returning top operator (-> / #>) in this scalar position,
        // or any malformed operand, sets error_ and returns false.
        bool resolve_jsonb_scalar_key(A_Expr* node, const name_collection_t& names, expressions::key_t& out_key);
        // Recursive worker: appends this chain's path segments (in order) and
        // sets `side` from the base operand. Accepts any nav operator.
        bool collect_jsonb_path(A_Expr* node,
                                const name_collection_t& names,
                                std::pmr::vector<std::pmr::string>& segments,
                                expressions::side_t& side);
        // Resolve the base (left-most) operand of a jsonb chain into its path
        // segments + side. A bare table name yields no segments (document root);
        // a column yields its name.
        bool resolve_jsonb_base(Node* lexpr,
                                const name_collection_t& names,
                                std::pmr::vector<std::pmr::string>& segments,
                                expressions::side_t& side);

        // Table-valued jsonb operators ('->','#>' expand; '-','#-' delete).
        // Collapse the chain into a single slash-joined prefix key (e.g. 'a/b').
        // Used in the SELECT list; validate_logical_plan turns the resulting
        // jsonb_expand / jsonb_delete expression into get_field columns.
        bool resolve_jsonb_prefix_key(A_Expr* node, const name_collection_t& names, expressions::key_t& out_key);
        // True if `node` is a bare identifier naming the FROM table/alias — i.e.
        // the document root. Distinguishes 't - x' (jsonb delete) from arithmetic.
        bool jsonb_lhs_is_table(Node* node, const name_collection_t& names) const;

        // jsonb key existence: '?' (one key), '?|' (any of), '?&' (all of).
        // Desugars each key to an IS NOT NULL test on the flattened path, then
        // combines with OR ('?'/'?|') or AND ('?&').
        expressions::expression_ptr transform_jsonb_exists(A_Expr* node,
                                                           const name_collection_t& names,
                                                           logical_plan::parameter_node_t* params,
                                                           std::string_view op);

        expressions::expression_ptr
        transform_null_test(NullTest* node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        logical_plan::node_ptr
        transform_function(RangeFunction& node, const name_collection_t& names, logical_plan::parameter_node_t* params);
        logical_plan::node_ptr
        transform_function(FuncCall& node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        // Build the logical node for a FROM-clause reference to a recursive CTE.
        // Returns an aggregate wrapping either a cte_scan (inside recursive member) or
        // a recursive_cte node (in the outer query). Returns nullptr on error.
        logical_plan::node_aggregate_ptr build_recursive_cte_ref(const std::string& cte_name,
                                                                 const std::string& effective_alias,
                                                                 logical_plan::execution_plan_t* plan);

        void join_dfs(std::pmr::memory_resource* resource,
                      JoinExpr* join,
                      logical_plan::node_join_ptr& node_join,
                      name_collection_t& names,
                      logical_plan::execution_plan_t* plan);

        expressions::update_expr_ptr
        transform_update_expr(Node* node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        std::string get_str_value(Node* node);

        core::parameter_id_t add_param_value(Node* node, logical_plan::parameter_node_t* params);

        std::pmr::memory_resource* resource_;
        const char* raw_sql_;
        const parser::parser_extension_registry_t* extensions_;
        std::pmr::unordered_map<size_t, core::parameter_id_t> parameter_map_;
        std::pmr::unordered_map<size_t, std::pmr::vector<insert_location_t>> parameter_insert_map_;
        vector::data_chunk_t parameter_insert_rows_;
        std::vector<deferred_limit_t> deferred_limits_;
        size_t aggregate_counter_{0};
        std::pmr::vector<expressions::expression_ptr> pending_internal_aggs_{resource_};
        std::pmr::unordered_map<std::string_view, SelectStmt*> cte_queries_{resource_};
        std::pmr::unordered_map<std::string, SelectStmt*> recursive_cte_queries_{resource_};
        bool transforming_recursive_member_{false};
        core::error_t error_;
    };
} // namespace components::sql::transform