#pragma once

#include "transform_result.hpp"
#include "utils.hpp"

#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/logical_plan/node_update.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/sql/parser/nodes/parsenodes.h>

#include <optional>

namespace components::sql::parser {
    class parser_extension_registry_t;
} // namespace components::sql::parser

namespace components::sql::transform {

    // There are some differences for expression parsing, depending on where it is placed
    enum class expression_placement_t
    {
        call,
        select,
        bind
    };

    struct expression_context_t {
        const name_collection_t& names;
        logical_plan::execution_plan_t* plan;

        expression_placement_t aggregates = expression_placement_t::call;
        logical_plan::node_ptr group = nullptr;
        const std::pmr::vector<expressions::expression_ptr>* group_keys = nullptr;
        // Temp placeholder, because documents still use casts as column selection
        bool cast_annotates_key = false;
        // Set when an operand is ARRAY(SELECT ...)
        bool* array_operand = nullptr;
    };

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
            , parameter_insert_rows_(resource_) {}

        transform_result transform(Node& node);
        // Lower a single statement node to a plan node (the dispatch switch);
        // transform() wraps the result with parameter bookkeeping. Subqueries are collected
        // into plan->sub_queries extension nodes route through plan->parameters.
        core::result_wrapper_t<logical_plan::node_ptr> transform(Node& node, logical_plan::execution_plan_t* plan);

        // Parse a bare SQL expression string (e.g. "age > 0") as if it were a WHERE clause.
        // Used to compile stored CHECK constraint expressions for runtime evaluation.
        // A CHECK is a predicate over one row, which is what a WHERE clause is, so it is parsed
        // as one: whatever the engine admits in a WHERE it admits in a CHECK, and there is no
        // second expression grammar to keep in step with the first.
        // params holds constants referenced by parameter_id_t inside the expression — the caller
        // must keep it alive for the lifetime of the predicate.
        struct check_expr_result {
            expressions::expression_ptr expr;
            logical_plan::parameter_node_ptr params;
        };
        // `params` lets several expressions share one parameter map, so the constants of every
        // CHECK on a table are addressed by distinct ids. A null one is created here.
        core::result_wrapper_t<check_expr_result> parse_where_expr(const std::string& expr_text,
                                                                   logical_plan::parameter_node_ptr params = nullptr);

    private:
        core::result_wrapper_t<logical_plan::node_ptr> transform_create_database(CreatedbStmt& node);
        core::result_wrapper_t<logical_plan::node_ptr> transform_drop_database(DropdbStmt& node);
        core::result_wrapper_t<logical_plan::node_ptr> transform_checkpoint(CheckPointStmt& node);
        core::result_wrapper_t<logical_plan::node_ptr> transform_vacuum(VacuumStmt& node);
        core::result_wrapper_t<logical_plan::node_ptr> transform_create_table(CreateStmt& node);
        core::result_wrapper_t<logical_plan::node_ptr> transform_drop(DropStmt& node);
        core::result_wrapper_t<logical_plan::node_ptr> transform_select(SelectStmt& node,
                                                                        logical_plan::execution_plan_t* plan);
        // Build a node_limit from a limitCount/limitOffset pair (nullptr when neither is present). A
        // ParamRef bound limit/offset is registered in deferred_limits_ (resolved later like the simple
        // SELECT path). Shared by the simple-select, the UNION tail-clause, the top-level VALUES, and
        // the DML (DELETE/UPDATE) LIMIT lowering — hence a raw (limitCount, limitOffset) pair rather
        // than a SelectStmt&.
        core::result_wrapper_t<logical_plan::node_ptr> build_limit_node(Node* limit_count,
                                                                        Node* limit_offset,
                                                                        const core::dbname_t& db,
                                                                        const core::relname_t& rel,
                                                                        logical_plan::execution_plan_t* plan);
        // Build the node_limit child for a DELETE/UPDATE ... [LIMIT n]. Returns an unlimited
        // limit node when limit_count is null; otherwise validates the count exactly like a
        // SELECT limit (integer / bound parameter). DML has NO OFFSET (grammar-enforced).
        core::result_wrapper_t<logical_plan::node_limit_ptr> build_dml_limit(Node* limit_count,
                                                                             const core::dbname_t& db,
                                                                             const core::relname_t& rel,
                                                                             logical_plan::execution_plan_t* plan);
        // Register a statement's WITH (CTE) definitions into cte_queries_ / recursive_cte_queries_ so the
        // body can reference them. Shared by SELECT (simple + UNION) and DML (DELETE/UPDATE/INSERT). A
        // data-modifying CTE (ctequery not a SELECT) is rejected cleanly (deferred). No-op on null.
        core::error_t register_with_ctes(WithClause* with_clause);
        core::result_wrapper_t<logical_plan::node_ptr> transform_update(UpdateStmt& node,
                                                                        logical_plan::execution_plan_t* plan);
        core::result_wrapper_t<logical_plan::node_ptr> transform_insert(InsertStmt& node,
                                                                        logical_plan::execution_plan_t* plan);
        core::result_wrapper_t<logical_plan::node_ptr> transform_delete(DeleteStmt& node,
                                                                        logical_plan::execution_plan_t* plan);
        core::result_wrapper_t<logical_plan::node_ptr> transform_create_index(IndexStmt& node);
        core::result_wrapper_t<logical_plan::node_ptr> transform_create_type(CompositeTypeStmt& node);
        core::result_wrapper_t<logical_plan::node_ptr> transform_create_enum_type(CreateEnumStmt& node);
        core::result_wrapper_t<logical_plan::node_ptr> transform_create_sequence(CreateSeqStmt& node);
        core::result_wrapper_t<logical_plan::node_ptr> transform_create_view(ViewStmt& node);
        // CREATE MATERIALIZED VIEW … AS SELECT … (PostgreSQL-canonical, relkind='m').
        // Body is transformed via transform_select; source's catalog_resolve_table
        // is hoisted to the outer sequence_t front so Pass 1 stamps source's
        // pg_attribute. The planner reads body_plan + stamped source metadata to
        // derive output schema before lowering to physical operators.
        core::result_wrapper_t<logical_plan::node_ptr> transform_create_matview(CreateTableAsStmt& cs,
                                                                                logical_plan::execution_plan_t* plan);
        // REFRESH MATERIALIZED VIEW [CONCURRENTLY] mv [WITH NO DATA].
        // Wrapped with catalog_resolve_table(mv) so Pass 1 stamps view_sql from
        // pg_rewrite.ev_action (already supported for relkind='m' by Phase A.A2).
        core::result_wrapper_t<logical_plan::node_ptr> transform_refresh_matview(RefreshMatViewStmt& rs);
        core::result_wrapper_t<logical_plan::node_ptr> transform_create_function(CreateFunctionStmt& node);
        // ALTER TABLE → node_alter_table_t. Multi-clause ALTER TABLE (multiple AT_AddColumn
        // etc) emits a sequence — currently only first command supported. RENAME TABLE not
        // here (T_RenameStmt routes separately).
        core::result_wrapper_t<logical_plan::node_ptr> transform_alter_table(AlterTableStmt& node,
                                                                             logical_plan::execution_plan_t* plan);
        // RENAME COLUMN comes through T_RenameStmt with renameType=OBJECT_COLUMN.
        // Routes here from the top-level transform() switch.
        core::result_wrapper_t<logical_plan::node_ptr> transform_rename(RenameStmt& node);
        // BEGIN / COMMIT / ROLLBACK; unsupported variants (SAVEPOINT / 2PC)
        // return nullptr (see impl).
        core::result_wrapper_t<logical_plan::node_ptr> transform_transaction(TransactionStmt& node);
        core::result_wrapper_t<logical_plan::node_ptr> transform_set_timezone(VariableSetStmt& node);
        // EXPLAIN / EXPLAIN ANALYZE: read the `analyze` option, restrict the inner to
        // SELECT/INSERT/UPDATE/DELETE, stamp plan->explain, and lower the inner so sub_queries.back()
        // stays the real query node. Output formatting is a host concern (the executor's renderer
        // registry, selected per-query by execution_plan_t::explain_render_id).
        core::result_wrapper_t<logical_plan::node_ptr> transform_explain(ExplainStmt& node,
                                                                         logical_plan::execution_plan_t* plan);

    private:
        using insert_location_t = std::pair<size_t, std::string>; // position in vector + string key

        core::result_wrapper_t<expressions::expression_ptr>
        transform_a_expr(A_Expr* node, const name_collection_t& names, logical_plan::execution_plan_t* plan);

        core::result_wrapper_t<expressions::expression_ptr>
        transform_predicate(Node* node, const name_collection_t& names, logical_plan::execution_plan_t* plan);

        core::result_wrapper_t<expressions::expression_ptr>
        transform_sublink_expr(SubLink* node, const name_collection_t& names, logical_plan::execution_plan_t* plan);

        // Arithmetic expression: returns scalar_expression_t
        core::result_wrapper_t<expressions::expression_ptr>
        transform_a_expr_arithmetic(A_Expr* node, const name_collection_t& names, logical_plan::execution_plan_t* plan);

        core::result_wrapper_t<expressions::param_storage> transform_expression(Node* node,
                                                                                const expression_context_t& context);
        // transform_expression wraps it with the grouping-key substitution
        // so every operand, at every nesting level, meets the same rule.
        core::result_wrapper_t<expressions::param_storage>
        transform_expression_impl(Node* node, const expression_context_t& context);
        // The name the group emits its n-th computed key under.
        static std::string group_key_alias(size_t index);

        // A param_storage in expression position.
        expressions::expression_ptr as_expression(expressions::param_storage operand);

        // Resolve any node to param_storage for arithmetic operand
        core::result_wrapper_t<expressions::param_storage>
        transform_a_expr_operand(Node* node, const name_collection_t& names, logical_plan::execution_plan_t* plan);

        core::result_wrapper_t<expressions::expression_ptr>
        lower_operator_function(A_Expr* node,
                                std::string_view op,
                                operator_function_t function,
                                const expression_context_t& context);

        // Handle T_A_Expr in SELECT target list (may contain aggregates)
        core::error_t transform_select_a_expr(A_Expr* node,
                                              const char* alias,
                                              const name_collection_t& names,
                                              logical_plan::execution_plan_t* plan,
                                              logical_plan::node_ptr& group);

        // Parse a RETURNING target list (List* of ResTarget) into scalar
        // projection expressions. Supports column references (including * and
        // table.*), constants/parameters, and arithmetic, each with an optional
        // AS alias.
        core::result_wrapper_t<std::pmr::vector<expressions::expression_ptr>>
        transform_returning(List* returning_list, const name_collection_t& names, logical_plan::execution_plan_t* plan);

        // Resolve SELECT operand — aggregates become separate group expressions
        core::result_wrapper_t<expressions::param_storage>
        resolve_select_operand(Node* node,
                               const name_collection_t& names,
                               logical_plan::execution_plan_t* plan,
                               logical_plan::node_ptr& group,
                               const std::pmr::vector<expressions::expression_ptr>* group_keys = nullptr);

        core::result_wrapper_t<expressions::expression_ptr>
        transform_a_expr_func(FuncCall* node, const name_collection_t& names, logical_plan::execution_plan_t* plan);

        // Lower an aggregate FILTER (WHERE p) clause by wrapping each aggregate argument in a CASE:
        //   agg(x)   FILTER (WHERE p)  ->  agg(CASE WHEN p THEN x END)
        //   count(*) FILTER (WHERE p)  ->  count(CASE WHEN p THEN 1 END)
        // Every supported aggregate skips NULLs, so rows where p is not TRUE (the CASE yields NULL)
        // are excluded -- exactly FILTER semantics -- and the predicate reuses the three-valued
        // CASE-WHEN evaluator (UNKNOWN excludes the row). `agg_filter` is FuncCall.agg_filter; a null
        // one returns `args` unchanged.
        core::result_wrapper_t<std::pmr::vector<expressions::param_storage>>
        apply_aggregate_filter(Node* agg_filter,
                               std::pmr::vector<expressions::param_storage> args,
                               const name_collection_t& names,
                               logical_plan::execution_plan_t* plan);

        // HAVING clause: resolve aggregate references to aliases from group node
        core::result_wrapper_t<expressions::expression_ptr>
        transform_having_expr(Node* node,
                              const name_collection_t& names,
                              logical_plan::execution_plan_t* plan,
                              const logical_plan::node_ptr& group,
                              const std::pmr::vector<expressions::expression_ptr>* group_keys);

        // Handle T_CaseExpr in SELECT target list
        core::error_t transform_select_case_expr(CaseExpr* node,
                                                 const char* alias,
                                                 const name_collection_t& names,
                                                 logical_plan::execution_plan_t* plan,
                                                 logical_plan::node_ptr& group);

        // Build a scalar_expression_ptr (type=case_expr) from a CaseExpr
        core::result_wrapper_t<expressions::expression_ptr> case_expr_to_scalar(CaseExpr* node,
                                                                                const char* alias,
                                                                                const name_collection_t& names,
                                                                                logical_plan::execution_plan_t* plan,
                                                                                logical_plan::node_ptr group);

        // Resolve a HAVING operand: FuncCall → aggregate alias key
        core::result_wrapper_t<expressions::param_storage>
        resolve_having_operand(Node* node,
                               const name_collection_t& names,
                               logical_plan::execution_plan_t* plan,
                               const logical_plan::node_ptr& group,
                               const std::pmr::vector<expressions::expression_ptr>* group_keys);

        core::result_wrapper_t<expressions::expression_ptr>
        transform_a_indirection(A_Indirection* node,
                                const name_collection_t& names,
                                logical_plan::execution_plan_t* plan);

        // --- JSONB navigation (-> ->> #> #>>) ----------------------------
        // Resolve a scalar (text-returning, ->> / #>>) jsonb navigation chain
        // into the single slash-joined column key it addresses (e.g.
        // `t -> 'a' ->> 'b'` -> key "a/b"). The chain collapses to one path:
        // the base operand (a bare table name contributes nothing/root, a
        // column contributes its name) followed by every operator's key(s).
        // On a table-returning top operator (-> / #>) in this scalar position,
        // or any malformed operand, answers with a refusal.
        core::result_wrapper_t<expressions::key_t> resolve_jsonb_scalar_key(A_Expr* node,
                                                                            const name_collection_t& names);
        // Recursive worker: appends this chain's path segments (in order) and
        // sets `side` from the base operand. Accepts any nav operator.
        core::error_t collect_jsonb_path(A_Expr* node,
                                         const name_collection_t& names,
                                         std::pmr::vector<std::pmr::string>& segments,
                                         expressions::side_t& side);
        // Resolve the base (left-most) operand of a jsonb chain into its path
        // segments + side. A bare table name yields no segments (document root);
        // a column yields its name.
        core::error_t resolve_jsonb_base(Node* lexpr,
                                         const name_collection_t& names,
                                         std::pmr::vector<std::pmr::string>& segments,
                                         expressions::side_t& side);

        // Table-valued jsonb operators ('->','#>' expand; '-','#-' delete).
        // Collapse the chain into a single slash-joined prefix key (e.g. 'a/b').
        // Used in the SELECT list; validate_logical_plan turns the resulting
        // jsonb_expand / jsonb_delete expression into get_field columns.
        core::result_wrapper_t<expressions::key_t> resolve_jsonb_prefix_key(A_Expr* node,
                                                                            const name_collection_t& names);
        // True if `node` is a bare identifier naming the FROM table/alias — i.e.
        // the document root. Distinguishes 't - x' (jsonb delete) from arithmetic.
        bool jsonb_lhs_is_table(Node* node, const name_collection_t& names) const;

        // jsonb key existence: '?' (one key), '?|' (any of), '?&' (all of).
        // Desugars each key to an IS NOT NULL test on the flattened path, then
        // combines with OR ('?'/'?|') or AND ('?&').
        core::result_wrapper_t<expressions::expression_ptr>
        transform_jsonb_exists(A_Expr* node,
                               const name_collection_t& names,
                               logical_plan::parameter_node_t* params,
                               std::string_view op);

        core::result_wrapper_t<expressions::expression_ptr> transform_null_test(NullTest* node,
                                                                                const expression_context_t& context);

        core::result_wrapper_t<logical_plan::node_ptr>
        transform_function(RangeFunction& node, const name_collection_t& names, logical_plan::parameter_node_t* params);
        core::result_wrapper_t<logical_plan::node_ptr>
        transform_function(FuncCall& node, const name_collection_t& names, logical_plan::parameter_node_t* params);

        // Build the logical node for a FROM-clause table function that is the right
        // side of a join. A column-ref argument references the outer (left) relation,
        // making the function implicitly LATERAL: each such argument is lowered to an
        // outer-bound parameter and recorded as a correlation on `node_join` (which is
        // marked lateral), so the lateral join operator rebinds it per outer row.
        core::result_wrapper_t<logical_plan::node_ptr> transform_from_function(RangeFunction& node,
                                                                               const name_collection_t& names,
                                                                               logical_plan::node_join_ptr& node_join,
                                                                               logical_plan::execution_plan_t* plan);

        // Build the logical node for a FROM-clause reference to a recursive CTE.
        // Returns an aggregate wrapping either a cte_scan (inside recursive member) or
        // a recursive_cte node (in the outer query). Returns nullptr on error.
        core::result_wrapper_t<logical_plan::node_aggregate_ptr>
        build_recursive_cte_ref(const std::string& cte_name,
                                const std::string& effective_alias,
                                logical_plan::execution_plan_t* plan);

        core::error_t join_dfs(std::pmr::memory_resource* resource,
                               JoinExpr* join,
                               logical_plan::node_join_ptr& node_join,
                               name_collection_t& names,
                               logical_plan::execution_plan_t* plan);

        // Build the source relation for a FROM/USING clause: a single table, a
        // (possibly LATERAL) join tree, a table function, or a derived table. A
        // comma-separated list is folded into a left-deep cross-join first. Returns
        // an aggregate wrapping the source, and populates `names` with the source's
        // left/right relations for predicate side-resolution. Shared by SELECT's
        // FROM, UPDATE's FROM, and DELETE's USING. `from_items` must be a non-empty List.
        core::result_wrapper_t<logical_plan::node_aggregate_ptr>
        transform_from_source(List* from_items, name_collection_t& names, logical_plan::execution_plan_t* plan);

        core::result_wrapper_t<expressions::expression_ptr>
        transform_update_expr(Node* node, const name_collection_t& names, logical_plan::execution_plan_t* plan);

        core::result_wrapper_t<std::string> get_str_value(Node* node);

        core::result_wrapper_t<core::parameter_id_t> add_param_value(Node* node,
                                                                     logical_plan::parameter_node_t* params);

        core::result_wrapper_t<logical_plan::node_ptr> transform_from_element(Node* item,
                                                                              qualified_name& slot_name,
                                                                              std::string& slot_alias,
                                                                              name_collection_t& names,
                                                                              logical_plan::node_join_ptr& node_join,
                                                                              logical_plan::execution_plan_t* plan);

        // While transforming the body of a LATERAL subquery, a column reference
        // qualified by an OUTER-scope relation (and not shadowed by an inner one) is
        // lowered to a correlated parameter: allocate one parameter per distinct outer
        // column key, record it as a correlation on lateral_join_ (marked lateral), and
        // return its id so callers emit the parameter in place of the column. The
        // lateral join operator rebinds each such parameter from the outer row before
        // re-running the inner sub-plan. Returns nullopt outside a LATERAL body or when
        // `ref` is an ordinary in-scope column.
        std::optional<core::parameter_id_t> try_lateral_correlate(ColumnRef* ref, const name_collection_t& inner_names);

        // Non-mutating predicate: true when `ref` is a qualified column that names an
        // OUTER-scope relation inside a LATERAL body (i.e. try_lateral_correlate would
        // correlate it). Used to reject a correlated column in a SELECT projection,
        // which the projection operator cannot re-read per outer row (constants are
        // bound once at plan build) — see create_plan_select.
        bool references_lateral_outer(ColumnRef* ref, const name_collection_t& inner_names) const;

        std::pmr::memory_resource* resource_;
        const char* raw_sql_;
        const parser::parser_extension_registry_t* extensions_;
        std::pmr::unordered_map<size_t, core::parameter_id_t> parameter_map_;
        std::pmr::unordered_map<size_t, std::pmr::vector<insert_location_t>> parameter_insert_map_;
        std::pmr::vector<vector::data_chunk_t> parameter_insert_rows_;
        std::vector<deferred_limit_t> deferred_limits_;
        size_t aggregate_counter_{0};
        std::pmr::vector<expressions::expression_ptr> pending_internal_aggs_{resource_};
        std::pmr::unordered_map<std::string_view, SelectStmt*> cte_queries_{resource_};
        std::pmr::unordered_map<std::string, SelectStmt*> recursive_cte_queries_{resource_};
        bool transforming_recursive_member_{false};

        // Every catalog lookup the statement depends on, accumulated across all
        // sub-queries and moved onto the execution_plan_t at the end of transform()
        logical_plan::catalog_resolves_t catalog_resolves_;

        // TODO: wrapp expressions in resolve node, and it won't be needed
        std::vector<std::string> cast_type_names_;

        void note_cast_type(const types::complex_logical_type& target);

        // LATERAL subquery correlation scope. Non-null only while transforming a
        // LATERAL subquery body. lateral_outer_names_ is the outer relation scope the
        // subquery may correlate against; lateral_join_ is the join the correlations
        // attach to; lateral_correlation_map_ dedups one parameter per outer key.
        const name_collection_t* lateral_outer_names_{nullptr};
        logical_plan::node_join_t* lateral_join_{nullptr};
        logical_plan::execution_plan_t* lateral_plan_{nullptr};
        std::pmr::unordered_map<std::string, core::parameter_id_t> lateral_correlation_map_{resource_};
    };
} // namespace components::sql::transform
