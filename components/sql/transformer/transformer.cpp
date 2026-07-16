#include "transformer.hpp"
#include "utils.hpp"

#include <components/logical_plan/execution_plan.hpp>
#include <components/logical_plan/node_aggregate.hpp>
#include <components/sql/parser/extension.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::sql::transform {

    namespace {
        // At the SELECT top-level we know the read dependency is the FROM-clause
        // table. transform_select returns a node_aggregate_t (single-table FROM)
        // or one whose first child is a join_t — same shape, so pulling
        // dbname/relname off the root aggregate is sufficient for the primary
        // table. TODO: emit one resolve per joined table (depth walk over the
        // SELECT plan).
        std::pair<std::string, std::string> select_primary_table_identity(const logical_plan::node_ptr& sel) {
            if (!sel)
                return {};
            using namespace logical_plan;
            if (sel->type() == node_type::aggregate_t) {
                const auto* agg = static_cast<const node_aggregate_t*>(sel.get());
                return {static_cast<const std::string&>(agg->dbname()),
                        static_cast<const std::string&>(agg->relname())};
            }
            return {};
        }

        // --- SORT ELIMINATION for a provably-unobservable sub-query ORDER BY ---------------
        //
        // A flattened sub-query root is either the aggregate itself (no FROM identity) or a
        // resolve-wrapping sequence_t (resolves at the front, the aggregate last). Unwrap to
        // the aggregate; return nullptr when the root is not an aggregate (nothing to strip).
        // Local mirror of dispatcher::effective_root_node to avoid a components->services dep.
        logical_plan::node_t* subquery_aggregate_root(const logical_plan::node_ptr& root) {
            using logical_plan::node_type;
            if (!root) {
                return nullptr;
            }
            logical_plan::node_t* n = root.get();
            if (n->type() == node_type::sequence_t) {
                const auto& kids = n->children();
                if (kids.empty() || !kids.front() ||
                    kids.front()->type() != node_type::catalog_resolve_t) {
                    return nullptr; // planner-style sequence_t, not a transformer resolve wrap
                }
                n = nullptr;
                for (auto it = kids.rbegin(); it != kids.rend(); ++it) {
                    if (*it && (*it)->type() != node_type::catalog_resolve_t) {
                        n = it->get();
                        break;
                    }
                }
            }
            return (n && n->type() == node_type::aggregate_t) ? n : nullptr;
        }

        // Remove a bare ORDER BY (a childless sort_t marker in the aggregate's flat pipeline)
        // when NO limit_t child is present. A LIMIT/OFFSET child makes the sort a top-N: the
        // ordering is then observable and the sort is kept. No-op if there is no sort child.
        void strip_bare_sort_child(logical_plan::node_t* agg) {
            using logical_plan::node_type;
            auto& kids = agg->children();
            for (const auto& c : kids) {
                if (c && c->type() == node_type::limit_t) {
                    return; // top-N / OFFSET window — the sort feeds an observable order
                }
            }
            for (auto it = kids.begin(); it != kids.end(); ++it) {
                if (*it && (*it)->type() == node_type::sort_t) {
                    // The flat aggregate model builds a childless sort marker. If a sort ever
                    // carried a pipeline child, re-splicing it is out of scope — keep the sort.
                    if (!(*it)->children().empty()) {
                        return;
                    }
                    kids.erase(it);
                    return;
                }
            }
        }

        // True when a compacted sub-query result is INDEPENDENT of the order of its rows:
        // EXISTS (bool), a scalar (single value — or a >1-row error either way), and IN / ANY /
        // ALL membership (array). Array-EQUALITY (`col = ARRAY(SELECT ...)`) is order-SENSITIVE
        // (element-wise compare against a positional array) and is deliberately excluded.
        bool order_insensitive_compaction(const logical_plan::id_result_mapping& m) {
            if (m.compacter == &components::vector::compact_to_bool_value ||
                m.compacter == &components::vector::compact_to_single_value) {
                return true;
            }
            return m.compacter == &components::vector::compact_to_array_value && !m.array_equality;
        }

        // Post-transform pass: for every flattened sub-query whose result is compacted through
        // an order-insensitive path, strip its bare (limit-less) ORDER BY. sub_query_results[i]
        // maps 1:1 to sub_queries[i] (the trailing sub_queries entry is the main query and has
        // no mapping — never touched, so a TOP-LEVEL ORDER BY is preserved).
        void eliminate_unobservable_subquery_sorts(logical_plan::execution_plan_t& plan) {
            const std::size_t n = plan.sub_query_results.size();
            for (std::size_t i = 0; i < n && i < plan.sub_queries.size(); ++i) {
                if (!order_insensitive_compaction(plan.sub_query_results[i])) {
                    continue;
                }
                if (auto* agg = subquery_aggregate_root(plan.sub_queries[i])) {
                    strip_bare_sort_child(agg);
                }
            }
        }
    } // namespace

    transform_result transformer::transform(Node& node) {
        logical_plan::execution_plan_t plan(resource_);

        plan.sub_queries.emplace_back(transform(node, &plan));

        if (has_error()) {
            return {resource_, std::move(error_)};
        } else {
            // Strip a provably-unobservable ORDER BY from every order-insensitively compacted
            // sub-query (IN / ANY / ALL / EXISTS / scalar) before the plan is finalized.
            eliminate_unobservable_subquery_sorts(plan);
            return {resource_,
                    std::move(plan),
                    std::move(parameter_map_),
                    std::move(parameter_insert_map_),
                    std::move(parameter_insert_rows_),
                    std::move(deferred_limits_)};
        }
    }

    logical_plan::node_ptr transformer::transform(Node& node, logical_plan::execution_plan_t* plan) {
        logical_plan::node_ptr log_node = nullptr;
        switch (node.type) {
            case T_CreatedbStmt: {
                auto& n = pg_cast<CreatedbStmt>(node);
                const std::string dbname = n.dbname ? std::string(n.dbname) : std::string{};
                log_node = transform_create_database(n);
                // Resolve the namespace name so a later patch can use the
                // resolve node to detect duplicates through the pipeline.
                log_node = maybe_wrap_with_catalog_resolve_namespace(resource_, dbname, std::move(log_node));
                break;
            }
            case T_DropdbStmt: {
                auto& n = pg_cast<DropdbStmt>(node);
                const std::string dbname = n.dbname ? std::string(n.dbname) : std::string{};
                log_node = transform_drop_database(n);
                log_node = maybe_wrap_with_catalog_resolve_namespace(resource_, dbname, std::move(log_node));
                break;
            }
            case T_CreateStmt:
                // Wrap is inside transform_create_table (mirrors DML pattern).
                log_node = transform_create_table(pg_cast<CreateStmt>(node));
                break;
            case T_DropStmt:
                log_node = transform_drop(pg_cast<DropStmt>(node));
                // TODO: DROP TABLE/INDEX/etc need per-removeType resolve wrap
                // (resolve_table or resolve_namespace). Out of scope for the
                // minimal hookup — transform_drop has 6 branches.
                break;
            case T_CompositeTypeStmt:
                log_node = transform_create_type(pg_cast<CompositeTypeStmt>(node));
                break;
            case T_CreateEnumStmt:
                log_node = transform_create_enum_type(pg_cast<CreateEnumStmt>(node));
                break;
            case T_SelectStmt: {
                log_node = transform_select(pg_cast<SelectStmt>(node), plan);
                // Stamp the primary FROM-clause table as a catalog dependency.
                // The transformer's aggregate wrapper at the root carries the
                // (dbname, relname); a future patch can walk joins to add
                // additional resolves.
                auto [db, rel] = select_primary_table_identity(log_node);
                if (!rel.empty()) {
                    log_node = maybe_wrap_with_catalog_resolve_table(resource_, db, rel, std::move(log_node));
                }
                break;
            }
            case T_UpdateStmt:
                log_node = transform_update(pg_cast<UpdateStmt>(node), plan);
                break;
            case T_InsertStmt:
                log_node = transform_insert(pg_cast<InsertStmt>(node), plan);
                break;
            case T_DeleteStmt:
                log_node = transform_delete(pg_cast<DeleteStmt>(node), plan);
                break;
            case T_ExplainStmt:
                log_node = transform_explain(pg_cast<ExplainStmt>(node), plan);
                break;
            case T_IndexStmt:
                // TODO: CREATE INDEX needs the parent table resolved — pull
                // (dbname, relname) out of IndexStmt.relation and wrap.
                log_node = transform_create_index(pg_cast<IndexStmt>(node));
                break;
            case T_CheckPointStmt:
                log_node = transform_checkpoint(pg_cast<CheckPointStmt>(node));
                break;
            case T_VacuumStmt:
                log_node = transform_vacuum(pg_cast<VacuumStmt>(node));
                break;
            case T_CreateSeqStmt:
                log_node = transform_create_sequence(pg_cast<CreateSeqStmt>(node));
                break;
            case T_ViewStmt:
                log_node = transform_create_view(pg_cast<ViewStmt>(node));
                break;
            case T_CreateTableAsStmt: {
                auto& cs = pg_cast<CreateTableAsStmt>(node);
                if (cs.relkind == OBJECT_MATVIEW) {
                    log_node = transform_create_matview(cs, plan);
                } else {
                    error_ = core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"CREATE TABLE AS without MATERIALIZED — see docs/pr496-followups.md #4",
                                         resource_});
                }
                break;
            }
            case T_RefreshMatViewStmt:
                log_node = transform_refresh_matview(pg_cast<RefreshMatViewStmt>(node));
                break;
            case T_CreateFunctionStmt:
                log_node = transform_create_function(pg_cast<CreateFunctionStmt>(node));
                break;
            case T_AlterTableStmt:
                // TODO: ALTER TABLE needs target table resolution — read the
                // AlterTableStmt.relation RangeVar and wrap.
                log_node = transform_alter_table(pg_cast<AlterTableStmt>(node));
                break;
            case T_RenameStmt:
                log_node = transform_rename(pg_cast<RenameStmt>(node));
                break;
            case T_TransactionStmt:
                log_node = transform_transaction(pg_cast<TransactionStmt>(node));
                break;
            case T_VariableSetStmt: {
                auto& set_stmt = pg_cast<VariableSetStmt>(node);
                std::string_view var_name = set_stmt.name ? set_stmt.name : "";
                if (var_name == "timezone") {
                    log_node = transform_set_timezone(set_stmt);
                } else {
                    error_ = core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"SET " + std::string(var_name) + " is not supported", resource_});
                }
                break;
            }
            case T_ExtensionNode: {
                // route to the owning extension's transform stage by extension_id
                auto& ext_node = pg_cast<ExtensionNode>(node);
                const std::string id = ext_node.extension_id ? ext_node.extension_id : "";
                const auto* extension = extensions_ ? extensions_->find(id) : nullptr;
                if (extension != nullptr && extension->transform != nullptr) {
                    log_node = extension->transform(resource_, &ext_node, plan->parameters.get());
                } else {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,
                                           std::pmr::string{"no transformer extension for '" + id + "'", resource_});
                }
                break;
            }
            default:
                error_ = core::error_t(
                    core::error_code_t::sql_parse_error,
                    std::pmr::string{"Unsupported node type: " + node_tag_to_string(node.type), resource_});
        }

        return log_node;
    }

    logical_plan::node_ptr transformer::transform_explain(ExplainStmt& node,
                                                          logical_plan::execution_plan_t* plan) {
        // EXPLAIN ANALYZE is signalled by an "analyze" DefElem in the options list. No style/format
        // option is read from SQL — output formatting is a host C++ concern (the executor's renderer
        // registry, selected per-query by execution_plan_t::explain_render_id).
        bool is_analyze = false;
        if (node.options) {
            for (auto data : node.options->lst) {
                auto* def = pg_ptr_cast<DefElem>(data.data);
                if (def && def->defname && std::string_view{def->defname} == "analyze") {
                    // ANALYZE carries an optional boolean (PostgreSQL defGetBoolean semantics): a
                    // missing arg is the bare `EXPLAIN ANALYZE` (enabled), an integer is nonzero-is-true,
                    // and a string is true/on/1 vs false/off/0. `EXPLAIN (ANALYZE false)` MUST stay
                    // plan-only so the inner DML is never executed.
                    if (def->arg == nullptr) {
                        is_analyze = true;
                    } else if (nodeTag(def->arg) == T_Integer) {
                        is_analyze = intVal(def->arg) != 0;
                    } else if (nodeTag(def->arg) == T_String) {
                        const std::string_view v{strVal(def->arg)};
                        is_analyze = !(v == "false" || v == "off" || v == "0" || v == "no" || v == "f" ||
                                       v == "n");
                    } else {
                        is_analyze = true;
                    }
                    break;
                }
            }
        }
        // Only read/DML inner statements are supported — the renderer walks the physical plan the
        // transformer already lowers for these; DDL/other inners are rejected.
        if (!node.query) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"EXPLAIN requires a statement", resource_});
            return nullptr;
        }
        switch (node.query->type) {
            case T_SelectStmt:
            case T_InsertStmt:
            case T_UpdateStmt:
            case T_DeleteStmt:
                break;
            default:
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"EXPLAIN of this statement is not supported", resource_});
                return nullptr;
        }
        plan->explain = is_analyze ? logical_plan::explain_type::analyze : logical_plan::explain_type::plan;
        // Lower the inner statement normally so sub_queries.back() stays the real query node.
        return transform(*node.query, plan);
    }

    bool transformer::has_error() const noexcept { return error_.contains_error(); }
} // namespace components::sql::transform