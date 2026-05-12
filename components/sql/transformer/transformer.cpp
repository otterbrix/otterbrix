#include "transformer.hpp"
#include "utils.hpp"

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_join.hpp>
#include <components/sql/parser/parser.h>

namespace components::sql::transform {

    namespace {
        // Phase 13 T13: at the SELECT top-level call site we know the read
        // dependency is the FROM-clause table. Peek at the resulting aggregate/
        // join node to recover (dbname, relname) for the catalog-resolve wrap.
        // transform_select returns either a node_aggregate_t (single-table FROM)
        // or a node_aggregate_t whose first child is a join_t — same shape, so
        // pulling dbname/relname off the root aggregate is sufficient for the
        // primary table. Other joined-in tables are visible through the join
        // subtree; emitting one resolve per joined table is left as TODO (would
        // need a depth walk over the SELECT plan, which is out of scope for the
        // minimal P12 hookup).
        std::pair<std::string, std::string>
        select_primary_table_identity(const logical_plan::node_ptr& sel) {
            if (!sel) return {};
            using namespace logical_plan;
            if (sel->type() == node_type::aggregate_t) {
                const auto* agg = static_cast<const node_aggregate_t*>(sel.get());
                return {agg->dbname(), agg->relname()};
            }
            return {};
        }
    } // namespace

    transform_result transformer::transform(Node& node) {
        auto params = logical_plan::make_parameter_node(resource_);
        logical_plan::node_ptr log_node;

        // TODO: Error handling
        switch (node.type) {
            case T_CreatedbStmt: {
                auto& n = pg_cast<CreatedbStmt>(node);
                const std::string dbname = n.dbname ? std::string(n.dbname) : std::string{};
                log_node = transform_create_database(n);
                // Phase 13 T13: CREATE DATABASE → resolve the namespace name so
                // a later patch can use the resolve node to detect duplicates
                // through the pipeline instead of catalog_view's side-channel.
                log_node = maybe_wrap_with_catalog_resolve_namespace(
                    resource_, dbname, std::move(log_node));
                break;
            }
            case T_DropdbStmt: {
                auto& n = pg_cast<DropdbStmt>(node);
                const std::string dbname = n.dbname ? std::string(n.dbname) : std::string{};
                log_node = transform_drop_database(n);
                // Phase 13 T13: DROP DATABASE resolves the namespace before drop.
                log_node = maybe_wrap_with_catalog_resolve_namespace(
                    resource_, dbname, std::move(log_node));
                break;
            }
            case T_CreateStmt:
                // Phase 13 T13: CREATE TABLE — resolve target namespace AND
                // future table name (existence check / collision). transform_
                // create_table builds the dbname/relname into the resulting
                // node, but we don't introspect it here to keep the call-site
                // change small; rely on the planner's downstream namespace_oid
                // resolution for now.
                // TODO: add catalog_resolve_namespace wrap by reading the
                // RangeVar dbname out of the CreateStmt node directly.
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
                log_node = transform_select(pg_cast<SelectStmt>(node), params.get());
                // Phase 13 T13: stamp the primary FROM-clause table as a
                // catalog dependency. The transformer's aggregate wrapper at
                // the root carries the (dbname, relname); a future patch can
                // walk joins to add additional resolves.
                auto [db, rel] = select_primary_table_identity(log_node);
                if (!rel.empty()) {
                    log_node = maybe_wrap_with_catalog_resolve_table(
                        resource_, db, rel, std::move(log_node));
                }
                break;
            }
            case T_UpdateStmt:
                log_node = transform_update(pg_cast<UpdateStmt>(node), params.get());
                break;
            case T_InsertStmt:
                log_node = transform_insert(pg_cast<InsertStmt>(node), params.get());
                break;
            case T_DeleteStmt:
                log_node = transform_delete(pg_cast<DeleteStmt>(node), params.get());
                break;
            case T_IndexStmt:
                // Phase 13 T13: CREATE INDEX needs the parent table resolved.
                // TODO: pull (dbname, relname) out of IndexStmt.relation and
                // wrap; transform_create_index returns a node_create_index_t
                // whose children include the indexed table identity.
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
            case T_CreateFunctionStmt:
                log_node = transform_create_function(pg_cast<CreateFunctionStmt>(node));
                break;
            case T_AlterTableStmt:
                // Phase 13 T13: ALTER TABLE needs target table resolution.
                // TODO: read the AlterTableStmt.relation RangeVar and wrap.
                log_node = transform_alter_table(pg_cast<AlterTableStmt>(node));
                break;
            case T_RenameStmt:
                log_node = transform_rename(pg_cast<RenameStmt>(node));
                break;
            case T_TransactionStmt:
                log_node = transform_transaction(pg_cast<TransactionStmt>(node));
                break;
            default:
                throw std::runtime_error("Unsupported node type: " + node_tag_to_string(node.type));
        }

        return {std::move(log_node),
                std::move(params),
                std::move(parameter_map_),
                std::move(parameter_insert_map_),
                std::move(parameter_insert_rows_)};
    }

    transformer::check_expr_result transformer::parse_where_expr(const std::string& expr_text) {
        std::string wrapped = "SELECT 1 WHERE " + expr_text;
        std::pmr::monotonic_buffer_resource arena(resource_);
        auto* raw_list = raw_parser(&arena, wrapped.c_str());
        if (!raw_list || raw_list->lst.empty()) {
            return {};
        }
        auto* raw = linitial(raw_list);
        if (!raw || nodeTag(raw) != T_SelectStmt) {
            return {};
        }
        auto* sel = pg_ptr_cast<SelectStmt>(raw);
        if (!sel->whereClause) {
            return {};
        }
        auto params = logical_plan::make_parameter_node(resource_);
        name_collection_t empty_names;
        try {
            expressions::expression_ptr expr;
            if (nodeTag(sel->whereClause) == T_NullTest) {
                expr = transform_null_test(pg_ptr_cast<NullTest>(sel->whereClause),
                                           empty_names, params.get());
            } else if (nodeTag(sel->whereClause) == T_FuncCall) {
                expr = transform_a_expr_func(pg_ptr_cast<FuncCall>(sel->whereClause),
                                              empty_names, params.get());
            } else {
                expr = transform_a_expr(pg_ptr_cast<A_Expr>(sel->whereClause),
                                        empty_names, params.get());
            }
            if (!expr) return {};
            return {std::move(expr), std::move(params)};
        } catch (...) {
            return {};
        }
    }

} // namespace components::sql::transform
