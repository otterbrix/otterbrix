#include "transformer.hpp"
#include "utils.hpp"

#include <components/logical_plan/node_aggregate.hpp>
#include <components/sql/parser/extension.hpp>

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
    } // namespace

    transform_result transformer::transform(Node& node) {
        logical_plan::execution_plan_t plan(resource_);

        plan.sub_queries.emplace_back(transform(node, &plan));

        if (has_error()) {
            return {resource_, std::move(error_)};
        } else {
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

    bool transformer::has_error() const noexcept { return error_.contains_error(); }
} // namespace components::sql::transform