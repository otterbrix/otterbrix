#include "transformer.hpp"
#include "utils.hpp"

#include <components/logical_plan/node_aggregate.hpp>

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
        auto params = logical_plan::make_parameter_node(resource_);
        logical_plan::node_ptr log_node;

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
                log_node = transform_select(pg_cast<SelectStmt>(node), params.get());
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
                log_node = transform_update(pg_cast<UpdateStmt>(node), params.get());
                break;
            case T_InsertStmt:
                log_node = transform_insert(pg_cast<InsertStmt>(node), params.get());
                break;
            case T_DeleteStmt:
                log_node = transform_delete(pg_cast<DeleteStmt>(node), params.get());
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
                    log_node = transform_create_matview(cs, params.get());
                } else {
                    error_ = core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{
                            "CREATE TABLE AS without MATERIALIZED — see docs/pr496-followups.md #4",
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
            default:
                error_ = core::error_t(
                    core::error_code_t::sql_parse_error,
                    std::pmr::string{"Unsupported node type: " + node_tag_to_string(node.type), resource_});
        }

        if (has_error()) {
            return {resource_, std::move(error_)};
        } else {
            return {resource_,
                    std::move(log_node),
                    std::move(params),
                    std::move(parameter_map_),
                    std::move(parameter_insert_map_),
                    std::move(parameter_insert_rows_)};
        }
    }

    bool transformer::has_error() const noexcept { return error_.contains_error(); }
} // namespace components::sql::transform