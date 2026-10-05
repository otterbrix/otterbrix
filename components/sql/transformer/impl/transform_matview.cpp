#include "view_body_text.hpp"

#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_view.hpp>
#include <components/logical_plan/node_refresh_matview.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {
    core::result_wrapper_t<logical_plan::node_ptr>
    transformer::transform_create_matview(CreateTableAsStmt& cs, logical_plan::execution_plan_t* plan) {
        if (!cs.query || cs.query->type != T_SelectStmt) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"CREATE MATERIALIZED VIEW requires a SELECT body", resource_});
        }
        if (!cs.into || !cs.into->rel) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"CREATE MATERIALIZED VIEW missing target relation", resource_});
        }

        // WITH DATA (PostgreSQL's default) is refused: nothing here populates a matview at
        // CREATE time — accepting it would silently report success with an empty matview. WITH NO
        // DATA (skipData, gram.y CreateMatViewStmt: `$5->skipData = !($8)`) works, and REFRESH
        // MATERIALIZED VIEW fills it.
        if (!cs.into->skipData) {
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"CREATE MATERIALIZED VIEW ... WITH DATA is not supported yet: the matview "
                                 "cannot be populated at CREATE time, so the result would be a silently empty "
                                 "matview. Write WITH NO DATA and fill it with REFRESH MATERIALIZED VIEW.",
                                 resource_});
        }

        // The column names come from the body; a list naming them otherwise would be dropped silently.
        if (cs.into->colNames != nullptr && list_length(cs.into->colNames) > 0) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"CREATE MATERIALIZED VIEW with a column name list is not supported "
                                                  "yet",
                                                  resource_});
        }

        VALUE_OR_RETURN(auto matview,
                        create_view_node(pg_cast<SelectStmt>(*cs.query),
                                         cs.query_location,
                                         cs.query_end_location,
                                         cs.into->rel,
                                         true,
                                         false,
                                         plan));
        register_types(cast_type_names_);
        return matview;
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_refresh_matview(RefreshMatViewStmt& rs) {
        if (!rs.relation) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"REFRESH MATERIALIZED VIEW missing relation", resource_});
        }
        auto qn = rangevar_to_qualified_name(rs.relation);
        auto node = logical_plan::make_node_refresh_matview(resource_, rs.concurrent, !rs.skipData);
        // The matview's identity stays ON the node: enrich binds it to a resolved
        // entry by name, whose metadata carries view_sql (Phase A.A2 reads
        // pg_rewrite.ev_action for relkind='m').
        set_target(*node, qn, target_slots::relation);
        register_table(qn.database.t, qn.collection.t, constraint_resolve_kind::none);
        return node;
    }
} // namespace components::sql::transform
