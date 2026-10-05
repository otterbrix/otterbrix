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

        // The body is bound as a view's is (transform_create_view): stored verbatim, run through the canonical path.
        VALUE_OR_RETURN(
            auto body_sql,
            view_body_text(resource_, raw_sql_, cs.query_location, cs.query_end_location, "CREATE MATERIALIZED VIEW"));
        const auto sub_queries_before = plan->sub_queries.size();
        VALUE_OR_RETURN(auto body, transform_select(pg_cast<SelectStmt>(*cs.query), plan));
        if (!body) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"materialized view body lowered to an empty plan", resource_});
        }
        if (plan->sub_queries.size() != sub_queries_before) {
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"a materialized view body containing a sub-query is not supported yet", resource_});
        }

        auto target_qn = rangevar_to_qualified_name(cs.into->rel);
        auto matview = logical_plan::make_node_create_view(resource_,
                                                           core::viewname_t{target_qn.collection.t},
                                                           core::query_sql_t{std::move(body_sql)});
        matview->set_materialized(true);
        matview->append_child(std::move(body));
        const std::string db_for_resolve = set_target(*matview, target_qn);
        register_catalog_resolve_namespace(resource_, &catalog_resolves_, db_for_resolve);
        register_catalog_resolve_types(resource_, &catalog_resolves_, cast_type_names_);
        return matview;
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_refresh_matview(RefreshMatViewStmt& rs) {
        if (!rs.relation) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"REFRESH MATERIALIZED VIEW missing relation", resource_});
        }
        auto qn = rangevar_to_qualified_name(rs.relation);
        auto node = logical_plan::make_node_refresh_matview(resource_,
                                                            core::matviewname_t{qn.collection.t},
                                                            rs.concurrent,
                                                            !rs.skipData);
        // The matview's identity stays ON the node: enrich binds it to a resolved
        // entry by name, whose metadata carries view_sql (Phase A.A2 reads
        // pg_rewrite.ev_action for relkind='m').
        set_target(*node, qn);
        register_catalog_resolve_table(resource_, &catalog_resolves_, qn.database.t, qn.collection.t);
        return node;
    }
} // namespace components::sql::transform
