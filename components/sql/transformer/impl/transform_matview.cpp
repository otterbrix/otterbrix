#include "view_body_text.hpp"

#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_matview.hpp>
#include <components/logical_plan/node_refresh_matview.hpp>
#include <components/logical_plan/node_sequence.hpp>
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

        // WITH DATA (the PostgreSQL default, i.e. the form without an explicit
        // WITH NO DATA) is REFUSED, loudly.
        //
        // Nothing in this pipeline populates a matview at CREATE time: the composite
        // operator_create_matview_t creates the heap and writes the catalog rows and
        // stops there, and REFRESH MATERIALIZED VIEW is not lowered either (see
        // planner.cpp, `case node_type::refresh_matview_t` — it returns the node
        // unchanged). Accepting `CREATE MATERIALIZED VIEW mv AS SELECT ...`
        // would therefore report SUCCESS and leave `SELECT * FROM mv` answering 0 rows
        // forever, with nothing said. Rule 6: a form we cannot honour is refused, not
        // silently downgraded. `WITH NO DATA` — the one form whose meaning IS an empty
        // matview — keeps working.
        //
        // The flag itself comes from the grammar: opt_with_data lands in
        // IntoClause::skipData (gram.y, CreateMatViewStmt: `$5->skipData = !($8)`), so
        // skipData is true if and only if the user wrote WITH NO DATA.
        if (!cs.into->skipData) {
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"CREATE MATERIALIZED VIEW ... WITH DATA is not supported yet: the matview "
                                 "cannot be populated at CREATE time and REFRESH MATERIALIZED VIEW is not "
                                 "implemented, so the result would be a silently empty matview. Write "
                                 "WITH NO DATA to create it empty on purpose.",
                                 resource_});
        }

        // 1. Body SQL — stored in pg_rewrite.ev_action verbatim, so it must be what
        //    the user wrote (see view_body_text.hpp).
        VALUE_OR_RETURN(auto body_sql,
                        view_body_text(resource_,
                                       raw_sql_,
                                       cs.query_location,
                                       cs.query_end_location,
                                       "CREATE MATERIALIZED VIEW"));

        // 2. Body plan — transform_select returns the consumer aggregate (NOT
        // wrapped with catalog_resolve_*). We hoist the source resolves below
        // so Pass 1 stamps source metadata visible to the planner.
        VALUE_OR_RETURN(auto body_aggregate, transform_select(pg_cast<SelectStmt>(*cs.query), plan));
        if (!body_aggregate) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"materialized view body lowered to an empty plan", resource_});
        }

        // 3. Source identity from the body's aggregate (single-table FROM).
        std::string source_db;
        std::string source_rel;
        if (body_aggregate->type() == logical_plan::node_type::aggregate_t) {
            auto* agg = static_cast<const logical_plan::node_aggregate_t*>(body_aggregate.get());
            source_db = static_cast<const std::string&>(agg->dbname());
            source_rel = static_cast<const std::string&>(agg->relname());
        }

        // 4. Matview target identity.
        auto target_qn = rangevar_to_qualified_name(cs.into->rel);
        const std::string mv_db = target_qn.dbname;
        const std::string mv_name = target_qn.relname;

        // 5. Build matview node carrying body plan as child[0].
        auto matview_node = logical_plan::make_node_create_matview(resource_,
                                                                   core::matviewname_t{mv_name},
                                                                   core::body_sql_t{std::move(body_sql)});
        matview_node->set_body_plan(body_aggregate);

        // 6. Both identities stay ON the node — enrich binds each to a resolved
        // entry by name and stamps namespace_oid + source_table_oid + the source's
        // columns (which the planner's derive_output_schema needs) from there.
        matview_node->set_dbname(mv_db);
        matview_node->set_source_dbname(source_db);
        matview_node->set_source_relname(source_rel);
        register_catalog_resolve_namespace(resource_, &catalog_resolves_, mv_db);
        register_catalog_resolve_table(resource_, &catalog_resolves_, source_db, source_rel);
        return matview_node;
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_refresh_matview(RefreshMatViewStmt& rs) {
        if (!rs.relation) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"REFRESH MATERIALIZED VIEW missing relation", resource_});
        }
        auto qn = rangevar_to_qualified_name(rs.relation);
        auto node = logical_plan::make_node_refresh_matview(resource_,
                                                            core::matviewname_t{qn.relname},
                                                            rs.concurrent,
                                                            !rs.skipData);
        // The matview's identity stays ON the node: enrich binds it to a resolved
        // entry by name, whose metadata carries view_sql (Phase A.A2 reads
        // pg_rewrite.ev_action for relkind='m').
        node->set_dbname(qn.dbname);
        register_catalog_resolve_table(resource_, &catalog_resolves_, qn.dbname, qn.relname);
        return node;
    }

} // namespace components::sql::transform
