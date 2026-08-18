#include <components/logical_plan/node_aggregate.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_matview.hpp>
#include <components/logical_plan/node_refresh_matview.hpp>
#include <components/logical_plan/node_sequence.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

#include <algorithm>
#include <cctype>

namespace components::sql::transform {

    namespace {
        // CREATE MATERIALIZED VIEW mv AS SELECT … — extract body text from the
        // raw SQL (after " AS "). Mirrors extract_view_query in transform_view.cpp.
        // The body SQL is stored in pg_rewrite.ev_action so REFRESH can re-parse.
        std::string extract_matview_body(const char* sql) {
            std::string s(sql);
            std::string upper(s);
            std::transform(upper.begin(), upper.end(), upper.begin(), [](unsigned char c) { return std::toupper(c); });
            auto pos = upper.find(" AS ");
            if (pos == std::string::npos) {
                return "SELECT *";
            }
            auto query = s.substr(pos + 4);
            while (!query.empty() && (query.back() == ';' || query.back() == ' ' || query.back() == '\n' ||
                                      query.back() == '\r' || query.back() == '\t')) {
                query.pop_back();
            }
            return query.empty() ? "SELECT *" : query;
        }
    } // namespace

    logical_plan::node_ptr transformer::transform_create_matview(CreateTableAsStmt& cs,
                                                                 logical_plan::execution_plan_t* plan) {
        if (!cs.query || cs.query->type != T_SelectStmt) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"CREATE MATERIALIZED VIEW requires a SELECT body", resource_});
            return nullptr;
        }
        if (!cs.into || !cs.into->rel) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"CREATE MATERIALIZED VIEW missing target relation", resource_});
            return nullptr;
        }

        // 1. Body SQL — for pg_rewrite.ev_action so REFRESH can re-parse later.
        std::string body_sql = raw_sql_ ? extract_matview_body(raw_sql_) : std::string("SELECT *");

        // 2. Body plan — transform_select returns the consumer aggregate (NOT
        // wrapped with catalog_resolve_*). We hoist the source resolves below
        // so Pass 1 stamps source metadata visible to the planner.
        auto body_aggregate = transform_select(pg_cast<SelectStmt>(*cs.query), plan);
        if (!body_aggregate || has_error()) {
            return nullptr;
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

    logical_plan::node_ptr transformer::transform_refresh_matview(RefreshMatViewStmt& rs) {
        if (!rs.relation) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"REFRESH MATERIALIZED VIEW missing relation", resource_});
            return nullptr;
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
