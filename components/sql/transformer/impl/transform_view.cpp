#include "view_body_text.hpp"

#include <components/logical_plan/node_create_view.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {
    core::result_wrapper_t<logical_plan::node_ptr>
    transformer::transform_create_view(ViewStmt& node, logical_plan::execution_plan_t* plan) {
        // Column aliases aren't propagated below, so a later `SELECT x FROM v` would
        // see mismatched names — refuse rather than half-support them.
        if (node.aliases != nullptr && list_length(node.aliases) > 0) {
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"CREATE VIEW with a column alias list is not supported yet", resource_});
        }
        if (!node.query || node.query->type != T_SelectStmt) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"CREATE VIEW requires a SELECT body", resource_});
        }
        VALUE_OR_RETURN(auto v,
                        create_view_node(pg_cast<SelectStmt>(*node.query),
                                         node.query_location,
                                         node.query_end_location,
                                         node.view,
                                         false,
                                         node.replace,
                                         plan));
        if (node.replace) {
            const auto* view = static_cast<const logical_plan::node_create_view_t*>(v.get());
            register_table(view->target().database.t, view->viewname().t, constraint_resolve_kind::none);
        }
        register_types(cast_type_names_);
        return v;
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::create_view_node(SelectStmt& query,
                                                                                 int query_location,
                                                                                 int query_end_location,
                                                                                 RangeVar* name,
                                                                                 bool materialized,
                                                                                 bool replace,
                                                                                 logical_plan::execution_plan_t* plan) {
        const std::string noun = materialized ? "materialized view" : "view";
        // Stored verbatim and re-parsed on every read, so it must match exactly what the user wrote.
        VALUE_OR_RETURN(auto query_sql,
                        view_body_text(resource_,
                                       raw_sql_,
                                       query_location,
                                       query_end_location,
                                       materialized ? "CREATE MATERIALIZED VIEW" : "CREATE VIEW"));

        // The body goes through the canonical path, so a broken body is refused here and not on the first read.
        const auto sub_queries_before = plan->sub_queries.size();
        VALUE_OR_RETURN(auto body, transform_select(query, plan));
        if (!body) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{noun + " body lowered to an empty plan", resource_});
        }
        // Expansion refuses the same shape on every read (view_expansion.cpp); a view nobody can read is not created.
        if (plan->sub_queries.size() != sub_queries_before) {
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"a " + noun + " body containing a sub-query is not supported yet", resource_});
        }

        auto qn = rangevar_to_qualified_name(name);
        auto v = logical_plan::make_node_create_view(resource_,
                                                     core::viewname_t{qn.collection.t},
                                                     core::query_sql_t{std::move(query_sql)},
                                                     materialized,
                                                     replace);
        v->append_child(std::move(body));
        register_namespace(set_target(*v, qn, target_slots::database));
        return logical_plan::node_ptr{std::move(v)};
    }
} // namespace components::sql::transform
