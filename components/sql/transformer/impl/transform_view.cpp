#include "view_body_text.hpp"

#include <components/logical_plan/node_create_view.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_view(ViewStmt& node) {
        // Column aliases aren't propagated below, so a later `SELECT x FROM v` would
        // see mismatched names — refuse rather than half-support them.
        if (node.aliases != nullptr && list_length(node.aliases) > 0) {
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"CREATE VIEW with a column alias list is not supported yet", resource_});
        }
        // Stored verbatim and re-parsed on every read, so it must match exactly what the user wrote.
        VALUE_OR_RETURN(
            auto query_sql,
            view_body_text(resource_, raw_sql_, node.query_location, node.query_end_location, "CREATE VIEW"));

        auto qn = rangevar_to_qualified_name(node.view);
        const std::string db_for_resolve = qn.dbname;

        auto v = logical_plan::make_node_create_view(resource_,
                                                     core::viewname_t{std::move(qn.relname)},
                                                     core::query_sql_t{std::move(query_sql)});
        v->set_dbname(db_for_resolve);
        register_catalog_resolve_namespace(resource_, &catalog_resolves_, db_for_resolve);
        return v;
    }

} // namespace components::sql::transform
