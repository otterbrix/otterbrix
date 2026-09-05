#include "view_body_text.hpp"

#include <components/logical_plan/node_create_view.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_create_view(ViewStmt& node) {
        // CREATE VIEW v (x, y) AS ... — the column aliases rename the body's output
        // columns. They are NOT carried anywhere below this point, so accepting the
        // form would store a body whose column names differ from the ones the view
        // promises, and `SELECT x FROM v` would fail with "column not found" once the
        // body is spliced in. Rule 6: refuse it here rather than half-support it.
        if (node.aliases != nullptr && list_length(node.aliases) > 0) {
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"CREATE VIEW with a column alias list is not supported yet", resource_});
        }
        // The body is stored verbatim and re-parsed on every read of the view, so it
        // must be exactly what the user wrote — see view_body_text.hpp for what this
        // replaces.
        VALUE_OR_RETURN(auto query_sql,
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
