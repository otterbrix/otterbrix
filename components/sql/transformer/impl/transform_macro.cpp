#include <components/logical_plan/node_create_macro.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_create_function(CreateFunctionStmt& node) {
        // Extract function name from qualified name list
        qualified_name qn;
        if (node.funcname) {
            auto& lst = node.funcname->lst;
            if (lst.size() == 1) {
                qn.relname = strVal(lst.front().data);
            } else if (lst.size() == 2) {
                auto it = lst.begin();
                qn.dbname = strVal(it++->data);
                qn.relname = strVal(it->data);
            }
        }

        // Extract parameters
        std::vector<std::string> params;
        if (node.parameters) {
            for (auto data : node.parameters->lst) {
                auto fp = pg_ptr_cast<FunctionParameter>(data.data);
                if (fp->name) {
                    params.emplace_back(fp->name);
                }
            }
        }

        // Extract body from options (AS clause)
        std::string body_sql;
        if (node.options) {
            for (auto data : node.options->lst) {
                auto def = pg_ptr_cast<DefElem>(data.data);
                if (def->defname && std::string(def->defname) == "as" && def->arg) {
                    // AS clause contains a list of strings
                    if (nodeTag(def->arg) == T_List) {
                        auto list = reinterpret_cast<List*>(def->arg);
                        if (!list->lst.empty()) {
                            body_sql = strVal(list->lst.front().data);
                        }
                    } else if (nodeTag(def->arg) == T_String) {
                        body_sql = strVal(def->arg);
                    }
                }
            }
        }

        const std::string db_for_resolve = qn.dbname;
        auto m = logical_plan::make_node_create_macro(resource_,
                                                      core::macroname_t{std::move(qn.relname)},
                                                      std::move(params),
                                                      core::body_sql_t{std::move(body_sql)});
        return maybe_wrap_with_catalog_resolve_namespace(resource_, db_for_resolve, std::move(m));
    }

} // namespace components::sql::transform
