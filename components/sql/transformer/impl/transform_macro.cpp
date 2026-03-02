#include <components/logical_plan/node_create_macro.hpp>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_create_function(CreateFunctionStmt& node) {
        // Extract function name from qualified name list
        std::string func_name;
        collection_full_name_t name;
        if (node.funcname) {
            auto& lst = node.funcname->lst;
            if (lst.size() == 1) {
                func_name = strVal(lst.front().data);
                name = collection_full_name_t{database_name_t(), func_name};
            } else if (lst.size() == 2) {
                auto it = lst.begin();
                auto db = strVal(it++->data);
                func_name = strVal(it->data);
                name = collection_full_name_t{db, func_name};
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

        return logical_plan::make_node_create_macro(resource_, name, std::move(params), std::move(body_sql));
    }

} // namespace components::sql::transform
