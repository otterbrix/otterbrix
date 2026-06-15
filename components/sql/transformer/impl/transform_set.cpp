#include <components/logical_plan/node_set_ef_search.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>
#include <algorithm>

namespace components::sql::transform {

    logical_plan::node_ptr transformer::transform_set(VariableSetStmt& node) {
        std::string name = node.name ? node.name : "";
        std::transform(name.begin(), name.end(), name.begin(), [](unsigned char c) { return std::tolower(c); });

        if (name != "hnsw.ef_search") {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"unrecognized configuration parameter \"" + name + "\"", resource_});
            return nullptr;
        }
        if (node.is_local) {
            error_ = core::error_t(core::error_code_t::sql_parse_error,
                                   std::pmr::string{"SET LOCAL is not supported", resource_});
            return nullptr;
        }

        switch (node.kind) {
            case VAR_SET_DEFAULT:
            case VAR_RESET:
                return logical_plan::make_node_set_ef_search(resource_, 0); // 0 = index default
            case VAR_SET_VALUE: {
                if (!node.args || node.args->lst.empty()) {
                    error_ = core::error_t(core::error_code_t::sql_parse_error,
                                           std::pmr::string{"invalid value for parameter \"hnsw.ef_search\"", resource_});
                    return nullptr;
                }
                auto* arg = pg_ptr_cast<A_Const>(node.args->lst.front().data);
                if (nodeTag(&arg->val) != T_Integer || intVal(&arg->val) <= 0) {
                    error_ = core::error_t(
                        core::error_code_t::sql_parse_error,
                        std::pmr::string{"invalid value for parameter \"hnsw.ef_search\": a positive integer is required",
                                         resource_});
                    return nullptr;
                }
                return logical_plan::make_node_set_ef_search(resource_, static_cast<std::size_t>(intVal(&arg->val)));
            }
            default:
                error_ = core::error_t(core::error_code_t::sql_parse_error,
                                       std::pmr::string{"unsupported SET syntax for \"hnsw.ef_search\"", resource_});
                return nullptr;
        }
    }

} // namespace components::sql::transform
