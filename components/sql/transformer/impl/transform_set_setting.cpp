#include "../transformer.hpp"
#include <components/catalog/settings.hpp>
#include <components/logical_plan/node_set_setting.hpp>

namespace components::sql::transform {

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_set_setting(VariableSetStmt& node) {
        std::string_view name = node.name ? node.name : "";
        const auto* setting = components::catalog::find_setting_by_sql_name(name);
        if (setting == nullptr) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"SET " + std::string(name) + " is not supported", resource_});
        }

        if (node.kind != VAR_SET_VALUE || !node.args || node.args->lst.empty()) {
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"SET " + std::string(setting->sql_name) + " requires a value", resource_});
        }

        auto* first_arg = pg_ptr_cast<Node>(node.args->lst.front().data);
        if (nodeTag(first_arg) != T_A_Const) {
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"SET " + std::string(setting->sql_name) + " requires a constant", resource_});
        }

        auto* constant = pg_ptr_cast<A_Const>(first_arg);
        if (constant->val.type != T_String) {
            return core::error_t(
                core::error_code_t::sql_parse_error,
                std::pmr::string{"SET " + std::string(setting->sql_name) + " requires a string constant", resource_});
        }

        return logical_plan::make_node_set_setting(resource_, setting->id, strVal(&constant->val));
    }

} // namespace components::sql::transform
