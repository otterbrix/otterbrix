#include "../transformer.hpp"
#include <components/logical_plan/node_set_timezone.hpp>

namespace components::sql::transform {

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_set_timezone(VariableSetStmt& node) {
        if (node.kind != VAR_SET_VALUE || !node.args || node.args->lst.empty()) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"SET TIMEZONE requires a value", resource_});
        }

        auto* first_arg = pg_ptr_cast<Node>(node.args->lst.front().data);
        if (nodeTag(first_arg) != T_A_Const) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"SET TIMEZONE requires a string constant", resource_});
        }

        auto* constant = pg_ptr_cast<A_Const>(first_arg);
        if (constant->val.type != T_String) {
            return core::error_t(core::error_code_t::sql_parse_error,
                                 std::pmr::string{"SET TIMEZONE requires a string constant", resource_});
        }

        std::string timezone_name = strVal(&constant->val);
        std::transform(timezone_name.begin(), timezone_name.end(), timezone_name.begin(), [](unsigned char character) {
            return static_cast<char>(std::tolower(character));
        });
        return logical_plan::make_node_set_timezone(resource_, std::move(timezone_name));
    }

} // namespace components::sql::transform
