#include "../transformer.hpp"
#include <components/catalog/settings.hpp>
#include <components/logical_plan/node_set_setting.hpp>

namespace components::sql::transform {

    namespace {
        std::string_view statement_of(const VariableSetStmt& node, catalog::setting_scope_t scope) {
            const bool resets = node.kind == VAR_RESET || node.kind == VAR_RESET_ALL;
            if (scope == catalog::setting_scope_t::system) {
                return resets ? "ALTER SYSTEM RESET" : "ALTER SYSTEM SET";
            }
            return resets ? "RESET" : "SET";
        }

        core::result_wrapper_t<const catalog::setting_def_t*>
        known_setting(std::string_view statement, const VariableSetStmt& node, std::pmr::memory_resource* resource) {
            std::string_view name = node.name ? node.name : "";
            const auto* setting = catalog::find_setting_by_sql_name(name);
            if (setting == nullptr) {
                return core::error_t(
                    core::error_code_t::sql_parse_error,
                    std::pmr::string{std::string(statement) + " " + std::string(name) + " is not supported", resource});
            }
            return setting;
        }

        core::result_wrapper_t<std::string_view> written_value(std::string_view statement,
                                                               const catalog::setting_def_t& setting,
                                                               const VariableSetStmt& node,
                                                               std::pmr::memory_resource* resource) {
            const auto refused = [&](std::string_view what) {
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{std::string(statement) + " " + std::string(setting.sql_name) +
                                                          " requires " + std::string(what),
                                                      resource});
            };
            if (!node.args || node.args->lst.empty()) {
                return refused("a value");
            }
            auto* first_arg = pg_ptr_cast<Node>(node.args->lst.front().data);
            if (nodeTag(first_arg) != T_A_Const) {
                return refused("a constant");
            }
            auto* constant = pg_ptr_cast<A_Const>(first_arg);
            if (constant->val.type != T_String) {
                return refused("a string constant");
            }
            return std::string_view{strVal(&constant->val)};
        }

        // SET, SET LOCAL and ALTER SYSTEM share the forms: a value, or the default (PostgreSQL 18: RESET name is
        // SET name TO DEFAULT, RESET ALL resets every setting).
        core::result_wrapper_t<logical_plan::node_ptr> transform_variable_set(const VariableSetStmt& node,
                                                                              catalog::setting_scope_t scope,
                                                                              std::pmr::memory_resource* r) {
            const auto statement = statement_of(node, scope);
            std::pmr::vector<catalog::setting_assignment_t> assignments{r};
            if (node.kind == VAR_RESET_ALL) {
                for (const auto& setting : catalog::all_settings()) {
                    assignments.push_back(catalog::setting_assignment_t::to_default(setting.id, r));
                }
                return logical_plan::node_ptr{logical_plan::make_node_set_setting(r, scope, std::move(assignments))};
            }
            auto setting = known_setting(statement, node, r);
            if (setting.has_error()) {
                return setting.error();
            }
            const auto id = setting.value()->id;
            if (node.kind == VAR_RESET || node.kind == VAR_SET_DEFAULT) {
                assignments.push_back(catalog::setting_assignment_t::to_default(id, r));
                return logical_plan::node_ptr{logical_plan::make_node_set_setting(r, scope, std::move(assignments))};
            }
            if (node.kind != VAR_SET_VALUE) {
                return core::error_t(core::error_code_t::sql_parse_error,
                                     std::pmr::string{std::string(statement) + " " +
                                                          std::string(setting.value()->sql_name) +
                                                          " takes a value or DEFAULT",
                                                      r});
            }
            auto value = written_value(statement, *setting.value(), node, r);
            if (value.has_error()) {
                return value.error();
            }
            assignments.emplace_back(id, value.value(), r);
            return logical_plan::node_ptr{logical_plan::make_node_set_setting(r, scope, std::move(assignments))};
        }
    } // namespace

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_set_setting(VariableSetStmt& node) {
        return transform_variable_set(node,
                                      node.is_local ? catalog::setting_scope_t::local
                                                    : catalog::setting_scope_t::session,
                                      resource_);
    }

    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_alter_system(AlterSystemStmt& node) {
        return transform_variable_set(*node.setstmt, catalog::setting_scope_t::system, resource_);
    }

} // namespace components::sql::transform
