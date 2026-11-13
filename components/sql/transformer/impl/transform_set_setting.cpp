#include "../transformer.hpp"
#include <components/catalog/settings.hpp>
#include <components/logical_plan/node_set_setting.hpp>
#include <components/sql/parser/nodes/makefuncs.h>
#include <core/pmr.hpp>

#include <algorithm>
#include <cstring>

namespace components::sql::transform {

    namespace {
        std::string_view statement_of(const VariableSetStmt& node, catalog::setting_scope_t scope) {
            const bool resets = node.kind == VAR_RESET || node.kind == VAR_RESET_ALL;
            if (scope == catalog::setting_scope_t::system) {
                return resets ? "ALTER SYSTEM RESET" : "ALTER SYSTEM SET";
            }
            return resets ? "RESET" : "SET";
        }

        // The current transaction's own modes: whatever the SET says they are the transaction's, no default stores
        // them, and RESET ALL leaves them (GUC_NO_RESET_ALL).
        bool is_transaction_mode(catalog::setting_id id) {
            return id == catalog::setting_id::transaction_read_only ||
                   id == catalog::setting_id::transaction_isolation ||
                   id == catalog::setting_id::transaction_deferrable;
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
            if (setting.id == catalog::setting_id::search_path && node.args->lst.size() > 1) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"search_path takes one place: [[connection.]database.]schema", resource});
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
        // SET [SESSION | LOCAL] TRANSACTION modes is the current transaction's, as SET LOCAL transaction_read_only;
        // SET SESSION CHARACTERISTICS AS TRANSACTION modes the session's default, as SET default_transaction_read_only
        // (PostgreSQL 18 ExecSetVariableStmt).
        core::result_wrapper_t<logical_plan::node_ptr> transform_transaction_modes(const VariableSetStmt& node,
                                                                                   std::pmr::memory_resource* r) {
            auto access = transaction_access_of(node.args, r);
            if (access.has_error()) {
                return access.error();
            }
            const bool session_wide = std::string_view{node.name} == "SESSION CHARACTERISTICS";
            const auto scope = session_wide ? catalog::setting_scope_t::session : catalog::setting_scope_t::local;
            std::pmr::vector<catalog::setting_assignment_t> assignments{r};
            if (access.value() != logical_plan::transaction_access_t::as_default) {
                assignments.emplace_back(session_wide ? catalog::setting_id::default_transaction_read_only
                                                      : catalog::setting_id::transaction_read_only,
                                         access.value() == logical_plan::transaction_access_t::read_only ? "on" : "off",
                                         r);
            }
            return logical_plan::node_ptr{logical_plan::make_node_set_setting(r, scope, std::move(assignments))};
        }

        core::result_wrapper_t<logical_plan::node_ptr> transform_variable_set(const VariableSetStmt& node,
                                                                              catalog::setting_scope_t scope,
                                                                              std::pmr::memory_resource* r) {
            if (node.kind == VAR_SET_MULTI && node.name != nullptr &&
                (std::string_view{node.name} == "TRANSACTION" ||
                 std::string_view{node.name} == "SESSION CHARACTERISTICS")) {
                return transform_transaction_modes(node, r);
            }
            const auto statement = statement_of(node, scope);
            std::pmr::vector<catalog::setting_assignment_t> assignments{r};
            if (node.kind == VAR_RESET_ALL) {
                for (const auto& setting : catalog::all_settings()) {
                    if (!is_transaction_mode(setting.id)) {
                        assignments.push_back(catalog::setting_assignment_t::to_default(setting.id, r));
                    }
                }
                return logical_plan::node_ptr{logical_plan::make_node_set_setting(r, scope, std::move(assignments))};
            }
            auto setting = known_setting(statement, node, r);
            if (setting.has_error()) {
                return setting.error();
            }
            const auto id = setting.value()->id;
            if (is_transaction_mode(id)) {
                if (scope == catalog::setting_scope_t::system) {
                    return core::error_t(core::error_code_t::invalid_parameter,
                                         std::pmr::string{std::string{setting.value()->sql_name} +
                                                              " is the current transaction's, and has no default to "
                                                              "store",
                                                          r});
                }
                scope = catalog::setting_scope_t::local;
            }
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

    namespace {
        char* text_on(std::pmr::memory_resource* arena, std::string_view text) {
            auto* out = static_cast<char*>(arena->allocate(text.size() + 1, alignof(char)));
            std::memcpy(out, text.data(), text.size());
            out[text.size()] = '\0';
            return out;
        }

        Node* string_constant(std::pmr::memory_resource* arena, std::string_view text) {
            auto* constant = makeNode(arena, A_Const);
            constant->val.type = T_String;
            constant->val.val.str = text_on(arena, text);
            constant->location = -1;
            return pg_ptr_cast<Node>(constant);
        }

        // current_setting('<name>')
        Node* current_setting_of(std::pmr::memory_resource* arena, std::string_view name) {
            return pg_ptr_cast<Node>(
                makeFuncCall(arena,
                             list_make1(arena, makeString(arena, text_on(arena, "current_setting"))),
                             list_make1(arena, string_constant(arena, name)),
                             -1));
        }

        ResTarget* target_of(std::pmr::memory_resource* arena, Node* value, std::string_view name) {
            auto* target = makeNode(arena, ResTarget);
            target->name = text_on(arena, name);
            target->val = value;
            target->location = -1;
            return target;
        }

        SelectStmt* select_of(std::pmr::memory_resource* arena, List* targets) {
            auto* select = makeNode(arena, SelectStmt);
            select->targetList = targets;
            return select;
        }
    } // namespace

    // SHOW name is SELECT current_setting('name'), its one column named as the setting is (PostgreSQL 18 GetPGVariable);
    // SHOW ALL is a row (name, setting, description) per setting, ordered by name.
    core::result_wrapper_t<logical_plan::node_ptr> transformer::transform_show(VariableShowStmt& node,
                                                                               logical_plan::execution_plan_t* plan) {
        core::pmr::arena_resource_t nodes_arena{resource_};
        const std::string_view name = node.name ? node.name : "";
        if (name != "all") {
            const auto* setting = catalog::find_setting_by_sql_name(name);
            const std::string_view column = setting != nullptr ? setting->catalog_name : name;
            auto* select = select_of(
                &nodes_arena,
                list_make1(&nodes_arena, target_of(&nodes_arena, current_setting_of(&nodes_arena, name), column)));
            return transform_select(*select, plan);
        }
        std::pmr::vector<const catalog::setting_def_t*> settings{resource_};
        for (const auto& known : catalog::all_settings()) {
            settings.push_back(&known);
        }
        std::sort(settings.begin(), settings.end(), [](const auto* left, const auto* right) {
            return std::lexicographical_compare(left->sql_name.begin(),
                                                left->sql_name.end(),
                                                right->sql_name.begin(),
                                                right->sql_name.end());
        });
        SelectStmt* all = nullptr;
        for (const auto* known : settings) {
            auto* row = select_of(
                &nodes_arena,
                list_make3(&nodes_arena,
                           target_of(&nodes_arena, string_constant(&nodes_arena, known->catalog_name), "name"),
                           target_of(&nodes_arena, current_setting_of(&nodes_arena, known->sql_name), "setting"),
                           target_of(&nodes_arena, string_constant(&nodes_arena, known->description), "description")));
            if (all == nullptr) {
                all = row;
                continue;
            }
            auto* both = makeNode(&nodes_arena, SelectStmt);
            both->op = SETOP_UNION;
            both->all = true;
            both->larg = all;
            both->rarg = row;
            all = both;
        }
        return transform_select(*all, plan);
    }

} // namespace components::sql::transform
