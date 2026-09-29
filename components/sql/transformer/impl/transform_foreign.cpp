#include <components/logical_plan/node_create_server.hpp>
#include <components/sql/parser/pg_functions.h>
#include <components/sql/transformer/transformer.hpp>
#include <components/sql/transformer/utils.hpp>

namespace components::sql::transform {

    namespace {
        core::result_wrapper_t<catalog::generic_options_t>
        generic_options_of(std::pmr::memory_resource* resource, List* options, std::string_view statement) {
            catalog::generic_options_t out(resource);
            if (options == nullptr) {
                return out;
            }
            for (auto data : options->lst) {
                if (data.data == nullptr || nodeTag(data.data) != T_DefElem) {
                    continue;
                }
                auto* def = pg_ptr_cast<DefElem>(data.data);
                const std::string_view name = def->defname ? std::string_view{def->defname} : std::string_view{};
                if (name.empty() || def->arg == nullptr || nodeTag(def->arg) != T_String) {
                    std::pmr::string msg{statement, resource};
                    msg.append(": every option needs a name and a string value");
                    return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
                }
                for (const auto& existing : out) {
                    if (existing.name == name) {
                        std::pmr::string msg{statement, resource};
                        msg.append(": option \"");
                        msg.append(name);
                        msg.append("\" provided more than once");
                        return core::error_t(core::error_code_t::sql_parse_error, std::move(msg));
                    }
                }
                out.emplace_back(std::pmr::string{name, resource}, std::pmr::string{strVal(def->arg), resource});
            }
            return out;
        }
    } // namespace

    core::result_wrapper_t<logical_plan::node_ptr>
    transformer::transform_create_server(CreateForeignServerStmt& node) {
        VALUE_OR_RETURN(auto options, generic_options_of(resource_, node.options, "CREATE SERVER"));
        // A server and a database share the first slot of a name, so the name must be free as a database too.
        register_catalog_resolve_namespace(resource_, &catalog_resolves_, std::string{node.servername});
        return logical_plan::node_ptr{logical_plan::make_node_create_server(resource_,
                                                                            std::string{node.servername},
                                                                            std::string{node.servertype},
                                                                            std::move(options))};
    }

} // namespace components::sql::transform
