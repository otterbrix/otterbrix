#include "foreign_connectors.hpp"

#include "context_storage.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/logical_plan/node_create_collection.hpp>
#include <components/logical_plan/node_drop.hpp>
#include <services/dispatcher/enrich_logical_plan.hpp>

namespace services {

    bool has_connector(std::string_view) noexcept { return false; }

    namespace {
        using components::logical_plan::catalog_resolves_t;
        using components::logical_plan::node_type;

        struct named_server_t {
            bool found{false};
            std::string_view type{};
        };

        // Every resolve kind that probes a name's first part records the server it turned out to be.
        named_server_t server_named(const catalog_resolves_t& resolves, std::string_view name) {
            if (name.empty()) {
                return {};
            }
            const auto matches = [name](const components::logical_plan::resolve_entry_t& entry,
                                        std::string_view first) {
                return entry.server_oid != components::catalog::INVALID_OID && first == name;
            };
            if (resolves.servers) {
                for (const auto& entry : resolves.servers->entries()) {
                    if (matches(entry, entry.relname)) {
                        return {true, entry.server_type};
                    }
                }
            }
            if (resolves.namespaces) {
                for (const auto& entry : resolves.namespaces->entries()) {
                    if (matches(entry, entry.dbname)) {
                        return {true, entry.server_type};
                    }
                }
            }
            if (resolves.tables) {
                for (const auto& entry : resolves.tables->entries()) {
                    if (matches(entry, entry.uid.empty() ? entry.dbname : entry.uid)) {
                        return {true, entry.server_type};
                    }
                }
            }
            return {};
        }

        bool writes_or_changes_a_target(node_type type) {
            switch (type) {
                case node_type::insert_t:
                case node_type::update_t:
                case node_type::delete_t:
                case node_type::create_collection_t:
                case node_type::drop_t:
                case node_type::alter_table_t:
                case node_type::create_index_t:
                case node_type::create_view_t:
                case node_type::create_sequence_t:
                case node_type::create_matview_t:
                case node_type::refresh_matview_t:
                case node_type::create_type_t:
                case node_type::create_macro_t:
                case node_type::create_constraint_t:
                    return true;
                default:
                    return false;
            }
        }

        // The cache writes federation's describe and the host's reset make: not statements on the remote object.
        bool writes_the_cache(const components::logical_plan::node_t* root) {
            if (root->type() == node_type::create_collection_t) {
                return static_cast<const components::logical_plan::node_create_collection_t*>(root)->relkind() ==
                       components::catalog::relkind::foreign;
            }
            if (root->type() == node_type::drop_t) {
                const auto kind = static_cast<const components::logical_plan::node_drop_t*>(root)->kind();
                return kind == components::logical_plan::drop_target_kind::remote_table ||
                       kind == components::logical_plan::drop_target_kind::server_cache;
            }
            return false;
        }

        bool touches_an_index(const components::logical_plan::node_t* root) {
            if (root->type() == node_type::create_index_t) {
                return true;
            }
            return root->type() == node_type::drop_t &&
                   static_cast<const components::logical_plan::node_drop_t*>(root)->kind() ==
                       components::logical_plan::drop_target_kind::index;
        }

        // server.name: the schema would have to be guessed (a connector default or whatever happens to be cached),
        // so the meaning of the query would depend on the cache. Refused before any lookup.
        core::error_t full_path_demanded(std::pmr::memory_resource* resource, std::string_view written) {
            std::pmr::string msg{"remote table \"", resource};
            msg.append(written);
            msg.append("\": write the full path as server.schema.table");
            return core::error_t{core::error_code_t::invalid_parameter, std::move(msg)};
        }

        core::error_t no_connector(std::pmr::memory_resource* resource, std::string_view what, std::string_view type) {
            std::pmr::string msg{what, resource};
            msg.append(": no connector for server type '");
            msg.append(type);
            msg.append("'");
            return core::error_t{core::error_code_t::connector_not_exists, std::move(msg)};
        }
    } // namespace

    core::error_t check_remote_names(std::pmr::memory_resource* resource,
                                     const components::logical_plan::node_t* root,
                                     const catalog_resolves_t& resolves) {
        for (const auto& function : resolves.qualified_functions) {
            const auto& first = function.unique_identifier.empty() ? function.database : function.unique_identifier;
            if (server_named(resolves, first).found) {
                std::pmr::string msg{"function '", resource};
                msg.append(function.to_string());
                msg.append("': functions of a remote server are not supported");
                return core::error_t{core::error_code_t::unimplemented_yet, std::move(msg)};
            }
        }
        // A DML/DDL target keeps its written slots only in external_targets; its table entry is (database, name).
        const auto written_longer = [&resolves](const components::logical_plan::resolve_entry_t& entry) {
            for (const auto& target : resolves.external_targets) {
                if (target.written.database == entry.dbname && target.written.collection == entry.relname &&
                    (!target.written.schema.empty() || !target.written.unique_identifier.empty())) {
                    return true;
                }
            }
            return false;
        };
        // PostgreSQL 18 refuses a foreign table as a referenced relation (tablecmds.c): not a table.
        for (const auto& referenced : resolves.referenced_tables) {
            const auto& first =
                referenced.unique_identifier.empty() ? referenced.database : referenced.unique_identifier;
            if (server_named(resolves, first).found) {
                std::pmr::string msg{"referenced relation \"", resource};
                msg.append(referenced.to_string());
                msg.append("\" is not a table");
                return core::error_t{core::error_code_t::invalid_constraint, std::move(msg)};
            }
        }
        if (resolves.tables) {
            for (const auto& entry : resolves.tables->entries()) {
                if (entry.server_oid != components::catalog::INVALID_OID && entry.uid.empty() &&
                    entry.schema.empty() && !written_longer(entry)) {
                    std::pmr::string written{entry.dbname, resource};
                    written.append(".");
                    written.append(entry.relname);
                    return full_path_demanded(resource, written);
                }
            }
        }
        if (root == nullptr || writes_the_cache(root)) {
            return core::error_t::no_error();
        }
        if (writes_or_changes_a_target(root->type())) {
            const auto first = services::catalog_resolve::statement_target_first_part(root, resolves);
            if (const auto server = server_named(resolves, first); server.found) {
                // Only a uid or a schema slot carries the rest of a remote path past its table name.
                const bool full_path = !resolves.external_targets.empty() &&
                                       (!resolves.external_targets.front().written.unique_identifier.empty() ||
                                        !resolves.external_targets.front().written.schema.empty());
                if (!full_path) {
                    std::pmr::string written{first, resource};
                    written.append(".…");
                    return full_path_demanded(resource, written);
                }
                if (touches_an_index(root)) {
                    std::pmr::string msg{"server \"", resource};
                    msg.append(first);
                    msg.append("\": indexes on remote tables are not supported");
                    return core::error_t{core::error_code_t::invalid_parameter, std::move(msg)};
                }
                if (!has_connector(server.type)) {
                    std::pmr::string what{"server \"", resource};
                    what.append(first);
                    what.append("\"");
                    return no_connector(resource, what, server.type);
                }
            }
        }
        if (resolves.tables) {
            for (const auto& entry : resolves.tables->entries()) {
                if (entry.server_oid == components::catalog::INVALID_OID || entry.table_md.has_value()) {
                    continue;
                }
                if (!has_connector(entry.server_type)) {
                    std::pmr::string what{"remote table ", resource};
                    if (!entry.uid.empty()) {
                        what.append(entry.uid);
                        what.append(".");
                    }
                    what.append(entry.dbname);
                    if (!entry.schema.empty()) {
                        what.append(".");
                        what.append(entry.schema);
                    }
                    what.append(".");
                    what.append(entry.relname);
                    return no_connector(resource, what, entry.server_type);
                }
            }
        }
        return core::error_t::no_error();
    }

    core::error_t check_foreign_connectors(std::pmr::memory_resource* resource,
                                           const components::logical_plan::node_ptr& root,
                                           const context_storage_t& context) {
        using components::logical_plan::node_type;
        if (!root) {
            return core::error_t::no_error();
        }
        switch (root->type()) {
            case node_type::aggregate_t:
            case node_type::match_t: {
                const auto* md = context.table_metadata_for(root->table_oid());
                if (md != nullptr && md->relkind == components::catalog::relkind::foreign &&
                    !has_connector(md->server_type)) {
                    std::pmr::string msg{"foreign table \"", resource};
                    msg.append(md->name);
                    msg.append("\": no connector for server type '");
                    msg.append(md->server_type);
                    msg.append("'");
                    return core::error_t{core::error_code_t::connector_not_exists, std::move(msg)};
                }
                break;
            }
            default:
                break;
        }
        for (const auto& child : root->children()) {
            if (auto err = check_foreign_connectors(resource, child, context); err.contains_error()) {
                return err;
            }
        }
        return core::error_t::no_error();
    }

} // namespace services
