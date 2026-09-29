#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <core/result_wrapper.hpp>
#include <services/collection/remote_servers.hpp>

#include <memory_resource>
#include <string_view>

namespace services {

    struct context_storage_t;

    // The one place that answers "is there a connector for this server type"; federation step 0
    // replaces the body with the connector registry lookup.
    bool has_connector(std::string_view server_type) noexcept;

    // Before the catalog is resolved: a table entry whose first part (uid, else database) is a registered server
    // is remote and gets that server's connector type; the catalog is not asked about it.
    void classify_remote_names(components::logical_plan::catalog_resolves_t& resolves, const remote_servers_t& servers);

    // Right after the catalog is resolved: routes what names a server (first part of a name). A function of a
    // server and an index on a remote table are refused by otterbrix itself; a read of, a write to or a change of
    // a remote object goes to the server type's connector. Without a connector for the type: connector_not_exists.
    core::error_t check_remote_names(std::pmr::memory_resource* resource,
                                     const components::logical_plan::node_t* root,
                                     const components::logical_plan::catalog_resolves_t& resolves,
                                     const remote_servers_t& servers);

    // Refuses a plan that reads a foreign table (relkind 'f') whose server type has no connector,
    // before physical-plan generation.
    core::error_t check_foreign_connectors(std::pmr::memory_resource* resource,
                                           const components::logical_plan::node_ptr& root,
                                           const context_storage_t& context);

} // namespace services
