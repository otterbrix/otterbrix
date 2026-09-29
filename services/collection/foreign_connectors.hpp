#pragma once

#include <components/logical_plan/node.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string_view>

namespace services {

    struct context_storage_t;

    // The one place that answers "is there a connector for this server type"; federation step 0
    // replaces the body with the connector registry lookup.
    bool has_connector(std::string_view server_type) noexcept;

    // Right after the catalog is resolved: routes what names a server (first part of a name). A function of a
    // server and an index on a remote table are refused by otterbrix itself; a statement that writes to or changes
    // a remote object goes to the server type's connector, and so does a read of a remote table whose
    // description is not cached yet (describe). Without a connector for the type: connector_not_exists.
    core::error_t check_remote_names(std::pmr::memory_resource* resource,
                                     const components::logical_plan::node_t* root,
                                     const components::logical_plan::catalog_resolves_t& resolves);

    // Refuses a plan that reads a foreign table (relkind 'f') whose server type has no connector,
    // before physical-plan generation.
    core::error_t check_foreign_connectors(std::pmr::memory_resource* resource,
                                           const components::logical_plan::node_ptr& root,
                                           const context_storage_t& context);

} // namespace services
