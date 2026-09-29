#pragma once

#include "node.hpp"

#include <components/catalog/generic_options.hpp>

#include <string>

namespace components::logical_plan {

    // CREATE SERVER name TYPE 'type' [OPTIONS (...)]: one pg_foreign_server row.
    class node_create_server_t final : public node_t {
    public:
        node_create_server_t(std::pmr::memory_resource* resource,
                             std::string servername,
                             std::string servertype,
                             components::catalog::generic_options_t options);

        const std::string& servername() const noexcept { return servername_; }
        const std::string& servertype() const noexcept { return servertype_; }
        const components::catalog::generic_options_t& options() const noexcept { return options_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string servername_;
        std::string servertype_;
        components::catalog::generic_options_t options_;
    };

    using node_create_server_ptr = boost::intrusive_ptr<node_create_server_t>;
    node_create_server_ptr make_node_create_server(std::pmr::memory_resource* resource,
                                                   std::string servername,
                                                   std::string servertype,
                                                   components::catalog::generic_options_t options);

} // namespace components::logical_plan
