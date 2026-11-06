#pragma once

#include "node.hpp"
#include <components/base/identifier_types.hpp>

namespace components::logical_plan {

    class node_create_database_t final : public node_t {
    public:
        // [connection.]database (no schema, no relation). A database of the host's connection
        // (pg_database.datconnection): only a host's plan creates one; SQL has no spelling for it.
        node_create_database_t(std::pmr::memory_resource* resource,
                               qualified_name_t database,
                               bool if_not_exists = false);

        bool if_not_exists() const noexcept { return if_not_exists_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        bool if_not_exists_;
    };

    using node_create_database_ptr = boost::intrusive_ptr<node_create_database_t>;
    node_create_database_ptr make_node_create_database(std::pmr::memory_resource* resource,
                                                       qualified_name_t database,
                                                       bool if_not_exists = false);

} // namespace components::logical_plan
