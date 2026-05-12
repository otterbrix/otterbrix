#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

namespace components::logical_plan {

    class node_catalog_resolve_type_t final : public node_t {
    public:
        explicit node_catalog_resolve_type_t(std::pmr::memory_resource* resource,
                                             std::string dbname,
                                             std::string type_name);

        const std::string& dbname() const noexcept { return dbname_; }
        const std::string& type_name() const noexcept { return type_name_; }
        components::catalog::oid_t type_oid() const noexcept { return type_oid_; }
        void set_type_oid(components::catalog::oid_t oid) noexcept { type_oid_ = oid; }

    private:
        std::string dbname_;
        std::string type_name_;
        components::catalog::oid_t type_oid_{components::catalog::INVALID_OID};

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    using node_catalog_resolve_type_ptr = boost::intrusive_ptr<node_catalog_resolve_type_t>;

    node_catalog_resolve_type_ptr make_node_catalog_resolve_type(std::pmr::memory_resource* resource,
                                                                 std::string dbname,
                                                                 std::string type_name);

} // namespace components::logical_plan
