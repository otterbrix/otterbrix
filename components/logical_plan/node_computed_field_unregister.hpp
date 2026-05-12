#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <string>

namespace components::logical_plan {

    class node_computed_field_unregister_t final : public node_t {
    public:
        node_computed_field_unregister_t(std::pmr::memory_resource* resource,
                                         std::string dbname,
                                         std::string relname,
                                         components::catalog::oid_t table_oid,
                                         std::string column_name);

        const std::string& column_name() const noexcept { return column_name_; }

        components::catalog::oid_t attoid() const noexcept { return attoid_; }
        void set_attoid(components::catalog::oid_t a) noexcept { attoid_ = a; }

        // Phase 9.W/10.D: role-named accessors.
        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        std::string column_name_;
        components::catalog::oid_t attoid_{components::catalog::INVALID_OID};
    };

    using node_computed_field_unregister_ptr = boost::intrusive_ptr<node_computed_field_unregister_t>;

} // namespace components::logical_plan
