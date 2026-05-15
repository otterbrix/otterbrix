#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

#include <string>

namespace components::logical_plan {

    class node_alter_column_rename_t final : public node_t {
    public:
        node_alter_column_rename_t(std::pmr::memory_resource* resource,
                                   components::catalog::oid_t table_oid,
                                   core::columnname_t old_name,
                                   core::columnname_t new_name);

        const std::string& old_name() const noexcept { return old_name_; }
        const std::string& new_name() const noexcept { return new_name_; }

        components::catalog::oid_t attoid() const noexcept { return attoid_; }
        void set_attoid(components::catalog::oid_t a) noexcept { attoid_ = a; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string old_name_;
        std::string new_name_;
        components::catalog::oid_t attoid_{components::catalog::INVALID_OID};
    };

    using node_alter_column_rename_ptr = boost::intrusive_ptr<node_alter_column_rename_t>;

} // namespace components::logical_plan
