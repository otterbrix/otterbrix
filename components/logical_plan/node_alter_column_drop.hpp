#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>

#include <string>

namespace components::logical_plan {

    // Planner-emitted DDL leaf for `ALTER TABLE ... DROP COLUMN`.
    class node_alter_column_drop_t final : public node_t {
    public:
        node_alter_column_drop_t(
            std::pmr::memory_resource* resource,
            components::catalog::oid_t table_oid,
            components::catalog::oid_t namespace_oid,
            core::columnname_t column_name,
            components::catalog::drop_behavior_t behavior = components::catalog::drop_behavior_t::cascade_);

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        const std::string& column_name() const noexcept { return column_name_; }
        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }

        components::catalog::oid_t attoid() const noexcept { return attoid_; }
        void set_attoid(components::catalog::oid_t a) noexcept { attoid_ = a; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        components::catalog::oid_t namespace_oid_;
        std::string column_name_;
        components::catalog::drop_behavior_t behavior_;
        components::catalog::oid_t attoid_{components::catalog::INVALID_OID};
    };

    using node_alter_column_drop_ptr = boost::intrusive_ptr<node_alter_column_drop_t>;

} // namespace components::logical_plan
