#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>

namespace components::logical_plan {

    class node_drop_view_t final : public node_t {
    public:
        explicit node_drop_view_t(std::pmr::memory_resource* resource, std::string dbname, std::string viewname);

        components::catalog::oid_t relation_oid() const noexcept { return relation_oid_; }
        void set_relation_oid(components::catalog::oid_t oid) noexcept { relation_oid_ = oid; }

        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }

        // Phase 9.W/10.D: role-named accessors. DROP VIEW user-typed identifiers; routing
        // uses relation_oid stamped by enrich.
        const std::string& viewname() const noexcept { return viewname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string viewname_;
        components::catalog::oid_t relation_oid_{components::catalog::INVALID_OID};
        components::catalog::drop_behavior_t behavior_{components::catalog::drop_behavior_t::cascade_};
    };

    using node_drop_view_ptr = boost::intrusive_ptr<node_drop_view_t>;
    node_drop_view_ptr make_node_drop_view(std::pmr::memory_resource* resource, std::string dbname, std::string viewname);

} // namespace components::logical_plan
