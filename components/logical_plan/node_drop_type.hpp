#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/results/ddl_result.hpp>

namespace components::logical_plan {

    class node_drop_type_t final : public node_t {
    public:
        explicit node_drop_type_t(std::pmr::memory_resource* resource);

        components::catalog::oid_t type_oid() const noexcept { return type_oid_; }
        void set_type_oid(components::catalog::oid_t oid) noexcept { type_oid_ = oid; }

        components::catalog::drop_behavior_t behavior() const noexcept { return behavior_; }

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;

        components::catalog::oid_t type_oid_{components::catalog::INVALID_OID};
        components::catalog::drop_behavior_t behavior_{components::catalog::drop_behavior_t::cascade_};
    };

    using node_drop_type_ptr = boost::intrusive_ptr<node_drop_type_t>;

    node_drop_type_ptr make_node_drop_type(std::pmr::memory_resource* resource);

} // namespace components::logical_plan
