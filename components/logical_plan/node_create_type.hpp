#pragma once

#include "node.hpp"
#include <components/catalog/catalog_oids.hpp>
#include <components/types/types.hpp>

namespace components::logical_plan {

    class node_create_type_t final : public node_t {
    public:
        explicit node_create_type_t(std::pmr::memory_resource* resource, types::complex_logical_type&& type);

        types::complex_logical_type& type() noexcept;
        const types::complex_logical_type& type() const noexcept;

        // Namespace OID set by the dispatcher: the user-specified database name on the
        // node (CREATE TYPE mydb.foo) is resolved to a pg_namespace OID before the
        // planner runs (mirrors node_create_collection_t::namespace_oid). Defaults to
        // INVALID_OID; planner falls back to public_namespace when unset.
        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;

        types::complex_logical_type type_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
    };

    using node_create_type_ptr = boost::intrusive_ptr<node_create_type_t>;

    node_create_type_ptr make_node_create_type(std::pmr::memory_resource* resource, types::complex_logical_type&& type);

} // namespace components::logical_plan
