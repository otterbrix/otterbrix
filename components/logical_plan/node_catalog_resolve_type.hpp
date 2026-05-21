#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/identifier_types.hpp>
#include <components/types/types.hpp>

#include <optional>

namespace components::logical_plan {

    // Full type metadata stamped by operator_resolve_type_t.
    // Carries decoded complex_logical_type + raw typdefspec + namespace.
    struct resolved_type_metadata_t {
        components::catalog::oid_t type_oid{components::catalog::INVALID_OID};
        components::catalog::oid_t namespace_oid{components::catalog::INVALID_OID};
        std::string name;
        components::types::complex_logical_type type;
        std::string typdefspec;
    };

    class node_catalog_resolve_type_t final : public node_t {
    public:
        explicit node_catalog_resolve_type_t(std::pmr::memory_resource* resource,
                                             core::dbname_t dbname,
                                             core::typename_t type_name);

        const std::string& dbname() const noexcept { return dbname_; }
        const std::string& type_name() const noexcept { return type_name_; }
        components::catalog::oid_t type_oid() const noexcept { return type_oid_; }
        void set_type_oid(components::catalog::oid_t oid) noexcept { type_oid_ = oid; }

        // Stamped by operator_resolve_type_t after pg_type read +
        // typdefspec decode. Empty optional means the resolve did not find a
        // matching type (caller treats as not-found).
        const std::optional<resolved_type_metadata_t>& resolved_metadata() const noexcept { return resolved_metadata_; }
        void set_resolved_metadata(resolved_type_metadata_t md) { resolved_metadata_ = std::move(md); }

    private:
        std::string dbname_;
        std::string type_name_;
        components::catalog::oid_t type_oid_{components::catalog::INVALID_OID};
        std::optional<resolved_type_metadata_t> resolved_metadata_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
    };

    using node_catalog_resolve_type_ptr = boost::intrusive_ptr<node_catalog_resolve_type_t>;

    node_catalog_resolve_type_ptr make_node_catalog_resolve_type(std::pmr::memory_resource* resource,
                                                                 core::dbname_t dbname,
                                                                 core::typename_t type_name);

} // namespace components::logical_plan
