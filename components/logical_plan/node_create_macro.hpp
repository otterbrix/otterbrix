#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

namespace components::logical_plan {

    class node_create_macro_t final : public node_t {
    public:
        node_create_macro_t(std::pmr::memory_resource* resource,
                            const collection_full_name_t& name,
                            std::vector<std::string> parameters,
                            std::string body_sql);

        const std::vector<std::string>& parameters() const { return parameters_; }
        const std::string& body_sql() const { return body_sql_; }

        // Namespace OID set by the enrich phase (dispatcher resolves the namespace
        // name via catalog_view and calls set_namespace_oid before handing the plan
        // to the planner). Returns INVALID_OID (0) when not yet enriched.
        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::vector<std::string> parameters_;
        std::string body_sql_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
    };

    using node_create_macro_ptr = boost::intrusive_ptr<node_create_macro_t>;
    node_create_macro_ptr make_node_create_macro(std::pmr::memory_resource* resource,
                                                 const collection_full_name_t& name,
                                                 std::vector<std::string> parameters,
                                                 std::string body_sql);

} // namespace components::logical_plan
