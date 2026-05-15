#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

namespace components::logical_plan {

    class node_create_macro_t final : public node_t {
    public:
        node_create_macro_t(std::pmr::memory_resource* resource,
                            core::macroname_t macroname,
                            std::vector<std::string> parameters,
                            core::body_sql_t body_sql);

        const std::vector<std::string>& parameters() const { return parameters_; }
        const std::string& body_sql() const { return body_sql_; }

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        const std::string& macroname() const noexcept { return macroname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string macroname_;
        std::vector<std::string> parameters_;
        std::string body_sql_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
    };

    using node_create_macro_ptr = boost::intrusive_ptr<node_create_macro_t>;
    node_create_macro_ptr make_node_create_macro(std::pmr::memory_resource* resource,
                                                 core::macroname_t macroname,
                                                 std::vector<std::string> parameters,
                                                 core::body_sql_t body_sql);

} // namespace components::logical_plan
