#pragma once

#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

namespace components::logical_plan {

    class node_create_macro_t final : public node_t {
    public:
        node_create_macro_t(std::pmr::memory_resource* resource,
                            std::string dbname,
                            std::string macroname,
                            std::vector<std::string> parameters,
                            std::string body_sql);

        const std::vector<std::string>& parameters() const { return parameters_; }
        const std::string& body_sql() const { return body_sql_; }

        components::catalog::oid_t namespace_oid() const noexcept { return namespace_oid_; }
        void set_namespace_oid(components::catalog::oid_t oid) noexcept { namespace_oid_ = oid; }

        // Phase 9.W/10.D: role-named accessors. CREATE MACRO writes pg_macro.proname.
        const std::string& macroname() const noexcept { return macroname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string macroname_;
        std::vector<std::string> parameters_;
        std::string body_sql_;
        components::catalog::oid_t namespace_oid_{components::catalog::INVALID_OID};
    };

    using node_create_macro_ptr = boost::intrusive_ptr<node_create_macro_t>;
    node_create_macro_ptr make_node_create_macro(std::pmr::memory_resource* resource,
                                                 std::string dbname,
                                                 std::string macroname,
                                                 std::vector<std::string> parameters,
                                                 std::string body_sql);

} // namespace components::logical_plan
