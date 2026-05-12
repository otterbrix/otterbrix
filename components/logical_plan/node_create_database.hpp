#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_create_database_t final : public node_t {
    public:
        // Phase 10.D: ctor takes role-named string instead of cfn struct.
        // CREATE DATABASE writes pg_namespace.nspname.
        explicit node_create_database_t(std::pmr::memory_resource* resource, std::string dbname);

        const std::string& dbname() const noexcept { return dbname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
    };

    using node_create_database_ptr = boost::intrusive_ptr<node_create_database_t>;
    node_create_database_ptr make_node_create_database(std::pmr::memory_resource* resource, std::string dbname);

} // namespace components::logical_plan
