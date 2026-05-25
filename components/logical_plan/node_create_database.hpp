#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

namespace components::logical_plan {

    class node_create_database_t final : public node_t {
    public:
        explicit node_create_database_t(std::pmr::memory_resource* resource,
                                        core::dbname_t dbname,
                                        bool if_not_exists = false);

        const std::string& dbname() const noexcept { return dbname_; }
        bool if_not_exists() const noexcept { return if_not_exists_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        bool if_not_exists_;
    };

    using node_create_database_ptr = boost::intrusive_ptr<node_create_database_t>;
    node_create_database_ptr
    make_node_create_database(std::pmr::memory_resource* resource, core::dbname_t dbname, bool if_not_exists = false);

} // namespace components::logical_plan
