#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_create_view_t final : public node_t {
    public:
        node_create_view_t(std::pmr::memory_resource* resource,
                           const collection_full_name_t& name,
                           std::string query_sql);

        const std::string& query_sql() const { return query_sql_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string query_sql_;
    };

    using node_create_view_ptr = boost::intrusive_ptr<node_create_view_t>;
    node_create_view_ptr make_node_create_view(std::pmr::memory_resource* resource,
                                               const collection_full_name_t& name,
                                               std::string query_sql);

} // namespace components::logical_plan
