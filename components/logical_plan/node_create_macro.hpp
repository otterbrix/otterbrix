#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_create_macro_t final : public node_t {
    public:
        node_create_macro_t(std::pmr::memory_resource* resource,
                            const collection_full_name_t& name,
                            std::vector<std::string> parameters,
                            std::string body_sql);

        const std::vector<std::string>& parameters() const { return parameters_; }
        const std::string& body_sql() const { return body_sql_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::vector<std::string> parameters_;
        std::string body_sql_;
    };

    using node_create_macro_ptr = boost::intrusive_ptr<node_create_macro_t>;
    node_create_macro_ptr make_node_create_macro(std::pmr::memory_resource* resource,
                                                 const collection_full_name_t& name,
                                                 std::vector<std::string> parameters,
                                                 std::string body_sql);

} // namespace components::logical_plan
