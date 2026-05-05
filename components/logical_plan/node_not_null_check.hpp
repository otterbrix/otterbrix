#pragma once

#include "node.hpp"

#include <string>
#include <vector>

namespace components::logical_plan {

    // Checks that no NOT NULL column has a null value in the incoming chunk.
    // Inserted by the planner above node_insert / node_update when the target
    // table has NOT NULL columns without defaults.
    class node_not_null_check_t final : public node_t {
    public:
        explicit node_not_null_check_t(std::pmr::memory_resource*    resource,
                                        const collection_full_name_t& collection,
                                        std::vector<std::string>      not_null_columns);

        const std::vector<std::string>& not_null_columns() const { return not_null_columns_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        std::vector<std::string> not_null_columns_;
    };

    using node_not_null_check_ptr = boost::intrusive_ptr<node_not_null_check_t>;

} // namespace components::logical_plan
