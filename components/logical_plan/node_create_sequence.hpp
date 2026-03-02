#pragma once

#include "node.hpp"

#include <cstdint>
#include <limits>

namespace components::logical_plan {

    class node_create_sequence_t final : public node_t {
    public:
        node_create_sequence_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& name,
                               int64_t start = 1,
                               int64_t increment = 1,
                               int64_t min_value = 1,
                               int64_t max_value = std::numeric_limits<int64_t>::max());

        int64_t start() const { return start_; }
        int64_t increment() const { return increment_; }
        int64_t min_value() const { return min_value_; }
        int64_t max_value() const { return max_value_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        int64_t start_;
        int64_t increment_;
        int64_t min_value_;
        int64_t max_value_;
    };

    using node_create_sequence_ptr = boost::intrusive_ptr<node_create_sequence_t>;
    node_create_sequence_ptr make_node_create_sequence(std::pmr::memory_resource* resource,
                                                       const collection_full_name_t& name,
                                                       int64_t start = 1,
                                                       int64_t increment = 1,
                                                       int64_t min_value = 1,
                                                       int64_t max_value = std::numeric_limits<int64_t>::max());

} // namespace components::logical_plan
