#pragma once

#include "node.hpp"

namespace components::logical_plan {

    // Ordered list of DDL sub-nodes executed sequentially without output forwarding.
    // Emitted by the planner for DDL operations that involve multiple catalog steps.
    class node_sequence_t final : public node_t {
    public:
        explicit node_sequence_t(std::pmr::memory_resource* resource);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_sequence_ptr = boost::intrusive_ptr<node_sequence_t>;

} // namespace components::logical_plan