#pragma once

#include "node.hpp"

namespace components::logical_plan {

    // Executes an ordered list of child nodes sequentially, without forwarding
    // output between them. Used for DDL pipelines (multiple primitive_write /
    // primitive_delete steps) and DELETE + FK cascade sequences.
    class node_sequence_t final : public node_t {
    public:
        explicit node_sequence_t(std::pmr::memory_resource*    resource,
                                  const collection_full_name_t& collection);

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;
    };

    using node_sequence_ptr = boost::intrusive_ptr<node_sequence_t>;

} // namespace components::logical_plan
