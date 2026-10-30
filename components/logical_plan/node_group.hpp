#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_group_t final : public node_t {
    public:
        explicit node_group_t(std::pmr::memory_resource* resource);

        const std::pmr::vector<components::types::complex_logical_type>& input_types() const noexcept {
            return input_types_;
        }
        void set_input_types(std::pmr::vector<components::types::complex_logical_type> types) {
            input_types_ = std::move(types);
        }

        size_t internal_aggregate_count{0};
        // Number of visible SELECT-clause columns recorded BEFORE the
        // transformer appends hidden internal aggregates for HAVING etc.
        // PR #479-style projection lineage uses this to know where the
        // visible SELECT list ends.
        size_t visible_select_count{0};

    private:
        std::pmr::vector<components::types::complex_logical_type> input_types_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_group_ptr = boost::intrusive_ptr<node_group_t>;

    node_group_ptr make_node_group(std::pmr::memory_resource* resource);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource, const std::vector<expression_ptr>& expressions);

    node_group_ptr make_node_group(std::pmr::memory_resource* resource,
                                   const std::pmr::vector<expression_ptr>& expressions);

} // namespace components::logical_plan
