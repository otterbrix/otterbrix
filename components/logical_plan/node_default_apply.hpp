#pragma once

#include "node.hpp"

#include <components/types/logical_value.hpp>

#include <cstdint>
#include <utility>
#include <vector>

namespace components::logical_plan {

    // Applies per-column default values to null cells in the incoming chunk.
    // Inserted by the planner above node_insert when the target table has columns
    // with default values. Each entry maps a column index to its default value.
    class node_default_apply_t final : public node_t {
    public:
        using default_entry_t = std::pair<std::size_t, components::types::logical_value_t>;

        explicit node_default_apply_t(std::pmr::memory_resource*      resource,
                                       const collection_full_name_t&   collection,
                                       std::vector<default_entry_t>    defaults);

        const std::vector<default_entry_t>& defaults() const { return defaults_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        std::vector<default_entry_t> defaults_;
    };

    using node_default_apply_ptr = boost::intrusive_ptr<node_default_apply_t>;

} // namespace components::logical_plan
