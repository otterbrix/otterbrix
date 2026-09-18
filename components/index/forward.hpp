#pragma once

#include <components/expressions/key.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/types/logical_value.hpp>

namespace components::index {

    using components::logical_plan::index_type;
    using key_t = expressions::key_t;
    using components::logical_plan::keys_base_storage_t;
    using value_t = types::logical_value_t;

    // What ONE index over a table publishes to the planner: the key set it covers and the backend
    // that answers it -- the pair lets the planner tell an ordered index from a hashed one over
    // the SAME column, a legal combination. Survives as a static constant per agent class, not
    // an accessor on an index object. An index's identity below the planner is its
    // pg_index.indexrelid; nothing here hands out positional ids.
    struct index_description_t {
        keys_base_storage_t keys;
        index_type type{index_type::no_valid};
    };

} // namespace components::index
