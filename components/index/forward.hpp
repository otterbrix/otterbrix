#pragma once

#include <components/expressions/key.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/types/logical_value.hpp>

namespace components::index {

    using components::logical_plan::index_type;
    using key_t = expressions::key_t;
    using components::logical_plan::keys_base_storage_t;
    using value_t = types::logical_value_t;

    // What ONE index over a table publishes to the planner: the key set it covers and the backend that
    // answers it. The pair is what lets the planner tell an ordered index from a hashed one OVER THE SAME
    // COLUMN, which is a legal pair -- and it is why the type survives as a static constant of each agent
    // class rather than as an accessor on the (now removed) index object.
    //
    // FOUR ALIASES ARE GONE FROM HERE. `id_index` was a uint32 handle -- an index's POSITION in the dead
    // index_engine_t's list -- and it was the SAME TYPE as catalog::oid_t, so the two were interchangeable
    // by accident. An index's identity below the planner is its pg_index.indexrelid (rule 16), and nothing
    // hands out positions any more. `query_t` and `result_set_t` named the parameters of two index_engine_t
    // free functions whose bodies were commented out. `INDEX_ID_UNDEFINED` was the sentinel a registration
    // returned when it failed.
    struct index_description_t {
        keys_base_storage_t keys;
        index_type type{index_type::no_valid};
    };

} // namespace components::index
