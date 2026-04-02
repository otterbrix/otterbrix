#include "utils.hpp"

using namespace components::types;
using namespace components::catalog;

namespace test {
    core::error_t create_single_column_table(const collection_full_name_t& name,
                                             complex_logical_type log_t,
                                             catalog& cat,
                                             std::pmr::memory_resource* resource) {
        log_t.set_alias(name.collection);
        schema sch(resource,
                   {components::table::column_definition_t{log_t.alias(), log_t}},
                   {field_description(1, true, "test")});
        return cat.create_table({resource, name}, {resource, sch});
    }
} // namespace test
