#include "utils.hpp"

using namespace components::types;
using namespace components::catalog;

namespace test {
    components::catalog::catalog_error create_single_column_table(const collection_full_name_t& name,
                                                                  complex_logical_type log_t,
                                                                  catalog& cat,
                                                                  std::pmr::memory_resource* resource) {
        log_t.set_alias(name.collection);
        schema sch(resource, create_struct("schema", {log_t}, {field_description(1, true, "test")}), {1});
        return cat.create_table({resource, name}, {resource, sch});
    }

    components::types::logical_type front_cursor_type(components::cursor::cursor_t_ptr cursor) {
        assert(cursor->type_data().size());
        return cursor->type_data().front().type();
    }
} // namespace test
