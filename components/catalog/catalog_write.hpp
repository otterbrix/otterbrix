#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::catalog {

    // Plain data carrier: one pg_catalog row to be appended.
    // Returned by build_create_table_writes so that catalog/ has no dependency on logical_plan/.
    struct catalog_write_t {
        collection_full_name_t table;
        vector::data_chunk_t   row;
    };

} // namespace components::catalog