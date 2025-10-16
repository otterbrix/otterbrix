#pragma once

#include "arrow.hpp"
#include "scaner/arrow_type.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>

#include <vector>

namespace components::vector::arrow {

    void to_arrow_schema(ArrowSchema* out_schema, const std::pmr::vector<types::complex_logical_type>& types);
    void to_arrow_array(data_chunk_t& input, ArrowArray* out_array);
    void populate_arrow_table_schema(arrow_table_schema_t& arrow_table, const ArrowSchema& arrow_schema);
    arrow_table_schema_t schema_from_arrow(ArrowSchema* schema);
    data_chunk_t data_chunk_from_arrow(std::pmr::memory_resource* resource,
                                       ArrowArray* arrow_array,
                                       arrow_table_schema_t converted_schema);

} // namespace components::vector::arrow
