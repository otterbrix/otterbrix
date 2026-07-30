#pragma once

#include "arrow.hpp"
#include "scaner/arrow_type.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <vector>

namespace components::vector::arrow {

    // A logical type Arrow has no format string for is an error, not an exception.
    //
    // Takes a schema and not a type list: Arrow names every exported column, and a column's
    // name has not been inside its type since M3-B5.
    [[nodiscard]] core::error_t to_arrow_schema(ArrowSchema* out_schema, const schema_t& schema);
    // A column type Arrow has no appender for, or a buffer that cannot be grown, is an error,
    // not an exception. out_array is left untouched when one is returned.
    [[nodiscard]] core::error_t to_arrow_array(data_chunk_t& input, ArrowArray* out_array);
    [[nodiscard]] core::error_t populate_arrow_table_schema(std::pmr::memory_resource* resource,
                                                            arrow_table_schema_t& arrow_table,
                                                            const ArrowSchema& arrow_schema);
    core::result_wrapper_t<arrow_table_schema_t> schema_from_arrow(std::pmr::memory_resource* resource,
                                                                   ArrowSchema* schema);
    core::result_wrapper_t<data_chunk_t> data_chunk_from_arrow(std::pmr::memory_resource* resource,
                                                               ArrowArray* arrow_array,
                                                               arrow_table_schema_t converted_schema);

} // namespace components::vector::arrow
