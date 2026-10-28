#pragma once

#include "arrow.hpp"
#include "scaner/arrow_type.hpp"

#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

#include <vector>

namespace components::vector::arrow {

    [[nodiscard]] core::error_t to_arrow_schema(std::pmr::memory_resource* resource,
                                                ArrowSchema* out_schema,
                                                const std::pmr::vector<types::complex_logical_type>& types);
    [[nodiscard]] core::error_t to_arrow_array(data_chunk_t& input, ArrowArray* out_array);
    [[nodiscard]] core::error_t populate_arrow_table_schema(std::pmr::memory_resource* resource,
                                                            arrow_table_schema_t& arrow_table,
                                                            const ArrowSchema& arrow_schema);
    core::result_wrapper_t<arrow_table_schema_t> schema_from_arrow(std::pmr::memory_resource* resource,
                                                                   ArrowSchema* schema);
    core::result_wrapper_t<std::pmr::vector<data_chunk_t>>
    data_chunks_from_arrow(std::pmr::memory_resource* resource,
                           ArrowArray* arrow_array,
                           arrow_table_schema_t converted_schema);

} // namespace components::vector::arrow
