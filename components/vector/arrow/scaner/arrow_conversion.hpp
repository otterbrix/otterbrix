#pragma once

#include "arrow_type.hpp"

namespace components::vector::arrow::scaner {

    void set_validity(vector_t& vector,
                      ArrowArray& array,
                      size_t chunk_offset,
                      size_t size,
                      int64_t parent_offset,
                      int64_t nested_offset,
                      bool add_null = false);

    void arrow_column_to_run_end_encoded(vector_t& vector,
                                         const ArrowArray& array,
                                         size_t chunk_offset,
                                         arrow_array_scan_state& array_state,
                                         size_t size,
                                         const arrow_type& arrow_type,
                                         int64_t nested_offset = -1,
                                         validity_mask_t* parent_mask = nullptr,
                                         uint64_t parent_offset = 0);

    void arrow_column_to_vector(vector_t& vector,
                                ArrowArray& array,
                                size_t chunk_offset,
                                arrow_array_scan_state& array_state,
                                size_t size,
                                const arrow_type& arrow_type,
                                int64_t nested_offset = -1,
                                validity_mask_t* parent_mask = nullptr,
                                uint64_t parent_offset = 0,
                                bool ignore_extensions = false);

    void arrow_column_to_dictionary(vector_t& vector,
                                    ArrowArray& array,
                                    size_t chunk_offset,
                                    arrow_array_scan_state& array_state,
                                    size_t size,
                                    const arrow_type& arrow_type,
                                    int64_t nested_offset = -1,
                                    const validity_mask_t* parent_mask = nullptr,
                                    uint64_t parent_offset = 0);

} // namespace components::vector::arrow::scaner