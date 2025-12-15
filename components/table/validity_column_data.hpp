#pragma once
#include "column_data.hpp"

namespace components::table {

    class validity_column_data_t : public column_data_t {
    public:
        validity_column_data_t(std::pmr::memory_resource* resource,
                               storage::block_manager_t& block_manager,
                               uint64_t column_index,
                               int64_t start_row,
                               column_data_t& parent);

        filter_propagate_result_t check_zonemap(column_scan_state& state, table_filter_t& filter) override;
        void append_data(column_append_state& state, vector::unified_vector_format& uvf, uint64_t count) override;
    };

} // namespace components::table