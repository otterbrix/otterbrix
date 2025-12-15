#include "validity_column_data.hpp"

namespace components::table {

    validity_column_data_t::validity_column_data_t(std::pmr::memory_resource* resource,
                                                   storage::block_manager_t& block_manager,
                                                   uint64_t column_index,
                                                   int64_t start_row,
                                                   column_data_t& parent)
        : column_data_t(resource,
                        block_manager,
                        column_index,
                        start_row,
                        types::complex_logical_type(types::logical_type::VALIDITY),
                        &parent) {}

    filter_propagate_result_t validity_column_data_t::check_zonemap(column_scan_state&, table_filter_t&) {
        return filter_propagate_result_t::NO_PRUNING_POSSIBLE;
    }

    void validity_column_data_t::append_data(column_append_state& state,
                                             vector::unified_vector_format& uvf,
                                             uint64_t count) {
        column_data_t::append_data(state, uvf, count);
    }

} // namespace components::table