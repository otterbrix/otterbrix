#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include <core/result_wrapper.hpp>

#include <components/table/base_statistics.hpp>
#include <components/table/persistent_column_data.hpp>
#include <components/table/storage/data_pointer.hpp>
#include <components/table/storage/partial_block_manager.hpp>

namespace components::table {

    class column_data_t;
    class column_segment_t;

    class column_checkpoint_state_t {
    public:
        column_checkpoint_state_t(column_data_t& column_data, storage::partial_block_manager_t& partial_block_manager);

        // Returns out_of_memory when pinning the segment buffer fails; true on success.
        core::result_wrapper_t<bool> flush_segment(column_segment_t& segment, uint64_t row_start, uint64_t tuple_count);

        persistent_column_data_t get_persistent_data() const;

    private:
        column_data_t& column_data_;
        storage::partial_block_manager_t& partial_block_manager_;
        std::vector<storage::data_pointer_t> data_pointers_;
    };

} // namespace components::table
