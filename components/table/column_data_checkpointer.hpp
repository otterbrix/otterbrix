#pragma once

#include <cstdint>
#include <memory>

#include <core/result_wrapper.hpp>

#include <components/table/persistent_column_data.hpp>

namespace components::table {

    namespace storage {
        class partial_block_manager_t;
    } // namespace storage

    class column_data_t;

    class column_data_checkpointer_t {
    public:
        column_data_checkpointer_t(column_data_t& column_data, storage::partial_block_manager_t& partial_block_manager);

        // Returns persistent data on success, out_of_memory when a segment pin fails during the
        // flush, and unimplemented_yet when the column still carries a committed-update overlay:
        // this walks segments only, and the overlay is not one, so serializing it would write the
        // PRE-update bytes over a value only the WAL still holds. Folding the overlay in is the
        // rebuild's job (data_table_t::compact), and the caller that skips the rebuild
        // (agent_disk_t::checkpoint_inner's failed-round retry) asks first.
        [[nodiscard]] core::result_wrapper_t<persistent_column_data_t> checkpoint();

    private:
        column_data_t& column_data_;
        storage::partial_block_manager_t& partial_block_manager_;
    };

} // namespace components::table
