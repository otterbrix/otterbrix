#pragma once

#include <cstdint>
#include <memory>

#include <components/table/persistent_column_data.hpp>

namespace components::table {

    namespace storage {
        class partial_block_manager_t;
    } // namespace storage

    class column_data_t;

    class column_data_checkpointer_t {
    public:
        column_data_checkpointer_t(column_data_t& column_data, storage::partial_block_manager_t& partial_block_manager);

        persistent_column_data_t checkpoint();

    private:
        column_data_t& column_data_;
        storage::partial_block_manager_t& partial_block_manager_;
    };

} // namespace components::table
