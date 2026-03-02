#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include <components/table/base_statistics.hpp>
#include <components/table/storage/data_pointer.hpp>

namespace components::table::storage {
    class metadata_writer_t;
    class metadata_reader_t;
} // namespace components::table::storage

namespace components::table {

    struct persistent_column_data_t {
        explicit persistent_column_data_t(std::pmr::memory_resource* resource)
            : statistics(resource) {}

        std::vector<storage::data_pointer_t> data_pointers;
        std::vector<std::unique_ptr<persistent_column_data_t>> child_columns;
        base_statistics_t statistics;
        std::vector<base_statistics_t> segment_statistics; // per-segment stats (parallel to data_pointers)

        void serialize(storage::metadata_writer_t& writer) const;
        static persistent_column_data_t deserialize(std::pmr::memory_resource* resource,
                                                    storage::metadata_reader_t& reader);
    };

} // namespace components::table
