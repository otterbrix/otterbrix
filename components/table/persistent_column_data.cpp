#include "persistent_column_data.hpp"

#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>

namespace components::table {
    void persistent_column_data_t::serialize(storage::metadata_writer_t& writer) const {
        // data pointers
        writer.write<uint32_t>(static_cast<uint32_t>(data_pointers.size()));
        for (const auto& dp : data_pointers) {
            dp.serialize(writer);
        }

        // validity (recursive, optional)
        writer.write<uint8_t>(validity ? 1 : 0);
        if (validity) {
            validity->serialize(writer);
        }

        // child columns (recursive)
        writer.write<uint32_t>(static_cast<uint32_t>(child_columns.size()));
        for (const auto& child : child_columns) {
            child->serialize(writer);
        }

        // statistics (v2 field)
        writer.write<uint8_t>(statistics.has_stats() ? 1 : 0);
        if (statistics.has_stats()) {
            statistics.serialize(writer);
        }

        // per-segment statistics (v3 field)
        writer.write<uint8_t>(segment_statistics.empty() ? 0 : 1);
        if (!segment_statistics.empty()) {
            writer.write<uint32_t>(static_cast<uint32_t>(segment_statistics.size()));
            for (const auto& seg_stats : segment_statistics) {
                writer.write<uint8_t>(seg_stats.has_stats() ? 1 : 0);
                if (seg_stats.has_stats()) {
                    seg_stats.serialize(writer);
                }
            }
        }
    }

    persistent_column_data_t persistent_column_data_t::deserialize(std::pmr::memory_resource* resource,
                                                                   storage::metadata_reader_t& reader) {
        persistent_column_data_t result(resource);

        auto dp_count = reader.read<uint32_t>();
        result.data_pointers.resize(dp_count);
        for (uint32_t i = 0; i < dp_count; i++) {
            result.data_pointers[i] = storage::data_pointer_t::deserialize(reader);
        }

        if (reader.read<uint8_t>() != 0) {
            result.validity =
                std::make_unique<persistent_column_data_t>(persistent_column_data_t::deserialize(resource, reader));
        }

        auto child_count = reader.read<uint32_t>();
        result.child_columns.resize(child_count);
        for (uint32_t i = 0; i < child_count; i++) {
            result.child_columns[i] =
                std::make_unique<persistent_column_data_t>(persistent_column_data_t::deserialize(resource, reader));
        }

        // statistics (v2 field) — read if available
        if (!reader.finished()) {
            auto has_stats_flag = reader.read<uint8_t>();
            if (has_stats_flag != 0) {
                result.statistics = base_statistics_t::deserialize(resource, reader);
            }
        }

        // per-segment statistics (v3 field) — read if available
        if (!reader.finished()) {
            auto has_seg_stats_flag = reader.read<uint8_t>();
            if (has_seg_stats_flag != 0) {
                auto seg_count = reader.read<uint32_t>();
                result.segment_statistics.reserve(seg_count);
                for (uint32_t i = 0; i < seg_count; i++) {
                    auto has_this = reader.read<uint8_t>();
                    if (has_this != 0) {
                        result.segment_statistics.push_back(base_statistics_t::deserialize(resource, reader));
                    } else {
                        result.segment_statistics.push_back(base_statistics_t(resource));
                    }
                }
            }
        }

        return result;
    }

} // namespace components::table

namespace components::table::storage {
    void row_group_pointer_t::serialize(metadata_writer_t& writer) const {
        writer.write<uint64_t>(row_start);
        writer.write<uint64_t>(tuple_count);

        writer.write<uint32_t>(static_cast<uint32_t>(columns.size()));
        for (const auto& column : columns) {
            column.serialize(writer);
        }

        writer.write<uint32_t>(static_cast<uint32_t>(deletes_pointers.size()));
        for (const auto& dp : deletes_pointers) {
            dp.serialize(writer);
        }
    }

    row_group_pointer_t row_group_pointer_t::deserialize(std::pmr::memory_resource* resource,
                                                         metadata_reader_t& reader) {
        row_group_pointer_t result;
        result.row_start = reader.read<uint64_t>();
        result.tuple_count = reader.read<uint64_t>();

        auto column_count = reader.read<uint32_t>();
        result.columns.reserve(column_count);
        for (uint32_t i = 0; i < column_count; i++) {
            result.columns.push_back(persistent_column_data_t::deserialize(resource, reader));
        }

        auto deletes_count = reader.read<uint32_t>();
        result.deletes_pointers.resize(deletes_count);
        for (uint32_t i = 0; i < deletes_count; i++) {
            result.deletes_pointers[i] = data_pointer_t::deserialize(reader);
        }

        return result;
    }
} // namespace components::table::storage
