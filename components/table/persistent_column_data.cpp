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

        // child columns (recursive)
        writer.write<uint32_t>(static_cast<uint32_t>(child_columns.size()));
        for (const auto& child : child_columns) {
            child->serialize(writer);
        }

        // statistics (v2 field)
        writer.write<uint8_t>(statistics.has_value() ? 1 : 0);
        if (statistics.has_value()) {
            statistics->serialize(writer);
        }

        // per-segment statistics (v3 field)
        writer.write<uint8_t>(segment_statistics.empty() ? 0 : 1);
        if (!segment_statistics.empty()) {
            writer.write<uint32_t>(static_cast<uint32_t>(segment_statistics.size()));
            for (const auto& seg_stats : segment_statistics) {
                writer.write<uint8_t>(seg_stats.has_value() ? 1 : 0);
                if (seg_stats.has_value()) {
                    seg_stats->serialize(writer);
                }
            }
        }
    }

    persistent_column_data_t
    persistent_column_data_t::deserialize(std::pmr::memory_resource* resource,
                                           storage::metadata_reader_t& reader) {
        persistent_column_data_t result;

        auto dp_count = reader.read<uint32_t>();
        result.data_pointers.resize(dp_count);
        for (uint32_t i = 0; i < dp_count; i++) {
            result.data_pointers[i] = storage::data_pointer_t::deserialize(reader);
        }

        auto child_count = reader.read<uint32_t>();
        result.child_columns.resize(child_count);
        for (uint32_t i = 0; i < child_count; i++) {
            result.child_columns[i] = std::make_unique<persistent_column_data_t>(
                persistent_column_data_t::deserialize(resource, reader));
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
                result.segment_statistics.resize(seg_count);
                for (uint32_t i = 0; i < seg_count; i++) {
                    auto has_this = reader.read<uint8_t>();
                    if (has_this != 0) {
                        result.segment_statistics[i] = base_statistics_t::deserialize(resource, reader);
                    }
                }
            }
        }

        return result;
    }

} // namespace components::table
