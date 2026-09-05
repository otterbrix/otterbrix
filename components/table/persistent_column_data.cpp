#include "persistent_column_data.hpp"

#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>

namespace components::table {

    void persistent_column_data_t::serialize(storage::metadata_writer_t& writer) const {
        // own entry count (see the header: not derivable from data_pointers for nested nodes)
        writer.write<uint64_t>(count);

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

        // statistics (always written — the reader consumes them unconditionally)
        writer.write<uint8_t>(statistics.has_stats() ? 1 : 0);
        if (statistics.has_stats()) {
            statistics.serialize(writer);
        }

        // per-segment statistics (always written — same contract)
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

        result.count = reader.read<uint64_t>();

        auto dp_count = reader.read<uint32_t>();
        result.data_pointers.resize(dp_count);
        for (uint32_t i = 0; i < dp_count; i++) {
            result.data_pointers[i] = storage::data_pointer_t::deserialize(reader);
        }

        auto child_count = reader.read<uint32_t>();
        result.child_columns.resize(child_count);
        for (uint32_t i = 0; i < child_count; i++) {
            result.child_columns[i] =
                std::make_unique<persistent_column_data_t>(persistent_column_data_t::deserialize(resource, reader));
        }

        // Statistics fields are read UNCONDITIONALLY. The `if (!reader.finished())` guards
        // that wrapped them were a backward-compatibility reader for record shapes no build of
        // this format ever ships (CURRENT_VERSION is 0 forever, serialize() above always
        // writes both fields) — and they never even guarded what they claimed: finished_ is
        // LAZY (it flips only when a read exhausts the chain), so at guard time it is false
        // for every record deserialized mid-stream and the branch was taken regardless. A
        // genuinely truncated record now lands in the reader's own sticky data_corruption
        // (read past end of chain), which the load boundary checks — loud, not defaulted
        // (rule 6, no fallback).
        auto has_stats_flag = reader.read<uint8_t>();
        if (has_stats_flag != 0) {
            result.statistics = base_statistics_t::deserialize(resource, reader);
        }

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

        return result;
    }

} // namespace components::table
