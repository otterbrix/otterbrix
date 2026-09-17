#include "column_checkpoint_state.hpp"

#include <cstring>
#include <map>
#include <memory_resource>
#include <vector>

#include <components/table/column_data.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/column_state.hpp>
#include <components/table/storage/block_manager.hpp>
#include <components/table/storage/buffer_handle.hpp>
#include <components/table/storage/buffer_manager.hpp>

namespace components::table {

    namespace {

        // Avoids a GCC 14 false-positive -Wstringop-overread from std::vector<std::byte>::operator<=>.
        struct byte_vector_less {
            bool operator()(const std::vector<std::byte>& a, const std::vector<std::byte>& b) const {
                if (a.size() != b.size())
                    return a.size() < b.size();
                if (a.empty())
                    return false;
                return std::memcmp(a.data(), b.data(), a.size()) < 0;
            }
        };

        bool is_constant_data(const std::byte* data, uint64_t type_size, uint64_t count) {
            if (count <= 1) {
                return true;
            }
            auto* base = data;
            for (uint64_t i = 1; i < count; i++) {
                if (std::memcmp(base, data + i * type_size, type_size) != 0) {
                    return false;
                }
            }
            return true;
        }

        uint32_t count_runs(const std::byte* data, uint64_t type_size, uint64_t count) {
            if (count == 0)
                return 0;
            uint32_t runs = 1;
            for (uint64_t i = 1; i < count; i++) {
                if (std::memcmp(data + (i - 1) * type_size, data + i * type_size, type_size) != 0) {
                    runs++;
                }
            }
            return runs;
        }

        // RLE buffer layout: [uint32_t num_runs][value(type_size bytes) + run_length(4 bytes)]...
        uint64_t
        build_rle_buffer(const std::byte* data, uint64_t type_size, uint64_t count, std::vector<std::byte>& out) {
            if (count == 0) {
                out.resize(sizeof(uint32_t));
                uint32_t zero = 0;
                std::memcpy(out.data(), &zero, sizeof(uint32_t));
                return sizeof(uint32_t);
            }

            uint32_t num_runs = count_runs(data, type_size, count);

            uint64_t entry_size = type_size + sizeof(uint32_t);
            uint64_t total_size = sizeof(uint32_t) + num_runs * entry_size;
            out.resize(total_size);

            auto* ptr = out.data();
            std::memcpy(ptr, &num_runs, sizeof(uint32_t));
            ptr += sizeof(uint32_t);

            uint32_t run_length = 1;
            for (uint64_t i = 1; i <= count; i++) {
                if (i < count && std::memcmp(data + (i - 1) * type_size, data + i * type_size, type_size) == 0) {
                    run_length++;
                } else {
                    std::memcpy(ptr, data + (i - 1) * type_size, type_size);
                    ptr += type_size;
                    std::memcpy(ptr, &run_length, sizeof(uint32_t));
                    ptr += sizeof(uint32_t);
                    run_length = 1;
                }
            }

            return total_size;
        }

        static constexpr uint16_t MAX_DICT_ENTRIES = 65535;

        struct dict_analysis_t {
            uint16_t num_unique{0};
            uint64_t compressed_size{0};
            std::map<std::vector<std::byte>, uint16_t, byte_vector_less> value_map;
        };

        dict_analysis_t analyze_dictionary(const std::byte* data, uint64_t type_size, uint64_t count) {
            dict_analysis_t result;
            if (count == 0)
                return result;

            std::map<std::vector<std::byte>, uint16_t, byte_vector_less> mapping;
            for (uint64_t i = 0; i < count; i++) {
                std::vector<std::byte> key(data + i * type_size, data + (i + 1) * type_size);
                if (mapping.find(key) == mapping.end()) {
                    if (mapping.size() >= MAX_DICT_ENTRIES) {
                        return result;
                    }
                    mapping[key] = static_cast<uint16_t>(mapping.size());
                }
            }

            result.num_unique = static_cast<uint16_t>(mapping.size());
            uint64_t index_size = (result.num_unique <= 256) ? 1 : 2;
            result.compressed_size = sizeof(uint16_t) + result.num_unique * type_size + count * index_size;
            result.value_map = std::move(mapping);
            return result;
        }

        // Dictionary buffer: [num_unique][values...][indices...], index 1 byte if num_unique<=256 else 2 bytes.
        uint64_t build_dict_buffer(const std::byte* data,
                                   uint64_t type_size,
                                   uint64_t count,
                                   const dict_analysis_t& analysis,
                                   std::vector<std::byte>& out) {
            out.resize(analysis.compressed_size);
            auto* ptr = out.data();

            std::memcpy(ptr, &analysis.num_unique, sizeof(uint16_t));
            ptr += sizeof(uint16_t);

            std::vector<const std::byte*> ordered(analysis.num_unique);
            for (auto& [key, idx] : analysis.value_map) {
                ordered[idx] = key.data();
            }
            for (uint16_t i = 0; i < analysis.num_unique; i++) {
                std::memcpy(ptr, ordered[i], type_size);
                ptr += type_size;
            }

            bool use_uint8 = (analysis.num_unique <= 256);
            for (uint64_t i = 0; i < count; i++) {
                std::vector<std::byte> key(data + i * type_size, data + (i + 1) * type_size);
                uint16_t idx = analysis.value_map.at(key);
                if (use_uint8) {
                    auto u8 = static_cast<uint8_t>(idx);
                    std::memcpy(ptr, &u8, 1);
                    ptr += 1;
                } else {
                    std::memcpy(ptr, &idx, 2);
                    ptr += 2;
                }
            }

            return analysis.compressed_size;
        }

    } // anonymous namespace

    column_checkpoint_state_t::column_checkpoint_state_t(column_data_t& column_data,
                                                         storage::partial_block_manager_t& partial_block_manager)
        : column_data_(column_data)
        , partial_block_manager_(partial_block_manager) {}

    core::result_wrapper_t<bool>
    column_checkpoint_state_t::flush_segment(column_segment_t& segment, uint64_t row_start, uint64_t tuple_count) {
        auto& block_manager = column_data_.block_manager();

        const auto phys = segment.type.to_physical_type();
        const bool is_fixed_size = (phys != types::physical_type::STRING && phys != types::physical_type::BIT &&
                                    phys != types::physical_type::INVALID);
        const auto loaded_compression = segment.compression();

        // A disk-backed segment (is_reloadable()) is READ-ONLY, so a final-form image can NAME the existing
        // block instead of copying it. Without this, every round rewrote the whole table -- measured offline:
        // 113 of 294 blocks (29.6 MB of a 77 MB file), ~2851 bytes/row.
        const bool disk_backed = segment.block && segment.block->is_reloadable();
        if (disk_backed) {
            const bool analyzable = is_fixed_size && tuple_count > 1 && segment.type_size > 0;
            const bool final_form = loaded_compression != compression::compression_type::UNCOMPRESSED ||
                                    phys == types::physical_type::STRING || !analyzable;
            if (final_form) {
                storage::data_pointer_t dp;
                dp.row_start = row_start;
                dp.tuple_count = tuple_count;
                dp.block_pointer =
                    storage::block_pointer_t(segment.block->block_id(), static_cast<uint32_t>(segment.block_offset()));
                dp.compression = loaded_compression;
                dp.segment_size = segment.segment_size();
                if (auto* state = segment.segment_state()) {
                    dp.overflow_blocks = state->additional_blocks();
                }
                data_pointers_.push_back(std::move(dp));
                return true;
            }
        }

        auto pinned = block_manager.buffer_manager.pin(segment.block);
        if (pinned.has_error()) {
            return pinned.convert_error<bool>();
        }
        auto& handle = pinned.value();
        auto* data = handle.ptr();

        // A compressed segment holds COMPRESSED bytes in the pinned buffer, not raw values; every producer
        // of compressed segments makes them disk-backed, so this is a defensive fallback, not dead code.
        if (loaded_compression != compression::compression_type::UNCOMPRESSED && data && segment.segment_size() > 0) {
            const auto compressed_size = segment.segment_size();
            auto* compressed_data = data + segment.block_offset();
            auto allocation = partial_block_manager_.get_block_allocation(compressed_size);
            partial_block_manager_.write_to_block(allocation.block_id,
                                                  allocation.offset_in_block,
                                                  compressed_data,
                                                  compressed_size);
            storage::data_pointer_t dp;
            dp.row_start = row_start;
            dp.tuple_count = tuple_count;
            dp.block_pointer = storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
            dp.compression = loaded_compression;
            dp.segment_size = compressed_size;
            data_pointers_.push_back(dp);
            return true;
        }

        if (is_fixed_size && tuple_count > 1 && data && segment.type_size > 0) {
            auto* segment_data = data + segment.block_offset();

            if (is_constant_data(segment_data, segment.type_size, tuple_count)) {
                auto constant_size = segment.type_size;
                auto allocation = partial_block_manager_.get_block_allocation(constant_size);
                partial_block_manager_.write_to_block(allocation.block_id,
                                                      allocation.offset_in_block,
                                                      segment_data,
                                                      constant_size);

                storage::data_pointer_t dp;
                dp.row_start = row_start;
                dp.tuple_count = tuple_count;
                dp.block_pointer = storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
                dp.compression = compression::compression_type::CONSTANT;
                dp.segment_size = constant_size;
                data_pointers_.push_back(dp);
                return true;
            }

            uint32_t num_runs = count_runs(segment_data, segment.type_size, tuple_count);
            uint64_t entry_size = segment.type_size + sizeof(uint32_t);
            uint64_t rle_size = sizeof(uint32_t) + num_runs * entry_size;
            uint64_t uncompressed_size = segment.type_size * tuple_count;

            if (rle_size < uncompressed_size) {
                std::vector<std::byte> rle_buf;
                build_rle_buffer(segment_data, segment.type_size, tuple_count, rle_buf);

                auto allocation = partial_block_manager_.get_block_allocation(rle_size);
                partial_block_manager_.write_to_block(allocation.block_id,
                                                      allocation.offset_in_block,
                                                      rle_buf.data(),
                                                      rle_size);

                storage::data_pointer_t dp;
                dp.row_start = row_start;
                dp.tuple_count = tuple_count;
                dp.block_pointer = storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
                dp.compression = compression::compression_type::RLE;
                dp.segment_size = rle_size;
                data_pointers_.push_back(dp);
                return true;
            }

            auto dict_info = analyze_dictionary(segment_data, segment.type_size, tuple_count);
            if (dict_info.num_unique > 1 && dict_info.compressed_size < uncompressed_size) {
                std::vector<std::byte> dict_buf;
                build_dict_buffer(segment_data, segment.type_size, tuple_count, dict_info, dict_buf);

                auto allocation = partial_block_manager_.get_block_allocation(dict_info.compressed_size);
                partial_block_manager_.write_to_block(allocation.block_id,
                                                      allocation.offset_in_block,
                                                      dict_buf.data(),
                                                      dict_info.compressed_size);

                storage::data_pointer_t dp;
                dp.row_start = row_start;
                dp.tuple_count = tuple_count;
                dp.block_pointer = storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
                dp.compression = compression::compression_type::DICTIONARY;
                dp.segment_size = dict_info.compressed_size;
                data_pointers_.push_back(dp);
                return true;
            }
        }

        auto segment_size = segment.segment_size();
        storage::data_pointer_t dp;

        if (disk_backed) {
            dp.row_start = row_start;
            dp.tuple_count = tuple_count;
            dp.block_pointer =
                storage::block_pointer_t(segment.block->block_id(), static_cast<uint32_t>(segment.block_offset()));
            dp.compression = compression::compression_type::UNCOMPRESSED;
            dp.segment_size = segment_size;
            data_pointers_.push_back(std::move(dp));
            return true;
        }

        // A STRING segment persists a TIGHT image: compact_string_dictionary trims slack left by partial fills
        // (measured before this: 8000 rows of 4090-byte text cost 9635 bytes/row, 1365 of that pure slack),
        // and persist_string_overflow moves TRANSIENT overflow blocks into real file blocks before rewriting.
        if (phys == types::physical_type::STRING && data && segment_size > 0 && tuple_count > 0) {
            auto* segment_data = data + segment.block_offset();
            std::pmr::vector<std::byte> rewritten(segment_size, std::byte{0}, column_data_.resource());
            std::memcpy(rewritten.data(), segment_data, segment_size);
            auto compacted = segment.compact_string_dictionary(rewritten.data(), segment_size, tuple_count);
            if (compacted.has_error()) {
                return compacted.convert_error<bool>();
            }
            segment_size = compacted.value();
            if (segment.references_string_overflow(rewritten.data(), segment_size, tuple_count)) {
                auto persisted = segment.persist_string_overflow(rewritten.data(),
                                                                 segment_size,
                                                                 tuple_count,
                                                                 partial_block_manager_,
                                                                 dp.overflow_blocks);
                if (persisted.has_error()) {
                    return persisted;
                }
            }
            auto string_allocation = partial_block_manager_.get_block_allocation(segment_size);
            partial_block_manager_.write_to_block(string_allocation.block_id,
                                                  string_allocation.offset_in_block,
                                                  rewritten.data(),
                                                  segment_size);
            dp.row_start = row_start;
            dp.tuple_count = tuple_count;
            dp.block_pointer = storage::block_pointer_t(string_allocation.block_id, string_allocation.offset_in_block);
            dp.compression = compression::compression_type::UNCOMPRESSED;
            dp.segment_size = segment_size;
            data_pointers_.push_back(std::move(dp));
            return true;
        }

        auto allocation = partial_block_manager_.get_block_allocation(segment_size);
        if (data && segment_size > 0) {
            // A segment packed via partial-block packing lives at a NON-ZERO block_offset in a shared block;
            // reading from data (offset 0) instead would copy a neighbour's bytes into the checkpoint.
            auto* segment_data = data + segment.block_offset();
            partial_block_manager_.write_to_block(allocation.block_id,
                                                  allocation.offset_in_block,
                                                  segment_data,
                                                  segment_size);
        }

        dp.row_start = row_start;
        dp.tuple_count = tuple_count;
        dp.block_pointer = storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
        dp.compression = compression::compression_type::UNCOMPRESSED;
        dp.segment_size = segment_size;
        data_pointers_.push_back(std::move(dp));
        return true;
    }

    persistent_column_data_t column_checkpoint_state_t::get_persistent_data() const {
        persistent_column_data_t result(column_data_.resource());
        result.data_pointers = data_pointers_;
        return result;
    }

} // namespace components::table
