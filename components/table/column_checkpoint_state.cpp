#include "column_checkpoint_state.hpp"

#include <cstring>
#include <map>
#include <vector>

#include <components/table/column_data.hpp>
#include <components/table/column_segment.hpp>
#include <components/table/storage/block_manager.hpp>
#include <components/table/storage/buffer_handle.hpp>
#include <components/table/storage/buffer_manager.hpp>

namespace components::table {

    namespace {

        // Custom comparator for std::vector<std::byte> keys — avoids GCC 14
        // false-positive -Wstringop-overread from std::vector<std::byte>::operator<=>
        struct byte_vector_less {
            bool operator()(const std::vector<std::byte>& a, const std::vector<std::byte>& b) const {
                if (a.size() != b.size()) return a.size() < b.size();
                if (a.empty()) return false;
                return std::memcmp(a.data(), b.data(), a.size()) < 0;
            }
        };

        // Check if all fixed-size values in a buffer are identical.
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

        // Count runs in fixed-size data. Returns number of runs.
        uint32_t count_runs(const std::byte* data, uint64_t type_size, uint64_t count) {
            if (count == 0) return 0;
            uint32_t runs = 1;
            for (uint64_t i = 1; i < count; i++) {
                if (std::memcmp(data + (i - 1) * type_size, data + i * type_size, type_size) != 0) {
                    runs++;
                }
            }
            return runs;
        }

        // Build RLE buffer: [uint32_t num_runs][value(ts bytes) + run_length(4 bytes)]...
        // Returns total compressed size.
        uint64_t build_rle_buffer(const std::byte* data, uint64_t type_size, uint64_t count,
                                  std::vector<std::byte>& out) {
            if (count == 0) {
                out.resize(sizeof(uint32_t));
                uint32_t zero = 0;
                std::memcpy(out.data(), &zero, sizeof(uint32_t));
                return sizeof(uint32_t);
            }

            // First pass: count runs
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
                    // Write value
                    std::memcpy(ptr, data + (i - 1) * type_size, type_size);
                    ptr += type_size;
                    // Write run length
                    std::memcpy(ptr, &run_length, sizeof(uint32_t));
                    ptr += sizeof(uint32_t);
                    run_length = 1;
                }
            }

            return total_size;
        }

        // Dictionary compression analysis.
        // Returns number of unique values, or 0 if dictionary is not beneficial.
        // max_dict_entries = 65535 (fits in uint16_t)
        static constexpr uint16_t MAX_DICT_ENTRIES = 65535;

        struct dict_analysis_t {
            uint16_t num_unique{0};
            uint64_t compressed_size{0};
            // value→index mapping stored as byte key
            std::map<std::vector<std::byte>, uint16_t, byte_vector_less> value_map;
        };

        dict_analysis_t analyze_dictionary(const std::byte* data, uint64_t type_size, uint64_t count) {
            dict_analysis_t result;
            if (count == 0) return result;

            std::map<std::vector<std::byte>, uint16_t, byte_vector_less> mapping;
            for (uint64_t i = 0; i < count; i++) {
                std::vector<std::byte> key(data + i * type_size, data + (i + 1) * type_size);
                if (mapping.find(key) == mapping.end()) {
                    if (mapping.size() >= MAX_DICT_ENTRIES) {
                        return result; // too many unique values
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

        // Build dictionary buffer:
        // [uint16_t num_unique][value_0(ts)]...[value_{n-1}(ts)][index_0]...[index_{count-1}]
        // index_size is 1 byte if num_unique<=256, else 2 bytes
        uint64_t build_dict_buffer(const std::byte* data, uint64_t type_size, uint64_t count,
                                   const dict_analysis_t& analysis, std::vector<std::byte>& out) {
            out.resize(analysis.compressed_size);
            auto* ptr = out.data();

            // Write num_unique
            std::memcpy(ptr, &analysis.num_unique, sizeof(uint16_t));
            ptr += sizeof(uint16_t);

            // Write dictionary values in index order
            std::vector<const std::byte*> ordered(analysis.num_unique);
            for (auto& [key, idx] : analysis.value_map) {
                ordered[idx] = key.data();
            }
            for (uint16_t i = 0; i < analysis.num_unique; i++) {
                std::memcpy(ptr, ordered[i], type_size);
                ptr += type_size;
            }

            // Write indices
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

    void column_checkpoint_state_t::flush_segment(column_segment_t& segment,
                                                   uint64_t row_start,
                                                   uint64_t tuple_count) {
        auto& block_manager = column_data_.block_manager();

        // pin the segment's buffer to get data
        auto handle = block_manager.buffer_manager.pin(segment.block);
        auto* data = handle.ptr();

        auto phys = segment.type.to_physical_type();
        bool is_fixed_size = (phys != types::physical_type::STRING &&
                              phys != types::physical_type::BIT &&
                              phys != types::physical_type::INVALID);

        if (is_fixed_size && tuple_count > 1 && data && segment.type_size > 0) {
            auto* segment_data = data + segment.block_offset();

            // Try CONSTANT compression (all values identical)
            if (is_constant_data(segment_data, segment.type_size, tuple_count)) {
                auto constant_size = segment.type_size;
                auto allocation = partial_block_manager_.get_block_allocation(constant_size);
                partial_block_manager_.write_to_block(
                    allocation.block_id, allocation.offset_in_block, segment_data, constant_size);

                storage::data_pointer_t dp;
                dp.row_start = row_start;
                dp.tuple_count = tuple_count;
                dp.block_pointer = storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
                dp.compression = compression::compression_type::CONSTANT;
                dp.segment_size = constant_size;
                data_pointers_.push_back(dp);
                return;
            }

            // Try RLE compression
            uint32_t num_runs = count_runs(segment_data, segment.type_size, tuple_count);
            uint64_t entry_size = segment.type_size + sizeof(uint32_t);
            uint64_t rle_size = sizeof(uint32_t) + num_runs * entry_size;
            uint64_t uncompressed_size = segment.type_size * tuple_count;

            if (rle_size < uncompressed_size) {
                std::vector<std::byte> rle_buf;
                build_rle_buffer(segment_data, segment.type_size, tuple_count, rle_buf);

                auto allocation = partial_block_manager_.get_block_allocation(rle_size);
                partial_block_manager_.write_to_block(
                    allocation.block_id, allocation.offset_in_block, rle_buf.data(), rle_size);

                storage::data_pointer_t dp;
                dp.row_start = row_start;
                dp.tuple_count = tuple_count;
                dp.block_pointer = storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
                dp.compression = compression::compression_type::RLE;
                dp.segment_size = rle_size;
                data_pointers_.push_back(dp);
                return;
            }

            // Try DICTIONARY compression (low-cardinality columns)
            auto dict_info = analyze_dictionary(segment_data, segment.type_size, tuple_count);
            if (dict_info.num_unique > 1 && dict_info.compressed_size < uncompressed_size) {
                std::vector<std::byte> dict_buf;
                build_dict_buffer(segment_data, segment.type_size, tuple_count, dict_info, dict_buf);

                auto allocation = partial_block_manager_.get_block_allocation(dict_info.compressed_size);
                partial_block_manager_.write_to_block(
                    allocation.block_id, allocation.offset_in_block, dict_buf.data(), dict_info.compressed_size);

                storage::data_pointer_t dp;
                dp.row_start = row_start;
                dp.tuple_count = tuple_count;
                dp.block_pointer = storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
                dp.compression = compression::compression_type::DICTIONARY;
                dp.segment_size = dict_info.compressed_size;
                data_pointers_.push_back(dp);
                return;
            }
        }

        // Default: UNCOMPRESSED
        auto segment_size = segment.segment_size();
        auto allocation = partial_block_manager_.get_block_allocation(segment_size);

        if (data && segment_size > 0) {
            partial_block_manager_.write_to_block(allocation.block_id, allocation.offset_in_block, data, segment_size);
        }

        storage::data_pointer_t dp;
        dp.row_start = row_start;
        dp.tuple_count = tuple_count;
        dp.block_pointer = storage::block_pointer_t(allocation.block_id, allocation.offset_in_block);
        dp.compression = compression::compression_type::UNCOMPRESSED;
        dp.segment_size = segment_size;
        data_pointers_.push_back(dp);
    }

    persistent_column_data_t column_checkpoint_state_t::get_persistent_data() const {
        persistent_column_data_t result(column_data_.resource());
        result.data_pointers = data_pointers_;
        return result;
    }

} // namespace components::table
