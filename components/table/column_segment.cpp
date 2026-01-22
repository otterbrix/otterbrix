#include "column_segment.hpp"

#include "column_state.hpp"
#include "storage/block_manager.hpp"
#include "storage/buffer_handle.hpp"
#include "storage/buffer_manager.hpp"

#include <expressions/compare_expression.hpp>

namespace components::table {

    namespace impl {

        static constexpr uint64_t DEFAULT_STRING_BLOCK_LIMIT = 4096;
        static constexpr uint64_t BIG_STRING_MARKER_BASE_SIZE = sizeof(uint32_t) + sizeof(int32_t);
        static constexpr uint32_t INVALID_BLOCK = uint32_t(-1);
        static constexpr uint32_t MAXIMUM_BLOCK = uint32_t(1) << 30;

        struct string_location_t {
            string_location_t(uint32_t block_id, int32_t offset)
                : block_id(block_id)
                , offset(offset) {}
            string_location_t() = default;
            bool is_valid(uint64_t block_size) {
                auto cast_block_size = static_cast<int32_t>(block_size);
                return offset < cast_block_size && (block_id == INVALID_BLOCK || block_id >= MAXIMUM_BLOCK);
            }
            uint32_t block_id;
            int32_t offset;
        };

        typedef struct {
            uint32_t dict_size;
            uint32_t dict_end;
            uint32_t index_buffer_offset;
            uint32_t index_buffer_count;
            uint32_t bitpacking_width;
        } dictionary_compression_header_t;

        static constexpr uint16_t DICTIONARY_HEADER_SIZE = sizeof(dictionary_compression_header_t);

        template<typename T>
        T load(void* ptr) {
            T ret;
            memcpy(&ret, ptr, sizeof(ret));
            return ret;
        }

        template<typename T>
        void store(const T& val, void* ptr) {
            memcpy(ptr, &val, sizeof(val));
        }

        struct string_dictionary_container_t {
            uint32_t size;
            uint32_t end;
        };

        string_dictionary_container_t dictionary(column_segment_t& segment, storage::buffer_handle_t& handle) {
            auto startptr = handle.ptr() + segment.block_offset();
            string_dictionary_container_t container;
            container.size = load<uint32_t>(startptr);
            container.end = load<uint32_t>(startptr + sizeof(uint32_t));
            return container;
        }

        void set_dictionary(column_segment_t& segment,
                            storage::buffer_handle_t& handle,
                            string_dictionary_container_t container) {
            auto startptr = handle.ptr() + segment.block_offset();
            store<uint32_t>(container.size, startptr);
            store<uint32_t>(container.end, startptr + sizeof(uint32_t));
        }

        void read_string_marker(std::byte* target, uint32_t& block_id, int32_t& offset) {
            memcpy(&block_id, target, sizeof(uint64_t));
            target += sizeof(uint64_t);
            memcpy(&offset, target, sizeof(int32_t));
        }

        string_location_t fetch_string_location(string_dictionary_container_t dict,
                                                std::byte* base_ptr,
                                                int32_t dict_offset,
                                                uint64_t block_size) {
            if (dict_offset + static_cast<int32_t>(block_size) < 0 || dict_offset > static_cast<int32_t>(block_size)) {
                throw std::runtime_error("fetch_string_location: incorrect pointer and size");
            }
            if (dict_offset >= 0) {
                return string_location_t(INVALID_BLOCK, dict_offset);
            }

            string_location_t result;
            read_string_marker(base_ptr + dict.end - static_cast<uint64_t>(-1 * dict_offset),
                               result.block_id,
                               result.offset);
            return result;
        }

        std::string_view read_string(std::byte* target, int32_t offset, uint32_t string_length) {
            auto ptr = target + offset;
            return std::string_view(reinterpret_cast<char*>(ptr), string_length);
        }

        std::string_view read_string_with_length(std::byte* target, int32_t offset) {
            auto ptr = target + offset;
            auto str_length = load<uint32_t>(ptr);
            return std::string_view(reinterpret_cast<char*>(ptr + sizeof(uint32_t)), str_length);
        }

        std::string_view fetch_string(string_dictionary_container_t dict,
                                      std::byte* base_ptr,
                                      string_location_t location,
                                      uint32_t string_length) {
            if (location.offset == 0) {
                return std::string_view(nullptr, 0);
            }
            return std::string_view(reinterpret_cast<char*>(base_ptr + dict.end - location.offset), string_length);
        }
        std::string_view fetch_string_from_dict(column_segment_t& segment,
                                                string_dictionary_container_t dict,
                                                std::byte* base_ptr,
                                                int32_t dict_offset,
                                                uint32_t string_length) {
            auto block_size = segment.block_manager().block_size();
            assert(dict_offset <= static_cast<int32_t>(block_size));
            string_location_t location = fetch_string_location(dict, base_ptr, dict_offset, block_size);
            return fetch_string(dict, base_ptr, location, string_length);
        }

        void write_string_memory(column_segment_t& segment,
                                 std::string_view string,
                                 uint64_t& result_block,
                                 int32_t& result_offset) {
            auto total_length = static_cast<uint32_t>(string.size() + sizeof(uint32_t));
            std::shared_ptr<storage::block_handle_t> block;
            storage::buffer_handle_t handle;

            auto& buffer_manager = segment.block->block_manager.buffer_manager;
            auto& state = segment.segment_state()->cast<uncompressed_string_segment_state>();
            if (!state.head || state.head->offset + total_length >= state.head->size) {
                auto alloc_size = std::max(static_cast<uint64_t>(total_length), segment.block_manager().block_size());
                auto new_block = std::make_unique<string_block_t>();
                new_block->offset = 0;
                new_block->size = alloc_size;
                handle = buffer_manager.allocate(storage::memory_tag::OVERFLOW_STRINGS, alloc_size, false);
                block = handle.block_handle();
                state.overflow_blocks.emplace(block->block_id(), new_block.get());
                new_block->block = std::move(block);
                new_block->next = std::move(state.head);
                state.head = std::move(new_block);
            } else {
                handle = buffer_manager.pin(state.head->block);
            }

            result_block = state.head->block->block_id();
            result_offset = static_cast<int32_t>(state.head->offset);

            auto ptr = handle.ptr() + state.head->offset;
            store<uint32_t>(static_cast<uint32_t>(string.size()), ptr);
            ptr += sizeof(uint32_t);
            memcpy(ptr, string.data(), string.size());
            state.head->offset += total_length;
        }

        void write_string(column_segment_t& segment,
                          std::string_view string,
                          uint64_t& result_block,
                          int32_t& result_offset) {
            write_string_memory(segment, string, result_block, result_offset);
        }

        uint64_t remaining_space(column_segment_t& segment, storage::buffer_handle_t& handle) {
            auto dict = dictionary(segment, handle);
            assert(dict.end == segment.segment_size());
            uint64_t used_space = dict.size + segment.count * sizeof(int32_t) + DICTIONARY_HEADER_SIZE;
            assert(segment.segment_size() >= used_space);
            return segment.segment_size() - used_space;
        }

        template<typename T>
        void fixed_size_fetch_row(column_segment_t& segment,
                                  column_fetch_state&,
                                  int64_t row_id,
                                  vector::vector_t& result,
                                  uint64_t result_idx) {
            auto& buffer_manager = segment.block->block_manager.buffer_manager;
            auto handle = buffer_manager.pin(segment.block);

            auto data_ptr = handle.ptr() + segment.block_offset() + static_cast<uint64_t>(row_id) * sizeof(T);

            memcpy(result.data() + result_idx * sizeof(T), data_ptr, sizeof(T));
        }

        void validity_fetch_row(column_segment_t& segment,
                                column_fetch_state&,
                                int64_t row_id,
                                vector::vector_t& result,
                                uint64_t result_idx) {
            assert(row_id >= 0 && row_id < static_cast<int64_t>(segment.count.load()));
            auto& buffer_manager = segment.block->block_manager.buffer_manager;
            auto handle = buffer_manager.pin(segment.block);
            auto dataptr = handle.ptr() + segment.block_offset();
            vector::validity_mask_t mask(reinterpret_cast<uint64_t*>(dataptr));
            auto& result_mask = result.validity();
            if (!mask.row_is_valid(static_cast<uint64_t>(row_id))) {
                result_mask.set_invalid(result_idx);
            }
        }

        void string_fetch_row(column_segment_t& segment,
                              column_fetch_state& state,
                              int64_t row_id,
                              vector::vector_t& result,
                              uint64_t result_idx) {
            auto& handle = state.get_or_insert_handle(segment);

            auto baseptr = handle.ptr() + segment.block_offset();
            auto dict = dictionary(segment, handle);
            auto base_data = reinterpret_cast<int32_t*>(baseptr + DICTIONARY_HEADER_SIZE);
            auto result_data = result.data<std::string_view>();

            auto dict_offset = base_data[row_id];
            uint32_t string_length;
            if (row_id == 0) {
                string_length = static_cast<uint32_t>(std::abs(dict_offset));
            } else {
                string_length = static_cast<uint32_t>(std::abs(dict_offset) - std::abs(base_data[row_id - 1]));
            }
            result_data[result_idx] = fetch_string_from_dict(segment, dict, baseptr, dict_offset, string_length);
        }

        template<typename T>
        bool fixed_size_check_row(column_segment_t& segment, int64_t row_id, const table_filter_t* filter) {
            auto& buffer_manager = segment.block->block_manager.buffer_manager;
            auto handle = buffer_manager.pin(segment.block);

            auto data_ptr = handle.ptr() + segment.block_offset() + static_cast<uint64_t>(row_id) * sizeof(T);
            const auto& const_filter = filter->cast<constant_filter_t>();
            return const_filter.compare(*reinterpret_cast<T*>(data_ptr));
        }

        bool validity_check_row(column_segment_t& segment, int64_t row_id, const table_filter_t* filter) {
            assert(row_id >= 0 && row_id < static_cast<int64_t>(segment.count.load()));
            auto& buffer_manager = segment.block->block_manager.buffer_manager;
            auto handle = buffer_manager.pin(segment.block);
            auto dataptr = handle.ptr() + segment.block_offset();
            vector::validity_mask_t mask(reinterpret_cast<uint64_t*>(dataptr));

            const auto& const_filter = filter->cast<constant_filter_t>();
            return const_filter.compare(mask.row_is_valid(static_cast<uint64_t>(row_id)));
        }

        bool string_check_row(column_segment_t& segment,
                              column_fetch_state& state,
                              int64_t row_id,
                              const table_filter_t* filter) {
            auto& handle = state.get_or_insert_handle(segment);

            auto baseptr = handle.ptr() + segment.block_offset();
            auto dict = dictionary(segment, handle);
            auto base_data = reinterpret_cast<int32_t*>(baseptr + DICTIONARY_HEADER_SIZE);

            auto dict_offset = base_data[row_id];
            uint32_t string_length;
            if (row_id == 0) {
                string_length = static_cast<uint32_t>(std::abs(dict_offset));
            } else {
                string_length = static_cast<uint32_t>(std::abs(dict_offset) - std::abs(base_data[row_id - 1]));
            }

            const auto& const_filter = filter->cast<constant_filter_t>();
            return const_filter.compare(fetch_string_from_dict(segment, dict, baseptr, dict_offset, string_length));
        }

        struct standard_fixed_size_t {
            template<typename T>
            static void append(std::byte* target,
                               uint64_t target_offset,
                               vector::unified_vector_format& uvf,
                               uint64_t offset,
                               uint64_t count) {
                auto sdata = uvf.get_data<T>();
                auto tdata = reinterpret_cast<T*>(target);
                if (!uvf.validity.all_valid()) {
                    for (uint64_t i = 0; i < count; i++) {
                        auto source_idx = uvf.referenced_indexing->get_index(offset + i);
                        auto target_idx = target_offset + i;
                        bool is_null = !uvf.validity.row_is_valid(source_idx);
                        if (!is_null) {
                            tdata[target_idx] = sdata[source_idx];
                        } else {
                            tdata[target_idx] = T(0);
                        }
                    }
                } else {
                    for (uint64_t i = 0; i < count; i++) {
                        auto source_idx = uvf.referenced_indexing->get_index(offset + i);
                        auto target_idx = target_offset + i;
                        tdata[target_idx] = sdata[source_idx];
                    }
                }
            }
        };

        struct list_fixed_size_t {
            template<typename T>
            static void append(std::byte* target,
                               uint64_t target_offset,
                               vector::unified_vector_format& uvf,
                               uint64_t offset,
                               uint64_t count) {
                auto sdata = uvf.get_data<uint64_t>();
                auto tdata = reinterpret_cast<uint64_t*>(target);
                for (uint64_t i = 0; i < count; i++) {
                    auto source_idx = uvf.referenced_indexing->get_index(offset + i);
                    auto target_idx = target_offset + i;
                    tdata[target_idx] = sdata[source_idx];
                }
            }
        };

        template<typename T, typename APPENDER>
        uint64_t append(storage::buffer_handle_t& handle,
                        column_segment_t& segment,
                        vector::unified_vector_format& data,
                        uint64_t offset,
                        uint64_t count) {
            assert(segment.block_offset() == 0);

            auto* target_ptr = handle.ptr();
            uint64_t max_tuple_count = segment.segment_size() / sizeof(T);
            uint64_t copy_count = std::min(count, max_tuple_count - segment.count);

            APPENDER::template append<T>(target_ptr, segment.count, data, offset, copy_count);
            segment.count += copy_count;
            return copy_count;
        }

        uint64_t validity_append(storage::buffer_handle_t& handle,
                                 column_segment_t& segment,
                                 vector::unified_vector_format& data,
                                 uint64_t offset,
                                 uint64_t vcount) {
            assert(segment.block_offset() == 0);

            auto max_tuples =
                segment.segment_size() / vector::validity_mask_t::STANDARD_MASK_SIZE * vector::DEFAULT_VECTOR_CAPACITY;
            uint64_t append_count = std::min(vcount, max_tuples - segment.count);
            if (data.validity.all_valid()) {
                segment.count += append_count;
                return append_count;
            }

            vector::validity_mask_t mask(reinterpret_cast<uint64_t*>(handle.ptr()));
            for (uint64_t i = 0; i < append_count; i++) {
                auto idx = data.referenced_indexing->get_index(offset + i);
                if (!data.validity.row_is_valid(idx)) {
                    mask.set_invalid(segment.count + i);
                }
            }
            segment.count += append_count;
            return append_count;
        }

        uint64_t string_block_limit(uint64_t block_size) {
            return std::min((block_size / 4) / 8 * 8, DEFAULT_STRING_BLOCK_LIMIT);
        }

        void write_string_marker(std::byte* target, uint64_t block_id, int64_t offset) {
            memcpy(target, &block_id, sizeof(uint64_t));
            target += sizeof(uint64_t);
            memcpy(target, &offset, sizeof(int64_t));
        }

        uint64_t
        string_append(column_segment_t& segment, vector::unified_vector_format& data, uint64_t offset, uint64_t count) {
            auto& buffer_manager = segment.block->block_manager.buffer_manager;
            auto handle = buffer_manager.pin(segment.block);
            assert(segment.block_offset() == 0);
            auto handle_ptr = handle.ptr();
            auto source_data = data.get_data<std::string_view>();
            auto result_data = reinterpret_cast<int32_t*>(handle_ptr + DICTIONARY_HEADER_SIZE);
            auto dictionary_size = reinterpret_cast<uint32_t*>(handle_ptr);
            auto dictionary_end = reinterpret_cast<uint32_t*>(handle_ptr + sizeof(uint32_t));

            uint64_t remaining = remaining_space(segment, handle);
            auto base_count = segment.count.load();
            for (uint64_t i = 0; i < count; i++) {
                auto source_idx = data.referenced_indexing->get_index(offset + i);
                auto target_idx = base_count + i;
                if (remaining < sizeof(int32_t)) {
                    segment.count += i;
                    return i;
                }
                remaining -= sizeof(int32_t);
                if (!data.validity.row_is_valid(source_idx)) {
                    if (target_idx > 0) {
                        result_data[target_idx] = result_data[target_idx - 1];
                    } else {
                        result_data[target_idx] = 0;
                    }
                    continue;
                }
                auto end = handle.ptr() + *dictionary_end;

                uint64_t string_length = source_data[source_idx].size();

                bool use_overflow_block = false;
                uint64_t required_space = string_length;
                if (required_space >= string_block_limit(segment.block_manager().block_size())) {
                    required_space = BIG_STRING_MARKER_BASE_SIZE;
                    use_overflow_block = true;
                }
                if (required_space > remaining) {
                    segment.count += i;
                    return i;
                }

                if (use_overflow_block) {
                    uint64_t block;
                    int32_t current_offset;
                    write_string(segment, source_data[source_idx], block, current_offset);
                    *dictionary_size += static_cast<uint32_t>(BIG_STRING_MARKER_BASE_SIZE);
                    remaining -= BIG_STRING_MARKER_BASE_SIZE;
                    auto dict_pos = end - *dictionary_size;

                    write_string_marker(dict_pos, block, current_offset);

                    assert(static_cast<uint64_t>(*dictionary_size) <= segment.block_manager().block_size());
                    result_data[target_idx] = -static_cast<int32_t>((*dictionary_size));
                } else {
                    assert(string_length < std::numeric_limits<uint16_t>::max());
                    *dictionary_size += static_cast<uint32_t>(required_space);
                    remaining -= required_space;
                    auto dict_pos = end - *dictionary_size;
                    memcpy(dict_pos, source_data[source_idx].data(), string_length);

                    assert(static_cast<uint64_t>(*dictionary_size) <= segment.block_manager().block_size());
                    result_data[target_idx] = static_cast<int32_t>(*dictionary_size);
                }
                assert(remaining_space(segment, handle) <= segment.block_manager().block_size());
            }
            segment.count += count;
            return count;
        }

        template<typename T>
        void fixed_size_scan_partial(column_segment_t& segment,
                                     column_scan_state& state,
                                     uint64_t scan_count,
                                     vector::vector_t& result,
                                     uint64_t result_offset) {
            auto start = segment.relative_index(state.row_index);

            auto data = state.scan_state->ptr() + segment.block_offset();
            auto source_data = data + static_cast<uint64_t>(start) * sizeof(T);

            result.set_vector_type(vector::vector_type::FLAT);
            memcpy(result.data() + result_offset * sizeof(T), source_data, scan_count * sizeof(T));
        }

        template<typename T>
        void fixed_size_scan(column_segment_t& segment, column_scan_state& state, uint64_t, vector::vector_t& result) {
            auto start = segment.relative_index(state.row_index);

            auto data = state.scan_state->ptr() + segment.block_offset();
            auto source_data = data + static_cast<uint64_t>(start) * sizeof(T);

            result.set_vector_type(vector::vector_type::FLAT);
            result.set_data(source_data);
        }

        void validity_scan_partial(column_segment_t& segment,
                                   column_scan_state& state,
                                   uint64_t scan_count,
                                   vector::vector_t& result,
                                   uint64_t result_offset) {
            auto start = segment.relative_index(state.row_index);

            static_assert(sizeof(uint64_t) == sizeof(uint64_t), "uint64_t should be 64-bit");
            auto& result_mask = result.validity();
            auto buffer_ptr = state.scan_state->ptr() + segment.block_offset();
            auto input_data = reinterpret_cast<uint64_t*>(buffer_ptr);

            auto result_data = result_mask.data();

            uint64_t result_entry = result_offset / vector::validity_mask_t::BITS_PER_VALUE;
            uint64_t result_idx = result_offset - result_entry * vector::validity_mask_t::BITS_PER_VALUE;
            uint64_t input_entry = static_cast<uint64_t>(start) / vector::validity_mask_t::BITS_PER_VALUE;
            uint64_t input_idx = static_cast<uint64_t>(start) - input_entry * vector::validity_mask_t::BITS_PER_VALUE;

            uint64_t pos = 0;
            while (pos < scan_count) {
                uint64_t current_result_idx = result_entry;
                uint64_t offset;
                uint64_t input_mask = input_data[input_entry];

                if (result_idx < input_idx) {
                    auto shift_amount = input_idx - result_idx;
                    assert(shift_amount > 0 && shift_amount <= vector::validity_mask_t::BITS_PER_VALUE);

                    input_mask = input_mask >> shift_amount;

                    input_mask |= vector::validity_details::UPPER_MASKS[shift_amount];

                    offset = vector::validity_mask_t::BITS_PER_VALUE - input_idx;
                    input_entry++;
                    input_idx = 0;
                    result_idx += offset;
                } else if (result_idx > input_idx) {
                    auto shift_amount = result_idx - input_idx;
                    assert(shift_amount > 0 && shift_amount <= vector::validity_mask_t::BITS_PER_VALUE);

                    input_mask = (input_mask & ~vector::validity_details::UPPER_MASKS[shift_amount]) << shift_amount;

                    input_mask |= vector::validity_details::LOWER_MASKS[shift_amount];

                    offset = vector::validity_mask_t::BITS_PER_VALUE - result_idx;
                    result_entry++;
                    result_idx = 0;
                    input_idx += offset;
                } else {
                    offset = vector::validity_mask_t::BITS_PER_VALUE - result_idx;
                    input_entry++;
                    result_entry++;
                    result_idx = input_idx = 0;
                }
                pos += offset;
                if (pos > scan_count) {
                    input_mask |= vector::validity_details::UPPER_MASKS[pos - scan_count];
                }
                if (input_mask != vector::validity_data_t::MAX_ENTRY) {
                    if (!result_data) {
                        result_mask = vector::validity_mask_t(result_mask.resource(), result_mask.count());
                        result_data = result_mask.data();
                    }
                    result_data[current_result_idx] &= input_mask;
                }
            }
        }

        void validity_scan(column_segment_t& segment,
                           column_scan_state& state,
                           uint64_t scan_count,
                           vector::vector_t& result) {
            result.flatten(scan_count);

            auto start = segment.relative_index(state.row_index);
            if (static_cast<uint64_t>(start) % vector::validity_mask_t::BITS_PER_VALUE == 0) {
                auto& result_mask = result.validity();
                auto buffer_ptr = state.scan_state->ptr() + segment.block_offset();
                auto input_data = reinterpret_cast<uint64_t*>(buffer_ptr);
                auto result_data = result_mask.data();
                uint64_t start_offset = static_cast<uint64_t>(start) / vector::validity_mask_t::BITS_PER_VALUE;
                uint64_t entry_scan_count = (scan_count + vector::validity_mask_t::BITS_PER_VALUE - 1) /
                                            vector::validity_mask_t::BITS_PER_VALUE;
                for (uint64_t i = 0; i < entry_scan_count; i++) {
                    auto input_entry = input_data[start_offset + i];
                    if (!result_data && input_entry == vector::validity_data_t::MAX_ENTRY) {
                        continue;
                    }
                    if (!result_data) {
                        result_mask = vector::validity_mask_t(result_mask.resource(), result_mask.count());
                        result_data = result_mask.data();
                    }
                    result_data[i] = input_entry;
                }
            } else {
                validity_scan_partial(segment, state, scan_count, result, 0);
            }
        }

        void string_scan_partial(column_segment_t& segment,
                                 column_scan_state& state,
                                 uint64_t scan_count,
                                 vector::vector_t& result,
                                 uint64_t result_offset) {
            auto start = segment.relative_index(state.row_index);

            auto baseptr = state.scan_state->ptr() + segment.block_offset();
            auto dict = dictionary(segment, *state.scan_state);
            auto base_data = reinterpret_cast<int32_t*>(baseptr + DICTIONARY_HEADER_SIZE);
            auto result_data = result.data<std::string_view>();

            int32_t previous_offset = start > 0 ? base_data[start - 1] : 0;

            for (uint64_t i = 0; i < scan_count; i++) {
                auto string_length = static_cast<uint32_t>(std::abs(base_data[static_cast<uint64_t>(start) + i]) -
                                                           std::abs(previous_offset));
                result_data[result_offset + i] = fetch_string_from_dict(segment,
                                                                        dict,
                                                                        baseptr,
                                                                        base_data[static_cast<uint64_t>(start) + i],
                                                                        string_length);
                previous_offset = base_data[static_cast<uint64_t>(start) + i];
            }
        }
    } // namespace impl

    column_segment_t::column_segment_t(std::shared_ptr<storage::block_handle_t> block,
                                       const types::complex_logical_type& type,
                                       int64_t start,
                                       uint64_t count,
                                       uint32_t block_id,
                                       uint64_t offset,
                                       uint64_t segment_size,
                                       std::unique_ptr<column_segment_state> segment_state)
        : segment_base_t(start, count)
        , type(type)
        , type_size(type.size())
        , block(std::move(block))
        , block_id_(block_id)
        , offset_(offset)
        , segment_size_(segment_size) {
        assert(!block || segment_size_ <= block_manager().block_size());

        if (type.type() == types::logical_type::VALIDITY) {
            auto& buffer_manager = this->block->block_manager.buffer_manager;
            if (block_id_ == storage::INVALID_BLOCK) {
                auto handle = buffer_manager.pin(this->block);
                memset(handle.ptr(), 0xFF, this->segment_size());
            }
        } else if (type.type() == types::logical_type::STRING_LITERAL) {
            auto& buffer_manager = this->block->block_manager.buffer_manager;
            if (block_id_ == storage::INVALID_BLOCK) {
                auto handle = buffer_manager.pin(this->block);
                impl::string_dictionary_container_t dictionary;
                dictionary.size = 0;
                dictionary.end = static_cast<uint32_t>(this->segment_size());
                set_dictionary(*this, handle, dictionary);
            }
            auto state = std::make_unique<uncompressed_string_segment_state>();
            if (segment_state) {
                state->on_disk_blocks = std::move(segment_state->blocks);
            }
            segment_state_ = std::move(state);
        }
    }

    column_segment_t::column_segment_t(column_segment_t&& other) noexcept
        : segment_base_t(other.start, other.count)
        , type(std::move(other.type))
        , type_size(other.type_size)
        , block(std::move(other.block))
        , block_id_(other.block_id_)
        , offset_(other.offset_)
        , segment_size_(other.segment_size_)
        , segment_state_(std::move(other.segment_state_)) {
        assert(!block || segment_size_ <= block_manager().block_size());
    }

    column_segment_t::column_segment_t(column_segment_t&& other, int64_t start)
        : segment_base_t(start, other.count.load())
        , type(std::move(other.type))
        , type_size(other.type_size)
        , block(std::move(other.block))
        , block_id_(other.block_id_)
        , offset_(other.offset_)
        , segment_size_(other.segment_size_)
        , segment_state_(std::move(other.segment_state_)) {
        assert(!block || segment_size_ <= block_manager().block_size());
    }

    uint64_t column_segment_t::segment_size() const { return segment_size_; }

    std::unique_ptr<column_segment_t> column_segment_t::create_segment(storage::buffer_manager_t& manager,
                                                                       const types::complex_logical_type& type,
                                                                       int64_t start,
                                                                       uint64_t segment_size,
                                                                       uint64_t block_size) {
        auto block = manager.register_transient_memory(segment_size, block_size);
        return std::make_unique<column_segment_t>(std::move(block),
                                                  type,
                                                  start,
                                                  0U,
                                                  storage::INVALID_BLOCK,
                                                  0U,
                                                  segment_size);
    }

    void column_segment_t::initialize_scan(column_scan_state& state) {
        switch (type.to_physical_type()) {
            case types::physical_type::BIT: {
                auto& buffer_manager = block->block_manager.buffer_manager;
                state.scan_state = std::make_unique<storage::buffer_handle_t>(buffer_manager.pin(block));
                break;
            }
            case types::physical_type::STRING: {
                auto& buffer_manager = block->block_manager.buffer_manager;
                state.scan_state = std::make_unique<storage::buffer_handle_t>(buffer_manager.pin(block));
                break;
            }
            default: {
                auto& buffer_manager = block->block_manager.buffer_manager;
                state.scan_state = std::make_unique<storage::buffer_handle_t>(buffer_manager.pin(block));
            }
        }
    }

    void column_segment_t::scan(column_scan_state& state,
                                uint64_t scan_count,
                                vector::vector_t& result,
                                uint64_t result_offset,
                                scan_vector_type scan_type) {
        if (scan_type == scan_vector_type::SCAN_ENTIRE_VECTOR) {
            //assert(result_offset == 0);
            scan(state, scan_count, result);
        } else {
            assert(result.get_vector_type() == vector::vector_type::FLAT);
            scan_partial(state, scan_count, result, result_offset);
            assert(result.get_vector_type() == vector::vector_type::FLAT);
        }
    }
    bool column_segment_t::check_predicate(int64_t row_id, const table_filter_t* filter) {
        switch (type.to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return impl::fixed_size_check_row<int8_t>(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::INT16:
                return impl::fixed_size_check_row<int16_t>(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::INT32:
                return impl::fixed_size_check_row<int32_t>(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::INT64:
                return impl::fixed_size_check_row<int64_t>(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::UINT8:
                return impl::fixed_size_check_row<uint8_t>(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::UINT16:
                return impl::fixed_size_check_row<uint16_t>(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::UINT32:
                return impl::fixed_size_check_row<uint32_t>(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::UINT64:
                return impl::fixed_size_check_row<uint64_t>(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::INT128:
                return impl::fixed_size_check_row<types::int128_t>(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::UINT128:
                return impl::fixed_size_check_row<types::uint128_t>(*this,
                                                                    static_cast<int64_t>(row_id - start),
                                                                    filter);
            case types::physical_type::FLOAT:
                return impl::fixed_size_check_row<float>(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::DOUBLE:
                return impl::fixed_size_check_row<double>(*this, static_cast<int64_t>(row_id - start), filter);
            // case types::physical_type::INTERVAL:
            // return impl::fixed_size_check_row<interval_t>(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::LIST:
                return impl::fixed_size_check_row<uint64_t>(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::BIT:
                return impl::validity_check_row(*this, static_cast<int64_t>(row_id - start), filter);
            case types::physical_type::STRING: {
                column_fetch_state state;
                return impl::string_check_row(*this, state, static_cast<int64_t>(row_id - start), filter);
            }
            default:
                throw std::logic_error("Unsupported type for FixedSizeUncompressed::GetFunction");
        }
    }

    void column_segment_t::fetch_row(column_fetch_state& state,
                                     int64_t row_id,
                                     vector::vector_t& result,
                                     uint64_t result_idx) {
        switch (type.to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return impl::fixed_size_fetch_row<int8_t>(*this,
                                                          state,
                                                          static_cast<int64_t>(row_id - start),
                                                          result,
                                                          result_idx);
            case types::physical_type::INT16:
                return impl::fixed_size_fetch_row<int16_t>(*this,
                                                           state,
                                                           static_cast<int64_t>(row_id - start),
                                                           result,
                                                           result_idx);
            case types::physical_type::INT32:
                return impl::fixed_size_fetch_row<int32_t>(*this,
                                                           state,
                                                           static_cast<int64_t>(row_id - start),
                                                           result,
                                                           result_idx);
            case types::physical_type::INT64:
                return impl::fixed_size_fetch_row<int64_t>(*this,
                                                           state,
                                                           static_cast<int64_t>(row_id - start),
                                                           result,
                                                           result_idx);
            case types::physical_type::UINT8:
                return impl::fixed_size_fetch_row<uint8_t>(*this,
                                                           state,
                                                           static_cast<int64_t>(row_id - start),
                                                           result,
                                                           result_idx);
            case types::physical_type::UINT16:
                return impl::fixed_size_fetch_row<uint16_t>(*this,
                                                            state,
                                                            static_cast<int64_t>(row_id - start),
                                                            result,
                                                            result_idx);
            case types::physical_type::UINT32:
                return impl::fixed_size_fetch_row<uint32_t>(*this,
                                                            state,
                                                            static_cast<int64_t>(row_id - start),
                                                            result,
                                                            result_idx);
            case types::physical_type::UINT64:
                return impl::fixed_size_fetch_row<uint64_t>(*this,
                                                            state,
                                                            static_cast<int64_t>(row_id - start),
                                                            result,
                                                            result_idx);
            case types::physical_type::INT128:
                return impl::fixed_size_fetch_row<types::int128_t>(*this,
                                                                   state,
                                                                   static_cast<int64_t>(row_id - start),
                                                                   result,
                                                                   result_idx);
            case types::physical_type::UINT128:
                return impl::fixed_size_fetch_row<types::uint128_t>(*this,
                                                                    state,
                                                                    static_cast<int64_t>(row_id - start),
                                                                    result,
                                                                    result_idx);
            case types::physical_type::FLOAT:
                return impl::fixed_size_fetch_row<float>(*this,
                                                         state,
                                                         static_cast<int64_t>(row_id - start),
                                                         result,
                                                         result_idx);
            case types::physical_type::DOUBLE:
                return impl::fixed_size_fetch_row<double>(*this,
                                                          state,
                                                          static_cast<int64_t>(row_id - start),
                                                          result,
                                                          result_idx);
            // case types::physical_type::INTERVAL:
            // return impl::fixed_size_fetch_row<interval_t>(*this, state, static_cast<int64_t>(row_id - start),
            // result, result_idx);
            case types::physical_type::LIST:
                return impl::fixed_size_fetch_row<uint64_t>(*this,
                                                            state,
                                                            static_cast<int64_t>(row_id - start),
                                                            result,
                                                            result_idx);
            case types::physical_type::BIT:
                return impl::validity_fetch_row(*this, state, static_cast<int64_t>(row_id - start), result, result_idx);
            case types::physical_type::STRING:
                return impl::string_fetch_row(*this, state, static_cast<int64_t>(row_id - start), result, result_idx);
            default:
                throw std::logic_error("Unsupported type for FixedSizeUncompressed::GetFunction");
        }
    }

    template<class T, class COMP, bool HAS_NULL>
    static uint64_t filter_selection(vector::unified_vector_format& uvf,
                                     T predicate,
                                     vector::indexing_vector_t& sel,
                                     uint64_t approved_tuple_count,
                                     vector::indexing_vector_t& result_sel) {
        auto& mask = uvf.validity;
        auto vec = uvf.get_data<T>();
        uint64_t result_count = 0;
        for (uint64_t i = 0; i < approved_tuple_count; i++) {
            auto idx = sel.get_index(i);
            auto vector_idx = uvf.referenced_indexing->get_index(idx);
            COMP comparator{};
            bool comparison_result =
                (!HAS_NULL || mask.row_is_valid(vector_idx)) && comparator(vec[vector_idx], predicate);
            result_sel.set_index(result_count, idx);
            result_count += comparison_result;
        }
        return result_count;
    }
    template<class T>
    static void filter_selection_switch(vector::unified_vector_format& uvf,
                                        T predicate,
                                        vector::indexing_vector_t& indexing,
                                        uint64_t& approved_tuple_count,
                                        expressions::compare_type comparison_type) {
        vector::indexing_vector_t new_indexing(indexing.resource(), approved_tuple_count);
        auto& mask = uvf.validity;
        // the inplace loops take the result as the last parameter
        switch (comparison_type) {
            case expressions::compare_type::eq: {
                if (mask.all_valid()) {
                    approved_tuple_count = filter_selection<T, std::equal_to<T>, false>(uvf,
                                                                                        predicate,
                                                                                        indexing,
                                                                                        approved_tuple_count,
                                                                                        new_indexing);
                } else {
                    approved_tuple_count = filter_selection<T, std::equal_to<T>, true>(uvf,
                                                                                       predicate,
                                                                                       indexing,
                                                                                       approved_tuple_count,
                                                                                       new_indexing);
                }
                break;
            }
            case expressions::compare_type::ne: {
                if (mask.all_valid()) {
                    approved_tuple_count = filter_selection<T, std::not_equal_to<T>, false>(uvf,
                                                                                            predicate,
                                                                                            indexing,
                                                                                            approved_tuple_count,
                                                                                            new_indexing);
                } else {
                    approved_tuple_count = filter_selection<T, std::not_equal_to<T>, true>(uvf,
                                                                                           predicate,
                                                                                           indexing,
                                                                                           approved_tuple_count,
                                                                                           new_indexing);
                }
                break;
            }
            case expressions::compare_type::lt: {
                if (mask.all_valid()) {
                    approved_tuple_count = filter_selection<T, std::less<T>, false>(uvf,
                                                                                    predicate,
                                                                                    indexing,
                                                                                    approved_tuple_count,
                                                                                    new_indexing);
                } else {
                    approved_tuple_count = filter_selection<T, std::less<T>, true>(uvf,
                                                                                   predicate,
                                                                                   indexing,
                                                                                   approved_tuple_count,
                                                                                   new_indexing);
                }
                break;
            }
            case expressions::compare_type::gt: {
                if (mask.all_valid()) {
                    approved_tuple_count = filter_selection<T, std::greater<T>, false>(uvf,
                                                                                       predicate,
                                                                                       indexing,
                                                                                       approved_tuple_count,
                                                                                       new_indexing);
                } else {
                    approved_tuple_count = filter_selection<T, std::greater<T>, true>(uvf,
                                                                                      predicate,
                                                                                      indexing,
                                                                                      approved_tuple_count,
                                                                                      new_indexing);
                }
                break;
            }
            case expressions::compare_type::lte: {
                if (mask.all_valid()) {
                    approved_tuple_count = filter_selection<T, std::less_equal<T>, false>(uvf,
                                                                                          predicate,
                                                                                          indexing,
                                                                                          approved_tuple_count,
                                                                                          new_indexing);
                } else {
                    approved_tuple_count = filter_selection<T, std::less_equal<T>, true>(uvf,
                                                                                         predicate,
                                                                                         indexing,
                                                                                         approved_tuple_count,
                                                                                         new_indexing);
                }
                break;
            }
            case expressions::compare_type::gte: {
                if (mask.all_valid()) {
                    approved_tuple_count = filter_selection<T, std::greater_equal<T>, false>(uvf,
                                                                                             predicate,
                                                                                             indexing,
                                                                                             approved_tuple_count,
                                                                                             new_indexing);
                } else {
                    approved_tuple_count = filter_selection<T, std::greater_equal<T>, true>(uvf,
                                                                                            predicate,
                                                                                            indexing,
                                                                                            approved_tuple_count,
                                                                                            new_indexing);
                }
                break;
            }
            default:
                throw std::logic_error("Unknown comparison type for filter");
        }
        indexing = new_indexing;
    }
    uint64_t column_segment_t::filter_indexing(vector::indexing_vector_t& indexing,
                                               vector::vector_t& vector,
                                               vector::unified_vector_format& uvf,
                                               const table_filter_t& filter,
                                               uint64_t,
                                               uint64_t& approved_tuple_count) {
        assert(filter.filter_type != expressions::compare_type::invalid);
        assert(!is_union_compare_condition(filter.filter_type));
        auto& constant_filter = filter.cast<constant_filter_t>();
        switch (vector.type().to_physical_type()) {
            case types::physical_type::UINT8: {
                auto predicate = constant_filter.constant.value<uint8_t>();
                filter_selection_switch<uint8_t>(uvf, predicate, indexing, approved_tuple_count, filter.filter_type);
                break;
            }
            case types::physical_type::UINT16: {
                auto predicate = constant_filter.constant.value<uint16_t>();
                filter_selection_switch<uint16_t>(uvf, predicate, indexing, approved_tuple_count, filter.filter_type);
                break;
            }
            case types::physical_type::UINT32: {
                auto predicate = constant_filter.constant.value<uint32_t>();
                filter_selection_switch<uint32_t>(uvf, predicate, indexing, approved_tuple_count, filter.filter_type);
                break;
            }
            case types::physical_type::UINT64: {
                auto predicate = constant_filter.constant.value<uint64_t>();
                filter_selection_switch<uint64_t>(uvf, predicate, indexing, approved_tuple_count, filter.filter_type);
                break;
            }
            case types::physical_type::UINT128: {
                auto predicate = constant_filter.constant.value<types::uint128_t>();
                filter_selection_switch<types::uint128_t>(uvf,
                                                          predicate,
                                                          indexing,
                                                          approved_tuple_count,
                                                          filter.filter_type);
                break;
            }
            case types::physical_type::INT8: {
                auto predicate = constant_filter.constant.value<int8_t>();
                filter_selection_switch<int8_t>(uvf, predicate, indexing, approved_tuple_count, filter.filter_type);
                break;
            }
            case types::physical_type::INT16: {
                auto predicate = constant_filter.constant.value<int16_t>();
                filter_selection_switch<int16_t>(uvf, predicate, indexing, approved_tuple_count, filter.filter_type);
                break;
            }
            case types::physical_type::INT32: {
                auto predicate = constant_filter.constant.value<int32_t>();
                filter_selection_switch<int32_t>(uvf, predicate, indexing, approved_tuple_count, filter.filter_type);
                break;
            }
            case types::physical_type::INT64: {
                auto predicate = constant_filter.constant.value<int64_t>();
                filter_selection_switch<int64_t>(uvf, predicate, indexing, approved_tuple_count, filter.filter_type);
                break;
            }
            case types::physical_type::INT128: {
                auto predicate = constant_filter.constant.value<types::int128_t>();
                filter_selection_switch<types::int128_t>(uvf,
                                                         predicate,
                                                         indexing,
                                                         approved_tuple_count,
                                                         filter.filter_type);
                break;
            }
            case types::physical_type::FLOAT: {
                auto predicate = constant_filter.constant.value<float>();
                filter_selection_switch<float>(uvf, predicate, indexing, approved_tuple_count, filter.filter_type);
                break;
            }
            case types::physical_type::DOUBLE: {
                auto predicate = constant_filter.constant.value<double>();
                filter_selection_switch<double>(uvf, predicate, indexing, approved_tuple_count, filter.filter_type);
                break;
            }
            case types::physical_type::STRING: {
                auto predicate = constant_filter.constant.value<std::string_view>();
                filter_selection_switch<std::string_view>(uvf,
                                                          predicate,
                                                          indexing,
                                                          approved_tuple_count,
                                                          filter.filter_type);
                break;
            }
            case types::physical_type::BOOL: {
                auto predicate = constant_filter.constant.value<bool>();
                filter_selection_switch<bool>(uvf, predicate, indexing, approved_tuple_count, filter.filter_type);
                break;
            }
            default:
                throw std::logic_error("Invalid type for filter value");
        }
        return approved_tuple_count;
    }

    void column_segment_t::skip(column_scan_state& state) { state.internal_index = state.row_index; }

    void column_segment_t::resize(uint64_t new_size) {
        assert(new_size > segment_size_);
        assert(offset_ == 0);
        assert(block && new_size <= block_manager().block_size());

        auto& buffer_manager = block->block_manager.buffer_manager;
        auto old_handle = buffer_manager.pin(block);
        auto new_handle = buffer_manager.allocate(storage::memory_tag::IN_MEMORY_TABLE, new_size);
        auto new_block = new_handle.block_handle();
        memcpy(new_handle.ptr(), old_handle.ptr(), segment_size_);

        this->block_id_ = new_block->block_id();
        this->block = std::move(new_block);
        this->segment_size_ = new_size;
    }

    void column_segment_t::initialize_append(column_append_state& state) {
        auto& buffer_manager = block->block_manager.buffer_manager;
        auto handle = buffer_manager.pin(block);
        state.handle = std::make_unique<storage::buffer_handle_t>(std::move(handle));
    }

    uint64_t column_segment_t::append(column_append_state& state,
                                      vector::unified_vector_format& data,
                                      uint64_t offset,
                                      uint64_t count) {
        switch (type.to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return impl::append<int8_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::INT16:
                return impl::append<int16_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::INT32:
                return impl::append<int32_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::INT64:
                return impl::append<int64_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::UINT8:
                return impl::append<uint8_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::UINT16:
                return impl::append<uint16_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::UINT32:
                return impl::append<uint32_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::UINT64:
                return impl::append<uint64_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::INT128:
                return impl::append<types::int128_t, impl::standard_fixed_size_t>(*state.handle,
                                                                                  *this,
                                                                                  data,
                                                                                  offset,
                                                                                  count);
            case types::physical_type::UINT128:
                return impl::append<types::uint128_t, impl::standard_fixed_size_t>(*state.handle,
                                                                                   *this,
                                                                                   data,
                                                                                   offset,
                                                                                   count);
            case types::physical_type::FLOAT:
                return impl::append<float, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::DOUBLE:
                return impl::append<double, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            // case types::physical_type::INTERVAL:
            // return impl::append<interval_t, impl::standard_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::LIST:
                return impl::append<uint64_t, impl::list_fixed_size_t>(*state.handle, *this, data, offset, count);
            case types::physical_type::BIT:
                return impl::validity_append(*state.handle, *this, data, offset, count);
            case types::physical_type::STRING:
                return impl::string_append(*this, data, offset, count);
            default:
                throw std::logic_error("Unsupported type for FixedSizeUncompressed::GetFunction");
        }
    }

    uint64_t column_segment_t::finalize_append(column_append_state& state) {
        state.handle.reset();
        switch (type.to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
            case types::physical_type::UINT8:
                return count;
            case types::physical_type::INT16:
            case types::physical_type::UINT16:
                return count * 2;
            case types::physical_type::INT32:
            case types::physical_type::UINT32:
            case types::physical_type::FLOAT:
                return count * 4;
            case types::physical_type::INT64:
            case types::physical_type::UINT64:
            case types::physical_type::LIST:
            case types::physical_type::DOUBLE:
                return count * 8;
            case types::physical_type::INT128:
            case types::physical_type::UINT128:
                // case types::physical_type::INTERVAL:
                return count * 16;
            case types::physical_type::BIT:
                return ((count + vector::DEFAULT_VECTOR_CAPACITY - 1) / vector::DEFAULT_VECTOR_CAPACITY) *
                       vector::validity_mask_t::STANDARD_MASK_SIZE;
            case types::physical_type::STRING: {
                auto& buffer_manager = block->block_manager.buffer_manager;
                auto handle = buffer_manager.pin(block);
                auto dict = impl::dictionary(*this, handle);
                assert(dict.end == segment_size_);
                auto offset_size = impl::DICTIONARY_HEADER_SIZE + count * sizeof(int32_t);
                auto total_size = offset_size + dict.size;

                auto block_size = block_manager().block_size();
                if (total_size >= block_size / 5 * 4) {
                    return segment_size_;
                }

                auto move_amount = segment_size_ - total_size;
                auto dataptr = handle.ptr();
                memmove(dataptr + offset_size, dataptr + dict.end - dict.size, dict.size);
                dict.end -= static_cast<uint32_t>(move_amount);
                assert(dict.end == total_size);
                set_dictionary(*this, handle, dict);
                return total_size;
            }
            default:
                throw std::logic_error("Unsupported type for FixedSizeUncompressed::GetFunction");
        }
    }

    void column_segment_t::revert_append(uint64_t start_row) {
        assert(type.to_physical_type() == types::physical_type::BIT);

        uint64_t start_bit = start_row - static_cast<uint64_t>(start);

        auto& buffer_manager = block->block_manager.buffer_manager;
        auto handle = buffer_manager.pin(block);
        uint64_t revert_start;
        if (start_bit % 8 != 0) {
            uint64_t byte_pos = start_bit / 8;
            uint64_t bit_end = (byte_pos + 1) * 8;
            vector::validity_mask_t mask(reinterpret_cast<uint64_t*>(handle.ptr()));
            for (uint64_t i = start_bit; i < bit_end; i++) {
                mask.set_valid(i);
            }
            revert_start = bit_end / 8;
        } else {
            revert_start = start_bit / 8;
        }
        memset(handle.ptr() + revert_start, 0xFF, segment_size_ - revert_start);
        count = start_row - static_cast<uint64_t>(start);
    }

    void column_segment_t::scan(column_scan_state& state, uint64_t scan_count, vector::vector_t& result) {
        switch (type.to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                impl::fixed_size_scan<int8_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::INT16:
                impl::fixed_size_scan<int16_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::INT32:
                impl::fixed_size_scan<int32_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::INT64:
                impl::fixed_size_scan<ino64_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::UINT8:
                impl::fixed_size_scan<uint8_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::UINT16:
                impl::fixed_size_scan<uint16_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::UINT32:
                impl::fixed_size_scan<uint32_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::UINT64:
                impl::fixed_size_scan<uint64_t>(*this, state, scan_count, result);
                break;
                // case types::physical_type::INT128:
                // impl::fixed_size_scan<int128_t>(*this, state, scan_count, result);
                break;
                // case types::physical_type::UINT128:
                // impl::fixed_size_scan<uin128_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::FLOAT:
                impl::fixed_size_scan<float>(*this, state, scan_count, result);
                break;
            case types::physical_type::DOUBLE:
                impl::fixed_size_scan<double>(*this, state, scan_count, result);
                break;
                // case types::physical_type::INTERVAL:
                // impl::fixed_size_scan<interval_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::LIST:
                impl::fixed_size_scan<uint64_t>(*this, state, scan_count, result);
                break;
            case types::physical_type::BIT:
                impl::validity_scan(*this, state, scan_count, result);
                break;
            case types::physical_type::STRING:
                impl::string_scan_partial(*this, state, scan_count, result, 0);
                break;
            default:
                throw std::logic_error("Unsupported type for FixedSizeUncompressed::GetFunction");
        }
    }

    void column_segment_t::scan_partial(column_scan_state& state,
                                        uint64_t scan_count,
                                        vector::vector_t& result,
                                        uint64_t result_offset) {
        switch (type.to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                impl::fixed_size_scan_partial<int8_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::INT16:
                impl::fixed_size_scan_partial<int16_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::INT32:
                impl::fixed_size_scan_partial<int32_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::INT64:
                impl::fixed_size_scan_partial<ino64_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::UINT8:
                impl::fixed_size_scan_partial<uint8_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::UINT16:
                impl::fixed_size_scan_partial<uint16_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::UINT32:
                impl::fixed_size_scan_partial<uint32_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::UINT64:
                impl::fixed_size_scan_partial<uint64_t>(*this, state, scan_count, result, result_offset);
                break;
                // case types::physical_type::INT128:
                // impl::fixed_size_scan_partial<int128_t>(*this, state, scan_count, result, result_offset);
                break;
                // case types::physical_type::UINT128:
                // impl::fixed_size_scan_partial<uin128_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::FLOAT:
                impl::fixed_size_scan_partial<float>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::DOUBLE:
                impl::fixed_size_scan_partial<double>(*this, state, scan_count, result, result_offset);
                break;
                // case types::physical_type::INTERVAL:
                // impl::fixed_size_scan_partial<interval_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::LIST:
                impl::fixed_size_scan_partial<uint64_t>(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::BIT:
                impl::validity_scan_partial(*this, state, scan_count, result, result_offset);
                break;
            case types::physical_type::STRING:
                impl::string_scan_partial(*this, state, scan_count, result, result_offset);
                break;
            default:
                throw std::logic_error("Unsupported type for FixedSizeUncompressed::GetFunction");
        }
    }

} // namespace components::table