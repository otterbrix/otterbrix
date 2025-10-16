#include "arrow_conversion.hpp"

#include <components/vector/arrow/arrow_string_view.hpp>

namespace components::vector::arrow::scaner {

    static void shift_right(uint8_t* ar, int size, int shift) {
        int carry = 0;
        while (shift--) {
            for (int i = size - 1; i >= 0; --i) {
                int next = (ar[i] & 1) ? 0x80 : 0;
                ar[i] = static_cast<uint8_t>(carry | (ar[i] >> 1));
                carry = next;
            }
        }
    }

    size_t get_effective_offset(const ArrowArray& array,
                                int64_t parent_offset,
                                size_t chunk_offset,
                                int64_t nested_offset = -1) {
        if (nested_offset != -1) {
            return static_cast<size_t>(array.offset + nested_offset);
        }
        return static_cast<size_t>(array.offset + parent_offset) + chunk_offset;
    }

    template<class T>
    T* arrow_buffer_data(ArrowArray& array, size_t buffer_idx) {
        return (T*) array.buffers[buffer_idx];
    }

    static void get_validity(validity_mask_t& mask,
                             ArrowArray& array,
                             size_t chunk_offset,
                             size_t size,
                             int64_t parent_offset,
                             int64_t nested_offset = -1,
                             bool add_null = false) {
        if (array.null_count != 0 && array.n_buffers > 0 && array.buffers[0]) {
            auto bit_offset = get_effective_offset(array, parent_offset, chunk_offset, nested_offset);
            auto n_bitmask_bytes = (size + 8 - 1) / 8;
            if (bit_offset % 8 == 0) {
                memcpy(mask.data(), arrow_buffer_data<std::byte>(array, 0) + bit_offset / 8, n_bitmask_bytes);
            } else {
                std::vector<std::byte> temp_null_mask(n_bitmask_bytes + 1);
                memcpy(temp_null_mask.data(),
                       arrow_buffer_data<std::byte>(array, 0) + bit_offset / 8,
                       n_bitmask_bytes + 1);
                shift_right(reinterpret_cast<uint8_t*>(temp_null_mask.data()),
                            static_cast<int>(n_bitmask_bytes + 1),
                            static_cast<int>(bit_offset % 8ull));
                memcpy(mask.data(), temp_null_mask.data(), n_bitmask_bytes);
            }
        }
        if (add_null) {
            mask.resize(mask.resource(), size + 1);
            mask.set_invalid(size);
        }
    }
    void set_validity(vector_t& vector,
                      ArrowArray& array,
                      size_t chunk_offset,
                      size_t size,
                      int64_t parent_offset,
                      int64_t nested_offset,
                      bool add_null) {
        assert(vector.get_vector_type() == vector_type::FLAT);
        auto& mask = vector.validity();
        get_validity(mask, array, chunk_offset, size, parent_offset, nested_offset, add_null);
    }
    template<class RUN_END_TYPE>
    static size_t find_run_index(const RUN_END_TYPE* run_ends, size_t count, size_t offset) {
        size_t begin = 0;
        size_t end = count;
        while (begin < end) {
            size_t middle = (begin + end) / 2;
            if (offset >= static_cast<size_t>(run_ends[middle])) {
                begin = middle + 1;
            } else {
                end = middle;
            }
        }
        return begin;
    }

    template<class RUN_END_TYPE, class VALUE_TYPE>
    static void flatten_run_ends(vector_t& result,
                                 arrow_run_end_encoding_state& run_end_encoding,
                                 size_t compressed_size,
                                 size_t scan_offset,
                                 size_t count) {
        auto& runs = *run_end_encoding.run_ends;
        auto& values = *run_end_encoding.values;

        unified_vector_format run_end_format(result.resource(), compressed_size);
        unified_vector_format value_format(result.resource(), compressed_size);
        runs.to_unified_format(compressed_size, run_end_format);
        values.to_unified_format(compressed_size, value_format);
        auto run_ends_data = run_end_format.get_data<RUN_END_TYPE>();
        auto values_data = value_format.get_data<VALUE_TYPE>();
        auto result_data = result.data<VALUE_TYPE>();
        auto& validity = result.validity();

        auto run = find_run_index(run_ends_data, compressed_size, scan_offset);
        size_t logical_index = scan_offset;
        size_t index = 0;
        if (value_format.validity.all_valid()) {
            for (; run < compressed_size; ++run) {
                auto run_end_index = run_end_format.referenced_indexing->get_index(run);
                auto value_index = value_format.referenced_indexing->get_index(run);
                auto& value = values_data[value_index];
                auto run_end = static_cast<size_t>(run_ends_data[run_end_index]);

                assert(run_end > (logical_index + index));
                auto to_scan = run_end - (logical_index + index);
                to_scan = std::min<size_t>(to_scan, (count - index));

                for (size_t i = 0; i < to_scan; i++) {
                    result_data[index + i] = value;
                }
                index += to_scan;
                if (index >= count) {
                    if (logical_index + index >= run_end) {
                        ++run;
                    }
                    break;
                }
            }
        } else {
            for (; run < compressed_size; ++run) {
                auto run_end_index = run_end_format.referenced_indexing->get_index(run);
                auto value_index = value_format.referenced_indexing->get_index(run);
                auto run_end = static_cast<size_t>(run_ends_data[run_end_index]);

                assert(run_end > (logical_index + index));
                auto to_scan = run_end - (logical_index + index);
                to_scan = std::min<size_t>(to_scan, (count - index));

                if (value_format.validity.row_is_valid(value_index)) {
                    auto& value = values_data[value_index];
                    for (size_t i = 0; i < to_scan; i++) {
                        result_data[index + i] = value;
                        validity.set_valid(index + i);
                    }
                } else {
                    for (size_t i = 0; i < to_scan; i++) {
                        validity.set_invalid(index + i);
                    }
                }
                index += to_scan;
                if (index >= count) {
                    if (logical_index + index >= run_end) {
                        ++run;
                    }
                    break;
                }
            }
        }
    }

    template<class RUN_END_TYPE>
    static void flatten_run_ends_switch(vector_t& result,
                                        arrow_run_end_encoding_state& run_end_encoding,
                                        size_t compressed_size,
                                        size_t scan_offset,
                                        size_t size) {
        auto& values = *run_end_encoding.values;
        auto type = values.type().to_physical_type();

        switch (type) {
            case types::physical_type::INT8:
                flatten_run_ends<RUN_END_TYPE, int8_t>(result, run_end_encoding, compressed_size, scan_offset, size);
                break;
            case types::physical_type::INT16:
                flatten_run_ends<RUN_END_TYPE, int16_t>(result, run_end_encoding, compressed_size, scan_offset, size);
                break;
            case types::physical_type::INT32:
                flatten_run_ends<RUN_END_TYPE, int32_t>(result, run_end_encoding, compressed_size, scan_offset, size);
                break;
            case types::physical_type::INT64:
                flatten_run_ends<RUN_END_TYPE, int64_t>(result, run_end_encoding, compressed_size, scan_offset, size);
                break;
            case types::physical_type::INT128:
                flatten_run_ends<RUN_END_TYPE, types::int128_t>(result,
                                                                run_end_encoding,
                                                                compressed_size,
                                                                scan_offset,
                                                                size);
                break;
            case types::physical_type::UINT8:
                flatten_run_ends<RUN_END_TYPE, uint8_t>(result, run_end_encoding, compressed_size, scan_offset, size);
                break;
            case types::physical_type::UINT16:
                flatten_run_ends<RUN_END_TYPE, uint16_t>(result, run_end_encoding, compressed_size, scan_offset, size);
                break;
            case types::physical_type::UINT32:
                flatten_run_ends<RUN_END_TYPE, uint32_t>(result, run_end_encoding, compressed_size, scan_offset, size);
                break;
            case types::physical_type::UINT64:
                flatten_run_ends<RUN_END_TYPE, uint64_t>(result, run_end_encoding, compressed_size, scan_offset, size);
                break;
            case types::physical_type::UINT128:
                flatten_run_ends<RUN_END_TYPE, types::uint128_t>(result,
                                                                 run_end_encoding,
                                                                 compressed_size,
                                                                 scan_offset,
                                                                 size);
                break;
            case types::physical_type::BOOL:
                flatten_run_ends<RUN_END_TYPE, bool>(result, run_end_encoding, compressed_size, scan_offset, size);
                break;
            case types::physical_type::FLOAT:
                flatten_run_ends<RUN_END_TYPE, float>(result, run_end_encoding, compressed_size, scan_offset, size);
                break;
            case types::physical_type::DOUBLE:
                flatten_run_ends<RUN_END_TYPE, double>(result, run_end_encoding, compressed_size, scan_offset, size);
                break;
            case types::physical_type::STRING: {
                result.set_auxiliary(values.auxiliary());
                flatten_run_ends<RUN_END_TYPE, std::string_view>(result,
                                                                 run_end_encoding,
                                                                 compressed_size,
                                                                 scan_offset,
                                                                 size);
                break;
            }
            default:
                throw std::logic_error("run_end_encoded value type not supported yet");
        }
    }
    void arrow_column_to_run_end_encoded(vector_t& vector,
                                         const ArrowArray& array,
                                         size_t chunk_offset,
                                         arrow_array_scan_state& array_state,
                                         size_t size,
                                         const arrow_type& arrow_type,
                                         int64_t nested_offset,
                                         validity_mask_t* parent_mask,
                                         uint64_t parent_offset) {
        assert(array.n_children == 2);
        auto& run_ends_array = *array.children[0];
        auto& values_array = *array.children[1];

        auto& struct_info = arrow_type.get_type_info<arrow_struct_info>();
        auto& run_ends_type = struct_info.get_child(0);
        auto& values_type = struct_info.get_child(1);
        assert(vector.type() == values_type.type());

        if (vector.get_buffer()) {
            vector.get_buffer()->set_auxiliary(std::make_unique<arrow_auxiliary_data_t>(array_state.owned_data));
        }

        assert(run_ends_array.length == values_array.length);
        auto compressed_size = static_cast<size_t>(run_ends_array.length);
        if (!array_state.run_end_encoding.run_ends) {
            assert(!array_state.run_end_encoding.values);
            array_state.run_end_encoding.run_ends =
                std::make_unique<vector_t>(vector.resource(), run_ends_type.type(), compressed_size);
            array_state.run_end_encoding.values =
                std::make_unique<vector_t>(vector.resource(), values_type.type(), compressed_size);

            arrow_column_to_vector(*array_state.run_end_encoding.run_ends,
                                   run_ends_array,
                                   chunk_offset,
                                   array_state,
                                   compressed_size,
                                   run_ends_type);
            auto& values = *array_state.run_end_encoding.values;
            set_validity(values,
                         values_array,
                         chunk_offset,
                         compressed_size,
                         static_cast<int64_t>(parent_offset),
                         nested_offset);
            arrow_column_to_vector(values, values_array, chunk_offset, array_state, compressed_size, values_type);
        }

        size_t scan_offset =
            get_effective_offset(array, static_cast<int64_t>(parent_offset), chunk_offset, nested_offset);
        auto type = run_ends_type.type().to_physical_type();
        switch (type) {
            case types::physical_type::INT16:
                flatten_run_ends_switch<int16_t>(vector,
                                                 array_state.run_end_encoding,
                                                 compressed_size,
                                                 scan_offset,
                                                 size);
                break;
            case types::physical_type::INT32:
                flatten_run_ends_switch<int32_t>(vector,
                                                 array_state.run_end_encoding,
                                                 compressed_size,
                                                 scan_offset,
                                                 size);
                break;
            case types::physical_type::INT64:
                flatten_run_ends_switch<int32_t>(vector,
                                                 array_state.run_end_encoding,
                                                 compressed_size,
                                                 scan_offset,
                                                 size);
                break;
            default:
                throw std::logic_error("Type not implemented for array_state.run_end_encoding");
        }
    }
    static void direct_conversion(vector_t& vector,
                                  ArrowArray& array,
                                  size_t chunk_offset,
                                  int64_t nested_offset,
                                  uint64_t parent_offset) {
        auto internal_type = vector.type().size();
        auto data_ptr =
            arrow_buffer_data<std::byte>(array, 1) +
            internal_type *
                get_effective_offset(array, static_cast<int64_t>(parent_offset), chunk_offset, nested_offset);
        vector.set_data(data_ptr);
    }
    template<class T>
    static void set_vector_string(vector_t& vector, size_t size, char* cdata, T* offsets) {
        auto strings = vector.data<std::string_view>();
        for (size_t row_idx = 0; row_idx < size; row_idx++) {
            if (vector.is_null(row_idx)) {
                continue;
            }
            auto cptr = cdata + offsets[row_idx];
            auto str_len = offsets[row_idx + 1] - offsets[row_idx];
            if (str_len > std::numeric_limits<uint32_t>::max()) {
                throw std::logic_error("OtterBrix does not support strings over 4 GB");
            }
            strings[row_idx] = std::string_view(cptr, static_cast<uint32_t>(str_len));
        }
    }

    static void set_vector_string_view(vector_t& vector, size_t size, ArrowArray& array, size_t current_pos) {
        auto strings = vector.data<std::string_view>();
        auto arrow_string = arrow_buffer_data<arrow_string_view_t>(array, 1) + current_pos;

        for (size_t row_idx = 0; row_idx < size; row_idx++) {
            if (vector.is_null(row_idx)) {
                continue;
            }
            auto length = static_cast<uint32_t>(arrow_string[row_idx].length());
            if (arrow_string[row_idx].is_inlined()) {
                strings[row_idx] = std::string_view(arrow_string[row_idx].inlined_data(), length);
            } else {
                auto buffer_index = static_cast<uint32_t>(arrow_string[row_idx].buffer_index());
                int32_t offset = arrow_string[row_idx].get_offset();
                assert(array.n_buffers > 2 + buffer_index);
                auto c_data = arrow_buffer_data<char>(array, 2 + buffer_index);
                strings[row_idx] = std::string_view(&c_data[offset], length);
            }
        }
    }

    template<class SRC>
    void convert_decimal(SRC src_ptr,
                         vector_t& vector,
                         ArrowArray& array,
                         size_t size,
                         int64_t nested_offset,
                         uint64_t parent_offset,
                         size_t chunk_offset,
                         validity_mask_t& val_mask,
                         decimal_bit_width arrow_bit_width) {
        switch (vector.type().to_physical_type()) {
            case types::physical_type::INT16: {
                auto tgt_ptr = vector.data<int16_t>();
                for (size_t row = 0; row < size; row++) {
                    if (val_mask.row_is_valid(row)) {
                        tgt_ptr[row] = static_cast<int16_t>(src_ptr[row]);
                    }
                }
                break;
            }
            case types::physical_type::INT32: {
                if (arrow_bit_width == decimal_bit_width::DECIMAL_32) {
                    vector.set_data(arrow_buffer_data<std::byte>(array, 1) +
                                    vector.type().size() * get_effective_offset(array,
                                                                                static_cast<int64_t>(parent_offset),
                                                                                chunk_offset,
                                                                                nested_offset));
                } else {
                    auto tgt_ptr = vector.data<int32_t>();
                    for (size_t row = 0; row < size; row++) {
                        if (val_mask.row_is_valid(row)) {
                            tgt_ptr[row] = static_cast<int32_t>(src_ptr[row]);
                        }
                    }
                }
                break;
            }
            case types::physical_type::INT64: {
                if (arrow_bit_width == decimal_bit_width::DECIMAL_64) {
                    vector.set_data(arrow_buffer_data<std::byte>(array, 1) +
                                    vector.type().size() * get_effective_offset(array,
                                                                                static_cast<int64_t>(parent_offset),
                                                                                chunk_offset,
                                                                                nested_offset));
                } else {
                    auto tgt_ptr = vector.data<int64_t>();
                    for (size_t row = 0; row < size; row++) {
                        if (val_mask.row_is_valid(row)) {
                            tgt_ptr[row] = static_cast<int64_t>(src_ptr[row]);
                        }
                    }
                }
                break;
            }
            case types::physical_type::INT128: {
                if (arrow_bit_width == decimal_bit_width::DECIMAL_128) {
                    vector.set_data(arrow_buffer_data<std::byte>(array, 1) +
                                    vector.type().size() * get_effective_offset(array,
                                                                                static_cast<int64_t>(parent_offset),
                                                                                chunk_offset,
                                                                                nested_offset));
                } else {
                    auto tgt_ptr = vector.data<types::int128_t>();
                    for (size_t row = 0; row < size; row++) {
                        if (val_mask.row_is_valid(row)) {
                            tgt_ptr[row] = static_cast<types::int128_t>(src_ptr[row]);
                        }
                    }
                }

                break;
            }
            default:
                throw std::logic_error("Unsupported physical type for Decimal");
        }
    }

    struct arrow_list_offset_data {
        size_t list_size = 0;
        size_t start_offset = 0;
    };

    template<class BUFFER_TYPE>
    static arrow_list_offset_data
    convert_arrow_list_offsets_templated(vector_t& vector, ArrowArray& array, size_t size, size_t effective_offset) {
        arrow_list_offset_data result;
        auto& start_offset = result.start_offset;
        auto& list_size = result.list_size;

        if (size == 0) {
            start_offset = 0;
            list_size = 0;
            return result;
        }

        size_t cur_offset = 0;
        auto offsets = arrow_buffer_data<BUFFER_TYPE>(array, 1) + effective_offset;
        start_offset = offsets[0];
        auto list_data = vector.data<types::list_entry_t>();
        for (size_t i = 0; i < size; i++) {
            auto& le = list_data[i];
            le.offset = cur_offset;
            le.length = offsets[i + 1] - offsets[i];
            cur_offset += le.length;
        }
        list_size = offsets[size];
        list_size -= start_offset;
        return result;
    }

    template<class BUFFER_TYPE>
    static arrow_list_offset_data convert_arrow_list_view_offsets_templated(vector_t& vector,
                                                                            ArrowArray& array,
                                                                            size_t size,
                                                                            size_t effective_offset) {
        arrow_list_offset_data result;
        auto& start_offset = result.start_offset;
        auto& list_size = result.list_size;

        list_size = 0;
        auto offsets = arrow_buffer_data<BUFFER_TYPE>(array, 1) + effective_offset;
        auto sizes = arrow_buffer_data<BUFFER_TYPE>(array, 2) + effective_offset;

        auto lowest_offset = size ? offsets[0] : 0;
        auto list_data = vector.data<types::list_entry_t>();
        for (size_t i = 0; i < size; i++) {
            auto& le = list_data[i];
            le.offset = offsets[i];
            le.length = sizes[i];
            list_size += le.length;
            if (sizes[i] != 0) {
                lowest_offset = std::min(lowest_offset, offsets[i]);
            }
        }
        start_offset = lowest_offset;
        if (start_offset) {
            for (size_t i = 0; i < size; i++) {
                auto& le = list_data[i];
                le.offset = le.offset <= start_offset ? 0 : le.offset - start_offset;
            }
        }
        return result;
    }

    static arrow_list_offset_data convert_arrow_list_offsets(vector_t& vector,
                                                             ArrowArray& array,
                                                             size_t size,
                                                             const arrow_type& arrow_type,
                                                             size_t effective_offset) {
        auto& list_info = arrow_type.get_type_info<arrow_list_info>();
        auto size_type = list_info.get_size_type();
        if (list_info.is_view()) {
            if (size_type == arrow_variable_size_type::NORMAL) {
                return convert_arrow_list_view_offsets_templated<uint32_t>(vector, array, size, effective_offset);
            } else {
                assert(size_type == arrow_variable_size_type::SUPER_SIZE);
                return convert_arrow_list_view_offsets_templated<uint64_t>(vector, array, size, effective_offset);
            }
        } else {
            if (size_type == arrow_variable_size_type::NORMAL) {
                return convert_arrow_list_offsets_templated<uint32_t>(vector, array, size, effective_offset);
            } else {
                assert(size_type == arrow_variable_size_type::SUPER_SIZE);
                return convert_arrow_list_offsets_templated<uint64_t>(vector, array, size, effective_offset);
            }
        }
    }
    static void arrow_to_list(vector_t& vector,
                              ArrowArray& array,
                              size_t chunk_offset,
                              arrow_array_scan_state& array_state,
                              size_t size,
                              const arrow_type& arrow_type,
                              int64_t nested_offset,
                              const validity_mask_t* parent_mask,
                              int64_t parent_offset) {
        auto& list_info = arrow_type.get_type_info<arrow_list_info>();
        set_validity(vector, array, chunk_offset, size, parent_offset, nested_offset);

        auto effective_offset = get_effective_offset(array, parent_offset, chunk_offset, nested_offset);
        auto list_data = convert_arrow_list_offsets(vector, array, size, arrow_type, effective_offset);
        auto& start_offset = list_data.start_offset;
        auto& list_size = list_data.list_size;

        vector.reserve(list_size);
        vector.set_list_size(list_size);
        auto& child_vector = vector.entry();
        set_validity(child_vector,
                     *array.children[0],
                     chunk_offset,
                     list_size,
                     array.offset,
                     static_cast<int64_t>(start_offset));
        auto& list_mask = vector.validity();
        if (parent_mask) {
            if (!parent_mask->all_valid()) {
                for (size_t i = 0; i < size; i++) {
                    if (!parent_mask->row_is_valid(i)) {
                        list_mask.set_invalid(i);
                    }
                }
            }
        }
        auto& child_state = array_state.get_child(0);
        auto& child_array = *array.children[0];
        auto& child_type = list_info.get_child();

        if (list_size == 0 && start_offset == 0) {
            assert(!child_array.dictionary);
            arrow_column_to_vector(child_vector, child_array, chunk_offset, child_state, list_size, child_type, -1);
            return;
        }

        auto array_physical_type = child_type.get_physical_type();
        switch (array_physical_type) {
            case arrow_array_physical_type::DICTIONARY_ENCODED:
                arrow_column_to_dictionary(child_vector,
                                           child_array,
                                           chunk_offset,
                                           child_state,
                                           list_size,
                                           child_type,
                                           static_cast<int64_t>(start_offset));
                break;
            case arrow_array_physical_type::RUN_END_ENCODED:
                arrow_column_to_run_end_encoded(child_vector,
                                                child_array,
                                                chunk_offset,
                                                child_state,
                                                list_size,
                                                child_type,
                                                static_cast<int64_t>(start_offset));
                break;
            case arrow_array_physical_type::DEFAULT:
                arrow_column_to_vector(child_vector,
                                       child_array,
                                       chunk_offset,
                                       child_state,
                                       list_size,
                                       child_type,
                                       static_cast<int64_t>(start_offset));
                break;
            default:
                throw std::logic_error("arrow_array_physical_type not recognized");
        }
    }

    static void arrow_to_array(vector_t& vector,
                               ArrowArray& array,
                               size_t chunk_offset,
                               arrow_array_scan_state& array_state,
                               size_t size,
                               const arrow_type& arrow_type,
                               int64_t nested_offset,
                               const validity_mask_t* parent_mask,
                               int64_t parent_offset) {
        auto& array_info = arrow_type.get_type_info<arrow_array_info>();
        auto array_size = array_info.fixed_size();
        auto child_count = array_size * size;
        auto child_offset = get_effective_offset(array, parent_offset, chunk_offset, nested_offset) * array_size;

        set_validity(vector, array, chunk_offset, size, parent_offset, nested_offset);

        auto& child_vector = vector.entry();
        set_validity(child_vector,
                     *array.children[0],
                     chunk_offset,
                     child_count,
                     array.offset,
                     static_cast<int64_t>(child_offset));

        auto& array_mask = vector.validity();
        if (parent_mask) {
            if (!parent_mask->all_valid()) {
                for (size_t i = 0; i < size; i++) {
                    if (!parent_mask->row_is_valid(i)) {
                        array_mask.set_invalid(i);
                    }
                }
            }
        }

        if (!array_mask.all_valid()) {
            auto& child_validity_mask = child_vector.validity();
            for (size_t i = 0; i < size; i++) {
                if (!array_mask.row_is_valid(i)) {
                    for (size_t j = 0; j < array_size; j++) {
                        child_validity_mask.set_invalid(i * array_size + j);
                    }
                }
            }
        }

        auto& child_state = array_state.get_child(0);
        auto& child_array = *array.children[0];
        auto& child_type = array_info.get_child();
        if (child_count == 0 && child_offset == 0) {
            assert(!child_array.dictionary);
            arrow_column_to_vector(child_vector, child_array, chunk_offset, child_state, child_count, child_type, -1);
        } else {
            if (child_array.dictionary) {
                arrow_column_to_dictionary(child_vector,
                                           child_array,
                                           chunk_offset,
                                           child_state,
                                           child_count,
                                           child_type,
                                           static_cast<int64_t>(child_offset));
            } else {
                arrow_column_to_vector(child_vector,
                                       child_array,
                                       chunk_offset,
                                       child_state,
                                       child_count,
                                       child_type,
                                       static_cast<int64_t>(child_offset));
            }
        }
    }

    void arrow_column_to_vector(vector_t& vector,
                                ArrowArray& array,
                                size_t chunk_offset,
                                arrow_array_scan_state& array_state,
                                size_t size,
                                const arrow_type& arrow_type,
                                int64_t nested_offset,
                                validity_mask_t* parent_mask,
                                uint64_t parent_offset,
                                bool ignore_extensions) {
        assert(!array.dictionary);
        if (!ignore_extensions && arrow_type.has_extension()) {
            if (arrow_type.extension_data->arrow_to_unique) {
                vector_t input_data(vector.resource(), arrow_type.extension_data->internal_type());
                arrow_column_to_vector(input_data,
                                       array,
                                       chunk_offset,
                                       array_state,
                                       size,
                                       arrow_type,
                                       nested_offset,
                                       parent_mask,
                                       parent_offset,
                                       /*ignore_extensions*/ true);
                arrow_type.extension_data->arrow_to_unique(input_data, vector, size);
                return;
            }
        }

        if (vector.get_buffer()) {
            vector.get_buffer()->set_auxiliary(std::make_unique<arrow_auxiliary_data_t>(array_state.owned_data));
        }
        switch (vector.type().type()) {
            case types::logical_type::NA:
                vector.reference(types::logical_value_t());
                break;
            case types::logical_type::BOOLEAN: {
                auto effective_offset =
                    get_effective_offset(array, static_cast<int64_t>(parent_offset), chunk_offset, nested_offset);
                auto src_ptr = arrow_buffer_data<uint8_t>(array, 1) + effective_offset / 8;
                auto tgt_ptr = vector.data<uint8_t>();
                int src_pos = 0;
                size_t cur_bit = effective_offset % 8;
                for (size_t row = 0; row < size; row++) {
                    if ((src_ptr[src_pos] & (1 << cur_bit)) == 0) {
                        tgt_ptr[row] = 0;
                    } else {
                        tgt_ptr[row] = 1;
                    }
                    cur_bit++;
                    if (cur_bit == 8) {
                        src_pos++;
                        cur_bit = 0;
                    }
                }
                break;
            }
            case types::logical_type::TINYINT:
            case types::logical_type::SMALLINT:
            case types::logical_type::INTEGER:
            case types::logical_type::FLOAT:
            case types::logical_type::DOUBLE:
            case types::logical_type::UTINYINT:
            case types::logical_type::USMALLINT:
            case types::logical_type::UINTEGER:
            case types::logical_type::UBIGINT:
            case types::logical_type::BIGINT:
            case types::logical_type::HUGEINT:
            case types::logical_type::UHUGEINT:
            case types::logical_type::TIMESTAMP_NS:
            case types::logical_type::TIMESTAMP_US:
            case types::logical_type::TIMESTAMP_MS:
            case types::logical_type::TIMESTAMP_SEC: {
                direct_conversion(vector, array, chunk_offset, nested_offset, parent_offset);
                break;
            }
            case types::logical_type::BLOB:
            case types::logical_type::BIT:
            case types::logical_type::STRING_LITERAL: {
                auto& string_info = arrow_type.get_type_info<arrow_string_info>();
                auto size_type = string_info.get_size_type();
                switch (size_type) {
                    case arrow_variable_size_type::SUPER_SIZE: {
                        auto cdata = arrow_buffer_data<char>(array, 2);
                        auto offsets = arrow_buffer_data<uint64_t>(array, 1) +
                                       get_effective_offset(array,
                                                            static_cast<int64_t>(parent_offset),
                                                            chunk_offset,
                                                            nested_offset);
                        set_vector_string(vector, size, cdata, offsets);
                        break;
                    }
                    case arrow_variable_size_type::NORMAL: {
                        auto cdata = arrow_buffer_data<char>(array, 2);
                        auto offsets = arrow_buffer_data<uint32_t>(array, 1) +
                                       get_effective_offset(array,
                                                            static_cast<int64_t>(parent_offset),
                                                            chunk_offset,
                                                            nested_offset);
                        set_vector_string(vector, size, cdata, offsets);
                        break;
                    }
                    case arrow_variable_size_type::VIEW: {
                        set_vector_string_view(vector,
                                               size,
                                               array,
                                               get_effective_offset(array,
                                                                    static_cast<int64_t>(parent_offset),
                                                                    chunk_offset,
                                                                    nested_offset));
                        break;
                    }
                    case arrow_variable_size_type::FIXED_SIZE: {
                        set_validity(vector,
                                     array,
                                     chunk_offset,
                                     size,
                                     static_cast<int64_t>(parent_offset),
                                     nested_offset);
                        auto fixed_size = string_info.fixed_size();
                        size_t offset = get_effective_offset(array,
                                                             static_cast<int64_t>(parent_offset),
                                                             chunk_offset,
                                                             nested_offset) *
                                        fixed_size;
                        auto cdata = arrow_buffer_data<char>(array, 1);
                        auto blob_len = fixed_size;
                        auto result = vector.data<std::string_view>();
                        for (size_t row_idx = 0; row_idx < size; row_idx++) {
                            if (vector.is_null(row_idx)) {
                                offset += blob_len;
                                continue;
                            }
                            if (!vector.auxiliary()) {
                                vector.set_auxiliary(std::make_shared<string_vector_buffer_t>(vector.resource()));
                            }
                            auto auxiliary = static_cast<string_vector_buffer_t*>(vector.auxiliary().get());
                            result[row_idx] = std::string_view((char*) auxiliary->insert(cdata + offset, blob_len));
                            offset += blob_len;
                        }
                    }
                }
                break;
            }
            case types::logical_type::DECIMAL: {
                auto val_mask = vector.validity();
                auto& datetime_info = arrow_type.get_type_info<arrow_decimal_info>();
                auto bit_width = datetime_info.get_bit_width();

                switch (bit_width) {
                    case decimal_bit_width::DECIMAL_32: {
                        auto src_ptr = arrow_buffer_data<int32_t>(array, 1) +
                                       get_effective_offset(array,
                                                            static_cast<int64_t>(parent_offset),
                                                            chunk_offset,
                                                            nested_offset);
                        convert_decimal(src_ptr,
                                        vector,
                                        array,
                                        size,
                                        nested_offset,
                                        parent_offset,
                                        chunk_offset,
                                        val_mask,
                                        bit_width);
                        break;
                    }

                    case decimal_bit_width::DECIMAL_64: {
                        auto src_ptr = arrow_buffer_data<int64_t>(array, 1) +
                                       get_effective_offset(array,
                                                            static_cast<int64_t>(parent_offset),
                                                            chunk_offset,
                                                            nested_offset);
                        convert_decimal(src_ptr,
                                        vector,
                                        array,
                                        size,
                                        nested_offset,
                                        parent_offset,
                                        chunk_offset,
                                        val_mask,
                                        bit_width);
                        break;
                    }

                    case decimal_bit_width::DECIMAL_128: {
                        auto src_ptr = arrow_buffer_data<types::int128_t>(array, 1) +
                                       get_effective_offset(array,
                                                            static_cast<int64_t>(parent_offset),
                                                            chunk_offset,
                                                            nested_offset);
                        convert_decimal(src_ptr,
                                        vector,
                                        array,
                                        size,
                                        nested_offset,
                                        parent_offset,
                                        chunk_offset,
                                        val_mask,
                                        bit_width);
                        break;
                    }
                    default:
                        throw std::logic_error("Unsupported precision for Arrow Decimal Type.");
                }
                break;
            }
            case types::logical_type::LIST: {
                arrow_to_list(vector,
                              array,
                              chunk_offset,
                              array_state,
                              size,
                              arrow_type,
                              nested_offset,
                              parent_mask,
                              static_cast<int64_t>(parent_offset));
                break;
            }
            case types::logical_type::ARRAY: {
                arrow_to_array(vector,
                               array,
                               chunk_offset,
                               array_state,
                               size,
                               arrow_type,
                               nested_offset,
                               parent_mask,
                               static_cast<int64_t>(parent_offset));
                break;
            }
            case types::logical_type::MAP: {
                arrow_to_list(vector,
                              array,
                              chunk_offset,
                              array_state,
                              size,
                              arrow_type,
                              nested_offset,
                              parent_mask,
                              static_cast<int64_t>(parent_offset));
                break;
            }
            case types::logical_type::STRUCT: {
                auto& struct_info = arrow_type.get_type_info<arrow_struct_info>();
                auto& child_entries = vector.entries();
                auto& struct_validity_mask = vector.validity();
                for (size_t child_idx = 0; child_idx < static_cast<size_t>(array.n_children); child_idx++) {
                    auto& child_entry = *child_entries[child_idx];
                    auto& child_array = *array.children[child_idx];
                    auto& child_type = struct_info.get_child(child_idx);
                    auto& child_state = array_state.get_child(child_idx);

                    set_validity(child_entry, child_array, chunk_offset, size, array.offset, nested_offset);
                    if (!struct_validity_mask.all_valid()) {
                        auto& child_validity_mark = child_entry.validity();
                        for (size_t i = 0; i < size; i++) {
                            if (!struct_validity_mask.row_is_valid(i)) {
                                child_validity_mark.set_invalid(i);
                            }
                        }
                    }

                    auto array_physical_type = child_type.get_physical_type();
                    switch (array_physical_type) {
                        case arrow_array_physical_type::DICTIONARY_ENCODED:
                            arrow_column_to_dictionary(child_entry,
                                                       child_array,
                                                       chunk_offset,
                                                       child_state,
                                                       size,
                                                       child_type,
                                                       nested_offset,
                                                       &struct_validity_mask,
                                                       static_cast<uint64_t>(array.offset));
                            break;
                        case arrow_array_physical_type::RUN_END_ENCODED:
                            arrow_column_to_run_end_encoded(child_entry,
                                                            child_array,
                                                            chunk_offset,
                                                            child_state,
                                                            size,
                                                            child_type,
                                                            nested_offset,
                                                            &struct_validity_mask,
                                                            static_cast<uint64_t>(array.offset));
                            break;
                        case arrow_array_physical_type::DEFAULT:
                            arrow_column_to_vector(child_entry,
                                                   child_array,
                                                   chunk_offset,
                                                   child_state,
                                                   size,
                                                   child_type,
                                                   nested_offset,
                                                   &struct_validity_mask,
                                                   static_cast<uint64_t>(array.offset),
                                                   false);
                            break;
                        default:
                            throw std::logic_error("arrow_array_physical_type not recognized");
                    }
                }
                break;
            }
            case types::logical_type::UNION: {
                auto type_ids = arrow_buffer_data<int8_t>(array, array.n_buffers == 1 ? 0 : 1);
                assert(type_ids);
                auto members = vector.type().child_types();

                auto& validity_mask = vector.validity();
                auto& union_info = arrow_type.get_type_info<arrow_struct_info>();
                std::vector<vector_t> children;
                for (size_t child_idx = 0; child_idx < static_cast<size_t>(array.n_children); child_idx++) {
                    vector_t child(vector.resource(), members[child_idx + 1], size);
                    auto& child_array = *array.children[child_idx];
                    auto& child_state = array_state.get_child(child_idx);
                    auto& child_type = union_info.get_child(child_idx);

                    set_validity(child,
                                 child_array,
                                 chunk_offset,
                                 size,
                                 static_cast<int64_t>(parent_offset),
                                 nested_offset);
                    auto array_physical_type = child_type.get_physical_type();

                    switch (array_physical_type) {
                        case arrow_array_physical_type::DICTIONARY_ENCODED:
                            arrow_column_to_dictionary(child, child_array, chunk_offset, child_state, size, child_type);
                            break;
                        case arrow_array_physical_type::RUN_END_ENCODED:
                            arrow_column_to_run_end_encoded(child,
                                                            child_array,
                                                            chunk_offset,
                                                            child_state,
                                                            size,
                                                            child_type);
                            break;
                        case arrow_array_physical_type::DEFAULT:
                            arrow_column_to_vector(child,
                                                   child_array,
                                                   chunk_offset,
                                                   child_state,
                                                   size,
                                                   child_type,
                                                   nested_offset,
                                                   &validity_mask,
                                                   false);
                            break;
                        default:
                            throw std::logic_error("arrow_array_physical_type not recognized");
                    }

                    children.push_back(std::move(child));
                }

                for (size_t row_idx = 0; row_idx < size; row_idx++) {
                    auto tag = static_cast<uint8_t>(type_ids[row_idx]);

                    auto out_of_range = tag >= array.n_children;
                    if (out_of_range) {
                        throw std::logic_error("Arrow union tag out of range");
                    }

                    const types::logical_value_t& value = children[tag].value(row_idx);
                    vector.set_value(row_idx,
                                     value.is_null() ? types::logical_value_t()
                                                     : types::logical_value_t::create_union(members, tag, value));
                }

                break;
            }
            default:
                throw std::logic_error("Unsupported type for arrow conversion");
        }
    }
    static bool can_contain_null(const ArrowArray& array, const validity_mask_t* parent_mask) {
        if (array.null_count > 0) {
            return true;
        }
        if (!parent_mask) {
            return false;
        }
        return !parent_mask->all_valid();
    }

    template<class T>
    static void set_indexing_vector_loop(indexing_vector_t& indexing, std::byte* indices_p, size_t size) {
        auto indices = reinterpret_cast<T*>(indices_p);
        for (size_t row = 0; row < size; row++) {
            indexing.set_index(row, static_cast<size_t>(indices[row]));
        }
    }

    template<class T>
    static void set_indexing_vector_loop_with_checks(indexing_vector_t& indexing, std::byte* indices_p, size_t size) {
        auto indices = reinterpret_cast<T*>(indices_p);
        for (size_t row = 0; row < size; row++) {
            indexing.set_index(row, static_cast<size_t>(indices[row]));
        }
    }

    template<class T>
    static void set_masked_indexing_vector_loop(indexing_vector_t& indexing,
                                                std::byte* indices_p,
                                                size_t size,
                                                validity_mask_t& mask,
                                                size_t last_element_pos) {
        auto indices = reinterpret_cast<T*>(indices_p);
        for (size_t row = 0; row < size; row++) {
            if (mask.row_is_valid(row)) {
                indexing.set_index(row, static_cast<size_t>(indices[row]));
            } else {
                indexing.set_index(row, last_element_pos);
            }
        }
    }

    static void set_indexing_vector(indexing_vector_t& indexing,
                                    std::byte* indices_p,
                                    const types::complex_logical_type& logical_type,
                                    size_t size,
                                    validity_mask_t* mask = nullptr,
                                    size_t last_element_pos = 0) {
        indexing.reset(size);

        if (mask) {
            switch (logical_type.type()) {
                case types::logical_type::UTINYINT:
                    set_masked_indexing_vector_loop<uint8_t>(indexing, indices_p, size, *mask, last_element_pos);
                    break;
                case types::logical_type::TINYINT:
                    set_masked_indexing_vector_loop<int8_t>(indexing, indices_p, size, *mask, last_element_pos);
                    break;
                case types::logical_type::USMALLINT:
                    set_masked_indexing_vector_loop<uint16_t>(indexing, indices_p, size, *mask, last_element_pos);
                    break;
                case types::logical_type::SMALLINT:
                    set_masked_indexing_vector_loop<int16_t>(indexing, indices_p, size, *mask, last_element_pos);
                    break;
                case types::logical_type::UINTEGER:
                    set_masked_indexing_vector_loop<uint32_t>(indexing, indices_p, size, *mask, last_element_pos);
                    break;
                case types::logical_type::INTEGER:
                    set_masked_indexing_vector_loop<int32_t>(indexing, indices_p, size, *mask, last_element_pos);
                    break;
                case types::logical_type::UBIGINT:
                    set_masked_indexing_vector_loop<uint64_t>(indexing, indices_p, size, *mask, last_element_pos);
                    break;
                case types::logical_type::BIGINT:
                    set_masked_indexing_vector_loop<int64_t>(indexing, indices_p, size, *mask, last_element_pos);
                    break;

                default:
                    throw std::logic_error("(Arrow) Unsupported type for selection vectors");
            }

        } else {
            switch (logical_type.type()) {
                case types::logical_type::UTINYINT:
                    set_indexing_vector_loop<uint8_t>(indexing, indices_p, size);
                    break;
                case types::logical_type::TINYINT:
                    set_indexing_vector_loop<int8_t>(indexing, indices_p, size);
                    break;
                case types::logical_type::USMALLINT:
                    set_indexing_vector_loop<uint16_t>(indexing, indices_p, size);
                    break;
                case types::logical_type::SMALLINT:
                    set_indexing_vector_loop<int16_t>(indexing, indices_p, size);
                    break;
                case types::logical_type::UINTEGER:
                    set_indexing_vector_loop<uint32_t>(indexing, indices_p, size);
                    break;
                case types::logical_type::INTEGER:
                    set_indexing_vector_loop<int32_t>(indexing, indices_p, size);
                    break;
                case types::logical_type::UBIGINT:
                    if (last_element_pos > std::numeric_limits<uint32_t>::max()) {
                        set_indexing_vector_loop_with_checks<uint64_t>(indexing, indices_p, size);
                    } else {
                        set_indexing_vector_loop<uint64_t>(indexing, indices_p, size);
                    }
                    break;
                case types::logical_type::BIGINT:
                    if (last_element_pos > std::numeric_limits<uint32_t>::max()) {
                        set_indexing_vector_loop_with_checks<int64_t>(indexing, indices_p, size);
                    } else {
                        set_indexing_vector_loop<int64_t>(indexing, indices_p, size);
                    }
                    break;
                default:
                    throw std::logic_error("(Arrow) Unsupported type for selection vectors");
            }
        }
    }

    void arrow_column_to_dictionary(vector_t& vector,
                                    ArrowArray& array,
                                    size_t chunk_offset,
                                    arrow_array_scan_state& array_state,
                                    size_t size,
                                    const arrow_type& arrow_type,
                                    int64_t nested_offset,
                                    const validity_mask_t* parent_mask,
                                    uint64_t parent_offset) {
        if (vector.get_buffer()) {
            vector.get_buffer()->set_auxiliary(std::make_unique<arrow_auxiliary_data_t>(array_state.owned_data));
        }
        assert(arrow_type.has_dictionary());
        const bool has_nulls = can_contain_null(array, parent_mask);
        if (array_state.cache_outdated(array.dictionary)) {
            auto base_vector = std::make_unique<vector_t>(vector.resource(),
                                                          vector.type(),
                                                          static_cast<size_t>(array.dictionary->length));
            set_validity(*base_vector,
                         *array.dictionary,
                         chunk_offset,
                         static_cast<size_t>(array.dictionary->length),
                         0,
                         0,
                         has_nulls);
            auto& dictionary_type = arrow_type.get_dictionary();
            auto arrow_physical_type = dictionary_type.get_physical_type();
            ;
            switch (arrow_physical_type) {
                case arrow_array_physical_type::DICTIONARY_ENCODED:
                    arrow_column_to_dictionary(*base_vector,
                                               *array.dictionary,
                                               chunk_offset,
                                               array_state,
                                               static_cast<size_t>(array.dictionary->length),
                                               dictionary_type);
                    break;
                case arrow_array_physical_type::RUN_END_ENCODED:
                    arrow_column_to_run_end_encoded(*base_vector,
                                                    *array.dictionary,
                                                    chunk_offset,
                                                    array_state,
                                                    static_cast<size_t>(array.dictionary->length),
                                                    dictionary_type);
                    break;
                case arrow_array_physical_type::DEFAULT:
                    arrow_column_to_vector(*base_vector,
                                           *array.dictionary,
                                           chunk_offset,
                                           array_state,
                                           static_cast<size_t>(array.dictionary->length),
                                           dictionary_type);
                    break;
                default:
                    throw std::logic_error("arrow_array_physical_type not recognized");
            };
            array_state.add_dictionary(std::move(base_vector), array.dictionary);
        }
        auto offset_type = arrow_type.type();
        auto indices =
            arrow_buffer_data<std::byte>(array, 1) +
            offset_type.size() *
                get_effective_offset(array, static_cast<int64_t>(parent_offset), chunk_offset, nested_offset);

        indexing_vector_t indexing(vector.resource());
        if (has_nulls) {
            validity_mask_t indices_validity(vector.resource(), size);
            get_validity(indices_validity, array, chunk_offset, size, static_cast<int64_t>(parent_offset));
            if (parent_mask && !parent_mask->all_valid()) {
                auto& struct_validity_mask = *parent_mask;
                for (size_t i = 0; i < size; i++) {
                    if (!struct_validity_mask.row_is_valid(i)) {
                        indices_validity.set_invalid(i);
                    }
                }
            }
            set_indexing_vector(indexing,
                                indices,
                                offset_type,
                                size,
                                &indices_validity,
                                static_cast<size_t>(array.dictionary->length));
        } else {
            set_indexing_vector(indexing, indices, offset_type, size);
        }
        vector.slice(array_state.get_dictionary(), indexing, size);
    }

} // namespace components::vector::arrow::scaner