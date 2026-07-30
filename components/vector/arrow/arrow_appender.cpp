#include "arrow_appender.hpp"

#include "appender/append_data.hpp"
#include "appender/bool_data.hpp"
#include "appender/datetime_data.hpp"
#include "appender/fixed_size_list_data.hpp"
#include "appender/list_data.hpp"
#include "appender/list_view_data.hpp"
#include "appender/map_data.hpp"
#include "appender/string_data.hpp"
#include "appender/struct_data.hpp"

#include <absl/numeric/int128.h>

#include <cassert>
#include <memory_resource>
#include <string>
#include <vector>

namespace components::vector::arrow {
    using appender::arrow_append_data_t;
    using types::complex_logical_type;
    using types::logical_type;
    using types::physical_type;

    arrow_appender_t::arrow_appender_t(std::pmr::vector<complex_logical_type> types)
        : types_(std::move(types)) {}

    core::result_wrapper_t<arrow_appender_t> arrow_appender_t::create(std::pmr::vector<complex_logical_type> types,
                                                                      uint64_t initial_capacity) {
        arrow_appender_t appender(std::move(types));
        for (auto& type : appender.types_) {
            auto entry = initialize_child(type, initial_capacity);
            if (entry.has_error()) {
                return entry.error();
            }
            appender.root_data_.push_back(std::move(entry.value()));
        }
        return appender;
    }

    core::error_t arrow_appender_t::append(data_chunk_t& input, uint64_t from, uint64_t to, uint64_t input_size) {
        assert(types_ == input.types());
        assert(to >= from);
        for (uint64_t i = 0; i < input.column_count(); i++) {
            if (auto error = root_data_[i]->append_vector(*root_data_[i], input.data[i], from, to, input_size);
                error.contains_error()) {
                return error;
            }
        }
        row_count_ += to - from;
        return core::error_t::no_error();
    }

    uint64_t arrow_appender_t::row_count() const { return row_count_; }

    void arrow_appender_t::release_array(ArrowArray* array) {
        if (!array || !array->release) {
            return;
        }
        auto holder = static_cast<arrow_append_data_t*>(array->private_data);
        for (int64_t i = 0; i < array->n_children; i++) {
            auto child = array->children[i];
            if (!child->release) {
                continue;
            }
            child->release(child);
            assert(!child->release);
        }
        if (array->dictionary && array->dictionary->release) {
            array->dictionary->release(array->dictionary);
        }
        array->release = nullptr;
        delete holder;
    }

    core::result_wrapper_t<ArrowArray*>
    arrow_appender_t::finalize_child(const types::complex_logical_type& type,
                                     std::unique_ptr<arrow_append_data_t> append_data_p) {
        auto result = std::make_unique<ArrowArray>();

        auto& append_data = *append_data_p;
        result->private_data = append_data_p.release();
        result->release = release_array;
        result->n_children = 0;
        result->null_count = 0;
        result->offset = 0;
        result->dictionary = nullptr;
        result->buffers = append_data.buffers.data();
        result->null_count = static_cast<int64_t>(append_data.null_count);
        result->length = static_cast<int64_t>(append_data.row_count);
        result->buffers[0] = append_data.validity_buffer().data();

        if (append_data.finalize) {
            if (auto error = append_data.finalize(append_data, type, result.get()); error.contains_error()) {
                return error;
            }
        }

        append_data.array = std::move(result);
        return append_data.array.get();
    }

    core::result_wrapper_t<ArrowArray> arrow_appender_t::finalize() {
        assert(root_data_.size() == types_.size());
        auto root_holder = std::make_unique<arrow_append_data_t>();

        ArrowArray result;
        add_children(*root_holder, types_.size());
        result.children = root_holder->child_pointers.data();
        result.n_children = static_cast<int64_t>(types_.size());

        result.length = static_cast<int64_t>(row_count());
        result.n_buffers = 1;
        result.buffers = root_holder->buffers.data();
        result.offset = 0;
        result.null_count = 0;
        result.dictionary = nullptr;
        root_holder->child_data = std::move(root_data_);

        for (uint64_t i = 0; i < root_holder->child_data.size(); i++) {
            auto child_array = arrow_appender_t::finalize_child(types_[i], std::move(root_holder->child_data[i]));
            if (child_array.has_error()) {
                return child_array.error();
            }
            root_holder->child_arrays[i] = *child_array.value();
        }

        result.private_data = root_holder.release();
        result.release = arrow_appender_t::release_array;
        return result;
    }

    template<class OP>
    static void initialize_appender_templated(arrow_append_data_t& append_data) {
        append_data.initialize = OP::initialize;
        append_data.append_vector = OP::append;
        append_data.finalize = OP::finalize;
    }

    [[nodiscard]] static core::error_t initialize_function_pointers(arrow_append_data_t& append_data,
                                                                    const types::complex_logical_type& type) {
        switch (type.type()) {
            case logical_type::BOOLEAN:
                initialize_appender_templated<appender::arrow_bool_data_t>(append_data);
                break;
            case logical_type::TINYINT:
                initialize_appender_templated<appender::arrow_scala_data<int8_t>>(append_data);
                break;
            case logical_type::SMALLINT:
                initialize_appender_templated<appender::arrow_scala_data<int16_t>>(append_data);
                break;
            case logical_type::INTEGER:
                initialize_appender_templated<appender::arrow_scala_data<int32_t>>(append_data);
                break;
            case logical_type::BIGINT:
                initialize_appender_templated<appender::arrow_scala_data<int64_t>>(append_data);
                break;
            case logical_type::HUGEINT:
                initialize_appender_templated<appender::arrow_scala_data<absl::int128>>(append_data);
                break;
            case logical_type::UHUGEINT:
                initialize_appender_templated<appender::arrow_scala_data<absl::uint128>>(append_data);
                break;
            case logical_type::UTINYINT:
                initialize_appender_templated<appender::arrow_scala_data<uint8_t>>(append_data);
                break;
            case logical_type::USMALLINT:
                initialize_appender_templated<appender::arrow_scala_data<uint16_t>>(append_data);
                break;
            case logical_type::UINTEGER:
                initialize_appender_templated<appender::arrow_scala_data<uint32_t>>(append_data);
                break;
            case logical_type::UBIGINT:
                initialize_appender_templated<appender::arrow_scala_data<uint64_t>>(append_data);
                break;
            case logical_type::FLOAT:
                initialize_appender_templated<appender::arrow_scala_data<float>>(append_data);
                break;
            case logical_type::DOUBLE:
                initialize_appender_templated<appender::arrow_scala_data<double>>(append_data);
                break;
            case logical_type::DECIMAL: {
                switch (type.to_physical_type()) {
                    case physical_type::INT16:
                        initialize_appender_templated<appender::arrow_scala_data<absl::int128, int16_t>>(append_data);
                        break;
                    case physical_type::INT32:
                        initialize_appender_templated<appender::arrow_scala_data<absl::int128, int32_t>>(append_data);
                        break;
                    case physical_type::INT64:
                        initialize_appender_templated<appender::arrow_scala_data<absl::int128, int64_t>>(append_data);
                        break;
                    case physical_type::INT128:
                        initialize_appender_templated<appender::arrow_scala_data<absl::int128>>(append_data);
                        break;
                    default:
                        // Unreachable: decimal_logical_type_extension fixes stored_as() once, in its
                        // constructor, to one of the four widths above (decimal_storage_type). Kept as
                        // an error rather than an assert so a future storage width fails loudly here
                        // instead of producing an unappended column.
                        return core::error_t(
                            core::error_code_t::unimplemented_yet,
                            std::pmr::string("arrow appender: no appender for DECIMAL stored as physical type " +
                                                 std::to_string(static_cast<int>(type.to_physical_type())),
                                             std::pmr::get_default_resource()));
                }
                break;
            }
            case logical_type::STRING_LITERAL:
            case logical_type::BLOB:
                initialize_appender_templated<appender::arrow_string_data_t<>>(append_data);
                break;
            case logical_type::STRUCT:
                initialize_appender_templated<appender::arrow_struct_data_t>(append_data);
                break;
            case logical_type::ARRAY:
                initialize_appender_templated<appender::arrow_fixed_size_list_data_t>(append_data);
                break;
            case logical_type::LIST: {
                if (arrow_use_list_view) {
                    if (arrow_offset == arrow_offset_size::LARGE) {
                        initialize_appender_templated<appender::arrow_list_view_data_t<>>(append_data);
                    } else {
                        initialize_appender_templated<appender::arrow_list_view_data_t<int32_t>>(append_data);
                    }
                } else {
                    if (arrow_offset == arrow_offset_size::LARGE) {
                        initialize_appender_templated<appender::arrow_list_data_t<>>(append_data);
                    } else {
                        initialize_appender_templated<appender::arrow_list_data_t<int32_t>>(append_data);
                    }
                }
                break;
            }
            case logical_type::MAP:
                initialize_appender_templated<appender::arrow_map_data_t<int32_t>>(append_data);
                break;
            case logical_type::DATE:
                initialize_appender_templated<appender::arrow_date_appender_t>(append_data);
                break;
            case logical_type::TIME:
                initialize_appender_templated<appender::arrow_time_appender_t>(append_data);
                break;
            case logical_type::TIMESTAMP:
            case logical_type::TIMESTAMP_TZ:
                initialize_appender_templated<appender::arrow_timestamp_appender_t>(append_data);
                break;
            case logical_type::INTERVAL:
                initialize_appender_templated<appender::arrow_interval_appender_t>(append_data);
                break;
            default:
                // Reachable: to_arrow_schema writes a format string for types this switch has no
                // appender for (NA -> "n" is the live case), so a schema can be built for a column
                // whose data cannot be appended.
                return core::error_t(core::error_code_t::unimplemented_yet,
                                     std::pmr::string("arrow appender: no appender for logical type " +
                                                          std::to_string(static_cast<int>(type.type())),
                                                      std::pmr::get_default_resource()));
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<std::unique_ptr<arrow_append_data_t>>
    arrow_appender_t::initialize_child(const types::complex_logical_type& type, const uint64_t capacity) {
        auto result = std::make_unique<arrow_append_data_t>();
        if (auto error = initialize_function_pointers(*result, type); error.contains_error()) {
            return error;
        }

        const auto byte_count = (capacity + 7) / 8;
        if (auto error = result->validity_buffer().reserve(byte_count); error.contains_error()) {
            return error;
        }
        if (auto error = result->initialize(*result, type, capacity); error.contains_error()) {
            return error;
        }
        return result;
    }

    void arrow_appender_t::add_children(arrow_append_data_t& data, const uint64_t count) {
        data.child_pointers.resize(count);
        data.child_arrays.resize(count);
        for (uint64_t i = 0; i < count; i++) {
            data.child_pointers[i] = &data.child_arrays[i];
        }
    }

} // namespace components::vector::arrow
