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
#include <cstdlib>
#include <vector>

namespace components::vector::arrow {
    using appender::arrow_append_data_t;
    using types::complex_logical_type;
    using types::logical_type;
    using types::physical_type;

    arrow_appender_t::arrow_appender_t(std::pmr::memory_resource* resource,
                                       const std::pmr::vector<complex_logical_type>& types)
        : resource_(resource)
        , types_(types, resource) {}

    core::result_wrapper_t<arrow_appender_t>
    arrow_appender_t::create(std::pmr::memory_resource* resource,
                             const std::pmr::vector<complex_logical_type>& types,
                             uint64_t initial_capacity) {
        arrow_appender_t appender(resource, types);
        for (auto& type : appender.types_) {
            auto entry = initialize_child(resource, type, initial_capacity);
            if (entry.has_error()) {
                return entry.convert_error<arrow_appender_t>();
            }
            appender.root_data_.push_back(std::move(entry.value()));
        }
        return appender;
    }

    core::error_t arrow_appender_t::append(data_chunk_t& input, uint64_t from, uint64_t to, uint64_t input_size) {
        assert(types_ == input.types());
        assert(to >= from);
        for (uint64_t i = 0; i < input.column_count(); i++) {
            if (auto err = root_data_[i]->append_vector(*root_data_[i], input.data[i], from, to, input_size);
                err.contains_error()) {
                return err;
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
            if (auto err = append_data.finalize(append_data, type, result.get()); err.contains_error()) {
                release_array(result.get());
                return err;
            }
        }

        append_data.array = std::move(result);
        return append_data.array.get();
    }

    core::result_wrapper_t<ArrowArray> arrow_appender_t::finalize() {
        assert(root_data_.size() == types_.size());
        auto root_holder = std::make_unique<arrow_append_data_t>(resource_);

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
        auto& holder = *root_holder;
        result.private_data = root_holder.release();
        result.release = arrow_appender_t::release_array;

        for (uint64_t i = 0; i < holder.child_data.size(); i++) {
            auto child = arrow_appender_t::finalize_child(types_[i], std::move(holder.child_data[i]));
            if (child.has_error()) {
                release_array(&result);
                return child.convert_error<ArrowArray>();
            }
            holder.child_arrays[i] = *child.value();
        }
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
                return core::error_t::no_error();
            case logical_type::TINYINT:
                initialize_appender_templated<appender::arrow_scala_data<int8_t>>(append_data);
                return core::error_t::no_error();
            case logical_type::SMALLINT:
                initialize_appender_templated<appender::arrow_scala_data<int16_t>>(append_data);
                return core::error_t::no_error();
            case logical_type::INTEGER:
                initialize_appender_templated<appender::arrow_scala_data<int32_t>>(append_data);
                return core::error_t::no_error();
            case logical_type::BIGINT:
                initialize_appender_templated<appender::arrow_scala_data<int64_t>>(append_data);
                return core::error_t::no_error();
            case logical_type::HUGEINT:
                initialize_appender_templated<appender::arrow_scala_data<absl::int128>>(append_data);
                return core::error_t::no_error();
            case logical_type::UHUGEINT:
                initialize_appender_templated<appender::arrow_scala_data<absl::uint128>>(append_data);
                return core::error_t::no_error();
            case logical_type::UTINYINT:
                initialize_appender_templated<appender::arrow_scala_data<uint8_t>>(append_data);
                return core::error_t::no_error();
            case logical_type::USMALLINT:
                initialize_appender_templated<appender::arrow_scala_data<uint16_t>>(append_data);
                return core::error_t::no_error();
            case logical_type::UINTEGER:
                initialize_appender_templated<appender::arrow_scala_data<uint32_t>>(append_data);
                return core::error_t::no_error();
            case logical_type::UBIGINT:
                initialize_appender_templated<appender::arrow_scala_data<uint64_t>>(append_data);
                return core::error_t::no_error();
            case logical_type::FLOAT:
                initialize_appender_templated<appender::arrow_scala_data<float>>(append_data);
                return core::error_t::no_error();
            case logical_type::DOUBLE:
                initialize_appender_templated<appender::arrow_scala_data<double>>(append_data);
                return core::error_t::no_error();
            case logical_type::DECIMAL: {
                switch (type.to_physical_type()) {
                    case physical_type::INT16:
                        initialize_appender_templated<appender::arrow_scala_data<absl::int128, int16_t>>(append_data);
                        return core::error_t::no_error();
                    case physical_type::INT32:
                        initialize_appender_templated<appender::arrow_scala_data<absl::int128, int32_t>>(append_data);
                        return core::error_t::no_error();
                    case physical_type::INT64:
                        initialize_appender_templated<appender::arrow_scala_data<absl::int128, int64_t>>(append_data);
                        return core::error_t::no_error();
                    case physical_type::INT128:
                        initialize_appender_templated<appender::arrow_scala_data<absl::int128>>(append_data);
                        return core::error_t::no_error();
                    case physical_type::BOOL:
                    case physical_type::UINT8:
                    case physical_type::INT8:
                    case physical_type::UINT16:
                    case physical_type::UINT32:
                    case physical_type::UINT64:
                    case physical_type::UINT128:
                    case physical_type::FLOAT:
                    case physical_type::DOUBLE:
                    case physical_type::STRING:
                    case physical_type::LIST:
                    case physical_type::STRUCT:
                    case physical_type::UNION:
                    case physical_type::ARRAY:
                    case physical_type::NA:
                    case physical_type::UNKNOWN:
                    case physical_type::BIT:
                    case physical_type::INVALID:
                        assert(false && "initialize_function_pointers: create_decimal stores DECIMAL as INT16..INT128");
                        std::abort();
                }
                assert(false && "initialize_function_pointers: physical_type out of its enumerators");
                std::abort();
            }
            case logical_type::STRING_LITERAL:
            case logical_type::BLOB:
                initialize_appender_templated<appender::arrow_string_data_t<>>(append_data);
                return core::error_t::no_error();
            case logical_type::STRUCT:
                initialize_appender_templated<appender::arrow_struct_data_t>(append_data);
                return core::error_t::no_error();
            case logical_type::ARRAY:
                initialize_appender_templated<appender::arrow_fixed_size_list_data_t>(append_data);
                return core::error_t::no_error();
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
                return core::error_t::no_error();
            }
            case logical_type::MAP:
                initialize_appender_templated<appender::arrow_map_data_t<int32_t>>(append_data);
                return core::error_t::no_error();
            case logical_type::DATE:
                initialize_appender_templated<appender::arrow_date_appender_t>(append_data);
                return core::error_t::no_error();
            case logical_type::TIME:
                initialize_appender_templated<appender::arrow_time_appender_t>(append_data);
                return core::error_t::no_error();
            case logical_type::TIMESTAMP:
            case logical_type::TIMESTAMP_TZ:
                initialize_appender_templated<appender::arrow_timestamp_appender_t>(append_data);
                return core::error_t::no_error();
            case logical_type::INTERVAL:
                initialize_appender_templated<appender::arrow_interval_appender_t>(append_data);
                return core::error_t::no_error();
            case logical_type::NA:
            case logical_type::ANY:
            case logical_type::USER:
            case logical_type::TIME_TZ:
            case logical_type::BIT:
            case logical_type::INTEGER_LITERAL:
            case logical_type::POINTER:
            case logical_type::VALIDITY:
            case logical_type::UUID:
            case logical_type::TABLE:
            case logical_type::ENUM:
            case logical_type::FUNCTION:
            case logical_type::LAMBDA:
            case logical_type::UNION:
            case logical_type::VARIANT:
            case logical_type::UNKNOWN:
            case logical_type::INVALID:
                return core::error_t(core::error_code_t::unimplemented_yet,
                                     std::pmr::string("Unsupported type in OtterBrix -> initialize_function_pointers()",
                                                      append_data.resource));
        }
        assert(false && "initialize_function_pointers: logical_type out of its enumerators");
        std::abort();
    }

    core::result_wrapper_t<std::unique_ptr<arrow_append_data_t>>
    arrow_appender_t::initialize_child(std::pmr::memory_resource* resource,
                                       const types::complex_logical_type& type,
                                       const uint64_t capacity) {
        auto result = std::make_unique<arrow_append_data_t>(resource);
        if (auto err = initialize_function_pointers(*result, type); err.contains_error()) {
            return err;
        }

        const auto byte_count = (capacity + 7) / 8;
        result->validity_buffer().reserve(byte_count);
        if (auto err = result->initialize(*result, type, capacity); err.contains_error()) {
            return err;
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
