#include "convert_value.hpp"

#include "util.hpp"

#include <common/typedefs.hpp>
#include <connection_environment/connection_environment.hpp>
#include <native/python_objects.hpp>

#include <limits>
#include <stdexcept>
#include <string>
#include <vector>

using components::types::complex_logical_type;
using components::types::logical_type;
using components::vector::vector_t;

namespace otterbrix { namespace util {

    namespace {

        core::error_t make_error(std::pmr::memory_resource* r, const char* what) {
            return core::error_t{core::error_code_t::other_error, std::pmr::string{what, r}};
        }

        // An integer or FLOAT cell as the double a DOUBLE column answers with.
        [[nodiscard]] bool numeric_as_double(const vector_t& leaf, uint64_t i, double& result) {
            switch (leaf.type().type()) {
                case logical_type::TINYINT:
                    result = static_cast<double>(leaf.data<int8_t>()[i]);
                    return true;
                case logical_type::SMALLINT:
                    result = static_cast<double>(leaf.data<int16_t>()[i]);
                    return true;
                case logical_type::INTEGER:
                    result = static_cast<double>(leaf.data<int32_t>()[i]);
                    return true;
                case logical_type::BIGINT:
                    result = static_cast<double>(leaf.data<int64_t>()[i]);
                    return true;
                case logical_type::UTINYINT:
                    result = static_cast<double>(leaf.data<uint8_t>()[i]);
                    return true;
                case logical_type::USMALLINT:
                    result = static_cast<double>(leaf.data<uint16_t>()[i]);
                    return true;
                case logical_type::UINTEGER:
                    result = static_cast<double>(leaf.data<uint32_t>()[i]);
                    return true;
                case logical_type::UBIGINT:
                    result = static_cast<double>(leaf.data<uint64_t>()[i]);
                    return true;
                case logical_type::FLOAT:
                    result = static_cast<double>(leaf.data<float>()[i]);
                    return true;
                default:
                    return false;
            }
        }

        // Unscaled value at the decimal's storage width; a 128-bit one keeps its low 64 bits.
        [[nodiscard]] int64_t decimal_unscaled(const vector_t& leaf, uint64_t i) {
            switch (leaf.type().to_physical_type()) {
                case components::types::physical_type::INT16:
                    return leaf.data<int16_t>()[i];
                case components::types::physical_type::INT32:
                    return leaf.data<int32_t>()[i];
                case components::types::physical_type::INT64:
                    return leaf.data<int64_t>()[i];
                default:
                    return static_cast<int64_t>(leaf.data<components::types::int128_t>()[i]);
            }
        }

        [[nodiscard]] py::object python_string(std::string_view text) { return py::str(text.data(), text.size()); }

        core::result_wrapper_t<py::object> to_python(const vector_t& column, uint64_t row);

        core::result_wrapper_t<py::object> to_python(const vector_t::nested_element_t& element) {
            const auto& leaf = *element.leaf;
            const auto index = element.index;
            if (element.is_null || leaf.type().type() == logical_type::NA) {
                return py::object(py::none());
            }
            auto* r = leaf.resource();
            auto& import_cache = connection_environment_t::import_cache();
            switch (leaf.type().type()) {
                case logical_type::BOOLEAN:
                    return py::object(py::cast(leaf.data<bool>()[index]));
                case logical_type::TINYINT:
                    return py::object(py::cast(leaf.data<int8_t>()[index]));
                case logical_type::SMALLINT:
                    return py::object(py::cast(leaf.data<int16_t>()[index]));
                case logical_type::INTEGER:
                    return py::object(py::cast(leaf.data<int32_t>()[index]));
                case logical_type::BIGINT:
                    return py::object(py::cast(leaf.data<int64_t>()[index]));
                case logical_type::UTINYINT:
                    return py::object(py::cast(leaf.data<uint8_t>()[index]));
                case logical_type::USMALLINT:
                    return py::object(py::cast(leaf.data<uint16_t>()[index]));
                case logical_type::UINTEGER:
                    return py::object(py::cast(leaf.data<uint32_t>()[index]));
                case logical_type::UBIGINT:
                    return py::object(py::cast(leaf.data<uint64_t>()[index]));
                case logical_type::HUGEINT:
                    return make_error(r, "OtterBrix doen\'t support hugeint conversation to python object");
                case logical_type::UHUGEINT:
                    return make_error(r, "OtterBrix doen\'t support uhugeint conversation to python object");
                case logical_type::FLOAT:
                    return py::object(py::cast(leaf.data<float>()[index]));
                case logical_type::DOUBLE:
                    return py::object(py::cast(leaf.data<double>()[index]));
                case logical_type::DECIMAL: {
                    const auto* extension =
                        leaf.type().extension_as<components::types::decimal_logical_type_extension>();
                    auto digits = std::to_string(decimal_unscaled(leaf, index));
                    auto pydigits = import_cache.decimal.Decimal()(digits);
                    return py::object(pydigits.attr("__truediv__")(py::int_(extension->scale())));
                }
                case logical_type::ENUM:
                    return make_error(r, "OtterBrix doen\'t support enum type");
                case logical_type::UNION:
                    return make_error(r, "OtterBrix doen\'t support union type");
                case logical_type::STRING_LITERAL:
                    return python_string(leaf.data<std::string_view>()[index]);
                case logical_type::UUID:
                case logical_type::BLOB:
                case logical_type::BIT:
                    return make_error(r, "Unimplemented type for value access");
                case logical_type::TIMESTAMP: {
                    const auto timestamp = leaf.data<int64_t>()[index];
                    if (timestamp == std::numeric_limits<int64_t>::max()) {
                        return py::reinterpret_borrow<py::object>(import_cache.datetime.datetime.max());
                    }
                    if (timestamp == std::numeric_limits<int64_t>::min()) {
                        return py::reinterpret_borrow<py::object>(import_cache.datetime.datetime.min());
                    }
                    return py::object(py::cast(timestamp));
                }
                case logical_type::LIST: {
                    const auto entry = leaf.data<components::types::list_entry_t>()[index];
                    py::list list;
                    for (uint64_t i = entry.offset; i < entry.offset + entry.length; i++) {
                        VALUE_OR_RETURN(auto element_value, to_python(leaf.entry(), i));
                        list.append(element_value);
                    }
                    return py::object(std::move(list));
                }
                case logical_type::ARRAY: {
                    const auto size = leaf.type().extension_as<components::types::array_logical_type_extension>()->size();
                    // do not remove the static cast here, it's required for building
                    // otterbrix-python with Emscripten.
                    //
                    // without this cast, a static_assert fails in pybind11
                    // because the return type of array_type::get_size is idx_t,
                    // which is typedef'd to uint64_t and ssize_t is 4 bytes with Emscripten
                    // and pybind11 requires that the input be castable to ssize_t
                    py::tuple array(static_cast<py::ssize_t>(size));
                    for (idx_t i = 0; i < size; i++) {
                        VALUE_OR_RETURN(auto element_value, to_python(leaf.entry(), index * size + i));
                        array[i] = element_value;
                    }
                    return py::object(std::move(array));
                }
                case logical_type::MAP: {
                    const auto entry = leaf.data<components::types::list_entry_t>()[index];
                    const auto& pairs = leaf.entry().entries();
                    const bool hashable = key_is_hashable(
                        leaf.type().extension_as<components::types::map_logical_type_extension>()->key());
                    py::dict map;
                    py::list keys;
                    py::list values;
                    for (uint64_t i = entry.offset; i < entry.offset + entry.length; i++) {
                        VALUE_OR_RETURN(auto key, to_python(*pairs[0], i));
                        VALUE_OR_RETURN(auto value, to_python(*pairs[1], i));
                        if (hashable) {
                            map[key] = value;
                        } else {
                            keys.append(key);
                            values.append(value);
                        }
                    }
                    if (!hashable) {
                        map["key"] = std::move(keys);
                        map["value"] = std::move(values);
                    }
                    return py::object(std::move(map));
                }
                case logical_type::STRUCT: {
                    const auto& child_types = leaf.type().child_types();
                    const auto& fields = leaf.entries();
                    // unnamed struct
                    if (child_types.empty() || child_types[0].alias().empty()) {
                        py::tuple tuple(fields.size());
                        for (idx_t i = 0; i < fields.size(); i++) {
                            VALUE_OR_RETURN(auto field, to_python(*fields[i], index));
                            tuple[i] = field;
                        }
                        return py::object(std::move(tuple));
                    }
                    py::dict dict;
                    for (idx_t i = 0; i < fields.size(); i++) {
                        VALUE_OR_RETURN(auto field, to_python(*fields[i], index));
                        dict[child_types[i].alias().c_str()] = field;
                    }
                    return py::object(std::move(dict));
                }
                default:
                    return make_error(r, "Unsupported type: ");
            }
        }

        core::result_wrapper_t<py::object> to_python(const vector_t& column, uint64_t row) {
            return to_python(column.resolve_nested_element(row, std::span<const size_t>{}, 0));
        }

    } // namespace

    py::object cell_to_python(const vector_t& column, uint64_t row, const complex_logical_type& declared) {
        const auto element = column.resolve_nested_element(row, std::span<const size_t>{}, 0);
        if (declared.type() == logical_type::DOUBLE && !element.is_null &&
            element.leaf->type().type() != logical_type::DOUBLE) {
            double number;
            if (numeric_as_double(*element.leaf, element.index, number)) {
                return py::cast(number);
            }
        }
        auto result = to_python(element);
        if (result.has_error()) {
            throw std::runtime_error(std::string(result.error().what));
        }
        return std::move(result.value());
    }

    py::dict cursor_row_to_python_dict(components::cursor::cursor_t_ptr& cursor,
                                       uint64_t row_idx,
                                       const std::vector<components::table::column_definition_t>& col_defs) {
        const auto location = locate_row(*cursor, row_idx);
        const auto& columns = cursor->chunks()[location.chunk].data;
        py::dict result;
        for (idx_t i = 0; i < col_defs.size(); i++) {
            result[py::str(col_defs[i].name())] = cell_to_python(columns[i], location.row, col_defs[i].type());
        }
        return result;
    }

}} // namespace otterbrix::util
