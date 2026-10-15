#include "python_conversion.hpp"

#include "python_objects.hpp"

#include <connection_environment/connection_environment.hpp>
#include <pybind11/pybind_wrapper.hpp>

#include <common/string_util/case_insensitive.hpp>
#include <common/typedefs.hpp>
#include <components/casts/cast_registry.hpp>
#include <components/casts/default_casts.hpp>
#include <components/catalog/type_display.hpp>
#include <components/vector/vector_builders.hpp>

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory_resource>
#include <optional>
#include <span>
#include <string>
#include <vector>

using components::types::complex_logical_type;
using components::types::logical_type;
using components::vector::vector_t;

namespace otterbrix {
    static core::error_t make_error(std::pmr::memory_resource* r, const std::string& what) {
        return core::error_t{core::error_code_t::other_error, std::pmr::string{what, r}};
    }

    namespace {
        //! One conversion of one python object: the registry is built when the first container needs it.
        struct converter_t {
            std::pmr::memory_resource* resource;
            std::optional<components::casts::cast_registry_t> registry;

            const components::casts::cast_registry_t& casts() {
                if (!registry) {
                    registry.emplace(resource);
                    components::casts::register_default_casts(*registry);
                }
                return *registry;
            }
        };

        core::result_wrapper_t<vector_t> convert(converter_t& c, py::handle ele);

        vector_t empty_map(std::pmr::memory_resource* r) {
            return components::vector::make_map(r,
                                                logical_type::NA,
                                                logical_type::NA,
                                                std::span<const vector_t>{},
                                                std::span<const vector_t>{});
        }

        //! format_type_be spells an unnamed STRUCT and a MAP as nothing, so those are spelled out here.
        std::string readable_type(const complex_logical_type& type) {
            switch (type.type()) {
                case logical_type::STRUCT: {
                    if (type.extension() != nullptr && !type.type_name().empty()) {
                        return components::catalog::format_type_be(type);
                    }
                    std::string name = "record(";
                    const auto& fields = type.child_types();
                    for (size_t i = 0; i < fields.size(); i++) {
                        name += (i == 0 ? "" : ", ") + fields[i].alias() + " " + readable_type(fields[i]);
                    }
                    return name + ")";
                }
                case logical_type::MAP: {
                    const auto* map = type.extension_as<components::types::map_logical_type_extension>();
                    return "map(" + readable_type(map->key()) + ", " + readable_type(map->value()) + ")";
                }
                case logical_type::LIST:
                    return readable_type(type.child_type()) + "[]";
                default: {
                    auto name = components::catalog::format_type_be(type);
                    return name.empty() ? "unknown" : name;
                }
            }
        }

        //! The distinct non-NULL types, in order of appearance.
        std::string type_names(std::pmr::memory_resource* resource,
                               const std::pmr::vector<complex_logical_type>& types) {
            std::pmr::vector<std::pmr::string> seen(resource);
            for (const auto& type : types) {
                if (type.type() == logical_type::NA) {
                    continue;
                }
                std::pmr::string name(readable_type(type), resource);
                if (std::find(seen.begin(), seen.end(), name) == seen.end()) {
                    seen.push_back(std::move(name));
                }
            }
            std::string names;
            for (const auto& name : seen) {
                if (!names.empty()) {
                    names += ", ";
                }
                names.append(name.data(), name.size());
            }
            return names;
        }

        //! Casts every element to the common type of all of them (NA when there are none) and returns it.
        core::result_wrapper_t<complex_logical_type> unify_element_types(converter_t& c,
                                                                         std::pmr::vector<vector_t>& elements) {
            if (elements.empty()) {
                return complex_logical_type{logical_type::NA};
            }
            std::pmr::vector<complex_logical_type> types(c.resource);
            types.reserve(elements.size());
            for (const auto& element : elements) {
                types.push_back(element.type());
            }
            auto common = c.casts().find_best_common_type(std::span<const complex_logical_type>{types});
            if (!common.has_value()) {
                return make_error(c.resource,
                                  "Cannot infer common type for elements: " + type_names(c.resource, types));
            }
            for (size_t i = 0; i < elements.size(); i++) {
                if (!common->casts[i]) {
                    continue;
                }
                vector_t converted(c.resource, common->type, 1);
                RETURN_IF_ERROR(common->casts[i](components::casts::cast_kind::cast,
                                                 components::vector::make_flat_copy(c.resource, elements[i], 1),
                                                 &converted,
                                                 components::graph_execution_context{},
                                                 1));
                elements[i] = std::move(converted);
            }
            return std::move(common->type);
        }

        //! The python sequence's items, each converted (a numpy array through tolist()).
        core::result_wrapper_t<std::pmr::vector<vector_t>> transform_sequence(converter_t& c, py::handle ele);
    } // namespace

    std::vector<std::string> transform_struct_keys(py::handle keys, idx_t size) {
        std::vector<std::string> res;
        res.reserve(size);
        for (idx_t i = 0; i < size; i++) {
            res.emplace_back(py::str(keys.attr("__getitem__")(i)));
        }
        return res;
    }

    static bool is_valid_map_component(const py::handle& component) {
        // The component is either NULL
        if (py::none().is(component)) {
            return true;
        }
        if (!py::hasattr(component, "__getitem__")) {
            return false;
        }
        if (!py::hasattr(component, "__len__")) {
            return false;
        }
        return true;
    }

    bool dictionary_has_map_format(const py_dictionary_t& dict) {
        if (dict.len != 2) {
            return false;
        }

        //{ 'key': [ .. keys .. ], 'value': [ .. values .. ]}
        auto keys_key = py::str("key");
        auto values_key = py::str("value");
        auto keys = dict[keys_key];
        auto values = dict[values_key];
        if (!keys || !values) {
            return false;
        }

        if (!is_valid_map_component(keys)) {
            return false;
        }
        if (!is_valid_map_component(values)) {
            return false;
        }

        // If either of the components is NULL, return early
        if (py::none().is(keys) || py::none().is(values)) {
            return true;
        }

        // Verify that both the keys and values are of the same length
        auto size = py::len(keys);
        if (size != py::len(values)) {
            return false;
        }
        return true;
    }

    namespace {
        core::result_wrapper_t<vector_t> transform_dictionary_to_struct(converter_t& c, const py_dictionary_t& dict) {
            auto struct_keys = transform_struct_keys(dict.keys, dict.len);

            case_insensitive_map_t<idx_t> key_mapping;
            for (idx_t i = 0; i < struct_keys.size(); i++) {
                key_mapping[struct_keys[i]] = i;
            }

            std::pmr::vector<vector_t> fields(c.resource);
            fields.reserve(dict.len);
            for (idx_t i = 0; i < dict.len; i++) {
                auto value_index = key_mapping[struct_keys[i]];
                VALUE_OR_RETURN(auto field, convert(c, dict.values.attr("__getitem__")(value_index)));
                fields.push_back(std::move(field));
            }
            std::pmr::vector<complex_logical_type> child_types(c.resource);
            child_types.reserve(fields.size());
            for (idx_t i = 0; i < fields.size(); i++) {
                auto ct = fields[i].type();
                ct.set_alias(struct_keys[i]);
                child_types.push_back(std::move(ct));
            }
            auto struct_type = complex_logical_type::create_struct("", child_types);
            return components::vector::make_struct(c.resource, struct_type, fields);
        }

        core::result_wrapper_t<vector_t> transform_dictionary_to_map(converter_t& c, const py_dictionary_t& dict) {
            auto keys = dict.values.attr("__getitem__")(0);
            auto values = dict.values.attr("__getitem__")(1);

            if (py::none().is(keys) || py::none().is(values)) {
                return components::vector::make_null(c.resource, logical_type::NA);
            }

            if (py::len(keys) == 0) {
                // dict == { 'key': [], 'value': [] }
                return empty_map(c.resource);
            }

            // dict == { 'key': [ ... ], 'value' : [ ... ] }
            VALUE_OR_RETURN(auto key_elements, transform_sequence(c, keys));
            VALUE_OR_RETURN(auto value_elements, transform_sequence(c, values));
            VALUE_OR_RETURN(auto key_type, unify_element_types(c, key_elements));
            VALUE_OR_RETURN(auto value_type, unify_element_types(c, value_elements));
            return components::vector::make_map(c.resource, key_type, value_type, key_elements, value_elements);
        }

        core::result_wrapper_t<vector_t> transform_dictionary(converter_t& c, const py_dictionary_t& dict) {
            //! DICT -> MAP FORMAT
            // keys() = [key, value]
            // values() = [ [n keys] ], [ [n values] ]

            //! DICT -> STRUCT FORMAT
            // keys() = ['a', .., 'n']
            // values() = [ val1, .., valn]
            if (dict.len == 0) {
                // dict == {}
                return empty_map(c.resource);
            }

            if (dictionary_has_map_format(dict)) {
                return transform_dictionary_to_map(c, dict);
            }
            return transform_dictionary_to_struct(c, dict);
        }

        core::result_wrapper_t<std::pmr::vector<vector_t>> transform_sequence(converter_t& c, py::handle ele) {
            auto type = get_python_object_type(ele);
            py::object listed;
            if (type == python_object_type_t::NdArray || type == python_object_type_t::NdDatetime) {
                listed = ele.attr("tolist")();
                ele = listed;
            }
            auto size = py::len(ele);
            std::pmr::vector<vector_t> elements(c.resource);
            elements.reserve(size);
            for (idx_t i = 0; i < size; i++) {
                VALUE_OR_RETURN(auto element, convert(c, ele.attr("__getitem__")(i)));
                elements.push_back(std::move(element));
            }
            return elements;
        }

        core::result_wrapper_t<vector_t> transform_list_value(converter_t& c, py::handle ele) {
            VALUE_OR_RETURN(auto elements, transform_sequence(c, ele));
            VALUE_OR_RETURN(auto element_type, unify_element_types(c, elements));
            return components::vector::make_list(c.resource, element_type, elements);
        }

        core::result_wrapper_t<vector_t> transform_python_integer(std::pmr::memory_resource* r, py::handle ele) {
            auto ptr = ele.ptr();
            const auto failure = [r]() {
                return make_error(r, "An error occurred attempting to convert a python integer");
            };

            int overflow;
            int64_t value = PyLong_AsLongLongAndOverflow(ptr, &overflow);
            if (overflow == -1) {
                PyErr_Clear();
                return failure();
            }
            if (overflow == 1) {
                uint64_t unsigned_value = PyLong_AsUnsignedLongLong(ptr);
                if (!PyErr_Occurred()) {
                    return components::vector::make_scalar(r, unsigned_value);
                }
                PyErr_Clear();
                double number = PyLong_AsDouble(ptr);
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfloat-equal"
                if (number == -1.0 && PyErr_Occurred())
#pragma GCC diagnostic pop
                {
                    PyErr_Clear();
                    return failure();
                }
                return components::vector::make_scalar(r, number);
            }
            if (value == -1 && PyErr_Occurred()) {
                return failure();
            }

            // To match default otterbrix behavior, numeric values without a specified type should not become a
            // smaller type than INT32
            if (value < static_cast<int64_t>(std::numeric_limits<int32_t>::min()) ||
                value > static_cast<int64_t>(std::numeric_limits<int32_t>::max())) {
                return components::vector::make_scalar(r, value);
            }
            return components::vector::make_scalar(r, static_cast<int32_t>(value));
        }

        core::result_wrapper_t<vector_t> convert(converter_t& c, py::handle ele) {
            auto* resource = c.resource;
            auto object_type = get_python_object_type(ele);

            switch (object_type) {
                case python_object_type_t::None:
                    return components::vector::make_null(resource, logical_type::NA);
                case python_object_type_t::Bool:
                    return components::vector::make_scalar(resource, ele.cast<bool>());
                case python_object_type_t::Integer:
                    return transform_python_integer(resource, ele);
                case python_object_type_t::Float:
                    if (std::isnan(PyFloat_AsDouble(ele.ptr()))) {
                        return components::vector::make_null(resource, logical_type::NA);
                    }
                    return components::vector::make_scalar(resource, ele.cast<double>());
                case python_object_type_t::Decimal: {
                    py_decimal_t decimal(ele);
                    return decimal.to_vector(resource);
                }
                case python_object_type_t::Uuid: {
                    return components::vector::make_scalar(resource, py::str(ele).cast<std::string>());
                }
                case python_object_type_t::String: {
                    return components::vector::make_scalar(resource, ele.cast<std::string>());
                }
                case python_object_type_t::ByteArray: {
                    return make_error(resource, "OtterBrix doens\'t support byte array conversation");
                }
                case python_object_type_t::MemoryView: {
                    py::memoryview py_view = ele.cast<py::memoryview>();
                    return make_error(resource, "OtterBrix doens\'t support memory view conversation");
                }
                case python_object_type_t::Bytes: {
                    return make_error(resource, "OtterBrix doens\'t support blob conversation");
                }
                case python_object_type_t::List:
                case python_object_type_t::Tuple:
                    return transform_list_value(c, ele);
                case python_object_type_t::Dict: {
                    py_dictionary_t dict = py_dictionary_t(py::reinterpret_borrow<py::object>(ele));
                    return transform_dictionary(c, dict);
                }
                case python_object_type_t::NdArray:
                case python_object_type_t::NdDatetime:
                    return convert(c, ele.attr("tolist")());
                case python_object_type_t::Other:
                    return make_error(resource,
                                      "No implementation: Unable to transform python value of type " +
                                          py::str(ele.get_type()).cast<std::string>() + " to OtterBrix logical_type");
                default:
                    return make_error(resource, "Object type recognized but not implemented!");
            }
        }
    } // namespace
    python_object_type_t get_python_object_type(py::handle& ele) {
        auto& import_cache = connection_environment_t::import_cache();

        if (ele.is_none()) {
            return python_object_type_t::None;
        } else if (ele.is(import_cache.pandas.na_t())) {
            return python_object_type_t::None;
        } else if (ele.is(import_cache.pandas.NA())) {
            return python_object_type_t::None;
        } else if (py::isinstance<py::bool_>(ele)) {
            return python_object_type_t::Bool;
        } else if (py::isinstance<py::int_>(ele)) {
            return python_object_type_t::Integer;
        } else if (py::isinstance<py::float_>(ele)) {
            return python_object_type_t::Float;
        } else if (py::isinstance(ele, import_cache.decimal.Decimal())) {
            return python_object_type_t::Decimal;
        } else if (py::isinstance(ele, import_cache.uuid.UUID())) {
            return python_object_type_t::Uuid;
        } else if (py::isinstance(ele, import_cache.datetime.datetime())) {
            return python_object_type_t::Datetime;
        } else if (py::isinstance(ele, import_cache.datetime.time())) {
            return python_object_type_t::Time;
        } else if (py::isinstance(ele, import_cache.datetime.date())) {
            return python_object_type_t::Date;
        } else if (py::isinstance(ele, import_cache.datetime.timedelta())) {
            return python_object_type_t::Timedelta;
        } else if (py::isinstance<py::str>(ele)) {
            return python_object_type_t::String;
        } else if (py::isinstance<py::bytearray>(ele)) {
            return python_object_type_t::ByteArray;
        } else if (py::isinstance<py::memoryview>(ele)) {
            return python_object_type_t::MemoryView;
        } else if (py::isinstance<py::bytes>(ele)) {
            return python_object_type_t::Bytes;
        } else if (py::isinstance<py::list>(ele)) {
            return python_object_type_t::List;
        } else if (py::isinstance<py::tuple>(ele)) {
            return python_object_type_t::Tuple;
        } else if (py::isinstance<py::dict>(ele)) {
            return python_object_type_t::Dict;
        } else if (ele.is(import_cache.numpy.ma.masked())) {
            return python_object_type_t::None;
        } else if (py::isinstance(ele, import_cache.numpy.ndarray())) {
            return python_object_type_t::NdArray;
        } else if (py::isinstance(ele, import_cache.numpy.datetime64())) {
            return python_object_type_t::NdDatetime;
        } else {
            return python_object_type_t::Other;
        }
    }

    core::result_wrapper_t<vector_t> transform_python_value(std::pmr::memory_resource* resource, py::handle ele) {
        converter_t c{resource, std::nullopt};
        return convert(c, ele);
    }
} // namespace otterbrix
