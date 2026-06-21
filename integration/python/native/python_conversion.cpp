#include "python_conversion.hpp"

#include "python_objects.hpp"

#include <connection_environment/connection_environment.hpp>
#include <pybind11/pybind_wrapper.hpp>

#include <common/string_util/case_insensitive.hpp>
#include <common/typedefs.hpp>

#include <limits>
#include <memory_resource>
#include <string>
#include <vector>

using components::types::complex_logical_type;
using components::types::logical_type;
using components::types::logical_value_t;
using components::types::physical_type;

namespace otterbrix {
    static core::error_t make_error(std::pmr::memory_resource* r, const std::string& what) {
        return core::error_t{core::error_code_t::other_error, std::pmr::string{what, r}};
    }

    // Returns an error when the two element types cannot be promoted to a common type.
    static core::error_t promote_element_type(complex_logical_type& acc,
                                              const complex_logical_type& observed,
                                              std::pmr::memory_resource* r) {
        auto observed_t = observed.type();
        if (observed_t == logical_type::NA) {
            return core::error_t::no_error();
        }
        auto acc_t = acc.type();
        if (acc_t == logical_type::NA) {
            acc = observed;
            return core::error_t::no_error();
        }
        if (acc_t == observed_t) {
            return core::error_t::no_error();
        }
        auto promoted = components::types::promote_type(acc_t, observed_t);
        if (promoted == logical_type::NA) {
            return make_error(r,
                              "Cannot infer common type for elements: incompatible logical_types " +
                                  std::to_string(static_cast<int>(acc_t)) + " and " +
                                  std::to_string(static_cast<int>(observed_t)));
        }
        acc = complex_logical_type(promoted);
        return core::error_t::no_error();
    }

    static logical_value_t empty_map_value(std::pmr::memory_resource* r) {
        return logical_value_t::create_map(r,
                                           logical_type::NA,
                                           logical_type::NA,
                                           std::vector<logical_value_t>(),
                                           std::vector<logical_value_t>());
    }

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

    core::result_wrapper_t<logical_value_t>
    transform_dictionary_to_struct(std::pmr::memory_resource* r,
                                   const py_dictionary_t& dict,
                                   const complex_logical_type& target_type = logical_type::UNKNOWN) {
        auto struct_keys = transform_struct_keys(dict.keys, dict.len);

        bool struct_target = target_type.type() == logical_type::STRUCT;
        components::types::struct_logical_type_extension* struct_extension = nullptr;
        if (struct_target) {
            struct_extension = static_cast<components::types::struct_logical_type_extension*>(target_type.extension());
            if (dict.len != struct_extension->child_types().size()) {
                return make_error(r,
                                  "We could not convert the object " + dict.to_string() +
                                      " to the target struct type with " +
                                      std::to_string(struct_extension->child_types().size()) + " fields");
            }
        }

        case_insensitive_map_t<idx_t> key_mapping;
        for (idx_t i = 0; i < struct_keys.size(); i++) {
            key_mapping[struct_keys[i]] = i;
        }

        std::vector<logical_value_t> struct_values;
        for (idx_t i = 0; i < dict.len; i++) {
            if (struct_target) {
                auto& key = struct_extension->child_types().at(i).alias();
                auto value_index = key_mapping[key];
                auto& child_type = target_type.child_types().at(i);
                auto val = transform_python_value(r, dict.values.attr("__getitem__")(value_index), child_type);
                if (val.has_error()) {
                    return val.error();
                }
                val.value().set_alias(key);
                struct_values.emplace_back(std::move(val.value()));
            } else {
                auto& key = struct_keys[i];
                auto value_index = key_mapping[key];
                auto child_type = logical_type::UNKNOWN;
                auto val = transform_python_value(r, dict.values.attr("__getitem__")(value_index), child_type);
                if (val.has_error()) {
                    return val.error();
                }
                val.value().set_alias(key);
                struct_values.emplace_back(std::move(val.value()));
            }
        }
        if (struct_target) {
            return logical_value_t::create_struct(r, target_type, struct_values);
        }
        std::pmr::vector<complex_logical_type> child_types(r);
        child_types.reserve(struct_values.size());
        for (idx_t i = 0; i < struct_values.size(); i++) {
            auto ct = struct_values[i].type();
            ct.set_alias(struct_keys[i]);
            child_types.push_back(std::move(ct));
        }
        auto struct_type = complex_logical_type::create_struct("", child_types);
        return logical_value_t::create_struct(r, struct_type, struct_values);
    }

    core::result_wrapper_t<logical_value_t>
    transform_struct_format_dictionary_to_map(std::pmr::memory_resource* r,
                                              const py_dictionary_t& dict,
                                              const complex_logical_type& target_type) {
        if (dict.len == 0) {
            return empty_map_value(r);
        }

        if (target_type.type() != logical_type::MAP) {
            return make_error(r, "Please provide a valid target type for transform from Python to Value");
        }

        if (py::none().is(dict.keys) || py::none().is(dict.values)) {
            return logical_value_t(r, logical_type::NA);
        }

        auto size = py::len(dict.keys);
        assert(size == py::len(dict.values));

        auto* map_extension = static_cast<components::types::map_logical_type_extension*>(target_type.extension());
        auto key_target = map_extension->key();
        auto value_target = map_extension->value();

        complex_logical_type key_type = logical_type::NA;
        complex_logical_type value_type = logical_type::NA;

        std::vector<logical_value_t> key_values;
        std::vector<logical_value_t> value_values;

        for (idx_t i = 0; i < size; i++) {
            auto new_key = transform_python_value(r, dict.keys.attr("__getitem__")(i), key_target);
            if (new_key.has_error()) {
                return new_key.error();
            }
            auto new_value = transform_python_value(r, dict.values.attr("__getitem__")(i), value_target);
            if (new_value.has_error()) {
                return new_value.error();
            }

            if (auto err = promote_element_type(key_type, new_key.value().type(), r); err.contains_error()) {
                return err;
            }
            if (auto err = promote_element_type(value_type, new_value.value().type(), r); err.contains_error()) {
                return err;
            }

            new_key.value().set_alias("key");
            new_value.value().set_alias("value");
            key_values.emplace_back(new_key.value());
            value_values.emplace_back(new_value.value());
        }

        if (key_type.type() == logical_type::NA) {
            key_type = key_target;
        }
        if (value_type.type() == logical_type::NA) {
            value_type = value_target;
        }

        return logical_value_t::create_map(r, key_type, value_type, key_values, value_values);
    }

    core::result_wrapper_t<logical_value_t>
    transform_dictionary_to_map(std::pmr::memory_resource* r,
                                const py_dictionary_t& dict,
                                const complex_logical_type& target_type = logical_type::UNKNOWN) {
        if (target_type.type() != logical_type::UNKNOWN && !dictionary_has_map_format(dict)) {
            // dict == { 'k1': v1, 'k2': v2, ..., 'kn': vn }
            return transform_struct_format_dictionary_to_map(r, dict, target_type);
        }

        auto keys = dict.values.attr("__getitem__")(0);
        auto values = dict.values.attr("__getitem__")(1);

        if (py::none().is(keys) || py::none().is(values)) {
            return logical_value_t(r, logical_type::NA);
        }

        auto key_size = py::len(keys);
        assert(key_size == py::len(values));
        if (key_size == 0) {
            // dict == { 'key': [], 'value': [] }
            return empty_map_value(r);
        }

        // dict == { 'key': [ ... ], 'value' : [ ... ] }
        complex_logical_type key_target = logical_type::UNKNOWN;
        complex_logical_type value_target = logical_type::UNKNOWN;

        if (target_type.type() != logical_type::UNKNOWN) {
            auto* map_extension = static_cast<components::types::map_logical_type_extension*>(target_type.extension());
            key_target = complex_logical_type::create_list(map_extension->key());
            value_target = complex_logical_type::create_list(map_extension->value());
        }

        auto key_list = transform_python_value(r, keys, key_target);
        if (key_list.has_error()) {
            return key_list.error();
        }
        auto value_list = transform_python_value(r, values, value_target);
        if (value_list.has_error()) {
            return value_list.error();
        }

        complex_logical_type key_type = logical_type::NA;
        complex_logical_type value_type = logical_type::NA;

        const auto& key_children = key_list.value().children();
        const auto& value_children = value_list.value().children();

        std::vector<logical_value_t> key_values;
        std::vector<logical_value_t> value_values;

        for (idx_t i = 0; i < key_size; i++) {
            logical_value_t new_key = key_children[i];
            logical_value_t new_value = value_children[i];

            if (auto err = promote_element_type(key_type, new_key.type(), r); err.contains_error()) {
                return err;
            }
            if (auto err = promote_element_type(value_type, new_value.type(), r); err.contains_error()) {
                return err;
            }

            new_key.set_alias("key");
            new_value.set_alias("value");
            key_values.emplace_back(new_key);
            value_values.emplace_back(new_value);
        }

        return logical_value_t::create_map(r, key_type, value_type, key_values, value_values);
    }

    core::result_wrapper_t<logical_value_t>
    transform_tuple_to_struct(std::pmr::memory_resource* r,
                              py::handle ele,
                              const complex_logical_type& target_type = logical_type::UNKNOWN) {
        auto tuple = py::cast<py::tuple>(ele);
        auto size = py::len(tuple);

        assert(target_type.type() == logical_type::STRUCT);
        auto child_types = target_type.child_types();
        auto child_count = child_types.size();
        if (size != child_count) {
            return make_error(r,
                              "Tried to create a STRUCT value from a tuple containing " + std::to_string(size) +
                                  " elements, but the " + "STRUCT consists of " + std::to_string(child_count) +
                                  " children");
        }
        std::vector<logical_value_t> children;
        for (idx_t i = 0; i < child_count; i++) {
            auto& type = child_types[i];
            const auto& name = target_type.child_types()[i].alias();
            auto element = py::handle(tuple[i]);
            auto converted_value = transform_python_value(r, element, type);
            if (converted_value.has_error()) {
                return converted_value.error();
            }
            converted_value.value().set_alias(name);
            children.emplace_back(std::move(converted_value.value()));
        }
        return logical_value_t::create_struct(r, target_type, children);
    }

    core::result_wrapper_t<logical_value_t>
    transform_list_value(std::pmr::memory_resource* r,
                         py::handle ele,
                         const complex_logical_type& target_type = logical_type::UNKNOWN) {
        auto size = py::len(ele);

        std::vector<logical_value_t> values;
        values.reserve(size);

        bool list_target = target_type.type() == logical_type::LIST;
        const auto& child_type = list_target ? target_type.child_type() : logical_type::UNKNOWN;

        complex_logical_type element_type = list_target ? child_type : complex_logical_type(logical_type::NA);
        for (idx_t i = 0; i < size; i++) {
            auto new_value = transform_python_value(r, ele.attr("__getitem__")(i), child_type);
            if (new_value.has_error()) {
                return new_value.error();
            }
            if (!list_target) {
                if (auto err = promote_element_type(element_type, new_value.value().type(), r); err.contains_error()) {
                    return err;
                }
            }
            values.push_back(std::move(new_value.value()));
        }
        return logical_value_t::create_list(r, element_type, values);
    }

    core::result_wrapper_t<logical_value_t>
    transform_array_value(std::pmr::memory_resource* r,
                          py::handle ele,
                          const complex_logical_type& target_type = logical_type::UNKNOWN) {
        auto size = py::len(ele);

        std::vector<logical_value_t> values;
        values.reserve(size);

        bool array_target = target_type.type() == logical_type::ARRAY;
        const auto& child_type = array_target ? target_type.child_type() : logical_type::UNKNOWN;

        complex_logical_type element_type = array_target ? child_type : complex_logical_type(logical_type::NA);
        for (idx_t i = 0; i < size; i++) {
            auto new_value = transform_python_value(r, ele.attr("__getitem__")(i), child_type);
            if (new_value.has_error()) {
                return new_value.error();
            }
            if (!array_target) {
                if (auto err = promote_element_type(element_type, new_value.value().type(), r); err.contains_error()) {
                    return err;
                }
            }
            values.push_back(std::move(new_value.value()));
        }

        return logical_value_t::create_array(r, element_type, values);
    }

    core::result_wrapper_t<logical_value_t> transform_dictionary(std::pmr::memory_resource* r,
                                                                 const py_dictionary_t& dict) {
        //! DICT -> MAP FORMAT
        // keys() = [key, value]
        // values() = [ [n keys] ], [ [n values] ]

        //! DICT -> STRUCT FORMAT
        // keys() = ['a', .., 'n']
        // values() = [ val1, .., valn]
        if (dict.len == 0) {
            // dict == {}
            return empty_map_value(r);
        }

        if (dictionary_has_map_format(dict)) {
            return transform_dictionary_to_map(r, dict);
        }
        return transform_dictionary_to_struct(r, dict);
    }

    bool try_transform_python_integer_to_double(logical_value_t& res, py::handle ele) {
        double number = PyLong_AsDouble(ele.ptr());
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wfloat-equal"
        if (number == -1.0 && PyErr_Occurred())
#pragma GCC diagnostic pop
        {
            PyErr_Clear();
            return false;
        }
        res = logical_value_t(res.resource(), number);
        return true;
    }

    void transform_python_unsigned(uint64_t value, logical_value_t& res) {
        auto* r = res.resource();
        if (value > static_cast<uint64_t>(std::numeric_limits<uint32_t>::max())) {
            res = logical_value_t(r, value);
        } else if (value > static_cast<uint64_t>(std::numeric_limits<uint16_t>::max())) {
            res = logical_value_t(r, static_cast<uint32_t>(value));
        } else if (value > static_cast<int64_t>(std::numeric_limits<uint16_t>::max())) {
            res = logical_value_t(r, static_cast<uint16_t>(value));
        } else {
            res = logical_value_t(r, static_cast<uint8_t>(value));
        }
    }

    bool try_sniff_python_numeric(logical_value_t& res, int64_t value) {
        auto* r = res.resource();
        if (value < static_cast<int64_t>(std::numeric_limits<int32_t>::min()) ||
            value > static_cast<int64_t>(std::numeric_limits<int32_t>::max())) {
            res = logical_value_t(r, value);
        } else {
            // To match default otterbrix behavior, numeric values without a specified type should not become a smaller type
            // than INT32
            res = logical_value_t(r, static_cast<int32_t>(value));
        }
        return true;
    }

    bool try_transform_python_numeric(logical_value_t& res, py::handle ele, const complex_logical_type& target_type) {
        // Hot per-cell path: write results into the caller-provided value's resource (R14-local),
        // and report hard failures as a false return rather than throwing (R2 hot-path carve-out).
        auto* r = res.resource();
        auto ptr = ele.ptr();

        int overflow;
        int64_t value = PyLong_AsLongLongAndOverflow(ptr, &overflow);
        if (overflow == -1) {
            PyErr_Clear();
            if (target_type.type() == logical_type::BIGINT) {
                // Failed to cast value: Python value <numeric_string> to INT64
                return false;
            }

            switch (target_type.type()) {
                case logical_type::TINYINT:
                    res = logical_value_t(r, int8_t(value));
                    return true;
                case logical_type::UTINYINT:
                    res = logical_value_t(r, uint8_t(value));
                    return true;
                case logical_type::SMALLINT:
                    res = logical_value_t(r, int16_t(value));
                    return true;
                case logical_type::USMALLINT:
                    res = logical_value_t(r, uint16_t(value));
                    return true;
                case logical_type::INTEGER:
                    res = logical_value_t(r, int32_t(value));
                    return true;
                case logical_type::UINTEGER:
                    res = logical_value_t(r, uint32_t(value));
                    return true;
                case logical_type::BIGINT:
                    res = logical_value_t(r, int64_t(value));
                    return true;
                case logical_type::UBIGINT:
                    res = logical_value_t(r, uint64_t(value));
                    return true;
                case logical_type::UHUGEINT:
                case logical_type::HUGEINT:
                default:
                    // OtterBrix has no a logical_value_t constructor for hugeint/uhugeint
                    return false;
            }
        } else if (overflow == 1) {
            if (target_type.to_physical_type() == physical_type::INT64) {
                // Failed to cast value: Python value <numeric_string> to INT64
                return false;
            }
            uint64_t unsigned_value = PyLong_AsUnsignedLongLong(ptr);
            if (PyErr_Occurred()) {
                PyErr_Clear();
                return try_transform_python_integer_to_double(res, ele);
            } else {
                transform_python_unsigned(unsigned_value, res);
            }
            PyErr_Clear();
            return true;
        } else if (value == -1 && PyErr_Occurred()) {
            return false;
        }

        // The value is int64_t or smaller

        switch (target_type.type()) {
            case logical_type::UNKNOWN:
                return try_sniff_python_numeric(res, value);
            case logical_type::HUGEINT: {
                // OtterBrix has no a logical_value_t constructor for hugeint/uhugeint
                return false;
            }
            case logical_type::UHUGEINT: {
                if (value < 0) {
                    return false;
                }
                // OtterBrix has no a logical_value_t constructor for hugeint/uhugeint
                return false;
            }
            case logical_type::BIGINT: {
                res = logical_value_t(r, int64_t(value));
                return true;
            }
            case logical_type::INTEGER: {
                if (value < std::numeric_limits<int32_t>::min() || value > std::numeric_limits<int32_t>::max()) {
                    return false;
                }
                res = logical_value_t(r, int32_t(value));
                return true;
            }
            case logical_type::SMALLINT: {
                if (value < std::numeric_limits<int16_t>::min() || value > std::numeric_limits<int16_t>::max()) {
                    return false;
                }
                res = logical_value_t(r, int16_t(value));
                return true;
            }
            case logical_type::TINYINT: {
                if (value < std::numeric_limits<int8_t>::min() || value > std::numeric_limits<int8_t>::max()) {
                    return false;
                }
                res = logical_value_t(r, int8_t(value));
                return true;
            }
            case logical_type::UBIGINT: {
                if (value < 0) {
                    return false;
                }
                res = logical_value_t(r, uint64_t(value));
                return true;
            }
            case logical_type::UINTEGER: {
                if (value < 0 || value > static_cast<int64_t>(std::numeric_limits<uint32_t>::max())) {
                    return false;
                }
                res = logical_value_t(r, uint32_t(value));
                return true;
            }
            case logical_type::USMALLINT: {
                if (value < 0 || value > static_cast<int64_t>(std::numeric_limits<uint16_t>::max())) {
                    return false;
                }
                res = logical_value_t(r, uint16_t(value));
                return true;
            }
            case logical_type::UTINYINT: {
                if (value < 0 || value > static_cast<int64_t>(std::numeric_limits<uint8_t>::max())) {
                    return false;
                }
                res = logical_value_t(r, uint8_t(value));
                return true;
            }
            default: {
                return try_sniff_python_numeric(res, value);
            }
        }
    }

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

    core::result_wrapper_t<logical_value_t> transform_python_value(std::pmr::memory_resource* resource,
                                                                   py::handle ele,
                                                                   const complex_logical_type& target_type,
                                                                   bool nan_as_null) {
        auto object_type = get_python_object_type(ele);

        switch (object_type) {
            case python_object_type_t::None:
                return logical_value_t(resource, logical_type::NA);
            case python_object_type_t::Bool:
                return logical_value_t(resource, ele.cast<bool>());
            case python_object_type_t::Integer: {
                logical_value_t integer(resource, logical_type::UNKNOWN);
                if (!try_transform_python_numeric(integer, ele, target_type)) {
                    return make_error(resource, "An error occurred attempting to convert a python integer");
                }
                return integer;
            }
            case python_object_type_t::Float:
                if (nan_as_null && std::isnan(PyFloat_AsDouble(ele.ptr()))) {
                    return logical_value_t(resource, logical_type::NA);
                }
                switch (target_type.type()) {
                    case logical_type::UNKNOWN:
                    case logical_type::DOUBLE: {
                        return logical_value_t(resource, ele.cast<double>());
                    }
                    case logical_type::FLOAT: {
                        return logical_value_t(resource, ele.cast<float>());
                    }
                    case logical_type::DECIMAL: {
                        return make_error(resource, "Can't losslessly convert from object of float to type ");
                    }
                    default:
                        return make_error(resource, "Could not convert 'float' to type ");
                }
            case python_object_type_t::Decimal: {
                py_decimal_t decimal(ele);
                return decimal.to_logical_value(resource);
            }
            case python_object_type_t::Uuid: {
                return logical_value_t(resource, py::str(ele).cast<std::string>());
            }
            case python_object_type_t::String: {
                auto stringified = ele.cast<std::string>();
                if (target_type.type() == logical_type::UNKNOWN || target_type.type() == logical_type::STRING_LITERAL) {
                    return logical_value_t(resource, stringified);
                }
                return logical_value_t(resource, stringified).cast_as(target_type, core::date::timezone_offset_t{});
            }
            case python_object_type_t::ByteArray: {
                return make_error(resource, "OtterBrix doens\'t support byte array conversation");
            }
            case python_object_type_t::MemoryView: {
                py::memoryview py_view = ele.cast<py::memoryview>();
                return make_error(resource, "OtterBrix doens\'t support memory view conversation");
            }
            case python_object_type_t::Bytes: {
                const std::string& ele_string = ele.cast<std::string>();
                switch (target_type.type()) {
                    case logical_type::UNKNOWN:
                    case logical_type::BLOB:
                        return make_error(resource, "OtterBrix doens\'t support blob conversation");
                    case logical_type::BIT:
                        return make_error(resource, "OtterBrix doens\'t support blob conversation");
                    default:
                        return make_error(resource, "Could not convert 'bytes' to type");
                }
            }
            case python_object_type_t::List:
                if (target_type.type() == logical_type::ARRAY) {
                    return transform_array_value(resource, ele, target_type);
                } else {
                    return transform_list_value(resource, ele, target_type);
                }
            case python_object_type_t::Dict: {
                py_dictionary_t dict = py_dictionary_t(py::reinterpret_borrow<py::object>(ele));
                switch (target_type.type()) {
                    case logical_type::STRUCT:
                        return transform_dictionary_to_struct(resource, dict, target_type);
                    case logical_type::MAP:
                        return transform_dictionary_to_map(resource, dict, target_type);
                    default:
                        return transform_dictionary(resource, dict);
                }
            }
            case python_object_type_t::Tuple: {
                switch (target_type.type()) {
                    case logical_type::STRUCT:
                        return transform_tuple_to_struct(resource, ele, target_type);
                    case logical_type::UNKNOWN:
                    case logical_type::LIST:
                        return transform_list_value(resource, ele, target_type);
                    case logical_type::ARRAY:
                        return transform_array_value(resource, ele, target_type);
                    default:
                        return make_error(resource, "Can't convert tuple to a Value of type ");
                }
            }
            case python_object_type_t::NdArray:
            case python_object_type_t::NdDatetime:
                return transform_python_value(resource, ele.attr("tolist")(), target_type, nan_as_null);
            case python_object_type_t::Other:
                return make_error(resource,
                                  "No implementation: Unable to transform python value of type " +
                                      py::str(ele.get_type()).cast<std::string>() + " to OtterBrix logical_type");
            default:
                return make_error(resource, "Object type recognized but not implemented!");
        }
    }
} // namespace otterbrix
