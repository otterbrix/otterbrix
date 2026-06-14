#include "pytype.hpp"
#include <memory>
#include "type_creation.hpp"

#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>
#include <common/string_util/string_util.hpp>
#include <common/typedefs.hpp>
#include <connection_environment/connection_environment.hpp>
#include <string>

using namespace components::types;

namespace otterbrix {

    namespace {
        std::string logical_type_name(logical_type type_) {
            switch (type_) {
                case logical_type::NA:
                    return "NA";
                case logical_type::ANY:
                    return "ANY";
                case logical_type::USER:
                    return "USER";
                case logical_type::BOOLEAN:
                    return "BOOLEAN";
                case logical_type::TINYINT:
                    return "TINYINT";
                case logical_type::SMALLINT:
                    return "SMALLINT";
                case logical_type::INTEGER:
                    return "INTEGER";
                case logical_type::BIGINT:
                    return "BIGINT";
                case logical_type::HUGEINT:
                    return "HUGEINT";
                case logical_type::DATE:
                    return "DATE";
                case logical_type::TIME:
                    return "TIME";
                case logical_type::TIME_TZ:
                    return "TIME_TZ";
                case logical_type::TIMESTAMP:
                    return "TIMESTAMP";
                case logical_type::TIMESTAMP_TZ:
                    return "TIMESTAMP_TZ";
                case logical_type::DECIMAL:
                    return "DECIMAL";
                case logical_type::FLOAT:
                    return "FLOAT";
                case logical_type::DOUBLE:
                    return "DOUBLE";
                case logical_type::BLOB:
                    return "BLOB";
                case logical_type::INTERVAL:
                    return "INTERVAL";
                case logical_type::UTINYINT:
                    return "UTINYINT";
                case logical_type::USMALLINT:
                    return "USMALLINT";
                case logical_type::UINTEGER:
                    return "UINTEGER";
                case logical_type::UBIGINT:
                    return "UBIGINT";
                case logical_type::UHUGEINT:
                    return "UHUGEINT";
                case logical_type::BIT:
                    return "BIT";
                case logical_type::STRING_LITERAL:
                    return "STRING_LITERAL";
                case logical_type::INTEGER_LITERAL:
                    return "INTEGER_LITERAL";
                case logical_type::POINTER:
                    return "POINTER";
                case logical_type::VALIDITY:
                    return "VALIDITY";
                case logical_type::UUID:
                    return "UUID";
                case logical_type::STRUCT:
                    return "STRUCT";
                case logical_type::LIST:
                    return "LIST";
                case logical_type::MAP:
                    return "MAP";
                case logical_type::TABLE:
                    return "TABLE";
                case logical_type::ENUM:
                    return "ENUM";
                case logical_type::FUNCTION:
                    return "FUNCTION";
                case logical_type::LAMBDA:
                    return "LAMBDA";
                case logical_type::UNION:
                    return "UNION";
                case logical_type::VARIANT:
                    return "VARIANT";
                case logical_type::ARRAY:
                    return "ARRAY";
                case logical_type::UNKNOWN:
                    return "UNKNOWN";
                case logical_type::INVALID:
                    return "INVALID";
            }
            return "UNKNOWN";
        }
    } // namespace

    // NOLINTNEXTLINE(readability-identifier-naming)
    bool py_generic_alias_t::check_(const py::handle &object) {
    	if (!module_is_loaded<types_cache_item_t>()) {
    		return false;
    	}
    	auto &import_cache = connection_environment_t::import_cache();
    	return py::isinstance(object, import_cache.types.generic_alias());
    }
    
    // NOLINTNEXTLINE(readability-identifier-naming)
    bool py_union_type_t::check_(const py::handle &object) {
    	auto types_loaded = module_is_loaded<types_cache_item_t>();
    	auto typing_loaded = module_is_loaded<typing_cache_item_t>();
    
    	if (!types_loaded && !typing_loaded) {
    		return false;
    	}
    
    	auto &import_cache = connection_environment_t::import_cache();
    	if (types_loaded && py::isinstance(object, import_cache.types.union_type())) {
    		return true;
    	}
    	if (typing_loaded && py::isinstance(object, import_cache.typing._UnionGenericAlias())) {
    		return true;
    	}
    	return false;
    }
    
    otterbrix_py_type_t::otterbrix_py_type_t(complex_logical_type value) : type_(std::move(value)) {
    }
    
    bool otterbrix_py_type_t::equals(const std::shared_ptr<otterbrix_py_type_t> &other) const {
    	if (!other) {
    		return false;
    	}
    	return type_ == other->type_;
    }
    
    std::shared_ptr<otterbrix_py_type_t> otterbrix_py_type_t::get_attribute(const std::string &name) const {
    	if (type_.type() == logical_type::STRUCT || type_.type() == logical_type::UNION) {
            const auto& children = type_.child_types();
    		for (idx_t i = 0; i < children.size(); i++) {
    			const auto &child = children[i];
    			if (string_utils::ci_equals(child.alias(), name)) {
    				return std::make_shared<otterbrix_py_type_t>(child);
    			}
    		}
    	}
    	if (type_.type() == logical_type::LIST && string_utils::ci_equals(name, "child")) {
    		return std::make_shared<otterbrix_py_type_t>(type_.child_type());
    	}
    	if (type_.type() == logical_type::MAP) {
            auto* extension = static_cast<map_logical_type_extension*>(type_.extension());
    		auto is_key = string_utils::ci_equals(name, "key");
    		auto is_value = string_utils::ci_equals(name, "value");
    		if (is_key) {
    			return std::make_shared<otterbrix_py_type_t>(extension->key());
    		} else if (is_value) {
    			return std::make_shared<otterbrix_py_type_t>(extension->value());
    		} else {
    			throw py::attribute_error("Tried to get a child from a map by the name of " + name + 
                    ", but this type only has 'key' and 'value' children");
    		}
    	}
    	throw py::attribute_error("Tried to get child type by the name of " + name + 
            ", but this type either isn't nested, or it doesn't have a child by that name");
    }
    
    static core::result_wrapper_t<complex_logical_type> from_object(const py::object &object,
                                                                   std::pmr::memory_resource *resource);

    namespace {
    enum class python_type_object_t : uint8_t {
    	INVALID,   // not convertible to our type
    	BASE,      // 'builtin' type objects
    	UNION,     // typing.union_type
    	COMPOSITE, // list|dict types
    	STRUCT,    // dictionary
    	STRING,    // string value
    };
    }
    
    static python_type_object_t get_type_object_type(const py::handle &type_object) {
    	if (py::isinstance<py::type>(type_object)) {
    		return python_type_object_t::BASE;
    	}
    	if (py::isinstance<py::str>(type_object)) {
    		return python_type_object_t::STRING;
    	}
    	if (py::isinstance<py_generic_alias_t>(type_object)) {
    		return python_type_object_t::COMPOSITE;
    	}
    	if (py::isinstance<py::dict>(type_object)) {
    		return python_type_object_t::STRUCT;
    	}
    	if (py::isinstance<py_union_type_t>(type_object)) {
    		return python_type_object_t::UNION;
    	}
    	return python_type_object_t::INVALID;
    }
    
    static core::result_wrapper_t<complex_logical_type> from_string(const std::string &type_str,
                                                                   std::pmr::memory_resource *resource) {
        auto ltype = type_creation::string_to_logical_type(type_str, resource);
        if (ltype.has_error()) {
            return ltype.convert_error<complex_logical_type>();
        }
        return complex_logical_type(ltype.value());
    }
    
    static bool from_numpy_type(const py::object &type_, complex_logical_type &result) {
    	// Since this is a type, we have to create an instance from it first.
    	auto obj = type_();
    	// We convert these to string because the underlying physical
    	// types of a numpy type aren't consistent on every platform
    	if (!py::hasattr(obj, "dtype")) {
    		return false;
    	}
    	std::string type_str = py::str(obj.attr("dtype"));
    	if (type_str == "bool") {
    		result = logical_type::BOOLEAN;
    	} else if (type_str == "int8") {
    		result = logical_type::TINYINT;
    	} else if (type_str == "uint8") {
    		result = logical_type::UTINYINT;
    	} else if (type_str == "int16") {
    		result = logical_type::SMALLINT;
    	} else if (type_str == "uint16") {
    		result = logical_type::USMALLINT;
    	} else if (type_str == "int32") {
    		result = logical_type::INTEGER;
    	} else if (type_str == "uint32") {
    		result = logical_type::UINTEGER;
    	} else if (type_str == "int64") {
    		result = logical_type::BIGINT;
    	} else if (type_str == "uint64") {
    		result = logical_type::UBIGINT;
    	} else if (type_str == "float16") {
    		result = logical_type::FLOAT;
    	} else if (type_str == "float32") {
    		result = logical_type::FLOAT;
    	} else if (type_str == "float64") {
    		result = logical_type::DOUBLE;
    	} else {
    		return false;
    	}
    	return true;
    }
    
    static core::result_wrapper_t<complex_logical_type> from_type(const py::type &obj,
                                                                 std::pmr::memory_resource *resource) {
    	py::module_ builtins = py::module_::import("builtins");
    	if (obj.is(builtins.attr("str"))) {
    		return complex_logical_type(logical_type::STRING_LITERAL);
    	}
    	if (obj.is(builtins.attr("int"))) {
    		return complex_logical_type(logical_type::BIGINT);
    	}
    	if (obj.is(builtins.attr("bytearray"))) {
    		return complex_logical_type(logical_type::BLOB);
    	}
    	if (obj.is(builtins.attr("bytes"))) {
    		return complex_logical_type(logical_type::BLOB);
    	}
    	if (obj.is(builtins.attr("float"))) {
    		return complex_logical_type(logical_type::DOUBLE);
    	}
    	if (obj.is(builtins.attr("bool"))) {
    		return complex_logical_type(logical_type::BOOLEAN);
    	}
    
    	complex_logical_type result;
    	if (from_numpy_type(obj, result)) {
    		return result;
    	}

    	return core::error_t(core::error_code_t::conversion_failure,
    	                     std::pmr::string("Could not convert from unknown 'type' to OtterBrixPyType", resource));
    }
    
    static bool is_map_type(const py::tuple &args) {
    	if (args.size() != 2) {
    		return false;
    	}
    	for (auto &arg : args) {
    		if (get_type_object_type(arg) == python_type_object_t::INVALID) {
    			return false;
    		}
    	}
    	return true;
    }
    
    static py::tuple filter_nones(const py::tuple &args) {
    	py::list result;
    
    	for (const auto &arg : args) {
    		py::object object = py::reinterpret_borrow<py::object>(arg);
    		if (object.is(py::none().get_type())) {
    			continue;
    		}
    		result.append(object);
    	}
    	return py::tuple(result);
    }
    
    static core::result_wrapper_t<complex_logical_type> from_union_type_internal(const py::tuple &args,
                                                                              std::pmr::memory_resource *resource) {
    	idx_t index = 1;
    	std::pmr::vector<complex_logical_type> members(resource);

    	for (const auto &arg : args) {
    		auto name = "u" + std::to_string(index++);
    		py::object object = py::reinterpret_borrow<py::object>(arg);
    		auto member = from_object(object, resource);
    		if (member.has_error()) {
    			return member;
    		}
    		members.push_back(std::move(member.value()));
            members.back().set_alias(name);
    	}

    	return core::error_t(core::error_code_t::unimplemented_yet,
    	                     std::pmr::string("Could\'t transrom object to OtterBrix Union. "
                "Has no complex_logical_type::create_union", resource));
    }

    static core::result_wrapper_t<complex_logical_type> from_union_type(const py::object &obj,
                                                                      std::pmr::memory_resource *resource) {
    	py::tuple args = obj.attr("__args__");

    	// py_optional_t inserts NoneType into the Union
    	// all types are nullable so we just filter the Nones
    	auto filtered_args = filter_nones(args);
    	if (filtered_args.size() == 1) {
    		// If only a single type is left, dont construct a UNION
    		return from_object(filtered_args[0], resource);
    	}
    	return from_union_type_internal(filtered_args, resource);
    };

    static core::result_wrapper_t<complex_logical_type> from_generic_alias(const py::object &obj,
                                                                         std::pmr::memory_resource *resource) {
    	py::module_ builtins = py::module_::import("builtins");
    	py::module_ types = py::module_::import("types");
    	auto generic_alias = types.attr("GenericAlias");
    	assert(py::isinstance(obj, generic_alias));
    	auto origin = obj.attr("__origin__");
    	py::tuple args = obj.attr("__args__");

    	if (origin.is(builtins.attr("list"))) {
    		if (args.size() != 1) {
    			return core::error_t(core::error_code_t::invalid_parameter,
    			                     std::pmr::string("Can only create a LIST from a single type", resource));
    		}
    		auto child = from_object(args[0], resource);
    		if (child.has_error()) {
    			return child;
    		}
    		return complex_logical_type::create_list(child.value());
    	}
    	if (origin.is(builtins.attr("dict"))) {
    		if (is_map_type(args)) {
    			auto key = from_object(args[0], resource);
    			if (key.has_error()) {
    				return key;
    			}
    			auto value = from_object(args[1], resource);
    			if (value.has_error()) {
    				return value;
    			}
    			return complex_logical_type::create_map(key.value(), value.value());
    		} else {
    			return core::error_t(core::error_code_t::invalid_parameter,
    			                     std::pmr::string("Can only create a MAP from a dict if args is formed correctly", resource));
    		}
    	}
    	std::string origin_type = py::str(origin);
    	return core::error_t(core::error_code_t::conversion_failure,
    	                     std::pmr::string("Could not convert from " + origin_type + " to OtterBrixPyType", resource));
    }

    static core::result_wrapper_t<complex_logical_type> from_dictionary(const py::object &obj,
                                                                       std::pmr::memory_resource *resource) {
    	auto dict = py::reinterpret_steal<py::dict>(obj);
    	std::pmr::vector<complex_logical_type> children(resource);
    	if (dict.size() == 0) {
    		return core::error_t(core::error_code_t::invalid_parameter,
    		                     std::pmr::string("Could not convert empty dictionary to a STRUCT type", resource));
    	}
    	children.reserve(dict.size());
    	for (auto &item : dict) {
    		auto &name_p = item.first;
    		auto type_p = py::reinterpret_borrow<py::object>(item.second);
    		std::string name = py::str(name_p);
    		auto type_ = from_object(type_p, resource);
    		if (type_.has_error()) {
    			return type_;
    		}
    		children.push_back(std::move(type_.value()));
            children.back().set_alias(name);
    	}
    	return complex_logical_type::create_struct("struct", std::move(children));
    }

    static core::result_wrapper_t<complex_logical_type> from_object(const py::object &object,
                                                                   std::pmr::memory_resource *resource) {
    	auto object_type = get_type_object_type(object);
    	switch (object_type) {
    	case python_type_object_t::BASE: {
    		return from_type(object, resource);
    	}
    	case python_type_object_t::COMPOSITE: {
    		return from_generic_alias(object, resource);
    	}
    	case python_type_object_t::STRUCT: {
    		return from_dictionary(object, resource);
    	}
    	case python_type_object_t::UNION: {
    		return from_union_type(object, resource);
    	}
    	case python_type_object_t::STRING: {
    		auto string_value = std::string(py::str(object));
    		return from_string(string_value, resource);
    	}
    	default: {
    		std::string actual_type = py::str(object.get_type());
    		return core::error_t(core::error_code_t::conversion_failure,
    		                     std::pmr::string("Could not convert from object of type " + actual_type + " to OtterBrixPyType", resource));
    	}
    	}
    }
    
    void otterbrix_py_type_t::initialize(py::handle &m) {
    	auto type_module = py::class_<otterbrix_py_type_t, std::shared_ptr<otterbrix_py_type_t>>(m, "OtterBrixPyType", py::module_local());
    
    	type_module.def("__repr__", &otterbrix_py_type_t::to_string, "Stringified representation of the type object");
    	type_module.def("__eq__", &otterbrix_py_type_t::equals, "Compare two types for equality", py::arg("other"));
    	type_module.def_property_readonly("id", &otterbrix_py_type_t::get_id);
    	type_module.def_property_readonly("children", &otterbrix_py_type_t::children);
    	type_module.def(py::init<>([](const std::string &type_str) {
            auto ltype = from_string(type_str, std::pmr::get_default_resource());
            if (ltype.has_error()) {
                throw std::runtime_error(ltype.error().what.c_str());
            }
    		return std::make_shared<otterbrix_py_type_t>(ltype.value());
    	}));
    	type_module.def(py::init<>([](const py::object &obj) {
    		auto ltype = from_object(obj, std::pmr::get_default_resource());
            if (ltype.has_error()) {
                throw std::runtime_error(ltype.error().what.c_str());
            }
    		return std::make_shared<otterbrix_py_type_t>(ltype.value());
    	}));
    	type_module.def("__getattr__", &otterbrix_py_type_t::get_attribute, "Get the child type by 'name'", py::arg("name"));
    	type_module.def("__getitem__", &otterbrix_py_type_t::get_attribute, "Get the child type by 'name'", py::arg("name"));
    
    	py::implicitly_convertible<py::object, otterbrix_py_type_t>();
    	py::implicitly_convertible<py::str, otterbrix_py_type_t>();
    }
    
    std::string otterbrix_py_type_t::to_string() const {
        return logical_type_name(type_.type());
    }
    
    py::list otterbrix_py_type_t::children() const {
    
    	switch (type_.type()) {
    	case logical_type::LIST:
    	case logical_type::STRUCT:
    	case logical_type::UNION:
    	case logical_type::MAP:
    	case logical_type::ARRAY:
    	case logical_type::ENUM:
    	case logical_type::DECIMAL:
    		break;
    	default:
    		throw std::runtime_error("This type is not nested so it doesn't have children");
    	}
    
    	py::list children;
    	auto id = type_.type();
    	if (id == logical_type::LIST) {
    		children.append(py::make_tuple("child", std::make_shared<otterbrix_py_type_t>(type_.child_type())));
    		return children;
    	}
    	if (id == logical_type::ARRAY) {
    		children.append(py::make_tuple("child", std::make_shared<otterbrix_py_type_t>(type_.child_type())));
			auto* extension = static_cast<array_logical_type_extension*>(type_.extension());
    		children.append(py::make_tuple("size", extension->size()));
    		return children;
    	}
    	if (id == logical_type::ENUM) {
            throw std::runtime_error("OtterBrix doesn\'t implement OtterBrix Enum methods");
    	}
    	if (id == logical_type::STRUCT || id == logical_type::UNION) {
    		const auto &struct_children = type_.child_types();
    		for (idx_t i = 0; i < struct_children.size(); i++) {
    			auto &child = struct_children[i];
    			children.append(
    			    py::make_tuple(child.alias(), std::make_shared<otterbrix_py_type_t>(child)));
    		}
    		return children;
    	}
    	if (id == logical_type::MAP) {
            auto* extension = static_cast<map_logical_type_extension*>(type_.extension());
    		children.append(py::make_tuple("key", std::make_shared<otterbrix_py_type_t>(extension->key())));
    		children.append(py::make_tuple("value", std::make_shared<otterbrix_py_type_t>(extension->value())));
    		return children;
    	}
    	if (id == logical_type::DECIMAL) {
            auto* extension = static_cast<decimal_logical_type_extension*>(type_.extension());
    		children.append(py::make_tuple("precision", extension->width()));
    		children.append(py::make_tuple("scale", extension->scale()));
    		return children;
    	}
    	throw std::runtime_error("Children is not implemented for this type");
    }
    
    std::string otterbrix_py_type_t::get_id() const {
        if (type_.type() == logical_type::NA) {
            return "null";
        }
        if (type_.type() == logical_type::TIMESTAMP) {
            return "timestamp";
        }
		if (type_.type() == logical_type::STRING_LITERAL) {
            return "varchar";
        }
        auto name = logical_type_name(type_.type());
        return string_utils::lower(name);
    }
    
    const complex_logical_type &otterbrix_py_type_t::type() const {
    	return type_;
    }

} // namespace otterbrix
