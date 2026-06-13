#include <iostream>
#include <memory>
#include "type_creation.hpp"
#include <string>
#include <unordered_map>


using namespace components::types;
namespace otterbrix {
    namespace TypeCreation {


        std::shared_ptr<OtterBrixPyType> MapType(const std::shared_ptr<OtterBrixPyType> &key_type,
                const std::shared_ptr<OtterBrixPyType> &value_type) {
        	auto map_type = complex_logical_type::create_map(key_type->Type(), value_type->Type());
        	return std::make_shared<OtterBrixPyType>(map_type);
        }

        std::shared_ptr<OtterBrixPyType> ListType(const std::shared_ptr<OtterBrixPyType> &type) {
        	auto array_type = complex_logical_type::create_list(type->Type());
        	return std::make_shared<OtterBrixPyType>(array_type);
        }

        std::shared_ptr<OtterBrixPyType> ArrayType(const std::shared_ptr<OtterBrixPyType> &type, idx_t size) {
        	auto array_type = complex_logical_type::create_array(type->Type(), size);
        	return std::make_shared<OtterBrixPyType>(array_type);
        }

        static core::result_wrapper_t<std::pmr::vector<complex_logical_type>>
        GetChildList(const py::object &container, std::pmr::memory_resource *resource) {
        	std::pmr::vector<complex_logical_type> types(resource);
        	if (py::isinstance<py::list>(container)) {
        		const py::list &fields = container;
        		idx_t i = 1;
        		for (auto &item : fields) {
        			std::shared_ptr<OtterBrixPyType> pytype;
        			if (!py::try_cast<std::shared_ptr<OtterBrixPyType>>(item, pytype)) {
        				std::string actual_type = py::str(item.get_type());
        				return core::error_t(core::error_code_t::invalid_parameter,
        				                     std::pmr::string("object has to be a list of OtterBrixPyType's, not " + actual_type, resource));
        			}
        			types.push_back(pytype->Type());
                    types.back().set_alias("v" + std::to_string(i++));
        		}
        		return types;
        	} else if (py::isinstance<py::dict>(container)) {
        		const py::dict &fields = container;
        		for (auto &item : fields) {
        			auto &name_p = item.first;
        			auto &type_p = item.second;
        			std::string name = py::str(name_p);
        			std::shared_ptr<OtterBrixPyType> pytype;
        			if (!py::try_cast<std::shared_ptr<OtterBrixPyType>>(type_p, pytype)) {
        				std::string actual_type = py::str(type_p.get_type());
        				return core::error_t(core::error_code_t::invalid_parameter,
        				                     std::pmr::string("object has to be a list of OtterBrixPyType's, not " + actual_type, resource));
        			}
        			types.push_back(pytype->Type());
                    types.back().set_alias(name);
        		}
        		return types;
        	} else {
        		std::string actual_type = py::str(container.get_type());
        		return core::error_t(core::error_code_t::invalid_parameter,
        		                     std::pmr::string("Can not construct a child list from object of type " + actual_type + ", only dict/list is supported", resource));
        	}
        }

        std::shared_ptr<OtterBrixPyType> StructType(const py::object &fields) {
        	auto* resource = std::pmr::get_default_resource();
        	auto types = GetChildList(fields, resource);
        	if (types.has_error()) {
        		throw std::runtime_error(types.error().what.c_str());
        	}
        	if (types.value().empty()) {
        		throw std::runtime_error("Can not create an empty struct type!");
        	}
        	auto struct_type = complex_logical_type::create_struct("struct", std::move(types.value()));
        	return std::make_shared<OtterBrixPyType>(struct_type);
        }

        std::shared_ptr<OtterBrixPyType> UnionType(const py::object & /*members*/) {
        	/*auto types = GetChildList(members);

        	if (types.empty()) {
        		throw std::runtime_error("Can not create an empty union type!");
        	}
        	auto union_type = complex_logical_type::create_union(std::move(types));
        	return std::make_shared<OtterBrixPyType>(union_type);*/
        	throw std::runtime_error("union_type creation method is not implemented yet");
        }

        std::shared_ptr<OtterBrixPyType> EnumType(const std::string & /*name*/, const std::shared_ptr<OtterBrixPyType> & /*type*/,
                const py::list & /*values_p*/) {
        	throw std::runtime_error("enum_type creation method is not implemented yet");
        }

        std::shared_ptr<OtterBrixPyType> DecimalType(int width, int scale) {
        	auto decimal_type = complex_logical_type::create_decimal(static_cast<uint8_t>(width),
        	                                                         static_cast<uint8_t>(scale));
        	return std::make_shared<OtterBrixPyType>(decimal_type);
        }

        std::shared_ptr<OtterBrixPyType> StringType(const std::string & /*collation*/) {
        	complex_logical_type type(logical_type::STRING_LITERAL);
        	/*if (collation.empty()) {
        		type = LogicalType::VARCHAR;
        	} else {
        		type = LogicalType::VARCHAR_COLLATION(collation);
        	}*/
        	return std::make_shared<OtterBrixPyType>(type);
        }

        core::result_wrapper_t<logical_type> StringToLogicalType(const std::string &type_str,
                                                                 std::pmr::memory_resource *resource) {
            static const std::unordered_map<std::string, logical_type> fromStrToType = {
                {"NULL", logical_type::NA},
                {"VARCHAR", logical_type::STRING_LITERAL},
                {"BIT", logical_type::BIT},
                {"UUID", logical_type::UUID},
                {"BLOB", logical_type::BLOB},
                {"BOOLEAN", logical_type::BOOLEAN},
                {"TIMESTAMP", logical_type::TIMESTAMP},
                {"TIMESTAMP_S", logical_type::TIMESTAMP},
                {"TIMESTAMP_MS", logical_type::TIMESTAMP},
                {"TIMESTAMP_NS", logical_type::TIMESTAMP},
                {"DOUBLE", logical_type::DOUBLE},
                {"FLOAT", logical_type::FLOAT},
                {"TINYINT", logical_type::TINYINT},
                {"UTINYINT", logical_type::UTINYINT},
                {"SMALLINT", logical_type::SMALLINT},
                {"USMALLINT", logical_type::USMALLINT},
                {"INTEGER", logical_type::INTEGER},
                {"UINTEGER", logical_type::UINTEGER},
                {"BIGINT", logical_type::BIGINT},
                {"UBIGINT", logical_type::UBIGINT},
                {"HUGEINT", logical_type::HUGEINT},
                {"UHUGEINT", logical_type::UHUGEINT}
            };
            auto it = fromStrToType.find(type_str);
            if (it != fromStrToType.end()) {
                return it->second;
            }
            return core::error_t(core::error_code_t::conversion_failure,
                                 std::pmr::string("Has no function to transform str " + type_str + " to OtterBrix type", resource));
        }

        std::shared_ptr<OtterBrixPyType> Type(const std::string &type_str) {
            auto ltype = StringToLogicalType(type_str, std::pmr::get_default_resource());
            if (ltype.has_error()) {
                throw std::runtime_error(ltype.error().what.c_str());
            }
            return std::make_shared<OtterBrixPyType>(ltype.value());
        }

        void Initialize(py::module_ m) {
            m.def("sqltype", &Type, "Create a type object by parsing the 'type_str' string",
                  py::arg("type_str"));
            m.def("dtype", &Type, "Create a type object by parsing the 'type_str' string",
                  py::arg("type_str"));
            m.def("type", &Type, "Create a type object by parsing the 'type_str' string",
                  py::arg("type_str"));
            m.def("array_type", &ArrayType, "Create an array type object of 'type'",
                  py::arg("type").none(false), py::arg("size"));
            m.def("list_type", &ListType, "Create a list type object of 'type'",
                  py::arg("type").none(false));
            m.def("union_type", &UnionType, "Create a union type object from 'members'",
                  py::arg("members").none(false));
            m.def("string_type", &StringType, "Create a string type with an optional collation",
                  py::arg("collation") = "");
            m.def("enum_type", &EnumType,
                  "Create an enum type of underlying 'type', consisting of the list of 'values'", py::arg("name"),
                  py::arg("type"), py::arg("values"));
            m.def("decimal_type", &DecimalType, "Create a decimal type with 'width' and 'scale'",
                  py::arg("width"), py::arg("scale"));
            m.def("struct_type", &StructType, "Create a struct type object from 'fields'",
                  py::arg("fields"));
            m.def("row_type", &StructType, "Create a struct type object from 'fields'", py::arg("fields"));
            m.def("map_type", &MapType, "Create a map type object from 'key_type' and 'value_type'",
                  py::arg("key").none(false), py::arg("value").none(false));

        }


    } // namespace TypeCreation
} // namespace otterbrix
