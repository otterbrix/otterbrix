#include <iostream>
#include "type_creation.hpp"

#include <core/types/unordered_map.hpp>

using namespace components::types;
namespace otterbrix {
    namespace TypeCreation {


        shared_ptr<OtterBrixPyType> MapType(const shared_ptr<OtterBrixPyType> &key_type,
                const shared_ptr<OtterBrixPyType> &value_type) {
        	auto map_type = complex_logical_type::create_map(key_type->Type(), value_type->Type());
        	return make_shared_ptr<OtterBrixPyType>(map_type);
        }

        shared_ptr<OtterBrixPyType> ListType(const shared_ptr<OtterBrixPyType> &type) {
        	auto array_type = complex_logical_type::create_list(type->Type());
        	return make_shared_ptr<OtterBrixPyType>(array_type);
        }

        shared_ptr<OtterBrixPyType> ArrayType(const shared_ptr<OtterBrixPyType> &type, idx_t size) {
        	auto array_type = complex_logical_type::create_array(type->Type(), size);
        	return make_shared_ptr<OtterBrixPyType>(array_type);
        }

        static vector<complex_logical_type> GetChildList(const py::object &container) {
        	vector<complex_logical_type> types;
        	if (py::isinstance<py::list>(container)) {
        		const py::list &fields = container;
        		idx_t i = 1;
        		for (auto &item : fields) {
        			shared_ptr<OtterBrixPyType> pytype;
        			if (!py::try_cast<shared_ptr<OtterBrixPyType>>(item, pytype)) {
        				string actual_type = py::str(item.get_type());
        				throw std::runtime_error("object has to be a list of OtterBrixPyType's, not " + actual_type);
        			}
        			types.push_back(pytype->Type());
                    types.back().set_alias("v" + to_string(i++));
        		}
        		return types;
        	} else if (py::isinstance<py::dict>(container)) {
        		const py::dict &fields = container;
        		for (auto &item : fields) {
        			auto &name_p = item.first;
        			auto &type_p = item.second;
        			string name = py::str(name_p);
        			shared_ptr<OtterBrixPyType> pytype;
        			if (!py::try_cast<shared_ptr<OtterBrixPyType>>(type_p, pytype)) {
        				string actual_type = py::str(type_p.get_type());
        				throw std::runtime_error("object has to be a list of OtterBrixPyType's, not " + actual_type);
        			}
        			types.push_back(pytype->Type());
                    types.back().set_alias(name);
        		}
        		return types;
        	} else {
        		string actual_type = py::str(container.get_type());
        		throw std::runtime_error(
        		    "Can not construct a child list from object of type " + actual_type + ", only dict/list is supported");
        	}
        }

        shared_ptr<OtterBrixPyType> StructType(const py::object &fields) {
        	auto types = GetChildList(fields);
        	if (types.empty()) {
        		throw std::runtime_error("Can not create an empty struct type!");
        	}
        	auto struct_type = complex_logical_type::create_struct(std::move(types));
        	return make_shared_ptr<OtterBrixPyType>(struct_type);
        }

        shared_ptr<OtterBrixPyType> UnionType(const py::object &members) {
        	/*auto types = GetChildList(members);

        	if (types.empty()) {
        		throw std::runtime_error("Can not create an empty union type!");
        	}
        	auto union_type = complex_logical_type::create_union(std::move(types));
        	return make_shared_ptr<OtterBrixPyType>(union_type);*/
        	throw std::runtime_error("union_type creation method is not implemented yet");
        }

        shared_ptr<OtterBrixPyType> EnumType(const string &name, const shared_ptr<OtterBrixPyType> &type,
                const py::list &values_p) {
        	throw std::runtime_error("enum_type creation method is not implemented yet");
        }

        shared_ptr<OtterBrixPyType> DecimalType(int width, int scale) {
        	auto decimal_type = complex_logical_type::create_decimal(width, scale);
        	return make_shared_ptr<OtterBrixPyType>(decimal_type);
        }

        shared_ptr<OtterBrixPyType> StringType(const string &collation) {
        	complex_logical_type type(logical_type::STRING_LITERAL);
        	/*if (collation.empty()) {
        		type = LogicalType::VARCHAR;
        	} else {
        		type = LogicalType::VARCHAR_COLLATION(collation);
        	}*/
        	return make_shared_ptr<OtterBrixPyType>(type);
        }

        shared_ptr<OtterBrixPyType> Type(const string &type_str) {

            unordered_map<string, logical_type> fromStrToType = {
                {"NULL", logical_type::NA},
                {"VARCHAR", logical_type::STRING_LITERAL},
                {"BIT", logical_type::BIT},
                {"UUID", logical_type::UUID},
                {"BLOB", logical_type::BLOB},
                {"BOOLEAN", logical_type::BOOLEAN},
                {"TIMESTAMP_S", logical_type::TIMESTAMP_SEC},
                {"TIMESTAMP_MS", logical_type::TIMESTAMP_MS},
                {"TIMESTAMP_NS", logical_type::TIMESTAMP_NS},
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
                return make_shared<OtterBrixPyType>(it->second);
            }
            throw std::runtime_error("Has no function to transform str " + type_str + " to OtterBrix type");
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
