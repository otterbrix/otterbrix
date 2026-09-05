#include "type_creation.hpp"
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>

using namespace components::types;
namespace otterbrix { namespace type_creation {

    // THE PYBIND TYPE SURFACE NOW HAS AN ARENA, AND IT ARRIVES AS AN ARGUMENT.
    //
    // Every factory here is a module-level pybind entry point: no connection, no space, no
    // caller object is in scope, and the complex_logical_type it builds is handed to the
    // interpreter, which may hold it for the rest of the process. That used to be the excuse
    // for a file-local module_arena() returning std::pmr::get_default_resource() -- the one
    // thing rule 14 forbids -- with a note saying the exit was an arena OWNED BY THE MODULE.
    //
    // This is that exit. initialize() takes the module's arena (created in main.cpp's
    // PYBIND11_MODULE body) and binds it into each entry point, so the arena is named by its
    // owner and the process default is never asked. It is a COUNTED reference, not a raw
    // memory_resource*: every factory below hands the same reference to the object it returns
    // (otterbrix_py_type_t::arena_), so a STRUCT or MAP -- whose child list is a pmr vector on
    // THIS arena for as long as the python object lives -- keeps the arena alive by itself,
    // and the module is free to be torn down first.
    //
    // EVERY factory takes the arena, including the ones that allocate nothing themselves
    // (list / array / string): the object they return has to carry the reference, so there is
    // no such thing here as a factory that does not need the arena.

    std::shared_ptr<otterbrix_py_type_t> map_type(const module_arena_ptr& arena,
                                                  const std::shared_ptr<otterbrix_py_type_t>& key_type,
                                                  const std::shared_ptr<otterbrix_py_type_t>& value_type) {
        // The MAP's entries struct is built on the arena and kept by the extension: this is
        // one of the two factories whose arena outlives the call, not just the error path.
        auto map_type = complex_logical_type::create_map(&arena->resource, key_type->type(), value_type->type());
        return std::make_shared<otterbrix_py_type_t>(arena, map_type);
    }

    std::shared_ptr<otterbrix_py_type_t> list_type(const module_arena_ptr& arena,
                                                   const std::shared_ptr<otterbrix_py_type_t>& type) {
        auto array_type = complex_logical_type::create_list(type->type());
        return std::make_shared<otterbrix_py_type_t>(arena, array_type);
    }

    std::shared_ptr<otterbrix_py_type_t>
    array_type(const module_arena_ptr& arena, const std::shared_ptr<otterbrix_py_type_t>& type, idx_t size) {
        auto array_type = complex_logical_type::create_array(type->type(), size);
        return std::make_shared<otterbrix_py_type_t>(arena, array_type);
    }

    static core::result_wrapper_t<std::pmr::vector<complex_logical_type>>
    get_child_list(const py::object& container, std::pmr::memory_resource* resource) {
        std::pmr::vector<complex_logical_type> types(resource);
        if (py::isinstance<py::list>(container)) {
            const py::list& fields = container;
            idx_t i = 1;
            for (auto& item : fields) {
                std::shared_ptr<otterbrix_py_type_t> pytype;
                if (!py::try_cast<std::shared_ptr<otterbrix_py_type_t>>(item, pytype)) {
                    std::string actual_type = py::str(item.get_type());
                    return core::error_t(
                        core::error_code_t::invalid_parameter,
                        std::pmr::string("object has to be a list of OtterBrixPyType's, not " + actual_type, resource));
                }
                types.push_back(pytype->type());
                types.back().set_alias("v" + std::to_string(i++));
            }
            return types;
        } else if (py::isinstance<py::dict>(container)) {
            const py::dict& fields = container;
            for (auto& item : fields) {
                auto& name_p = item.first;
                auto& type_p = item.second;
                std::string name = py::str(name_p);
                std::shared_ptr<otterbrix_py_type_t> pytype;
                if (!py::try_cast<std::shared_ptr<otterbrix_py_type_t>>(type_p, pytype)) {
                    std::string actual_type = py::str(type_p.get_type());
                    return core::error_t(
                        core::error_code_t::invalid_parameter,
                        std::pmr::string("object has to be a list of OtterBrixPyType's, not " + actual_type, resource));
                }
                types.push_back(pytype->type());
                types.back().set_alias(name);
            }
            return types;
        } else {
            std::string actual_type = py::str(container.get_type());
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string("Can not construct a child list from object of type " + actual_type +
                                                      ", only dict/list is supported",
                                                  resource));
        }
    }

    std::shared_ptr<otterbrix_py_type_t> struct_type(const module_arena_ptr& arena, const py::object& fields) {
        auto types = get_child_list(fields, &arena->resource);
        if (types.has_error()) {
            throw std::runtime_error(types.error().what.c_str());
        }
        if (types.value().empty()) {
            throw std::runtime_error("Can not create an empty struct type!");
        }
        auto struct_type = complex_logical_type::create_struct("struct", std::move(types.value()));
        return std::make_shared<otterbrix_py_type_t>(arena, struct_type);
    }

    std::shared_ptr<otterbrix_py_type_t> union_type(const module_arena_ptr& /*arena*/, const py::object& /*members*/) {
        /*auto types = get_child_list(members);

        	if (types.empty()) {
        		throw std::runtime_error("Can not create an empty union type!");
        	}
        	auto union_type = complex_logical_type::create_union(std::move(types));
        	return std::make_shared<otterbrix_py_type_t>(union_type);*/
        throw std::runtime_error("union_type creation method is not implemented yet");
    }

    std::shared_ptr<otterbrix_py_type_t> enum_type(const module_arena_ptr& /*arena*/,
                                                   const std::string& /*name*/,
                                                   const std::shared_ptr<otterbrix_py_type_t>& /*type*/,
                                                   const py::list& /*values_p*/) {
        throw std::runtime_error("enum_type creation method is not implemented yet");
    }

    std::shared_ptr<otterbrix_py_type_t> decimal_type(const module_arena_ptr& arena, int width, int scale) {
        // Range-check before narrowing, then let create_decimal own the window: an
        // out-of-window DECIMAL built here would be a python-side type the engine can
        // write and never read back.
        if (width < 0 || scale < 0 || width > components::types::DECIMAL_MAX_WIDTH ||
            scale > components::types::DECIMAL_MAX_WIDTH) {
            throw std::runtime_error("decimal_type: width and scale are out of range");
        }
        // Not a literal pair: width/scale come from Python, so scale > width reaches the
        // refusal and its message is what the exception below carries. It therefore needs a
        // real arena, and the module's arena is the one this boundary is handed.
        auto decimal_type = complex_logical_type::create_decimal(&arena->resource,
                                                                 static_cast<uint8_t>(width),
                                                                 static_cast<uint8_t>(scale));
        if (decimal_type.has_error()) {
            throw std::runtime_error(std::string(decimal_type.error().what));
        }
        return std::make_shared<otterbrix_py_type_t>(arena, std::move(decimal_type.value()));
    }

    std::shared_ptr<otterbrix_py_type_t> string_type(const module_arena_ptr& arena, const std::string& /*collation*/) {
        complex_logical_type type(logical_type::STRING_LITERAL);
        /*if (collation.empty()) {
        		type = LogicalType::VARCHAR;
        	} else {
        		type = LogicalType::VARCHAR_COLLATION(collation);
        	}*/
        return std::make_shared<otterbrix_py_type_t>(arena, type);
    }

    core::result_wrapper_t<logical_type> string_to_logical_type(const std::string& type_str,
                                                                std::pmr::memory_resource* resource) {
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
            {"UHUGEINT", logical_type::UHUGEINT}};
        auto it = fromStrToType.find(type_str);
        if (it != fromStrToType.end()) {
            return it->second;
        }
        return core::error_t(
            core::error_code_t::conversion_failure,
            std::pmr::string("Has no function to transform str " + type_str + " to OtterBrix type", resource));
    }

    std::shared_ptr<otterbrix_py_type_t> type(const module_arena_ptr& arena, const std::string& type_str) {
        auto ltype = string_to_logical_type(type_str, &arena->resource);
        if (ltype.has_error()) {
            throw std::runtime_error(ltype.error().what.c_str());
        }
        return std::make_shared<otterbrix_py_type_t>(arena, ltype.value());
    }

    void initialize(py::module_ m, const module_arena_ptr& arena) {
        // Rule 6: the module's arena is a precondition, not a hint. Every lambda below
        // captures a reference to it and will dereference it on the first call from Python,
        // so a null one has to die here, at import, and not inside a factory much later.
        // A THROW and not an assert, because an assert is exactly what NDEBUG deletes and the
        // shipping build would then meet the null one dereference later.
        if (!arena) {
            throw std::runtime_error("type_creation::initialize needs the module's arena");
        }

        // Every entry point is bound through a lambda that CAPTURES the arena by value, which
        // makes the bound function object an owner too. That is the whole difference between
        // "the module named its arena" and "the entry point reached for the process global":
        // one captured reference per door.
        const auto sqltype_doc = "Create a type object by parsing the 'type_str' string";
        auto typed = [arena](const std::string& type_str) { return type(arena, type_str); };
        m.def("sqltype", typed, sqltype_doc, py::arg("type_str"));
        m.def("dtype", typed, sqltype_doc, py::arg("type_str"));
        m.def("type", typed, sqltype_doc, py::arg("type_str"));
        m.def(
            "array_type",
            [arena](const std::shared_ptr<otterbrix_py_type_t>& type, idx_t size) {
                return array_type(arena, type, size);
            },
            "Create an array type object of 'type'",
            py::arg("type").none(false),
            py::arg("size"));
        m.def(
            "list_type",
            [arena](const std::shared_ptr<otterbrix_py_type_t>& type) { return list_type(arena, type); },
            "Create a list type object of 'type'",
            py::arg("type").none(false));
        m.def(
            "union_type",
            [arena](const py::object& members) { return union_type(arena, members); },
            "Create a union type object from 'members'",
            py::arg("members").none(false));
        m.def(
            "string_type",
            [arena](const std::string& collation) { return string_type(arena, collation); },
            "Create a string type with an optional collation",
            py::arg("collation") = "");
        m.def(
            "enum_type",
            [arena](const std::string& name, const std::shared_ptr<otterbrix_py_type_t>& type, const py::list& values) {
                return enum_type(arena, name, type, values);
            },
            "Create an enum type of underlying 'type', consisting of the list of 'values'",
            py::arg("name"),
            py::arg("type"),
            py::arg("values"));
        m.def(
            "decimal_type",
            [arena](int width, int scale) { return decimal_type(arena, width, scale); },
            "Create a decimal type with 'width' and 'scale'",
            py::arg("width"),
            py::arg("scale"));
        auto structured = [arena](const py::object& fields) { return struct_type(arena, fields); };
        m.def("struct_type", structured, "Create a struct type object from 'fields'", py::arg("fields"));
        m.def("row_type", structured, "Create a struct type object from 'fields'", py::arg("fields"));
        m.def(
            "map_type",
            [arena](const std::shared_ptr<otterbrix_py_type_t>& key,
                    const std::shared_ptr<otterbrix_py_type_t>& value) { return map_type(arena, key, value); },
            "Create a map type object from 'key_type' and 'value_type'",
            py::arg("key").none(false),
            py::arg("value").none(false));
    }

}} // namespace otterbrix::type_creation
