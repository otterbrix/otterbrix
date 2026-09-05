#pragma once

#include "pytype.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <common/typedefs.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>
#include <memory>

#include <memory_resource>
#include <module_arena.hpp>
#include <string>
#include <unordered_map>
#include <vector>

namespace otterbrix { namespace type_creation {

    // Maps a SQL/OtterBrix type string (e.g. "VARCHAR", "BIGINT") to its logical_type.
    // Returns a conversion_failure error if the string has no known mapping.
    core::result_wrapper_t<components::types::logical_type> string_to_logical_type(const std::string& type_str,
                                                                                   std::pmr::memory_resource* resource);

    // EVERY factory below takes the module's arena, and takes it as a COUNTED reference.
    // Two of them ALLOCATE OUT OF IT for the object's whole life -- map_type and struct_type
    // build the child list the extension then keeps (struct_logical_type_extension copies
    // with fields.get_allocator()); two only allocate a REFUSAL MESSAGE on it (decimal_type,
    // type); the rest allocate nothing at all. They all still take it, because the
    // otterbrix_py_type_t they return has to carry the reference: that object is handed to
    // the interpreter, which may hold it after the module itself is gone, and the reference
    // is what keeps the arena from being released underneath it. See module_arena.hpp.
    std::shared_ptr<otterbrix_py_type_t> map_type(const module_arena_ptr& arena,
                                                  const std::shared_ptr<otterbrix_py_type_t>& key_type,
                                                  const std::shared_ptr<otterbrix_py_type_t>& value_type);

    std::shared_ptr<otterbrix_py_type_t> list_type(const module_arena_ptr& arena,
                                                   const std::shared_ptr<otterbrix_py_type_t>& type);

    std::shared_ptr<otterbrix_py_type_t>
    array_type(const module_arena_ptr& arena, const std::shared_ptr<otterbrix_py_type_t>& type, idx_t size);

    std::shared_ptr<otterbrix_py_type_t> struct_type(const module_arena_ptr& arena, const py::object& fields);

    std::shared_ptr<otterbrix_py_type_t> union_type(const module_arena_ptr& arena, const py::object& members);

    std::shared_ptr<otterbrix_py_type_t> enum_type(const module_arena_ptr& arena,
                                                   const std::string& name,
                                                   const std::shared_ptr<otterbrix_py_type_t>& type,
                                                   const py::list& values_p);

    std::shared_ptr<otterbrix_py_type_t> decimal_type(const module_arena_ptr& arena, int width, int scale);

    std::shared_ptr<otterbrix_py_type_t> string_type(const module_arena_ptr& arena, const std::string& collation);

    std::shared_ptr<otterbrix_py_type_t> type(const module_arena_ptr& arena, const std::string& type_str);

    // `arena` is the module's arena; see the note above and its definition in main.cpp.
    void initialize(py::module_ m, const module_arena_ptr& arena);
}} // namespace otterbrix::type_creation
