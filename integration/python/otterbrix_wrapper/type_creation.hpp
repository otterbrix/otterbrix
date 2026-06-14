#pragma once

#include "pytype.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>
#include <common/typedefs.hpp>
#include <memory>

#include <memory_resource>
#include <string>
#include <unordered_map>
#include <vector>

namespace otterbrix {
    namespace type_creation {

        // Maps a SQL/OtterBrix type string (e.g. "VARCHAR", "BIGINT") to its logical_type.
        // Returns a conversion_failure error if the string has no known mapping.
        core::result_wrapper_t<components::types::logical_type>
        string_to_logical_type(const std::string &type_str, std::pmr::memory_resource *resource);

        std::shared_ptr<otterbrix_py_type_t> map_type(const std::shared_ptr<otterbrix_py_type_t> &key_type,
                const std::shared_ptr<otterbrix_py_type_t> &value_type);

        std::shared_ptr<otterbrix_py_type_t> list_type(const std::shared_ptr<otterbrix_py_type_t> &type);

        std::shared_ptr<otterbrix_py_type_t> array_type(const std::shared_ptr<otterbrix_py_type_t> &type, idx_t size);

        std::shared_ptr<otterbrix_py_type_t> struct_type(const py::object &fields);

        std::shared_ptr<otterbrix_py_type_t> union_type(const py::object &members); 

        std::shared_ptr<otterbrix_py_type_t> enum_type(const std::string &name, const std::shared_ptr<otterbrix_py_type_t> &type,
                const py::list &values_p); 

        std::shared_ptr<otterbrix_py_type_t> decimal_type(int width, int scale);

        std::shared_ptr<otterbrix_py_type_t> string_type(const std::string &collation);

        std::shared_ptr<otterbrix_py_type_t> type(const std::string &type_str); 

        void initialize(py::module_ m);
    } // namespace type_creation
} // namespace otterbrix
