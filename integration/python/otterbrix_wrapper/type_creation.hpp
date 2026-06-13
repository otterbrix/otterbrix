#pragma once

#include "pytype.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>
#include <core/typedefs.hpp>
#include <memory>

#include <memory_resource>
#include <string>
#include <unordered_map>
#include <vector>

namespace otterbrix {
    namespace TypeCreation {

        // Maps a SQL/OtterBrix type string (e.g. "VARCHAR", "BIGINT") to its logical_type.
        // Returns a conversion_failure error if the string has no known mapping.
        core::result_wrapper_t<components::types::logical_type>
        StringToLogicalType(const std::string &type_str, std::pmr::memory_resource *resource);

        std::shared_ptr<OtterBrixPyType> MapType(const std::shared_ptr<OtterBrixPyType> &key_type,
                const std::shared_ptr<OtterBrixPyType> &value_type);

        std::shared_ptr<OtterBrixPyType> ListType(const std::shared_ptr<OtterBrixPyType> &type);

        std::shared_ptr<OtterBrixPyType> ArrayType(const std::shared_ptr<OtterBrixPyType> &type, idx_t size);

        std::shared_ptr<OtterBrixPyType> StructType(const py::object &fields);

        std::shared_ptr<OtterBrixPyType> UnionType(const py::object &members); 

        std::shared_ptr<OtterBrixPyType> EnumType(const std::string &name, const std::shared_ptr<OtterBrixPyType> &type,
                const py::list &values_p); 

        std::shared_ptr<OtterBrixPyType> DecimalType(int width, int scale);

        std::shared_ptr<OtterBrixPyType> StringType(const std::string &collation);

        std::shared_ptr<OtterBrixPyType> Type(const std::string &type_str); 

        void Initialize(py::module_ m);
    } // namespace TypeCreation
} // namespace otterbrix
