#pragma once

#include "pytype.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <components/types/types.hpp>
#include <core/typedefs.hpp>
#include <core/types/memory.hpp>
#include <core/types/string.hpp>
#include <core/types/vector.hpp>
#include <core/types/unordered_map.hpp>

namespace otterbrix {
    namespace TypeCreation {

        shared_ptr<OtterBrixPyType> MapType(const shared_ptr<OtterBrixPyType> &key_type,
                const shared_ptr<OtterBrixPyType> &value_type);

        shared_ptr<OtterBrixPyType> ListType(const shared_ptr<OtterBrixPyType> &type);

        shared_ptr<OtterBrixPyType> ArrayType(const shared_ptr<OtterBrixPyType> &type, idx_t size);

        shared_ptr<OtterBrixPyType> StructType(const py::object &fields);

        shared_ptr<OtterBrixPyType> UnionType(const py::object &members); 

        shared_ptr<OtterBrixPyType> EnumType(const string &name, const shared_ptr<OtterBrixPyType> &type,
                const py::list &values_p); 

        shared_ptr<OtterBrixPyType> DecimalType(int width, int scale);

        shared_ptr<OtterBrixPyType> StringType(const string &collation);

        shared_ptr<OtterBrixPyType> Type(const string &type_str); 

        void Initialize(py::module_ m);
    } // namespace TypeCreation
} // namespace otterbrix
