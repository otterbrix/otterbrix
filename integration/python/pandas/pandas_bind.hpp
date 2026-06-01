#pragma once

#include "pandas_column.hpp"

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/python_object_container.hpp>
#include <numpy/numpy_type.hpp>

#include <components/configuration/configuration.hpp>
#include <components/types/types.hpp>
#include <core/types/string.hpp>
#include <core/types/vector.hpp>

namespace otterbrix {

struct RegisteredArray {
	explicit RegisteredArray(py::array numpy_array) : numpy_array(std::move(numpy_array)) {
	}
	py::array numpy_array;
};

struct PandasColumnBindData {
	NumpyType numpy_type;
	unique_ptr<PandasColumn> pandas_col;
	unique_ptr<RegisteredArray> mask;
	//! Only for categorical types
	string internal_categorical_type;
	//! Hold ownership of objects created during scanning
	PythonObjectContainer object_str_val;
};

struct Pandas {
	static void Bind(py::handle df, vector<PandasColumnBindData> &out,
	                 vector<components::types::complex_logical_type> &return_types,
                     vector<string> &names,
                     const configuration::config_pandas &cfg = {});
};

} // namespace otterbrix
