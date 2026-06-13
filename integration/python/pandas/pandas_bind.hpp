#pragma once

#include "pandas_column.hpp"
#include <memory>

#include <pybind11/pybind_wrapper.hpp>
#include <pybind11/python_object_container.hpp>
#include <numpy/numpy_type.hpp>

#include <components/configuration/configuration.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>
#include <vector>

namespace otterbrix {

struct RegisteredArray {
	explicit RegisteredArray(py::array numpy_array) : numpy_array(std::move(numpy_array)) {
	}
	py::array numpy_array;
};

struct PandasColumnBindData {
	NumpyType numpy_type;
	std::unique_ptr<PandasColumn> pandas_col;
	std::unique_ptr<RegisteredArray> mask;
	//! Only for categorical types
	std::string internal_categorical_type;
	//! Hold ownership of objects created during scanning
	PythonObjectContainer object_str_val;
};

struct Pandas {
	static core::error_t Bind(std::pmr::memory_resource *resource, py::handle df, std::vector<PandasColumnBindData> &out,
	                 std::vector<components::types::complex_logical_type> &return_types,
                     std::vector<std::string> &names,
                     const configuration::config_pandas &cfg = {});
};

} // namespace otterbrix
