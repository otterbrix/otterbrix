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

struct registered_array_t {
	explicit registered_array_t(py::array numpy_array) : numpy_array(std::move(numpy_array)) {
	}
	py::array numpy_array;
};

struct pandas_column_bind_data_t {
	numpy_type_t numpy_type;
	std::unique_ptr<pandas_column_t> pandas_col;
	std::unique_ptr<registered_array_t> mask;
	//! Only for categorical types
	std::string internal_categorical_type;
	//! Hold ownership of objects created during scanning
	python_object_container_t object_str_val;
};

struct pandas_t {
	static core::error_t bind(std::pmr::memory_resource *resource, py::handle df, std::vector<pandas_column_bind_data_t> &out,
	                 std::vector<components::types::complex_logical_type> &return_types,
                     std::vector<std::string> &names,
                     const configuration::config_pandas &cfg = {});
};

} // namespace otterbrix
