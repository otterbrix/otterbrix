#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/configuration/configuration.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>
#include <vector>

namespace otterbrix {

struct pandas_column_bind_data_t;

struct numpy_bind_t {
	static core::error_t bind(std::pmr::memory_resource *resource, py::handle df, std::vector<pandas_column_bind_data_t> &out,
	                 std::vector<components::types::complex_logical_type> &return_types,
                     std::vector<std::string> &names,
                     const configuration::config_pandas &cfg = {});
};

} // namespace otterbrix
