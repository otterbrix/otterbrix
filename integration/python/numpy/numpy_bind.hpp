#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/configuration/configuration.hpp>
#include <components/types/types.hpp>
#include <core/result_wrapper.hpp>

#include <memory_resource>
#include <string>
#include <vector>

namespace otterbrix {

struct PandasColumnBindData;

struct NumpyBind {
	static core::error_t Bind(std::pmr::memory_resource *resource, py::handle df, std::vector<PandasColumnBindData> &out,
	                 std::vector<components::types::complex_logical_type> &return_types,
                     std::vector<std::string> &names,
                     const configuration::config_pandas &cfg = {});
};

} // namespace otterbrix
