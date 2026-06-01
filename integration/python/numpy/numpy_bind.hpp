#pragma once

#include <pybind11/pybind_wrapper.hpp>

#include <components/configuration/configuration.hpp>
#include <components/types/types.hpp>
#include <core/types/vector.hpp>
#include <core/types/string.hpp>

namespace otterbrix {

struct PandasColumnBindData;

struct NumpyBind {
	static void Bind(py::handle df, vector<PandasColumnBindData> &out,
	                 vector<components::types::complex_logical_type> &return_types,
                     vector<string> &names,
                     const configuration::config_pandas &cfg = {});
};

} // namespace otterbrix
