#pragma once

#include "python_import_cache_modules.hpp"

#include <pybind11/pybind_wrapper.hpp>


#include <functional>
#include <stack>

namespace otterbrix {

struct python_importer_t {
public:
	static py::handle import(std::stack<std::reference_wrapper<python_import_cache_item_t>> &hierarchy, bool load = true);
};

} // namespace otterbrix
