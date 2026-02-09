#pragma once

#include "python_import_cache_modules.hpp"

#include <pybind11/pybind_wrapper.hpp>

#include <core/types/stack.hpp>

#include <functional>

namespace otterbrix {

struct PythonImporter {
public:
	static py::handle Import(stack<std::reference_wrapper<PythonImportCacheItem>> &hierarchy, bool load = true);
};

} // namespace otterbrix
