#pragma once

#include "python_import_cache_modules.hpp"

#include <pybind11/pybind_wrapper.hpp>
#include <vector>


namespace otterbrix {

struct PythonImportCache {
public:
	explicit PythonImportCache();
	~PythonImportCache();

public:
	PyarrowCacheItem pyarrow;
	PandasCacheItem pandas;
	DatetimeCacheItem datetime;
	DecimalCacheItem decimal;
	IpythonCacheItem IPython;
	IpywidgetsCacheItem ipywidgets;
	NumpyCacheItem numpy;
	PathlibCacheItem pathlib;
	PolarsCacheItem polars;
	PytzCacheItem pytz;
	TypesCacheItem types;
	TypingCacheItem typing;
	UuidCacheItem uuid;
	CollectionsCacheItem collections;

public:
	py::handle AddCache(py::object item);
private:

	std::vector<py::object> owned_objects;
};

} // namespace otterbrix
