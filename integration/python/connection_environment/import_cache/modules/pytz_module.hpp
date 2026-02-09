#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct PytzCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "pytz";

public:
	PytzCacheItem() : PythonImportCacheItem("pytz"), timezone("timezone", this) {
	}
	~PytzCacheItem() override {
	}

	PythonImportCacheItem timezone;
};

} // namespace otterbrix
