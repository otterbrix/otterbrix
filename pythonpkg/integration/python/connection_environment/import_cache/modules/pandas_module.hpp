#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct PandasCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "pandas";

public:
	PandasCacheItem()
	    : PythonImportCacheItem("pandas"), DataFrame("DataFrame", this), isnull("isnull", this),
	      ArrowDtype("ArrowDtype", this), NaT("NaT", this), NA("NA", this) {
	}
	~PandasCacheItem() override {
	}

	PythonImportCacheItem DataFrame;
	PythonImportCacheItem isnull;
	PythonImportCacheItem ArrowDtype;
	PythonImportCacheItem NaT;
	PythonImportCacheItem NA;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace otterbrix
