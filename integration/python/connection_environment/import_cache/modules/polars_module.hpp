#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct PolarsCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "polars";

public:
	PolarsCacheItem() : PythonImportCacheItem("polars"), DataFrame("DataFrame", this), LazyFrame("LazyFrame", this) {
	}
	~PolarsCacheItem() override {
	}

	PythonImportCacheItem DataFrame;
	PythonImportCacheItem LazyFrame;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace otterbrix
