#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct IpywidgetsCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "ipywidgets";

public:
	IpywidgetsCacheItem() : PythonImportCacheItem("ipywidgets"), FloatProgress("FloatProgress", this) {
	}
	~IpywidgetsCacheItem() override {
	}

	PythonImportCacheItem FloatProgress;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace otterbrix
