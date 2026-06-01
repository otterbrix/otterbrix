#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct PathlibCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "pathlib";

public:
	PathlibCacheItem() : PythonImportCacheItem("pathlib"), Path("Path", this) {
	}
	~PathlibCacheItem() override {
	}

	PythonImportCacheItem Path;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace otterbrix
