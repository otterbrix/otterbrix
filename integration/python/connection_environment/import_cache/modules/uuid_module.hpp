#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct UuidCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "uuid";

public:
	UuidCacheItem() : PythonImportCacheItem("uuid"), UUID("UUID", this) {
	}
	~UuidCacheItem() override {
	}

	PythonImportCacheItem UUID;
};

} // namespace otterbrix
