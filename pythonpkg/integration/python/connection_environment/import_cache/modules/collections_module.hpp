#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct CollectionsAbcCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "collections.abc";

public:
	CollectionsAbcCacheItem()
	    : PythonImportCacheItem("collections.abc"), Iterable("Iterable", this), Mapping("Mapping", this) {
	}
	~CollectionsAbcCacheItem() override {
	}

	PythonImportCacheItem Iterable;
	PythonImportCacheItem Mapping;
};

struct CollectionsCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "collections";

public:
	CollectionsCacheItem() : PythonImportCacheItem("collections"), abc() {
	}
	~CollectionsCacheItem() override {
	}

	CollectionsAbcCacheItem abc;
};

} // namespace otterbrix
