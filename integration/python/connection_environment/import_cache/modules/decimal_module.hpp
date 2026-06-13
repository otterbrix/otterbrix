#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct DecimalCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "decimal";

public:
	DecimalCacheItem() : PythonImportCacheItem("decimal"), Decimal("Decimal", this) {
	}
	~DecimalCacheItem() override {
	}

	PythonImportCacheItem Decimal;
};

} // namespace otterbrix
