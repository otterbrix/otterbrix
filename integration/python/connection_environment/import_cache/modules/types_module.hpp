#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct TypesCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "types";

public:
	TypesCacheItem()
	    : PythonImportCacheItem("types"), UnionType("UnionType", this), GenericAlias("GenericAlias", this),
	      BuiltinFunctionType("BuiltinFunctionType", this) {
	}
	~TypesCacheItem() override {
	}

	PythonImportCacheItem UnionType;
	PythonImportCacheItem GenericAlias;
	PythonImportCacheItem BuiltinFunctionType;
};

} // namespace otterbrix
