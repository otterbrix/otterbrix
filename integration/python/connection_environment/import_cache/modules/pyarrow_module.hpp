#pragma once

#include "../python_import_cache_item.hpp"

namespace otterbrix {

struct PyarrowDatasetCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "pyarrow.dataset";

public:
	PyarrowDatasetCacheItem()
	    : PythonImportCacheItem("pyarrow.dataset"), Scanner("Scanner", this), Dataset("Dataset", this) {
	}
	~PyarrowDatasetCacheItem() override {
	}

	PythonImportCacheItem Scanner;
	PythonImportCacheItem Dataset;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

struct PyarrowCacheItem : public PythonImportCacheItem {

public:
	static constexpr const char *Name = "pyarrow";

public:
	PyarrowCacheItem()
	    : PythonImportCacheItem("pyarrow"), dataset(), Table("Table", this),
	      RecordBatchReader("RecordBatchReader", this) {
	}
	~PyarrowCacheItem() override {
	}

	PyarrowDatasetCacheItem dataset;
	PythonImportCacheItem Table;
	PythonImportCacheItem RecordBatchReader;

protected:
	bool IsRequired() const override final {
		return false;
	}
};

} // namespace otterbrix
