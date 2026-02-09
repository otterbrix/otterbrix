#pragma once

#include <pybind11/pybind_wrapper.hpp>
#include <core/types/string.hpp>
#include <core/types/optional_ptr.hpp>

namespace otterbrix {

struct PythonImportCache;

struct PythonImportCacheItem {
public:
	PythonImportCacheItem(const string &name, optional_ptr<PythonImportCacheItem> parent)
	    : name(name), is_module(false), load_succeeded(false), parent(parent), object(nullptr) {
	}
	PythonImportCacheItem(const string &name)
	    : name(name), is_module(true), load_succeeded(false), parent(nullptr), object(nullptr) {
	}

	virtual ~PythonImportCacheItem() {
	}

public:
	bool LoadSucceeded() const;
	bool IsLoaded() const;
	py::handle operator()(bool load = true);
	py::handle Load(PythonImportCache &cache, py::handle source, bool load);

protected:
	virtual bool IsRequired() const {
		return true;
	}

private:
	py::handle AddCache(PythonImportCache &cache, py::object object);
	void LoadAttribute(PythonImportCache &cache, py::handle source);
	void LoadModule(PythonImportCache &cache);

private:
	//! The name of the item
	string name;
	//! Whether the item is a module
	bool is_module;
	//! Whether or not we attempted to load the item
	bool load_succeeded;
	//! The parent of this item (either a module or an attribute)
	optional_ptr<PythonImportCacheItem> parent;
	//! The stored item
	py::handle object;
};

} // namespace otterbrix
