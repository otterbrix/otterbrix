#include "python_import_cache.hpp"
#include "python_import_cache_item.hpp"

#include "importer.hpp"


#include <functional>
#include <stdexcept>
#include <stack>

namespace otterbrix {

//===--------------------------------------------------------------------===//
// python_import_cache_item_t (SUPER CLASS)
//===--------------------------------------------------------------------===//

py::handle python_import_cache_item_t::operator()(bool load) {
	if (is_loaded()) {
		return object;
	}
	std::stack<std::reference_wrapper<python_import_cache_item_t>> hierarchy;

	optional_ptr<python_import_cache_item_t> item = this;
	while (item) {
		hierarchy.emplace(*item);
		item = item->parent;
	}
	return python_importer_t::import(hierarchy, load);
}

bool python_import_cache_item_t::load_succeeded() const {
	return load_succeeded_;
}

inline bool python_import_cache_item_t::is_loaded() const {
	return object.ptr() != nullptr;
}

py::handle python_import_cache_item_t::add_cache(python_import_cache_t &cache, py::object object) {
	return cache.add_cache(std::move(object));
}

void python_import_cache_item_t::load_module(python_import_cache_t &cache) {
	try {
		py::gil_assert();
		object = add_cache(cache, std::move(py::module::import(name.c_str())));
		load_succeeded_ = true;
	} catch (py::error_already_set &e) {
		if (is_required()) {
			throw std::runtime_error(
			    "Required module "+name+" failed to import, due to the following Python exception:\n"+e.what());
		}
		object = nullptr;
		return;
	}
}

void python_import_cache_item_t::load_attribute(python_import_cache_t &cache, py::handle source) {
	if (py::hasattr(source, name.c_str())) {
		object = add_cache(cache, std::move(source.attr(name.c_str())));
	} else {
		object = nullptr;
	}
}

py::handle python_import_cache_item_t::load(python_import_cache_t &cache, py::handle source, bool load) {
	if (is_loaded()) {
		return object;
	}
	if (!load) {
		// Don't load the item if it's not already loaded
		return object;
	}
	if (is_module) {
		load_module(cache);
	} else {
		load_attribute(cache, source);
	}
	return object;
}

//===--------------------------------------------------------------------===//
// python_import_cache_t (CONTAINER)
//===--------------------------------------------------------------------===//

python_import_cache_t::python_import_cache_t() = default;

python_import_cache_t::~python_import_cache_t() {
	try {
		py::gil_scoped_acquire acquire;
		owned_objects.clear();
	} catch (...) { // NOLINT
	}
}

py::handle python_import_cache_t::add_cache(py::object item) {
	auto object_ptr = item.ptr();
	owned_objects.push_back(std::move(item));
	return object_ptr;
}

} // namespace otterbrix
