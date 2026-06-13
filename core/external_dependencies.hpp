#pragma once

#include "string_util/case_insensitive.hpp"
#include "types/memory.hpp"
#include "types/string.hpp"
#include <functional>

#pragma once

namespace otterbrix {

class DependencyItem {
public:
	virtual ~DependencyItem() {};

public:
	template <class TARGET>
	TARGET &Cast() {
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		return reinterpret_cast<const TARGET &>(*this);
	}
};

using dependency_scan_t = std::function<void(const string &name, shared_ptr<DependencyItem> item)>;

class ExternalDependency {
public:
	explicit ExternalDependency() {
	}
	~ExternalDependency() {
	}

public:
	void AddDependency(const string &name, shared_ptr<DependencyItem> item) {
		objects[name] = std::move(item);
	}
	shared_ptr<DependencyItem> GetDependency(const string &name) const {
		auto it = objects.find(name);
		if (it == objects.end()) {
			return nullptr;
		}
		return it->second;
	}
	void ScanDependencies(const dependency_scan_t &callback) {
		for (auto &kv : objects) {
			callback(kv.first, kv.second);
		}
	}

private:
	//! The objects encompassed by this dependency
	case_insensitive_map_t<shared_ptr<DependencyItem>> objects;
};

} // namespace otterbrix
