#pragma once

#include <core/string_util/case_insensitive.hpp>
#include <memory>

#include <string>
#include <utility>

namespace otterbrix {

//! R16: identity of a scan dependency slot is fixed and finite, so it is keyed by an enum
//! (a small oid) instead of free-form strings. The canonical string for each kind is kept
//! only for the legacy string-keyed API still used at the arrow/pandas get-sites.
enum class dependency_kind_t : uint8_t
{
	data = 0,           //! the python object the scanner reads rows from
	copy,               //! a defensive copy of the source frame, when one was taken
	replacement_cache,  //! keeps the factory + python object alive for the replacement scan
};

inline const std::string& dependency_kind_name(dependency_kind_t kind) {
	static const std::string names[] = {"data", "copy", "replacement_cache"};
	return names[static_cast<uint8_t>(kind)];
}

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

class ExternalDependency {
public:
	explicit ExternalDependency() {
	}
	~ExternalDependency() {
	}

public:
	//! R14: this registry is the sole owner of its DependencyItems (single-owner: each item lives
	//! in exactly one ExternalDependency, which itself lives in exactly one move-only TableRef).
	//! GetDependency therefore hands back a non-owning observer pointer; the scan's FunctionData
	//! borrows it for a lifetime strictly nested inside this registry's.
	//! R16: enum-keyed identity API (preferred).
	void AddDependency(dependency_kind_t kind, std::unique_ptr<DependencyItem> item) {
		objects[dependency_kind_name(kind)] = std::move(item);
	}
	DependencyItem *GetDependency(dependency_kind_t kind) const {
		return GetDependency(dependency_kind_name(kind));
	}

	//! Legacy string-keyed API, retained only for the out-of-zone arrow/pandas get-sites.
	void AddDependency(const std::string &name, std::unique_ptr<DependencyItem> item) {
		objects[name] = std::move(item);
	}
	DependencyItem *GetDependency(const std::string &name) const {
		auto it = objects.find(name);
		if (it == objects.end()) {
			return nullptr;
		}
		return it->second.get();
	}

	//! R14-local: compile-time callback instead of a std::function. CALLBACK is invoked as
	//! callback(const string&, DependencyItem*) for every dependency (observer pointer, no ownership).
	template <class CALLBACK>
	void ScanDependencies(CALLBACK &&callback) {
		for (auto &kv : objects) {
			callback(kv.first, kv.second.get());
		}
	}

private:
	//! The objects encompassed by this dependency. This registry owns them outright.
	case_insensitive_map_t<std::unique_ptr<DependencyItem>> objects;
};

} // namespace otterbrix
