#pragma once

#include <common/string_util/case_insensitive.hpp>
#include <memory>

#include <string>
#include <utility>

namespace otterbrix {

    //! R16: identity of a scan dependency slot is fixed and finite, so it is keyed by an enum
    //! (a small oid) instead of free-form strings. The canonical string for each kind is kept
    //! only for the legacy string-keyed API still used at the arrow/pandas get-sites.
    enum class dependency_kind_t : uint8_t
    {
        data = 0,          //! the python object the scanner reads rows from
        copy,              //! a defensive copy of the source frame, when one was taken
        replacement_cache, //! keeps the factory + python object alive for the replacement scan
    };

    inline const std::string& dependency_kind_name(dependency_kind_t kind) {
        static const std::string names[] = {"data", "copy", "replacement_cache"};
        return names[static_cast<uint8_t>(kind)];
    }

    class dependency_item_t {
    public:
        virtual ~dependency_item_t(){};

    public:
        template<class TARGET>
        TARGET& cast() {
            return reinterpret_cast<TARGET&>(*this);
        }
        template<class TARGET>
        const TARGET& cast() const {
            return reinterpret_cast<const TARGET&>(*this);
        }
    };

    class external_dependency_t {
    public:
        explicit external_dependency_t() {}
        ~external_dependency_t() {}

    public:
        //! R14: this registry is the sole owner of its dependency_items (single-owner: each item lives
        //! in exactly one external_dependency_t, which itself lives in exactly one move-only table_ref_t).
        //! get_dependency therefore hands back a non-owning observer pointer; the scan's function_data_t
        //! borrows it for a lifetime strictly nested inside this registry's.
        //! R16: enum-keyed identity API (preferred).
        void add_dependency(dependency_kind_t kind, std::unique_ptr<dependency_item_t> item) {
            objects[dependency_kind_name(kind)] = std::move(item);
        }
        dependency_item_t* get_dependency(dependency_kind_t kind) const {
            return get_dependency(dependency_kind_name(kind));
        }

        //! Legacy string-keyed API, retained only for the out-of-zone arrow/pandas get-sites.
        void add_dependency(const std::string& name, std::unique_ptr<dependency_item_t> item) {
            objects[name] = std::move(item);
        }
        dependency_item_t* get_dependency(const std::string& name) const {
            auto it = objects.find(name);
            if (it == objects.end()) {
                return nullptr;
            }
            return it->second.get();
        }

        //! R14-local: compile-time callback instead of a std::function. CALLBACK is invoked as
        //! callback(const string&, dependency_item_t*) for every dependency (observer pointer, no ownership).
        template<class CALLBACK>
        void scan_dependencies(CALLBACK&& callback) {
            for (auto& kv : objects) {
                callback(kv.first, kv.second.get());
            }
        }

    private:
        //! The objects encompassed by this dependency. This registry owns them outright.
        case_insensitive_map_t<std::unique_ptr<dependency_item_t>> objects;
    };

} // namespace otterbrix
