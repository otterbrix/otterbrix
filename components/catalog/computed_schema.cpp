#include "computed_schema.hpp"

namespace components::catalog {
    computed_schema::computed_schema(std::pmr::memory_resource* resource)
        : fields_(resource)
        , existing_versions_(resource) {}

    void computed_schema::append(std::pmr::string json, const types::complex_logical_type& type) {
        if (try_use_refcout(json, type, true)) {
            return;
        }

        // otherwise a new version is created
        fields_.insert(json, type);
        auto ref = std::cref(fields_.find(json).ref_counted());
        ref.get().get_version(ref.get().latest_version_id().value()).add_ref();
        existing_versions_.emplace(json, std::move(ref));
    }

    void computed_schema::drop(std::pmr::string json, const types::complex_logical_type& type) {
        if (try_use_refcout(json, type, false)) {
            return;
        }

        // if did not return, type didn't exist
        // todo: define if it is an error
    }

    void computed_schema::drop_n(std::pmr::string json, const types::complex_logical_type& type, size_t n) {
        if (try_use_refcout(json, type, false, n)) {
            return;
        }
    }

    std::vector<types::complex_logical_type> computed_schema::find_field_versions(const std::pmr::string& name) const {
        std::vector<types::complex_logical_type> retval;
        auto it = existing_versions_.find(name);
        assert(it != existing_versions_.end());
        auto& vrs = it->second.get().get_versions();
        retval.reserve(vrs.size());

        for (const auto& [_, v] : vrs) {
            if (v.is_alive()) {
                retval.push_back(v.value);
            }
        }

        return retval;
    }

    types::complex_logical_type computed_schema::latest_types_struct() const {
        std::vector<types::complex_logical_type> retval;
        retval.reserve(existing_versions_.size());
        for (const auto& [name, entry] : existing_versions_) {
            auto& v = entry.get();
            if (auto id = v.latest_version_id(); static_cast<bool>(id) && v.get_version(*id).is_alive()) {
                retval.push_back(v.get_version(id.value()).value);
                retval.back().set_alias(name.c_str());
            }
        }

        return types::complex_logical_type::create_struct(std::move(retval));
    }

    bool computed_schema::try_use_refcout(const std::pmr::string& json,
                                          const types::complex_logical_type& type,
                                          bool is_append,
                                          size_t n) {
        if (auto it_all_versions = existing_versions_.find(json); it_all_versions != existing_versions_.end()) {
            auto& versioned_value = it_all_versions->second.get();
            auto it_map = versioned_value.get_versions();
            auto it_version = std::find_if(
                it_map.begin(),
                it_map.end(),
                [&type](const std::pair<std::size_t, versioned_entry<types::complex_logical_type>>& entry) {
                    return entry.second.value == type;
                });

            if (it_version != it_map.end()) {
                auto& refcount = versioned_value.get_version(it_version->first);
                // version existed prior to this add, extend or reduce its lifetime
                if (is_append) {
                    refcount.add_ref();
                } else {
                    refcount.release_n(n);
                    if (!it_all_versions->second.get().has_alive_versions()) {
                        // deleted last version, do cleanup
                        existing_versions_.erase(it_all_versions);
                        fields_.erase(json);
                    }
                }

                return true;
            }
        }

        return false;
    }
} // namespace components::catalog