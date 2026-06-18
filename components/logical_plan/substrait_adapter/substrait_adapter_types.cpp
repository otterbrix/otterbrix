#include "substrait_adapter_types.hpp"

namespace components::logical_plan::substrait_adapter {

    int32_t field_mapping_t::get_or_add(const expressions::key_t& key) { return get_or_add(key.as_string()); }

    int32_t field_mapping_t::get_or_add(const std::string& name) {
        auto it = index.find(name);
        if (it != index.end()) {
            return it->second;
        }
        auto idx = static_cast<int32_t>(names.size());
        names.emplace_back(name);
        types.emplace_back(make_string_type());
        index.emplace(name, idx);
        return idx;
    }

    int32_t field_mapping_t::get_or_add(const std::string& name, const substrait::Type& type) {
        auto idx = get_or_add(name);
        set_type(idx, type);
        return idx;
    }

    bool field_mapping_t::contains(int32_t idx) const {
        return idx >= 0 && static_cast<size_t>(idx) < names.size();
    }

    const std::string& field_mapping_t::name_or_empty(int32_t idx) const {
        static const std::string empty;
        return contains(idx) ? names[static_cast<size_t>(idx)] : empty;
    }

    void field_mapping_t::set_type(int32_t idx, const substrait::Type& type) {
        if (idx < 0) {
            return;
        }
        auto size = static_cast<size_t>(idx);
        if (types.size() <= size) {
            types.resize(size + 1, make_string_type());
        }
        types[size] = type;
    }

    substrait::Type field_mapping_t::type_or_default(int32_t idx) const {
        auto size = static_cast<size_t>(idx);
        if (idx >= 0 && size < types.size()) {
            return types[size];
        }
        return make_string_type();
    }

} // namespace components::logical_plan::substrait_adapter
