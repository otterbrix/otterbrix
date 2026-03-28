#include "computed_schema.hpp"

namespace components::catalog {
    computed_schema::computed_schema(std::pmr::memory_resource* resource)
        : fields_(resource)
        , column_order_(resource) {}

    void computed_schema::append(std::pmr::string field_name, const types::complex_logical_type& type) {
        auto& list = fields_[field_name];
        for (const auto& t : list) {
            if (t == type) {
                return;
            }
        }
        list.emplace_back(type);
        column_order_.emplace_back(field_name, type);
    }

    void computed_schema::append_n(std::pmr::string field_name,
                                   const types::complex_logical_type& type,
                                   size_t /*n*/) {
        append(std::move(field_name), type);
    }

    void computed_schema::drop(std::pmr::string field_name, const types::complex_logical_type& type) {
        auto it = fields_.find(field_name);
        if (it == fields_.end()) {
            return;
        }
        auto& list = it->second;
        list.erase(std::remove(list.begin(), list.end(), type), list.end());
        if (list.empty()) {
            fields_.erase(it);
        }
    }

    void computed_schema::drop_n(std::pmr::string field_name,
                                  const types::complex_logical_type& type,
                                  size_t /*n*/) {
        drop(std::move(field_name), type);
    }

    std::vector<types::complex_logical_type>
    computed_schema::find_field_versions(const std::pmr::string& field_name) const {
        auto it = fields_.find(field_name);
        if (it == fields_.end()) {
            return {};
        }
        return {it->second.begin(), it->second.end()};
    }

    types::complex_logical_type computed_schema::latest_types_struct() const {
        std::vector<types::complex_logical_type> retval;
        retval.reserve(column_order_.size());
        for (const auto& [name, type] : column_order_) {
            if (!has_type(name, type)) {
                continue;
            }
            auto t = type;
            t.set_alias(name.c_str());
            retval.push_back(std::move(t));
        }
        return types::complex_logical_type::create_struct("latest_types", std::move(retval));
    }

    bool computed_schema::has_type(const std::pmr::string& field_name,
                                    const types::complex_logical_type& type) const {
        auto it = fields_.find(field_name);
        if (it == fields_.end()) {
            return false;
        }
        for (const auto& t : it->second) {
            if (t == type) {
                return true;
            }
        }
        return false;
    }
} // namespace components::catalog
