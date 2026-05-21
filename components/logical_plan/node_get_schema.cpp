#include "node_get_schema.hpp"

namespace components::logical_plan {

    node_get_schema_t::node_get_schema_t(std::pmr::memory_resource* resource,
                                         std::pmr::vector<std::pair<std::string, std::string>> ids)
        : node_t(resource, node_type::get_schema_t)
        , ids_(std::move(ids)) {}

    hash_t node_get_schema_t::hash_impl() const { return 0; }

    std::string node_get_schema_t::to_string_impl() const {
        std::string out = "$get_schema[";
        for (std::size_t i = 0; i < ids_.size(); ++i) {
            if (i)
                out.push_back(',');
            out.append(ids_[i].first);
            out.push_back('.');
            out.append(ids_[i].second);
        }
        out.push_back(']');
        return out;
    }

} // namespace components::logical_plan
