#include "node_default_apply.hpp"

#include <sstream>

namespace components::logical_plan {

    node_default_apply_t::node_default_apply_t(std::pmr::memory_resource*    resource,
                                                const collection_full_name_t& collection,
                                                std::vector<default_entry_t>  defaults)
        : node_t(resource, node_type::default_apply_t, collection)
        , defaults_(std::move(defaults)) {}

    hash_t      node_default_apply_t::hash_impl()      const { return 0; }
    std::string node_default_apply_t::to_string_impl() const {
        std::ostringstream s;
        s << "$default_apply: " << collection_name();
        return s.str();
    }

} // namespace components::logical_plan
