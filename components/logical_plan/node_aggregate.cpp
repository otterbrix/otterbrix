#include "node_aggregate.hpp"

#include <sstream>

namespace components::logical_plan {

    node_aggregate_t::node_aggregate_t(std::pmr::memory_resource* resource,
                                       core::dbname_t dbname,
                                       core::relname_t relname)
        : node_t(resource, node_type::aggregate_t)
        , uid_(std::string{})
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , distinct_on_keys_(resource) {}

    node_aggregate_t::node_aggregate_t(std::pmr::memory_resource* resource,
                                       core::uid_t uid,
                                       core::dbname_t dbname,
                                       core::relname_t relname)
        : node_t(resource, node_type::aggregate_t)
        , uid_(std::move(uid))
        , dbname_(std::move(dbname))
        , relname_(std::move(relname))
        , distinct_on_keys_(resource) {}

    hash_t node_aggregate_t::hash_impl() const { return 0; }

    std::string node_aggregate_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$aggregate: {";
        bool is_first = true;
        for (const auto& child : children_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << child->to_string();
        }
        // Rendered only for DISTINCT ON (non-empty); plain DISTINCT / non-DISTINCT keep the
        // historical "$aggregate: {<children>}" form that plan-string tests pin.
        if (!distinct_on_keys_.empty()) {
            if (!is_first) {
                stream << ", ";
            }
            stream << "$distinct_on: [";
            bool key_first = true;
            for (const auto& key : distinct_on_keys_) {
                if (key_first) {
                    key_first = false;
                } else {
                    stream << ", ";
                }
                stream << key.as_string();
            }
            stream << "]";
        }
        stream << "}";
        return stream.str();
    }

    node_aggregate_ptr
    make_node_aggregate(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname) {
        return {new node_aggregate_t(resource, std::move(dbname), std::move(relname))};
    }

    node_aggregate_ptr make_node_aggregate(std::pmr::memory_resource* resource,
                                           core::uid_t uid,
                                           core::dbname_t dbname,
                                           core::relname_t relname) {
        return {new node_aggregate_t(resource, std::move(uid), std::move(dbname), std::move(relname))};
    }

} // namespace components::logical_plan
