#include "node_drop.hpp"

#include <boost/container_hash/hash.hpp>
#include <sstream>

namespace components::logical_plan {

    node_drop_t::node_drop_t(std::pmr::memory_resource* resource, drop_target_kind kind)
        : node_t(resource, node_type::drop_t)
        , kind_(kind) {}

    std::string node_drop_t::to_string_impl() const {
        std::stringstream stream;
        switch (kind_) {
            case drop_target_kind::database:
                stream << "$drop_database: <oid:" << static_cast<std::uint64_t>(namespace_oid_) << ">";
                break;
            case drop_target_kind::collection:
                stream << "$drop_collection: <oid:" << static_cast<std::uint64_t>(table_oid()) << ">";
                break;
            case drop_target_kind::type:
                stream << "$drop_type: <oid:" << static_cast<std::uint64_t>(type_oid_) << ">";
                break;
            case drop_target_kind::sequence:
                stream << "$drop_sequence: <oid:" << static_cast<std::uint64_t>(table_oid()) << ">";
                break;
            case drop_target_kind::view:
                stream << "$drop_view: <oid:" << static_cast<std::uint64_t>(table_oid()) << ">";
                break;
            case drop_target_kind::macro:
                stream << "$drop_macro: <oid:" << static_cast<std::uint64_t>(table_oid()) << ">";
                break;
            case drop_target_kind::index:
                stream << "$drop_index: <oid:" << static_cast<std::uint64_t>(index_oid_) << ">";
                break;
        }
        return stream.str();
    }

    node_drop_ptr make_node_drop(std::pmr::memory_resource* resource, drop_target_kind kind) {
        return {new node_drop_t{resource, kind}};
    }

} // namespace components::logical_plan
