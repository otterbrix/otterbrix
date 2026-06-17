#include "node_drop.hpp"

#include <boost/container_hash/hash.hpp>
#include <sstream>

namespace components::logical_plan {

    node_drop_t::node_drop_t(std::pmr::memory_resource* resource, drop_target_kind kind)
        : node_t(resource, node_type::drop_t)
        , kind_(kind) {}

    hash_t node_drop_t::hash_impl() const {
        // node_t::hash() combines type_ + hash_impl(); fold kind_ (and the
        // per-kind OID payload) here so the drop variants land in distinct
        // buckets of any node-keyed container despite sharing node_type::drop_t.
        hash_t hash_value{0};
        boost::hash_combine(hash_value, static_cast<uint8_t>(kind_));
        switch (kind_) {
            case drop_target_kind::database:
                boost::hash_combine(hash_value, static_cast<hash_t>(namespace_oid_));
                break;
            case drop_target_kind::collection:
                boost::hash_combine(hash_value, static_cast<hash_t>(namespace_oid_));
                boost::hash_combine(hash_value, static_cast<hash_t>(table_oid()));
                break;
            case drop_target_kind::type:
                boost::hash_combine(hash_value, static_cast<hash_t>(type_oid_));
                break;
            case drop_target_kind::sequence:
            case drop_target_kind::view:
            case drop_target_kind::macro:
                boost::hash_combine(hash_value, static_cast<hash_t>(table_oid()));
                break;
            case drop_target_kind::index:
                boost::hash_combine(hash_value, static_cast<hash_t>(namespace_oid_));
                boost::hash_combine(hash_value, static_cast<hash_t>(index_oid_));
                boost::hash_combine(hash_value, static_cast<hash_t>(table_oid()));
                break;
        }
        return hash_value;
    }

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
