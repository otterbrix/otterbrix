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
        //
        // The statement's OWN fields fold in too: at plan time the OIDs alone are worthless
        // (enrich hasn't stamped them yet), so `DROP TABLE a` vs `DROP TABLE b` would otherwise hash identically.
        hash_t hash_value{0};
        boost::hash_combine(hash_value, static_cast<uint8_t>(kind_));
        boost::hash_combine(hash_value, dbname_);
        boost::hash_combine(hash_value, relname_);
        boost::hash_combine(hash_value, index_name_);
        boost::hash_combine(hash_value, server_name_);
        boost::hash_combine(hash_value, uid_);
        boost::hash_combine(hash_value, schema_);
        boost::hash_combine(hash_value, static_cast<uint8_t>(behavior_));
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
            case drop_target_kind::materialized_view:
            case drop_target_kind::macro:
                boost::hash_combine(hash_value, static_cast<hash_t>(table_oid()));
                break;
            case drop_target_kind::index:
                boost::hash_combine(hash_value, static_cast<hash_t>(namespace_oid_));
                boost::hash_combine(hash_value, static_cast<hash_t>(index_oid_));
                boost::hash_combine(hash_value, static_cast<hash_t>(table_oid()));
                break;
            case drop_target_kind::server:
            case drop_target_kind::server_cache:
                boost::hash_combine(hash_value, static_cast<hash_t>(server_oid_));
                break;
            case drop_target_kind::remote_table:
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
            case drop_target_kind::materialized_view:
                stream << "$drop_materialized_view: <oid:" << static_cast<std::uint64_t>(table_oid()) << ">";
                break;
            case drop_target_kind::macro:
                stream << "$drop_macro: <oid:" << static_cast<std::uint64_t>(table_oid()) << ">";
                break;
            case drop_target_kind::index:
                stream << "$drop_index: <oid:" << static_cast<std::uint64_t>(index_oid_) << ">";
                break;
            case drop_target_kind::server:
                stream << "$drop_server: <oid:" << static_cast<std::uint64_t>(server_oid_) << ">";
                break;
            case drop_target_kind::remote_table:
                stream << "$forget_remote_table: <oid:" << static_cast<std::uint64_t>(table_oid()) << ">";
                break;
            case drop_target_kind::server_cache:
                stream << "$forget_server_cache: <oid:" << static_cast<std::uint64_t>(server_oid_) << ">";
                break;
        }
        return stream.str();
    }

    node_drop_ptr make_node_drop(std::pmr::memory_resource* resource, drop_target_kind kind) {
        return {new node_drop_t{resource, kind}};
    }

    core::result_wrapper_t<node_ptr>
    make_node_forget_remote_table(std::pmr::memory_resource* resource,
                                  std::string server,
                                  std::vector<std::string> path) {
        if (path.size() != 2 && path.size() != 3) {
            return core::error_t{core::error_code_t::invalid_parameter,
                                 std::pmr::string{"a recorded remote table is named by its canonical path inside the "
                                                  "server: schema.table or db.schema.table",
                                                  resource}};
        }
        auto node = make_node_drop(resource, drop_target_kind::remote_table);
        node->set_relname(path.back());
        node->set_behavior(components::catalog::drop_behavior_t::cascade_);
        const bool has_db = path.size() == 3;
        node->set_dbname(has_db ? std::move(path[0]) : std::string{});
        node->set_remote_slots(std::move(server), std::move(path[has_db ? 1 : 0]));
        return node_ptr{std::move(node)};
    }

    node_ptr make_node_forget_server_cache(std::pmr::memory_resource* resource, std::string server) {
        auto node = make_node_drop(resource, drop_target_kind::server_cache);
        node->set_server_name(std::move(server));
        node->set_behavior(components::catalog::drop_behavior_t::cascade_);
        return node;
    }

} // namespace components::logical_plan
