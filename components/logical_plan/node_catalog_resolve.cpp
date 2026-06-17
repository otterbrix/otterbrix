#include "node_catalog_resolve.hpp"

#include <boost/container_hash/hash.hpp>
#include <sstream>
#include <utility>

namespace components::logical_plan {

    node_catalog_resolve_t::node_catalog_resolve_t(std::pmr::memory_resource* resource, resolve_kind kind)
        : node_t(resource, node_type::catalog_resolve_t)
        , kind_(kind) {}

    // The five former per-target resolve nodes differed by node_type and so
    // always landed in distinct buckets. Now that they share
    // node_type::catalog_resolve_t, fold kind_ into hash_impl() to preserve the
    // distinction. The pre-merge nodes all returned a constant 0 hash_impl, so
    // no per-field payload is folded here.
    hash_t node_catalog_resolve_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, static_cast<uint8_t>(kind_));
        return hash_value;
    }

    std::string node_catalog_resolve_t::to_string_impl() const {
        std::stringstream stream;
        switch (kind_) {
            case resolve_kind::table:
                stream << "$catalog_resolve_table: " << dbname_ << "." << relname_;
                if (namespace_oid_ != components::catalog::INVALID_OID) {
                    stream << " (ns_oid=" << namespace_oid_ << ")";
                }
                break;
            case resolve_kind::namespace_:
                stream << "$catalog_resolve_namespace: " << dbname_ << " (oid=" << namespace_oid_ << ")";
                break;
            case resolve_kind::database:
                stream << "$catalog_resolve_database: " << dbname_ << " (oid=" << database_oid_ << ")";
                break;
            case resolve_kind::type:
                stream << "$catalog_resolve_type: dbname: " << dbname_ << ", type_name: " << type_name_
                       << ", type_oid: " << type_oid_;
                break;
            case resolve_kind::constraint:
                stream << "$catalog_resolve_constraint: ";
                stream << (direction_ == resolve_direction::outgoing ? "outgoing" : "referencing");
                if (target_) {
                    stream << " target=" << target_->dbname() << "." << target_->relname();
                }
                break;
        }
        return stream.str();
    }

    node_catalog_resolve_ptr make_node_catalog_resolve_table(std::pmr::memory_resource* resource,
                                                             core::dbname_t dbname,
                                                             core::relname_t relname) {
        auto node = boost::intrusive_ptr(new node_catalog_resolve_t{resource, resolve_kind::table});
        node->set_dbname(std::move(dbname));
        node->set_relname(std::move(relname));
        return node;
    }

    node_catalog_resolve_ptr make_node_catalog_resolve_namespace(std::pmr::memory_resource* resource,
                                                                 core::dbname_t dbname) {
        auto node = boost::intrusive_ptr(new node_catalog_resolve_t{resource, resolve_kind::namespace_});
        node->set_dbname(std::move(dbname));
        return node;
    }

    node_catalog_resolve_ptr make_node_catalog_resolve_database(std::pmr::memory_resource* resource,
                                                                core::dbname_t dbname) {
        auto node = boost::intrusive_ptr(new node_catalog_resolve_t{resource, resolve_kind::database});
        node->set_dbname(std::move(dbname));
        return node;
    }

    node_catalog_resolve_ptr make_node_catalog_resolve_type(std::pmr::memory_resource* resource,
                                                            core::dbname_t dbname,
                                                            core::typename_t type_name) {
        auto node = boost::intrusive_ptr(new node_catalog_resolve_t{resource, resolve_kind::type});
        node->set_dbname(std::move(dbname));
        node->set_type_name(std::move(type_name));
        return node;
    }

    node_catalog_resolve_ptr make_node_catalog_resolve_constraint(std::pmr::memory_resource* resource,
                                                                  node_catalog_resolve_t* target,
                                                                  resolve_direction direction) {
        auto node = boost::intrusive_ptr(new node_catalog_resolve_t{resource, resolve_kind::constraint});
        node->set_target(target);
        node->set_direction(direction);
        return node;
    }

} // namespace components::logical_plan
