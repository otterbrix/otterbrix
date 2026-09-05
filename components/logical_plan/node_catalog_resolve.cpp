#include "node_catalog_resolve.hpp"

#include <boost/container_hash/hash.hpp>
#include <sstream>
#include <utility>

namespace components::logical_plan {

    bool resolve_entry_t::operator==(const resolve_entry_t& other) const noexcept {
        return dbname == other.dbname && relname == other.relname && type_name == other.type_name &&
               direction == other.direction && target == other.target && names_only == other.names_only;
    }

    node_catalog_resolve_t::node_catalog_resolve_t(std::pmr::memory_resource* resource, resolve_kind kind)
        : node_t(resource, node_type::catalog_resolve_t)
        , kind_(kind)
        , entries_(resource) {}

    std::size_t node_catalog_resolve_t::add(resolve_entry_t entry) {
        for (std::size_t index = 0; index < entries_.size(); index++) {
            if (entries_[index] == entry) {
                return index;
            }
        }
        entries_.push_back(std::move(entry));
        return entries_.size() - 1;
    }

    std::size_t node_catalog_resolve_t::find(std::string_view dbname, std::string_view name) const noexcept {
        for (std::size_t index = 0; index < entries_.size(); index++) {
            const auto& entry = entries_[index];
            if (entry.dbname != dbname) {
                continue;
            }
            if (kind_ == resolve_kind::type ? entry.type_name == name : entry.relname == name) {
                return index;
            }
        }
        return resolve_entry_t::no_target;
    }

    // Fold kind_ into the hash so the per-kind resolve nodes land in distinct
    // buckets of any node-keyed container (they all share
    // node_type::catalog_resolve_t). No per-entry payload is folded.
    hash_t node_catalog_resolve_t::hash_impl() const {
        hash_t hash_value{0};
        boost::hash_combine(hash_value, static_cast<uint8_t>(kind_));
        return hash_value;
    }

    std::string node_catalog_resolve_t::to_string_impl() const {
        std::stringstream stream;
        for (const auto& entry : entries_) {
            if (stream.tellp() != std::streampos{0}) {
                stream << ", ";
            }
            switch (kind_) {
                case resolve_kind::table:
                    stream << "$catalog_resolve_table: " << entry.dbname << "." << entry.relname;
                    if (entry.namespace_oid != components::catalog::INVALID_OID) {
                        stream << " (ns_oid=" << entry.namespace_oid << ")";
                    }
                    break;
                case resolve_kind::namespace_:
                    stream << "$catalog_resolve_namespace: " << entry.dbname << " (oid=" << entry.namespace_oid << ")";
                    break;
                case resolve_kind::database:
                    stream << "$catalog_resolve_database: " << entry.dbname << " (oid=" << entry.database_oid << ")";
                    break;
                case resolve_kind::type:
                    stream << "$catalog_resolve_type: dbname: " << entry.dbname << ", type_name: " << entry.type_name
                           << ", type_oid: " << entry.type_oid;
                    break;
                case resolve_kind::constraint:
                    stream << "$catalog_resolve_constraint: "
                           << (entry.direction == resolve_direction::outgoing ? "outgoing" : "referencing")
                           << " target=" << entry.target;
                    break;
            }
        }
        return stream.str();
    }

    node_catalog_resolve_ptr make_node_catalog_resolve(std::pmr::memory_resource* resource, resolve_kind kind) {
        return boost::intrusive_ptr(new node_catalog_resolve_t{resource, kind});
    }

    node_catalog_resolve_t& catalog_resolves_t::ensure(std::pmr::memory_resource* resource, resolve_kind kind) {
        node_catalog_resolve_ptr* slot = nullptr;
        switch (kind) {
            case resolve_kind::database:
                slot = &database;
                break;
            case resolve_kind::namespace_:
                slot = &namespaces;
                break;
            case resolve_kind::table:
                slot = &tables;
                break;
            case resolve_kind::type:
                slot = &types;
                break;
            case resolve_kind::constraint:
                slot = &constraints;
                break;
        }
        if (!*slot) {
            *slot = make_node_catalog_resolve(resource, kind);
        }
        return **slot;
    }

    bool catalog_resolves_t::empty() const noexcept {
        for (const auto* slot : {&database, &namespaces, &tables, &types, &constraints}) {
            if (*slot && !(*slot)->empty()) {
                return false;
            }
        }
        return true;
    }

    const resolve_entry_t* catalog_resolves_t::namespace_entry(std::string_view dbname) const noexcept {
        if (!namespaces || dbname.empty()) {
            return nullptr;
        }
        for (const auto& entry : namespaces->entries()) {
            if (entry.dbname == dbname) {
                return &entry;
            }
        }
        return nullptr;
    }

    const resolve_entry_t* catalog_resolves_t::table_entry(std::string_view dbname,
                                                           std::string_view relname) const noexcept {
        if (!tables || relname.empty()) {
            return nullptr;
        }
        const auto index = tables->find(dbname, relname);
        return index == resolve_entry_t::no_target ? nullptr : &tables->entries()[index];
    }

    const resolve_entry_t* catalog_resolves_t::type_entry(std::string_view dbname,
                                                          std::string_view type_name) const noexcept {
        if (!types || type_name.empty()) {
            return nullptr;
        }
        const auto index = types->find(dbname, type_name);
        return index == resolve_entry_t::no_target ? nullptr : &types->entries()[index];
    }

    components::catalog::oid_t catalog_resolves_t::namespace_oid(std::string_view dbname) const noexcept {
        const auto* entry = namespace_entry(dbname);
        return entry ? entry->namespace_oid : components::catalog::INVALID_OID;
    }

    const resolved_table_metadata_t* catalog_resolves_t::table_md(std::string_view dbname,
                                                                  std::string_view relname) const noexcept {
        const auto* entry = table_entry(dbname, relname);
        return (entry && entry->table_md.has_value()) ? &entry->table_md.value() : nullptr;
    }

    const resolved_table_metadata_t* catalog_resolves_t::table_md(components::catalog::oid_t table_oid) const noexcept {
        if (!tables || table_oid == components::catalog::INVALID_OID) {
            return nullptr;
        }
        for (const auto& entry : tables->entries()) {
            if (entry.table_md.has_value() && entry.table_md->table_oid == table_oid) {
                return &entry.table_md.value();
            }
        }
        return nullptr;
    }

    const resolved_type_metadata_t* catalog_resolves_t::type_md(std::string_view dbname,
                                                                std::string_view type_name) const noexcept {
        const auto* entry = type_entry(dbname, type_name);
        return (entry && entry->type_md.has_value()) ? &entry->type_md.value() : nullptr;
    }

    const resolve_entry_t* catalog_resolves_t::constraints_for(components::catalog::oid_t table_oid,
                                                               resolve_direction direction) const noexcept {
        if (!constraints || !tables || table_oid == components::catalog::INVALID_OID) {
            return nullptr;
        }
        for (const auto& entry : constraints->entries()) {
            if (entry.direction != direction || entry.names_only || entry.target >= tables->entries().size()) {
                continue;
            }
            const auto& target_md = tables->entries()[entry.target].table_md;
            if (target_md.has_value() && target_md->table_oid == table_oid) {
                return &entry;
            }
        }
        return nullptr;
    }

    const resolve_entry_t*
    catalog_resolves_t::constraint_names_for(components::catalog::oid_t table_oid) const noexcept {
        if (!constraints || !tables || table_oid == components::catalog::INVALID_OID) {
            return nullptr;
        }
        for (const auto& entry : constraints->entries()) {
            if (entry.direction != resolve_direction::outgoing || entry.target >= tables->entries().size()) {
                continue;
            }
            const auto& target_md = tables->entries()[entry.target].table_md;
            if (target_md.has_value() && target_md->table_oid == table_oid) {
                return &entry;
            }
        }
        return nullptr;
    }

} // namespace components::logical_plan