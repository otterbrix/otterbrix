#pragma once

#include "node.hpp"

#include "identifier_types.hpp"

namespace components::logical_plan {

    class node_extension_t;
    using node_extension_ptr = boost::intrusive_ptr<node_extension_t>;

    // Extension carrier leaf — the host-customization source node. Pure DATA:
    // just its (dbname, relname) logical identity. It carries NO host state and
    // NO operator factory — a logical-plan node is not a storage slot. The host
    // keeps its own per-source runtime data in its own structures, keyed by this
    // node's identity ((db, rel) or the resolved oid); the physical-plan
    // generator's injected create_plan rule reads that identity to build the
    // host operator.
    //
    // Identity is a REGISTERED CATALOG TABLE: the host registers the external
    // table in the engine catalog (create_collection) and gives the node its
    // (dbname, relname). The node is then resolved / typed / named EXACTLY like
    // any table — the catalog is the single source of truth for its schema and
    // its display name (shown by EXPLAIN via the resolved oid). The ONE thing
    // that differs from a normal table is LOWERING: a host operator instead of a
    // disk scan.
    class node_extension_t final : public node_t {
    public:
        node_extension_t(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname);

        const std::string& dbname() const noexcept { return dbname_; }
        const std::string& relname() const noexcept { return relname_; }

    private:
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
    };

    node_extension_ptr
    make_node_extension(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname);

} // namespace components::logical_plan
