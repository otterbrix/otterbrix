#pragma once

#include "node.hpp"

#include "identifier_types.hpp"

namespace components::logical_plan {

    class node_extension_t;
    using node_extension_ptr = boost::intrusive_ptr<node_extension_t>;

    // Opaque host data riding on the extension node. The engine NEVER looks
    // inside — the host subclasses this with whatever its factory needs
    // (backend actor address, connection key, dialect SQL, ...).
    class node_extension_payload_t : public boost::intrusive_ref_counter<node_extension_payload_t> {
    public:
        virtual ~node_extension_payload_t() = default;
    };
    using node_extension_payload_ptr = boost::intrusive_ptr<node_extension_payload_t>;

    // Extension carrier leaf — the host-customization source node. Pure DATA:
    // (dbname, relname) + opaque host payload. No operator factory here — the
    // node never builds a physical operator (that would invert layering). The
    // host registers ONE factory at engine construction (base_spaces -> dispatcher
    // -> executor -> context_storage.extension_factory); the physical-plan
    // generator's `case extension_t` calls it, reading this node's payload.
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
        node_extension_t(std::pmr::memory_resource* resource,
                         core::dbname_t dbname,
                         core::relname_t relname,
                         node_extension_payload_ptr payload);

        const std::string& dbname() const noexcept { return dbname_; }
        const std::string& relname() const noexcept { return relname_; }
        const node_extension_payload_ptr& payload() const noexcept { return payload_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        node_extension_payload_ptr payload_;
    };

    node_extension_ptr make_node_extension(std::pmr::memory_resource* resource,
                                           core::dbname_t dbname,
                                           core::relname_t relname,
                                           node_extension_payload_ptr payload);

} // namespace components::logical_plan
