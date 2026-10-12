#pragma once

#include "node.hpp"
#include <components/base/identifier_types.hpp>

#include <components/catalog/catalog_oids.hpp>

namespace components::logical_plan {

    // REFRESH MATERIALIZED VIEW mv [WITH NO DATA] (PostgreSQL semantics).
    // The resolve of mv stamps resolved_metadata.view_sql (pg_rewrite.ev_action, the body SQL
    // written at CREATE MATERIALIZED VIEW time); the executor runs DELETE FROM mv and, WITH DATA,
    // INSERT INTO mv <body> in the statement's transaction. CONCURRENTLY is refused by the transformer.
    class node_refresh_matview_t final : public node_t {
    public:
        node_refresh_matview_t(std::pmr::memory_resource* resource, bool with_data);

        bool with_data() const noexcept { return with_data_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        bool with_data_{true};
    };

    using node_refresh_matview_ptr = boost::intrusive_ptr<node_refresh_matview_t>;

    node_refresh_matview_ptr make_node_refresh_matview(std::pmr::memory_resource* resource, bool with_data);

} // namespace components::logical_plan
