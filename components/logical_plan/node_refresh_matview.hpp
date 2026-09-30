#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

namespace components::logical_plan {

    // REFRESH MATERIALIZED VIEW mv [WITH NO DATA] (PostgreSQL semantics).
    // The resolve of mv stamps resolved_metadata.view_sql (pg_rewrite.ev_action, the body SQL
    // written at CREATE MATERIALIZED VIEW time); the executor runs DELETE FROM mv and, WITH DATA,
    // INSERT INTO mv <body> in the statement's transaction. concurrent is parsed but ignored.
    class node_refresh_matview_t final : public node_t {
    public:
        node_refresh_matview_t(std::pmr::memory_resource* resource,
                               core::matviewname_t matviewname,
                               bool concurrent,
                               bool with_data);

        const std::string& matviewname() const noexcept { return matviewname_; }
        bool concurrent() const noexcept { return concurrent_; }
        bool with_data() const noexcept { return with_data_; }

        // Namespace the matview lives in, as written. With matviewname() it is how
        // enrich binds this node to its resolved table entry.
        const std::string& dbname() const noexcept { return dbname_; }
        void set_dbname(std::string dbname) { dbname_ = std::move(dbname); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string matviewname_;
        std::string dbname_;
        bool concurrent_{false};
        bool with_data_{true};
    };

    using node_refresh_matview_ptr = boost::intrusive_ptr<node_refresh_matview_t>;

    node_refresh_matview_ptr make_node_refresh_matview(std::pmr::memory_resource* resource,
                                                       core::matviewname_t matviewname,
                                                       bool concurrent,
                                                       bool with_data);

} // namespace components::logical_plan
