#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/catalog/catalog_oids.hpp>

namespace components::logical_plan {

    // REFRESH MATERIALIZED VIEW mv [WITH NO DATA] (PostgreSQL semantics).
    // The wrapping sequence_t includes catalog_resolve_table(mv) which Pass 1
    // stamps with resolved_metadata.view_sql (from pg_rewrite.ev_action, the
    // body SQL written at CREATE MATERIALIZED VIEW time). The planner reads
    // view_sql, re-parses + re-transforms, and lowers to
    //   sequence_t(delete_t(mv, all_true), insert_t(target=mv, source=re_body_plan))
    // First iteration: concurrent is parsed but ignored (followups #3).
    class node_refresh_matview_t final : public node_t {
    public:
        node_refresh_matview_t(std::pmr::memory_resource* resource,
                               core::matviewname_t matviewname,
                               bool concurrent,
                               bool with_data);

        const std::string& matviewname() const noexcept { return matviewname_; }
        bool concurrent() const noexcept { return concurrent_; }
        bool with_data() const noexcept { return with_data_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string matviewname_;
        bool concurrent_{false};
        bool with_data_{true};
    };

    using node_refresh_matview_ptr = boost::intrusive_ptr<node_refresh_matview_t>;

    node_refresh_matview_ptr make_node_refresh_matview(std::pmr::memory_resource* resource,
                                                       core::matviewname_t matviewname,
                                                       bool concurrent,
                                                       bool with_data);

} // namespace components::logical_plan
