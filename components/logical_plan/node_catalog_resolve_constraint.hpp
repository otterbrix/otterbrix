#pragma once

#include "node.hpp"
#include "node_catalog_resolve_table.hpp"

#include <components/catalog/fk_info.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace components::logical_plan {

    // Pipeline FK + CHECK constraint resolution.
    // Carries a back-pointer to a sibling node_catalog_resolve_table_t (whose
    // Pass 1 stamp provides the target's table_oid). The corresponding
    // operator_resolve_constraint_t reads pg_constraint (+ pg_attribute /
    // pg_class / pg_namespace for FK metadata) and stamps the result vectors
    // on this node so enrich can read them via the plan_resolve_index.
    //
    // direction_t::outgoing — scan pg_constraint by conrelid (INSERT/UPDATE).
    //   Stamps fks() (contype='f', child=target) and check_exprs() (contype='c').
    // direction_t::referencing — scan pg_constraint by confrelid (DELETE).
    //   Stamps fks() (contype='f', parent=target) including child table info.
    class node_catalog_resolve_constraint_t final : public node_t {
    public:
        enum class direction_t : uint8_t
        {
            outgoing,
            referencing
        };

        node_catalog_resolve_constraint_t(std::pmr::memory_resource* resource,
                                          node_catalog_resolve_table_t* target,
                                          direction_t direction);

        node_catalog_resolve_table_t* target() const noexcept { return target_; }
        direction_t direction() const noexcept { return direction_; }

        const std::vector<components::catalog::fk_info_t>& fks() const noexcept { return fks_; }
        void set_fks(std::vector<components::catalog::fk_info_t> v) { fks_ = std::move(v); }

        const std::vector<std::pair<std::string, std::string>>& check_exprs() const noexcept { return check_exprs_; }
        void set_check_exprs(std::vector<std::pair<std::string, std::string>> v) { check_exprs_ = std::move(v); }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        node_catalog_resolve_table_t* target_{nullptr};
        direction_t direction_{direction_t::outgoing};
        std::vector<components::catalog::fk_info_t> fks_;
        std::vector<std::pair<std::string, std::string>> check_exprs_;
    };

    using node_catalog_resolve_constraint_ptr = boost::intrusive_ptr<node_catalog_resolve_constraint_t>;

    node_catalog_resolve_constraint_ptr
    make_node_catalog_resolve_constraint(std::pmr::memory_resource* resource,
                                         node_catalog_resolve_table_t* target,
                                         node_catalog_resolve_constraint_t::direction_t direction);

} // namespace components::logical_plan