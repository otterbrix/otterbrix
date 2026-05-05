#pragma once

#include "node.hpp"

#include <components/catalog/results/fk_result.hpp>

namespace components::logical_plan {

    // FK parent-existence check for INSERT / UPDATE on the referencing table.
    // Inserted by the planner above node_insert / node_update for each outgoing FK.
    class node_fk_check_t final : public node_t {
    public:
        explicit node_fk_check_t(std::pmr::memory_resource*         resource,
                                   const collection_full_name_t&      collection,
                                   components::catalog::resolved_fk_t fk);

        const components::catalog::resolved_fk_t& fk() const { return fk_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        components::catalog::resolved_fk_t fk_;
    };

    using node_fk_check_ptr = boost::intrusive_ptr<node_fk_check_t>;

} // namespace components::logical_plan
