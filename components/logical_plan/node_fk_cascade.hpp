#pragma once

#include "node.hpp"

#include <components/catalog/results/fk_result.hpp>

namespace components::logical_plan {

    // FK referential action (CASCADE / SET NULL) for DELETE / UPDATE on the
    // referenced table. Inserted by the planner alongside node_delete / node_update
    // for each FK that references the target table.
    class node_fk_cascade_t final : public node_t {
    public:
        explicit node_fk_cascade_t(std::pmr::memory_resource*         resource,
                                    const collection_full_name_t&      collection,
                                    components::catalog::resolved_fk_t fk);

        const components::catalog::resolved_fk_t& fk() const { return fk_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        components::catalog::resolved_fk_t fk_;
    };

    using node_fk_cascade_ptr = boost::intrusive_ptr<node_fk_cascade_t>;

} // namespace components::logical_plan
