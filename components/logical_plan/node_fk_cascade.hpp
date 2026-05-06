#pragma once

#include "node.hpp"

#include <components/catalog/fk_info.hpp>

namespace components::logical_plan {

    // Planner-emitted FK cascade node appended after a DELETE.
    // Carries one referencing FK constraint; the operator scans the child
    // table at runtime and applies the configured ON DELETE action.
    class node_fk_cascade_t final : public node_t {
    public:
        node_fk_cascade_t(std::pmr::memory_resource*   resource,
                          const collection_full_name_t& collection,
                          catalog::fk_info_t            fk);

        const catalog::fk_info_t& fk() const noexcept { return fk_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        catalog::fk_info_t fk_;
    };

    using node_fk_cascade_ptr = boost::intrusive_ptr<node_fk_cascade_t>;

} // namespace components::logical_plan