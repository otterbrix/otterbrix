#pragma once

#include "node.hpp"

#include <components/catalog/fk_info.hpp>

namespace components::logical_plan {

    // Planner-emitted FK enforcement node wrapping an INSERT or UPDATE.
    // Carries one outgoing FK constraint; the operator performs a key lookup
    // on the parent table at runtime to verify referential integrity.
    class node_fk_check_t final : public node_t {
    public:
        node_fk_check_t(std::pmr::memory_resource*   resource,
                        const collection_full_name_t& collection,
                        catalog::fk_info_t            fk);

        const catalog::fk_info_t& fk() const noexcept { return fk_; }

    private:
        hash_t      hash_impl()      const override;
        std::string to_string_impl() const override;

        catalog::fk_info_t fk_;
    };

    using node_fk_check_ptr = boost::intrusive_ptr<node_fk_check_t>;

} // namespace components::logical_plan