#pragma once

#include "node.hpp"

#include <components/catalog/fk_info.hpp>

namespace components::logical_plan {

    class node_fk_cascade_t final : public node_t {
    public:
        node_fk_cascade_t(std::pmr::memory_resource* resource,
                          std::string dbname,
                          std::string relname,
                          catalog::fk_info_t fk);

        const catalog::fk_info_t& fk() const noexcept { return fk_; }

        // Phase 9.W/10.D: role-named accessors. FK cascade operator-feeder identity.
        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        catalog::fk_info_t fk_;
    };

    using node_fk_cascade_ptr = boost::intrusive_ptr<node_fk_cascade_t>;

} // namespace components::logical_plan
