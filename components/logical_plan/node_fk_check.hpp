#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/catalog/fk_info.hpp>

namespace components::logical_plan {

    class node_fk_check_t final : public node_t {
    public:
        node_fk_check_t(std::pmr::memory_resource* resource,
                        core::dbname_t dbname,
                        core::relname_t relname,
                        catalog::fk_info_t fk);

        const catalog::fk_info_t& fk() const noexcept { return fk_; }

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        catalog::fk_info_t fk_;
    };

    using node_fk_check_ptr = boost::intrusive_ptr<node_fk_check_t>;

} // namespace components::logical_plan
