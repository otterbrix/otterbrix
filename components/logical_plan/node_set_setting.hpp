#pragma once

#include "node.hpp"

#include <components/catalog/settings.hpp>

#include <string>

namespace components::logical_plan {

    // SET, SET LOCAL and ALTER SYSTEM, with RESET and SET ... TO DEFAULT: the values as written, canonicalised when the
    // statement runs.
    class node_set_setting_t final : public node_t {
    public:
        node_set_setting_t(std::pmr::memory_resource* resource,
                           catalog::setting_scope_t scope,
                           std::pmr::vector<catalog::setting_assignment_t> assignments);

        catalog::setting_scope_t scope() const noexcept;
        const std::pmr::vector<catalog::setting_assignment_t>& assignments() const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        catalog::setting_scope_t scope_;
        std::pmr::vector<catalog::setting_assignment_t> assignments_;
    };

    using node_set_setting_ptr = boost::intrusive_ptr<node_set_setting_t>;
    node_set_setting_ptr make_node_set_setting(std::pmr::memory_resource* resource,
                                               catalog::setting_scope_t scope,
                                               std::pmr::vector<catalog::setting_assignment_t> assignments);
    node_set_setting_ptr make_node_set_setting(std::pmr::memory_resource* resource,
                                               catalog::setting_scope_t scope,
                                               catalog::setting_id id,
                                               std::string_view value);

} // namespace components::logical_plan
