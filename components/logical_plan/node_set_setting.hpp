#pragma once

#include "node.hpp"

#include <components/catalog/settings.hpp>

#include <string>

namespace components::logical_plan {

    class node_set_setting_t final : public node_t {
    public:
        node_set_setting_t(std::pmr::memory_resource* resource, catalog::setting_id id, std::string value);

        catalog::setting_id setting() const noexcept;
        const std::string& value() const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        catalog::setting_id setting_;
        std::string value_;
    };

    using node_set_setting_ptr = boost::intrusive_ptr<node_set_setting_t>;
    node_set_setting_ptr
    make_node_set_setting(std::pmr::memory_resource* resource, catalog::setting_id id, std::string value);

} // namespace components::logical_plan
