#pragma once

#include "node.hpp"

#include <string>

namespace components::logical_plan {

    // DEALLOCATE [PREPARE] name | ALL. The executor stamps whether the session has a statement of that name.
    class node_deallocate_t final : public node_t {
    public:
        node_deallocate_t(std::pmr::memory_resource* resource, std::string name, bool all);

        const std::string& name() const noexcept;
        bool all() const noexcept;

        void set_exists(bool exists) noexcept;
        bool exists() const noexcept;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string name_;
        bool all_;
        bool exists_{false};
    };

    using node_deallocate_ptr = boost::intrusive_ptr<node_deallocate_t>;
    node_deallocate_ptr make_node_deallocate(std::pmr::memory_resource* resource, std::string name);
    node_deallocate_ptr make_node_deallocate_all(std::pmr::memory_resource* resource);

} // namespace components::logical_plan
