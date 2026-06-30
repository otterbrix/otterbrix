#pragma once

#include "identifier_types.hpp"
#include "node.hpp"

#include <components/expressions/compare_expression.hpp>

namespace components::logical_plan {

    class node_sort_t final : public node_t {
    public:
        // Memory budget for this logical sort. Stamped by the optimizer rule
        // spill_strategy from context_storage_t::disk_config (R10) and read by
        // create_plan_sort to choose the in-memory vs external-merge-sort operator.
        // This is an ANNOTATION only — it does NOT change the logical semantics.
        enum class exec_strategy : uint8_t
        {
            in_memory,
            spill
        };

        explicit node_sort_t(std::pmr::memory_resource* resource, core::dbname_t dbname, core::relname_t relname);

        exec_strategy strategy() const noexcept;
        // Stamped by the optimizer spill_strategy rule when
        // disk_config->spill_enabled is set.
        void set_strategy(exec_strategy s) noexcept;

        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        std::string dbname_;
        std::string relname_;
        exec_strategy strategy_{exec_strategy::in_memory};
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
    };

    using node_sort_ptr = boost::intrusive_ptr<node_sort_t>;

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 core::dbname_t dbname,
                                 core::relname_t relname,
                                 const std::vector<expressions::expression_ptr>& expressions);

    node_sort_ptr make_node_sort(std::pmr::memory_resource* resource,
                                 core::dbname_t dbname,
                                 core::relname_t relname,
                                 const std::pmr::vector<expressions::expression_ptr>& expressions);

} // namespace components::logical_plan
