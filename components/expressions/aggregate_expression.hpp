#pragma once

#include "expression.hpp"
#include "function_expression.hpp"
#include "key.hpp"
#include <components/compute/function.hpp>

#include <memory_resource>

namespace components::expressions {

    class aggregate_expression_t;
    using aggregate_expression_ptr = boost::intrusive_ptr<aggregate_expression_t>;

    // A reduction marker (from N to 1 row)
    // Any number (and kinds) of expressions could be below and above
    class aggregate_expression_t final : public expression_i {
    public:
        aggregate_expression_t(const aggregate_expression_t&) = delete;
        aggregate_expression_t(aggregate_expression_t&&) noexcept = default;

        aggregate_expression_t(std::pmr::memory_resource* resource, const std::string& function_name, const key_t& key);
        aggregate_expression_t(const expression_ptr& call, const key_t& key);

        // Always a function_expression_t with aggregat efunction
        const expression_ptr& child() const noexcept { return child_; }

        const std::string& function_name() const;
        void add_function_uid(compute::function_uid uid);
        compute::function_uid function_uid() const;
        std::pmr::vector<param_storage>& params();
        const std::pmr::vector<param_storage>& params() const;

        void append_param(const param_storage& param);

        void set_distinct(bool d);
        bool is_distinct() const;

        void set_mergeable(bool m) { mergeable_ = m; }
        [[nodiscard]] bool is_mergeable() const { return mergeable_; }

    private:
        expression_ptr child_;
        // Resolved at validate from the function's is_mergeable() capability
        bool mergeable_{false};

        function_expression_t* call() noexcept;
        const function_expression_t* call() const noexcept;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        bool equal_impl(const expression_i* rhs) const override;
    };

    aggregate_expression_ptr
    make_aggregate_expression(std::pmr::memory_resource* resource, const std::string& function_name, const key_t& key);
    aggregate_expression_ptr make_aggregate_expression(std::pmr::memory_resource* resource,
                                                       const std::string& function_name);
    aggregate_expression_ptr make_aggregate_expression(std::pmr::memory_resource* resource,
                                                       const std::string& function_name,
                                                       const key_t& name,
                                                       const key_t& key);
    aggregate_expression_ptr make_aggregate_over(const expression_ptr& call, const key_t& key);

} // namespace components::expressions
