#pragma once

#include "expression.hpp"
#include <components/base/collection_full_name.hpp>
#include <components/compute/function.hpp>

#include <memory_resource>

namespace components::expressions {
    class function_expression_t;
    using function_expression_ptr = boost::intrusive_ptr<function_expression_t>;

    class function_expression_t final : public expression_i {
    public:
        function_expression_t(const function_expression_t&) = delete;
        function_expression_t(function_expression_t&&) noexcept = default;
        ~function_expression_t() override = default;

        function_expression_t(std::pmr::memory_resource* resource, function_qualified_name_t&& name);
        function_expression_t(std::pmr::memory_resource* resource,
                              function_qualified_name_t&& name,
                              std::pmr::vector<param_storage>&& args);

        const std::string& name() const noexcept;
        const function_qualified_name_t& full_name() const noexcept;
        std::pmr::vector<param_storage>& args() noexcept;
        const std::pmr::vector<param_storage>& args() const noexcept;
        // The function and the kernel signature (index into get_signatures()) the validation chose.
        void set_pin(compute::function_pin_t pin) noexcept;
        const compute::function_pin_t& pin() const noexcept;
        compute::function_uid function_uid() const;
        // The pinned function itself, owned as a cast_expression_t owns its cast: graphs build without a registry.
        void set_function(compute::function_ptr function) noexcept;
        const compute::function* function() const noexcept;
        // A view read: the call resolves among these alone, not among every function of its name.
        void set_pins(std::pmr::vector<compute::function_pin_t> pins);
        const std::pmr::vector<compute::function_pin_t>& pins() const noexcept;

        void set_key(const key_t& key);

        void set_distinct(bool distinct) noexcept;
        bool is_distinct() const noexcept;

        void set_star_argument(bool star) noexcept;
        bool has_star_argument() const noexcept;

    private:
        function_qualified_name_t name_;
        std::pmr::vector<param_storage> args_;
        bool distinct_{false};
        bool star_argument_{false};
        compute::function_pin_t pin_;
        compute::function_ptr function_;
        std::pmr::vector<compute::function_pin_t> pins_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        bool equal_impl(const expression_i* rhs) const override;
    };

    function_expression_ptr make_function_expression(std::pmr::memory_resource* resource,
                                                     function_qualified_name_t&& name);
    function_expression_ptr make_function_expression(std::pmr::memory_resource* resource,
                                                     function_qualified_name_t&& name,
                                                     std::pmr::vector<param_storage>&& args);
} // namespace components::expressions
