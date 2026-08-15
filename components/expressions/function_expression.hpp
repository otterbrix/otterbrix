#pragma once

#include "expression.hpp"
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

        function_expression_t(std::pmr::memory_resource* resource, std::string&& name);
        function_expression_t(std::pmr::memory_resource* resource,
                              std::string&& name,
                              std::pmr::vector<param_storage>&& args);

        const std::string& name() const noexcept;
        std::pmr::vector<param_storage>& args() noexcept;
        const std::pmr::vector<param_storage>& args() const noexcept;
        void add_function_uid(compute::function_uid uid);
        compute::function_uid function_uid() const;

        void set_key(const key_t& key);

        void set_distinct(bool distinct) noexcept;
        bool is_distinct() const noexcept;

        void set_star_argument(bool star) noexcept;
        bool has_star_argument() const noexcept;

    private:
        std::string name_;
        std::pmr::vector<param_storage> args_;
        bool distinct_{false};
        bool star_argument_{false};
        compute::function_uid function_uid_{compute::invalid_function_uid};

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        bool equal_impl(const expression_i* rhs) const override;
    };

    function_expression_ptr make_function_expression(std::pmr::memory_resource* resource, std::string&& name);
    function_expression_ptr make_function_expression(std::pmr::memory_resource* resource,
                                                     std::string&& name,
                                                     std::pmr::vector<param_storage>&& args);

} // namespace components::expressions
