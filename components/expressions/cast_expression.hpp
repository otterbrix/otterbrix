#pragma once

#include "expression.hpp"
#include "key.hpp"

#include <components/casts/cast_function.hpp>
#include <components/types/types.hpp>

#include <memory_resource>

namespace components::expressions {

    class cast_expression_t final : public expression_i {
    public:
        cast_expression_t(const cast_expression_t&) = delete;
        cast_expression_t(cast_expression_t&&) noexcept = default;
        ~cast_expression_t() override = default;

        cast_expression_t(std::pmr::memory_resource* resource,
                          const param_storage& child,
                          const types::complex_logical_type& target,
                          casts::cast_t cast,
                          casts::cast_kind kind);

        const param_storage& child() const noexcept { return child_; }
        param_storage& child() noexcept { return child_; }

        const casts::cast_t& cast() const noexcept { return cast_; }
        casts::cast_kind kind() const noexcept { return kind_; }

        void set_cast(casts::cast_t cast) { cast_ = std::move(cast); }

    private:
        param_storage child_;
        casts::cast_t cast_;
        casts::cast_kind kind_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        bool equal_impl(const expression_i* rhs) const override;
    };

    using cast_expression_ptr = boost::intrusive_ptr<cast_expression_t>;

    cast_expression_ptr make_cast_expression(std::pmr::memory_resource* resource,
                                             const param_storage& child,
                                             const types::complex_logical_type& target,
                                             casts::cast_t cast,
                                             casts::cast_kind kind);

} // namespace components::expressions