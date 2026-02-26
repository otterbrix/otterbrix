#pragma once

#include "expression.hpp"
#include "key.hpp"
#include <memory_resource>

namespace components::expressions {

    class aggregate_expression_t;
    using aggregate_expression_ptr = boost::intrusive_ptr<aggregate_expression_t>;

    class aggregate_expression_t final : public expression_i {
    public:
        aggregate_expression_t(const aggregate_expression_t&) = delete;
        aggregate_expression_t(aggregate_expression_t&&) noexcept = default;

        aggregate_expression_t(std::pmr::memory_resource* resource, aggregate_type type, const key_t& key);

        aggregate_type type() const;
        const key_t& key() const;
        const std::pmr::vector<param_storage>& params() const;

        void append_param(const param_storage& param);

        void set_distinct(bool d) { distinct_ = d; }
        bool is_distinct() const { return distinct_; }

    private:
        aggregate_type type_;
        key_t key_;
        std::pmr::vector<param_storage> params_;
        bool distinct_{false};

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        bool equal_impl(const expression_i* rhs) const override;
    };

    aggregate_expression_ptr
    make_aggregate_expression(std::pmr::memory_resource* resource, aggregate_type type, const key_t& key);
    aggregate_expression_ptr make_aggregate_expression(std::pmr::memory_resource* resource, aggregate_type type);
    aggregate_expression_ptr make_aggregate_expression(std::pmr::memory_resource* resource,
                                                       aggregate_type type,
                                                       const key_t& name,
                                                       const key_t& key);

    aggregate_type get_aggregate_type(const std::string& key);
    bool is_aggregate_type(const std::string& key);

} // namespace components::expressions