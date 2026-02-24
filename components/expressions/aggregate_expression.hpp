#pragma once

#include "expression.hpp"
#include "key.hpp"
#include <components/compute/function.hpp>

#include <memory_resource>

namespace components::expressions {

    class aggregate_expression_t;
    using aggregate_expression_ptr = boost::intrusive_ptr<aggregate_expression_t>;

    class aggregate_expression_t final : public expression_i {
    public:
        aggregate_expression_t(const aggregate_expression_t&) = delete;
        aggregate_expression_t(aggregate_expression_t&&) noexcept = default;

        aggregate_expression_t(std::pmr::memory_resource* resource, const std::string& function_name, const key_t& key);

        const key_t& key() const;
        const std::string& function_name() const;
        void add_function_uid(compute::function_uid uid);
        compute::function_uid function_uid() const;
        std::pmr::vector<param_storage>& params();
        const std::pmr::vector<param_storage>& params() const;

        void append_param(const param_storage& param);

        static expression_ptr deserialize(serializer::msgpack_deserializer_t* deserializer);

    private:
        std::string function_name_;
        compute::function_uid function_uid_{compute::invalid_function_uid};
        key_t key_;
        std::pmr::vector<param_storage> params_;

        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        bool equal_impl(const expression_i* rhs) const override;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const override;
    };

    aggregate_expression_ptr
    make_aggregate_expression(std::pmr::memory_resource* resource, const std::string& function_name, const key_t& key);
    aggregate_expression_ptr make_aggregate_expression(std::pmr::memory_resource* resource,
                                                       const std::string& function_name);
    aggregate_expression_ptr make_aggregate_expression(std::pmr::memory_resource* resource,
                                                       const std::string& function_name,
                                                       const key_t& name,
                                                       const key_t& key);

} // namespace components::expressions