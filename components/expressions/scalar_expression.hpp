#pragma once

#include "expression.hpp"
#include "key.hpp"
#include <memory_resource>

namespace components::expressions {

    class scalar_expression_t;
    using scalar_expression_ptr = boost::intrusive_ptr<scalar_expression_t>;

    class scalar_expression_t : public expression_i {
    public:
        scalar_expression_t(const scalar_expression_t&) = delete;
        scalar_expression_t(scalar_expression_t&&) noexcept = default;

        scalar_expression_t(std::pmr::memory_resource* resource, scalar_type type, const key_t& key);

        scalar_type type() const;
        key_t& key();
        const key_t& key() const;
        std::pmr::vector<param_storage>& params();
        const std::pmr::vector<param_storage>& params() const;

        void append_param(const param_storage& param);

        static expression_ptr deserialize(serializer::msgpack_deserializer_t* deserializer);

    private:
        scalar_type type_;
        key_t key_;
        std::pmr::vector<param_storage> params_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        bool equal_impl(const expression_i* rhs) const final;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const final;
    };

    scalar_expression_ptr
    make_scalar_expression(std::pmr::memory_resource* resource, scalar_type type, const key_t& key);
    scalar_expression_ptr make_scalar_expression(std::pmr::memory_resource* resource, scalar_type type);
    scalar_expression_ptr
    make_scalar_expression(std::pmr::memory_resource* resource, scalar_type type, const key_t& name, const key_t& key);

    scalar_type get_scalar_type(const std::string& key);
    bool is_scalar_type(const std::string& key);

} // namespace components::expressions