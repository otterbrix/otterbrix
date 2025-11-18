#pragma once

#include "expression.hpp"
#include <memory_resource>

namespace components::expressions {

    class function_expression_t;
    using function_expression_ptr = boost::intrusive_ptr<function_expression_t>;

    class function_expression_t : public expression_i {
    public:
        function_expression_t(const function_expression_t&) = delete;
        function_expression_t(function_expression_t&&) noexcept = default;
        ~function_expression_t() final = default;

        function_expression_t(std::pmr::memory_resource* resource, std::string&& name);
        function_expression_t(std::pmr::memory_resource* resource,
                              std::string&& name,
                              std::pmr::vector<core::parameter_id_t>&& args);

        const std::string& name() const noexcept;
        const std::pmr::vector<core::parameter_id_t>& args() const noexcept;

        static expression_ptr deserialize(serializer::msgpack_deserializer_t* deserializer);

    private:
        std::string name_;
        std::pmr::vector<core::parameter_id_t> args_;

        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        bool equal_impl(const expression_i* rhs) const final;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const final;
    };

    function_expression_ptr make_function_expression(std::pmr::memory_resource* resource, std::string&& name);
    function_expression_ptr make_function_expression(std::pmr::memory_resource* resource,
                                                     std::string&& name,
                                                     std::pmr::vector<core::parameter_id_t>&& args);

} // namespace components::expressions
