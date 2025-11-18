#include "function_expression.hpp"
#include <boost/container_hash/hash.hpp>
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>
#include <sstream>

namespace components::expressions {

    function_expression_t::function_expression_t(std::pmr::memory_resource* resource, std::string&& name)
        : expression_i(expression_group::function)
        , name_(std::move(name))
        , args_(resource) {}

    function_expression_t::function_expression_t(std::pmr::memory_resource*,
                                                 std::string&& name,
                                                 std::pmr::vector<core::parameter_id_t>&& args)
        : expression_i(expression_group::function)
        , name_(std::move(name))
        , args_(std::move(args)) {}

    const std::string& function_expression_t::name() const noexcept { return name_; }

    const std::pmr::vector<core::parameter_id_t>& function_expression_t::args() const noexcept { return args_; }

    expression_ptr function_expression_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto name = deserializer->deserialize_string(1);
        auto args = deserializer->deserialize_param_ids(2);
        return make_function_expression(deserializer->resource(), std::move(name), std::move(args));
    }

    hash_t function_expression_t::hash_impl() const { return 0; }

    std::string function_expression_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$function: {";
        stream << "name: {\"" << name_ << "\"}, ";
        stream << "args: {";
        bool is_first = true;
        for (const auto id : args_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << "#" << std::to_string(id);
        }
        stream << "}}";
        return stream.str();
    }

    bool function_expression_t::equal_impl(const expression_i* rhs) const {
        auto* other = static_cast<const function_expression_t*>(rhs);
        return name_ == other->name_ && args_ == other->args_;
    }

    void function_expression_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(3);
        serializer->append_enum(serializer::serialization_type::expression_function);
        serializer->append(name_);
        serializer->append(args_);
        serializer->end_array();
    }

    function_expression_ptr make_function_expression(std::pmr::memory_resource* resource, std::string&& name) {
        return {new function_expression_t(resource, std::move(name))};
    }

    function_expression_ptr make_function_expression(std::pmr::memory_resource* resource,
                                                     std::string&& name,
                                                     std::pmr::vector<core::parameter_id_t>&& args) {
        return {new function_expression_t(resource, std::move(name), std::move(args))};
    }

} // namespace components::expressions
