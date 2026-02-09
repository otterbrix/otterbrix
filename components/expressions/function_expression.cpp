#include "function_expression.hpp"
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
                                                 std::pmr::vector<param_storage>&& args)
        : expression_i(expression_group::function)
        , name_(std::move(name))
        , args_(std::move(args)) {}

    const std::string& function_expression_t::name() const noexcept { return name_; }

    std::pmr::vector<param_storage>& function_expression_t::args() noexcept { return args_; }

    const std::pmr::vector<param_storage>& function_expression_t::args() const noexcept { return args_; }

    void function_expression_t::add_function_uid(compute::function_uid uid) { function_uid_ = uid; }

    compute::function_uid function_expression_t::function_uid() const { return function_uid_; }

    expression_ptr function_expression_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto name = deserializer->deserialize_string(1);
        std::pmr::vector<param_storage> args(deserializer->resource());
        deserializer->advance_array(2);
        args.reserve(deserializer->current_array_size());
        for (size_t i = 0; i < args.capacity(); i++) {
            args.emplace_back(deserialize_param_storage(deserializer, i));
        }
        deserializer->pop_array();
        return make_function_expression(deserializer->resource(), std::move(name), std::move(args));
    }

    hash_t function_expression_t::hash_impl() const { return 0; }

    std::string function_expression_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$function: {";
        stream << "name: {\"" << name_ << "\"}, ";
        stream << "args: {";
        bool is_first = true;
        for (const auto& id : args_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << id;
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
        serializer->start_array(args_.size());
        for (const auto& arg : args_) {
            serialize_param_storage(serializer, arg);
        }
        serializer->end_array();
        serializer->end_array();
    }

    function_expression_ptr make_function_expression(std::pmr::memory_resource* resource, std::string&& name) {
        return {new function_expression_t(resource, std::move(name))};
    }

    function_expression_ptr make_function_expression(std::pmr::memory_resource* resource,
                                                     std::string&& name,
                                                     std::pmr::vector<param_storage>&& args) {
        return {new function_expression_t(resource, std::move(name), std::move(args))};
    }

} // namespace components::expressions
