#include "node_function.hpp"

#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

#include <sstream>

namespace components::logical_plan {

    node_function_t::node_function_t(std::pmr::memory_resource* resource, std::string&& name)
        : node_t(resource, node_type::function_t, {})
        , name_(std::move(name)) {}

    node_function_t::node_function_t(std::pmr::memory_resource* resource,
                                     std::string&& name,
                                     std::pmr::vector<expressions::param_storage>&& args)
        : node_t(resource, node_type::function_t, {})
        , name_(std::move(name))
        , args_(std::move(args)) {}

    const std::string& node_function_t::name() const noexcept { return name_; }

    const std::pmr::vector<expressions::param_storage>& node_function_t::args() const noexcept { return args_; }

    void node_function_t::add_function_uid(compute::function_uid uid) { function_uid_ = uid; }

    compute::function_uid node_function_t::function_uid() const { return function_uid_; }

    node_function_ptr node_function_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto name = deserializer->deserialize_string(1);
        std::pmr::vector<expressions::param_storage> args(deserializer->resource());
        deserializer->advance_array(2);
        args.reserve(deserializer->current_array_size());
        for (size_t i = 0; i < args.capacity(); i++) {
            args.emplace_back(expressions::deserialize_param_storage(deserializer, i));
        }
        deserializer->pop_array();
        return make_node_function(deserializer->resource(), std::move(name), std::move(args));
    }

    hash_t node_function_t::hash_impl() const { return 0; }

    std::string node_function_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$function: {";
        stream << "name: {\"" << name_ << "\"}, ";
        stream << "args: {";
        bool is_first = true;
        for (const auto& arg : args_) {
            if (is_first) {
                is_first = false;
            } else {
                stream << ", ";
            }
            stream << arg;
        }
        stream << "}}";
        return stream.str();
    }

    void node_function_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(3);
        serializer->append_enum(serializer::serialization_type::logical_node_function);
        serializer->append(name_);
        serializer->start_array(args_.size());
        for (const auto& arg : args_) {
            expressions::serialize_param_storage(serializer, arg);
        }
        serializer->end_array();
        serializer->end_array();
    }

    node_function_ptr make_node_function(std::pmr::memory_resource* resource, std::string&& name) {
        return {new node_function_t(resource, std::move(name))};
    }

    node_function_ptr make_node_function(std::pmr::memory_resource* resource,
                                         std::string&& name,
                                         std::pmr::vector<expressions::param_storage>&& args) {
        return {new node_function_t(resource, std::move(name), std::move(args))};
    }

} // namespace components::logical_plan
