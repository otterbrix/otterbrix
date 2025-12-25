#include "node_drop_type.hpp"
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>

namespace components::logical_plan {

    node_drop_type_t::node_drop_type_t(std::pmr::memory_resource* resource, std::string&& name)
        : node_t(resource, node_type::drop_type_t, {})
        , name_(std::move(name)) {}

    const std::string& node_drop_type_t::name() const noexcept { return name_; }

    node_drop_type_ptr node_drop_type_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        auto name = deserializer->deserialize_string(1);
        return make_node_drop_type(deserializer->resource(), std::move(name));
    }

    hash_t node_drop_type_t::hash_impl() const { return 0; }

    std::string node_drop_type_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$drop_type: name: " << name_;
        return stream.str();
    }

    void node_drop_type_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(2);
        serializer->append_enum(serializer::serialization_type::logical_node_drop_type);
        serializer->append(name_);
        serializer->end_array();
    }

    node_drop_type_ptr make_node_drop_type(std::pmr::memory_resource* resource, std::string&& name) {
        return {new node_drop_type_t{resource, std::move(name)}};
    }

} // namespace components::logical_plan