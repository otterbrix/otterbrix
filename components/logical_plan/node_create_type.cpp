#include "node_create_type.hpp"
#include <components/serialization/deserializer.hpp>
#include <components/serialization/serializer.hpp>
#include <components/types/logical_value.hpp>

namespace components::logical_plan {

    node_create_type_t::node_create_type_t(std::pmr::memory_resource* resource, types::complex_logical_type&& type)
        : node_t(resource, node_type::create_type_t, {})
        , type_(std::move(type)) {}

    types::complex_logical_type& node_create_type_t::type() noexcept { return type_; }

    const types::complex_logical_type& node_create_type_t::type() const noexcept { return type_; }

    node_create_type_ptr node_create_type_t::deserialize(serializer::msgpack_deserializer_t* deserializer) {
        deserializer->advance_array(1);
        auto type = types::complex_logical_type::deserialize(deserializer);
        deserializer->pop_array();
        return make_node_create_type(deserializer->resource(), std::move(type));
    }

    hash_t node_create_type_t::hash_impl() const { return 0; }

    std::string node_create_type_t::to_string_impl() const {
        std::stringstream stream;
        stream << "$create_type: name: " << type_.type_name() << ", fields:[ ";
        if (type_.type() == types::logical_type::ENUM) {
            const auto& entries = static_cast<const types::enum_logical_type_extension*>(type_.extension())->entries();
            for (const auto& entry : entries) {
                stream << entry.type().alias() << '=' << entry.value<int>() << ' ';
            }
        } else {
            for (const auto& entry : type_.child_types()) {
                stream << entry.alias() << ' ';
            }
        }
        stream << "]";
        return stream.str();
    }

    void node_create_type_t::serialize_impl(serializer::msgpack_serializer_t* serializer) const {
        serializer->start_array(2);
        serializer->append_enum(serializer::serialization_type::logical_node_create_type);
        type_.serialize(serializer);
        serializer->end_array();
    }

    node_create_type_ptr make_node_create_type(std::pmr::memory_resource* resource,
                                               types::complex_logical_type&& type) {
        return {new node_create_type_t{resource, std::move(type)}};
    }

} // namespace components::logical_plan