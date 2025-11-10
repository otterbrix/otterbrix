#pragma once

#include "node.hpp"

namespace components::logical_plan {

    class node_function_t final : public node_t {
    public:
        explicit node_function_t(std::pmr::memory_resource* resource, std::string&& name);
        explicit node_function_t(std::pmr::memory_resource* resource,
                                 std::string&& name,
                                 std::pmr::vector<core::parameter_id_t>&& args);

        void add_argument(core::parameter_id_t arg);

        const std::string& name() const noexcept;
        const std::pmr::vector<core::parameter_id_t>& args() const noexcept;

        static node_ptr deserialize(serializer::msgpack_deserializer_t* deserializer);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const final;

        std::string name_;
        std::pmr::vector<core::parameter_id_t> args_;
    };

    using node_function_ptr = boost::intrusive_ptr<node_function_t>;

    node_function_ptr make_node_function(std::pmr::memory_resource* resource, std::string&& name);
    node_function_ptr make_node_function(std::pmr::memory_resource* resource,
                                         std::string&& name,
                                         std::pmr::vector<core::parameter_id_t>&& args);

} // namespace components::logical_plan
