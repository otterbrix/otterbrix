#pragma once

#include "node.hpp"
#include <components/compute/function.hpp>

namespace components::logical_plan {

    class node_function_t final : public node_t {
    public:
        explicit node_function_t(std::pmr::memory_resource* resource, std::string&& name);
        explicit node_function_t(std::pmr::memory_resource* resource,
                                 std::string&& name,
                                 std::pmr::vector<expressions::param_storage>&& args);

        const std::string& name() const noexcept;
        const std::pmr::vector<expressions::param_storage>& args() const noexcept;
        void add_function_uid(compute::function_uid uid);
        compute::function_uid function_uid() const;

        static boost::intrusive_ptr<node_function_t> deserialize(serializer::msgpack_deserializer_t* deserializer);

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const override;

        std::string name_;
        std::pmr::vector<expressions::param_storage> args_;
        compute::function_uid function_uid_{compute::invalid_function_uid};
    };

    using node_function_ptr = boost::intrusive_ptr<node_function_t>;

    node_function_ptr make_node_function(std::pmr::memory_resource* resource, std::string&& name);
    node_function_ptr make_node_function(std::pmr::memory_resource* resource,
                                         std::string&& name,
                                         std::pmr::vector<expressions::param_storage>&& args);

} // namespace components::logical_plan
