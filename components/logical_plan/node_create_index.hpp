#pragma once

#include "node.hpp"
#include <components/expressions/key.hpp>

namespace components::logical_plan {

    using keys_base_storage_t = std::pmr::vector<components::expressions::key_t>;

    enum class index_type : uint8_t
    {
        single,
        composite,
        multikey,
        hashed,
        wildcard,
        no_valid = 255
    };

    class node_create_index_t final : public node_t {
    public:
        explicit node_create_index_t(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const std::string& name = "unnamed",
                                     index_type type = index_type::single);

        const std::string& name() const noexcept;
        index_type type() const noexcept;
        keys_base_storage_t& keys() noexcept;

        static node_ptr deserialize(serializer::msgpack_deserializer_t* deserializer);

    private:
        hash_t hash_impl() const final;
        std::string to_string_impl() const final;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const final;

        std::string name_;
        keys_base_storage_t keys_;
        index_type index_type_;
    };

    using node_create_index_ptr = boost::intrusive_ptr<node_create_index_t>;

    node_create_index_ptr make_node_create_index(std::pmr::memory_resource* resource,
                                                 const collection_full_name_t& collection,
                                                 const std::string& name = "unnamed",
                                                 index_type type = index_type::single);

} // namespace components::logical_plan