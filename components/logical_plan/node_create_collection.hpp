#pragma once

#include "node.hpp"

#include <components/table/column_definition.hpp>
#include <components/table/constraint.hpp>
#include <components/types/types.hpp>

namespace components::logical_plan {

    class node_create_collection_t final : public node_t {
    public:
        explicit node_create_collection_t(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          std::pmr::vector<types::complex_logical_type> schema = {});

        node_create_collection_t(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 std::pmr::vector<types::complex_logical_type> schema,
                                 std::vector<table::column_definition_t> column_definitions,
                                 std::vector<table::table_constraint_t> constraints);

        static boost::intrusive_ptr<node_create_collection_t>
        deserialize(serializer::msgpack_deserializer_t* deserializer);

        std::pmr::vector<types::complex_logical_type>& schema();
        const std::pmr::vector<types::complex_logical_type>& schema() const;

        const std::vector<table::column_definition_t>& column_definitions() const;
        const std::vector<table::table_constraint_t>& constraints() const;

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;
        void serialize_impl(serializer::msgpack_serializer_t* serializer) const override;

        std::pmr::vector<types::complex_logical_type> schema_;
        std::vector<table::column_definition_t> column_definitions_;
        std::vector<table::table_constraint_t> constraints_;
    };

    using node_create_collection_ptr = boost::intrusive_ptr<node_create_collection_t>;
    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           std::pmr::vector<types::complex_logical_type> schema = {});

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           std::pmr::vector<types::complex_logical_type> schema,
                                                           std::vector<table::column_definition_t> column_definitions,
                                                           std::vector<table::table_constraint_t> constraints);

} // namespace components::logical_plan
