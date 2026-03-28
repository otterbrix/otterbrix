#pragma once

#include "node.hpp"

#include <components/table/column_definition.hpp>
#include <components/table/constraint.hpp>
#include <components/types/types.hpp>
#include <string>
#include <unordered_set>

namespace components::logical_plan {

    class node_create_collection_t final : public node_t {
    public:
        explicit node_create_collection_t(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          bool disk_storage = false,
                                          uint64_t sparse_threshold = 0);

        explicit node_create_collection_t(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          bool disk_storage,
                                          uint64_t sparse_threshold,
                                          std::unordered_set<std::string> pinned_columns);

        node_create_collection_t(std::pmr::memory_resource* resource,
                                 const collection_full_name_t& collection,
                                 std::vector<table::column_definition_t> column_definitions,
                                 std::vector<table::table_constraint_t> constraints,
                                 bool disk_storage = false);

        std::pmr::vector<types::complex_logical_type> schema() const;

        std::vector<table::column_definition_t>& column_definitions();
        const std::vector<table::column_definition_t>& column_definitions() const;
        const std::vector<table::table_constraint_t>& constraints() const;

        bool is_disk_storage() const { return disk_storage_; }
        uint64_t sparse_threshold() const { return sparse_threshold_; }
        const std::unordered_set<std::string>& pinned_columns() const { return pinned_columns_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::vector<table::column_definition_t> column_definitions_;
        std::vector<table::table_constraint_t> constraints_;
        bool disk_storage_{false};
        uint64_t sparse_threshold_{0};
        std::unordered_set<std::string> pinned_columns_;
    };

    using node_create_collection_ptr = boost::intrusive_ptr<node_create_collection_t>;
    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           uint64_t sparse_threshold = 0);

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           uint64_t sparse_threshold,
                                                           std::unordered_set<std::string> pinned_columns);

    node_create_collection_ptr make_node_create_collection(std::pmr::memory_resource* resource,
                                                           const collection_full_name_t& collection,
                                                           std::vector<table::column_definition_t> column_definitions,
                                                           std::vector<table::table_constraint_t> constraints,
                                                           bool disk_storage = false);

} // namespace components::logical_plan
