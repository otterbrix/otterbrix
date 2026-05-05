#pragma once

#include "node.hpp"

#include <components/vector/data_chunk.hpp>

namespace components::logical_plan {

    class node_insert_t final : public node_t {
    public:
        explicit node_insert_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        std::pmr::vector<expressions::key_t>& key_translation();
        const std::pmr::vector<expressions::key_t>& key_translation() const;

        // Catalog metadata attached by the dispatcher's enrich pass.
        void set_not_null_cols(std::vector<std::string> v) { not_null_cols_ = std::move(v); }

        const std::vector<std::string>& not_null_cols() const { return not_null_cols_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::pmr::vector<expressions::key_t> key_translation_;

        std::vector<std::string> not_null_cols_;
    };

    using node_insert_ptr = boost::intrusive_ptr<node_insert_t>;

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const components::vector::data_chunk_t& chunk);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     components::vector::data_chunk_t&& chunk);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     components::vector::data_chunk_t&& chunk,
                                     std::pmr::vector<expressions::key_t>&& key_translation);

} // namespace components::logical_plan
