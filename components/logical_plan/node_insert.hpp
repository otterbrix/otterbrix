#pragma once

#include "node.hpp"

#include <components/catalog/fk_info.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::logical_plan {

    class node_insert_t final : public node_t {
    public:
        explicit node_insert_t(std::pmr::memory_resource* resource);

        std::pmr::vector<expressions::key_t>& key_translation();
        const std::pmr::vector<expressions::key_t>& key_translation() const;

        std::pmr::vector<expressions::expression_ptr>& returning();
        const std::pmr::vector<expressions::expression_ptr>& returning() const;

        // Catalog metadata attached by the dispatcher's enrich pass.
        void set_not_null_cols(std::vector<std::string> v) { not_null_cols_ = std::move(v); }
        const std::vector<std::string>& not_null_cols() const { return not_null_cols_; }

        void set_outgoing_fks(std::vector<catalog::fk_info_t> v) { outgoing_fks_ = std::move(v); }
        const std::vector<catalog::fk_info_t>& outgoing_fks() const { return outgoing_fks_; }

        // CHECK constraint expressions loaded from pg_constraint: (name, expr_string) pairs.
        void set_check_exprs(std::vector<std::pair<std::string, std::string>> v) { check_exprs_ = std::move(v); }
        const std::vector<std::pair<std::string, std::string>>& check_exprs() const { return check_exprs_; }

        // Fixed-ARRAY columns that are NOT NULL and have no DEFAULT: (column, declared size).
        // A value shorter than the size cannot fill the array and has no default to pad
        // from, so it must be rejected with an error before the append rather than silently
        // dropped. Validated per column at execution time by operator_check_constraint.
        void set_array_size_reqs(std::vector<std::pair<std::string, uint64_t>> v) { array_size_reqs_ = std::move(v); }
        const std::vector<std::pair<std::string, uint64_t>>& array_size_reqs() const { return array_size_reqs_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::pmr::vector<expressions::key_t> key_translation_;
        std::pmr::vector<expressions::expression_ptr> returning_;

        std::vector<std::string> not_null_cols_;
        std::vector<catalog::fk_info_t> outgoing_fks_;
        std::vector<std::pair<std::string, std::string>> check_exprs_;  // (name, expr)
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs_; // (name, declared array size)
    };

    using node_insert_ptr = boost::intrusive_ptr<node_insert_t>;

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const components::vector::data_chunk_t& chunk);
    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const components::vector::data_chunk_t& chunk);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource, components::vector::data_chunk_t&& chunk);
    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     components::vector::data_chunk_t&& chunk);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     components::vector::data_chunk_t&& chunk,
                                     std::pmr::vector<expressions::key_t>&& key_translation);
    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     components::vector::data_chunk_t&& chunk,
                                     std::pmr::vector<expressions::key_t>&& key_translation);

} // namespace components::logical_plan
