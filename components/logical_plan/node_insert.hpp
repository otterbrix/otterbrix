#pragma once

#include "node.hpp"

#include <components/catalog/results/fk_result.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::logical_plan {

    class node_insert_t final : public node_t {
    public:
        explicit node_insert_t(std::pmr::memory_resource* resource, const collection_full_name_t& collection);

        std::pmr::vector<expressions::key_t>& key_translation();
        const std::pmr::vector<expressions::key_t>& key_translation() const;

        // Catalog metadata attached by the dispatcher's enrich pass.
        void set_outgoing_fks(std::vector<catalog::resolved_fk_t> v) { outgoing_fks_ = std::move(v); }
        void set_check_exprs(std::vector<std::string> v)              { check_exprs_  = std::move(v); }
        void set_not_null_cols(std::vector<std::string> v)            { not_null_cols_ = std::move(v); }
        void set_has_defaults(bool v)                                  { has_defaults_ = v; }

        const std::vector<catalog::resolved_fk_t>& outgoing_fks()  const { return outgoing_fks_; }
        const std::vector<std::string>&             check_exprs()   const { return check_exprs_; }
        const std::vector<std::string>&             not_null_cols() const { return not_null_cols_; }
        bool                                        has_defaults()  const { return has_defaults_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::pmr::vector<expressions::key_t> key_translation_;

        std::vector<catalog::resolved_fk_t> outgoing_fks_;
        std::vector<std::string>            check_exprs_;
        std::vector<std::string>            not_null_cols_;
        bool                                has_defaults_{false};
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
