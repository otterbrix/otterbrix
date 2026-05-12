#pragma once

#include "node.hpp"

#include <components/catalog/fk_info.hpp>
#include <components/vector/data_chunk.hpp>

namespace components::logical_plan {

    class node_insert_t final : public node_t {
    public:
        explicit node_insert_t(std::pmr::memory_resource* resource, std::string dbname, std::string relname);

        std::pmr::vector<expressions::key_t>& key_translation();
        const std::pmr::vector<expressions::key_t>& key_translation() const;

        // Catalog metadata attached by the dispatcher's enrich pass.
        void set_not_null_cols(std::vector<std::string> v) { not_null_cols_ = std::move(v); }
        const std::vector<std::string>& not_null_cols() const { return not_null_cols_; }

        void set_outgoing_fks(std::vector<catalog::fk_info_t> v) { outgoing_fks_ = std::move(v); }
        const std::vector<catalog::fk_info_t>& outgoing_fks() const { return outgoing_fks_; }

        // CHECK constraint expressions loaded from pg_constraint: (name, expr_string) pairs.
        void set_check_exprs(std::vector<std::pair<std::string, std::string>> v) { check_exprs_ = std::move(v); }
        const std::vector<std::pair<std::string, std::string>>& check_exprs() const { return check_exprs_; }

        // Phase 9.W/10.D: role-named accessors. INSERT target table identity at parser stage;
        // routing in resolved-stage code uses table_oid().
        const std::string& relname() const noexcept { return relname_; }
        const std::string& dbname() const noexcept { return dbname_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        std::string dbname_;
        std::string relname_;
        std::pmr::vector<expressions::key_t> key_translation_;

        std::vector<std::string> not_null_cols_;
        std::vector<catalog::fk_info_t> outgoing_fks_;
        std::vector<std::pair<std::string, std::string>> check_exprs_; // (name, expr)
    };

    using node_insert_ptr = boost::intrusive_ptr<node_insert_t>;

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource, std::string dbname, std::string relname);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     std::string dbname,
                                     std::string relname,
                                     const components::vector::data_chunk_t& chunk);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     std::string dbname,
                                     std::string relname,
                                     components::vector::data_chunk_t&& chunk);

    node_insert_ptr make_node_insert(std::pmr::memory_resource* resource,
                                     std::string dbname,
                                     std::string relname,
                                     components::vector::data_chunk_t&& chunk,
                                     std::pmr::vector<expressions::key_t>&& key_translation);

} // namespace components::logical_plan
