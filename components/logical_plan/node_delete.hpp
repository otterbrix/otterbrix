#pragma once

#include "node.hpp"
#include "node_limit.hpp"
#include "node_match.hpp"

#include <components/catalog/fk_info.hpp>

namespace components::logical_plan {

    class node_delete_t final : public node_t {
    public:
        explicit node_delete_t(std::pmr::memory_resource* resource,
                               const collection_full_name_t& collection_to,
                               const collection_full_name_t& collection_from,
                               const node_match_ptr& match,
                               const node_limit_ptr& limit);

        const collection_full_name_t& collection_from() const;

        // FK referencing metadata: FKs where this table is the parent.
        // Populated by enrich_plan when the table has referencing FK constraints.
        void set_referencing_fks(std::vector<catalog::fk_info_t> v) { referencing_fks_ = std::move(v); }
        const std::vector<catalog::fk_info_t>& referencing_fks() const { return referencing_fks_; }

    private:
        hash_t hash_impl() const override;
        std::string to_string_impl() const override;

        collection_full_name_t collection_from_;
        std::vector<catalog::fk_info_t> referencing_fks_;
    };

    using node_delete_ptr = boost::intrusive_ptr<node_delete_t>;

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection,
                                          const node_match_ptr& match);

    node_delete_ptr make_node_delete_many(std::pmr::memory_resource* resource,
                                          const collection_full_name_t& collection_to,
                                          const collection_full_name_t& collection_from,
                                          const node_match_ptr& match);

    node_delete_ptr make_node_delete_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection,
                                         const node_match_ptr& match);

    node_delete_ptr make_node_delete_one(std::pmr::memory_resource* resource,
                                         const collection_full_name_t& collection_to,
                                         const collection_full_name_t& collection_from,
                                         const node_match_ptr& match);

    node_delete_ptr make_node_delete(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit);

    node_delete_ptr make_node_delete(std::pmr::memory_resource* resource,
                                     const collection_full_name_t& collection_to,
                                     const collection_full_name_t& collection_from,
                                     const node_match_ptr& match,
                                     const node_limit_ptr& limit);

} // namespace components::logical_plan