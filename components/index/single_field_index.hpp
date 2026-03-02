#pragma once

#include <memory>
#include <unordered_map>

#include <core/btree/btree.hpp>

#include "forward.hpp"
#include "index.hpp"

namespace components::index {

    class single_field_index_t final : public index_t {
    public:
        using comparator_t = std::less<value_t>;
        using storage_t = core::pmr::btree::multi_btree_t<value_t, index_value_t, comparator_t>;
        using const_iterator = storage_t::const_iterator;

        single_field_index_t(std::pmr::memory_resource*, std::string name, const keys_base_storage_t&);
        ~single_field_index_t() override;

    private:
        class impl_t final : public index_t::iterator::iterator_impl_t {
        public:
            explicit impl_t(const_iterator iterator);
            index_t::iterator::reference value_ref() const final;
            iterator_impl_t* next() final;
            bool equals(const iterator_impl_t* other) const final;
            bool not_equals(const iterator_impl_t* other) const final;
            iterator_impl_t* copy() const final;

        private:
            const_iterator iterator_;
        };

        auto insert_impl(value_t, index_value_t value) -> void final;
        auto remove_impl(value_t key) -> void final;
        range find_impl(const value_t& value) const final;
        range lower_bound_impl(const value_t& value) const final;
        range upper_bound_impl(const value_t& value) const final;
        iterator cbegin_impl() const final;
        iterator cend_impl() const final;

        void insert_txn_impl(value_t key, int64_t row_index, uint64_t txn_id) final;
        void mark_delete_impl(value_t key, int64_t row_index, uint64_t txn_id) final;
        void commit_insert_impl(uint64_t txn_id, uint64_t commit_id) final;
        void commit_delete_impl(uint64_t txn_id, uint64_t commit_id) final;
        void revert_insert_impl(uint64_t txn_id) final;
        void cleanup_versions_impl(uint64_t lowest_active) final;
        void for_each_pending_insert_impl(uint64_t txn_id,
                                          const std::function<void(const value_t&, int64_t)>& fn) const final;
        void for_each_pending_delete_impl(uint64_t txn_id,
                                          const std::function<void(const value_t&, int64_t)>& fn) const final;

        void clean_memory_to_new_elements_impl(std::size_t count) final;

    private:
        storage_t storage_;

        // Pending txn tracking for O(k) commit/revert
        using pending_entry = std::pair<value_t, int64_t>; // key, row_index
        std::unordered_map<uint64_t, std::vector<pending_entry>> pending_inserts_;
        std::unordered_map<uint64_t, std::vector<pending_entry>> pending_deletes_;
    };

} // namespace components::index