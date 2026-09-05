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

        single_field_index_t(std::pmr::memory_resource*, catalog::oid_t oid, const keys_base_storage_t&);
        ~single_field_index_t() override;

    private:
        class impl_t final : public index_t::iterator::iterator_impl_t {
        public:
            explicit impl_t(const_iterator iterator);
            kind_t kind() const noexcept final { return kind_t::ram_single_field; }
            index_t::iterator::reference value_ref() const final;
            iterator_impl_t* next() final;
            bool equals(const iterator_impl_t* other) const final;
            bool not_equals(const iterator_impl_t* other) const final;
            iterator_impl_t* copy() const final;

        private:
            const_iterator iterator_;
        };

        // An ordered b-tree: every predicate is answerable, so both bounds are real.
        [[nodiscard]] bool supports_ordered_probe_impl() const noexcept final { return true; }

        auto insert_impl(value_t, index_value_t value, core::date::timezone_offset_t local_timezone) -> void final;
        auto remove_impl(value_t key, core::date::timezone_offset_t local_timezone) -> void final;
        range find_impl(const value_t& value, core::date::timezone_offset_t local_timezone) const final;
        range lower_bound_impl(const value_t& value, core::date::timezone_offset_t local_timezone) const final;
        range upper_bound_impl(const value_t& value, core::date::timezone_offset_t local_timezone) const final;
        iterator cbegin_impl() const final;
        iterator cend_impl() const final;

        void insert_txn_impl(value_t key,
                             int64_t row_index,
                             uint64_t txn_id,
                             core::date::timezone_offset_t local_timezone) final;
        void mark_delete_impl(value_t key,
                              int64_t row_index,
                              uint64_t txn_id,
                              core::date::timezone_offset_t local_timezone) final;
        void commit_insert_impl(uint64_t txn_id, uint64_t commit_id) final;
        void commit_delete_impl(uint64_t txn_id, uint64_t commit_id) final;
        void revert_insert_impl(uint64_t txn_id) final;
        void revert_delete_impl(uint64_t txn_id) final;
        void cleanup_versions_impl(uint64_t lowest_active) final;
        index_t::pending_entries_t pending_inserts_impl(uint64_t txn_id) const final;
        index_t::pending_entries_t pending_deletes_impl(uint64_t txn_id) const final;
        // Unreachable by contract, and LOUD rather than a no-op -- see index.hpp. An
        // in-memory index holds its committed and pending entries in one structure and
        // answers both from find_impl, so it is never read through a disk agent and
        // never has a txn-local half to fold back in.
        void merge_uncommitted_rows_impl(const value_t& key,
                                         uint64_t txn_id,
                                         core::date::timezone_offset_t local_timezone,
                                         std::pmr::vector<int64_t>& rows) const final;

        void clean_memory_to_new_elements_impl(std::size_t count) final;

    private:
        storage_t storage_;
        components::types::complex_logical_type stored_type_{types::logical_type::NA};

        // Pending txn tracking for O(k) commit/revert
        using pending_entry = std::pair<value_t, int64_t>; // key, row_index
        std::unordered_map<uint64_t, std::vector<pending_entry>> pending_inserts_;
        std::unordered_map<uint64_t, std::vector<pending_entry>> pending_deletes_;
    };

} // namespace components::index