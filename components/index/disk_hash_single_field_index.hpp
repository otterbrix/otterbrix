#pragma once

#include "disk_hash_storage.hpp"
#include "forward.hpp"
#include "index.hpp"

#include <cassert>
#include <memory>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <vector>

namespace components::index {

    // Hash index facade for disk-backed hashed indexes.
    // Keeps only txn-pending rows in memory; committed/query path is handled by disk agent.
    class disk_hash_single_field_index_t final : public index_t {
    public:
        using result_storage_t = std::pmr::vector<index_value_t>;
        using pending_row_t = std::pair<std::pmr::string, int64_t>;
        using pending_rows_t = std::pmr::vector<pending_row_t>;
        using pending_txn_map_t = std::pmr::unordered_map<uint64_t, pending_rows_t>;
        using const_iterator = result_storage_t::const_iterator;

        disk_hash_single_field_index_t(std::pmr::memory_resource* resource,
                                       std::string name,
                                       const keys_base_storage_t& keys,
                                       disk_hash_storage_ptr storage);
        ~disk_hash_single_field_index_t() override;

    private:
        class impl_t final : public index_t::iterator::iterator_impl_t {
        public:
            explicit impl_t(const_iterator iterator)
                : iterator_(iterator) {}
            index_t::iterator::reference value_ref() const final { return *iterator_; }
            iterator_impl_t* next() final {
                ++iterator_;
                return this;
            }
            bool equals(const iterator_impl_t* other) const final {
                return iterator_ == dynamic_cast<const impl_t*>(other)->iterator_;
            }
            bool not_equals(const iterator_impl_t* other) const final {
                return iterator_ != dynamic_cast<const impl_t*>(other)->iterator_;
            }
            iterator_impl_t* copy() const final { return new impl_t(*this); }

        private:
            const_iterator iterator_;
        };

        auto insert_impl(value_t, index_value_t, core::date::timezone_offset_t local_timezone) -> void final;
        auto remove_impl(value_t, core::date::timezone_offset_t local_timezone) -> void final;
        range find_impl(const value_t&, core::date::timezone_offset_t local_timezone) const final;
        range lower_bound_impl(const value_t&, core::date::timezone_offset_t local_timezone) const final;
        range upper_bound_impl(const value_t&, core::date::timezone_offset_t local_timezone) const final;
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
        void for_each_pending_insert_impl(uint64_t txn_id,
                                          const std::function<void(const value_t&, int64_t)>& fn) const final;
        void for_each_pending_delete_impl(uint64_t txn_id,
                                          const std::function<void(const value_t&, int64_t)>& fn) const final;
        void clean_memory_to_new_elements_impl(std::size_t count) final;

        disk_hash_storage_ptr disk_table_;
        mutable result_storage_t scratch_results_;
        pending_txn_map_t pending_inserts_;
        pending_txn_map_t pending_deletes_;

        value_t normalize_key(const value_t& key, core::date::timezone_offset_t local_timezone) const;
        std::string encode_key(const value_t& key, core::date::timezone_offset_t local_timezone) const;
        disk_hash_storage_t& storage_ref() const;
    };

} // namespace components::index
