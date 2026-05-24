#pragma once

#include "forward.hpp"
#include "disk_hash_storage.hpp"
#include "index.hpp"

#include <filesystem>
#include <memory_resource>
#include <string>
#include <vector>

namespace components::index {

    // Hash index facade for disk-backed hashed indexes.
    // Keeps only txn-pending rows in memory; committed/query path is handled by disk agent.
    class disk_hash_single_field_index_t final : public index_t {
    public:
        using result_storage_t = std::pmr::vector<index_value_t>;
        using const_iterator = result_storage_t::const_iterator;

        disk_hash_single_field_index_t(std::pmr::memory_resource* resource, std::string name, const keys_base_storage_t& keys);
        ~disk_hash_single_field_index_t() override;
        void configure_storage(const std::filesystem::path& file_path);

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

        auto insert_impl(value_t, index_value_t) -> void final;
        auto remove_impl(value_t) -> void final;
        range find_impl(const value_t&) const final;
        range lower_bound_impl(const value_t&) const final;
        range upper_bound_impl(const value_t&) const final;
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

        std::unique_ptr<disk_hash_storage_t> disk_table_;
        mutable result_storage_t scratch_results_;

        std::string encode_key(const value_t& key) const;

    public:
        void set_disk_storage(std::unique_ptr<disk_hash_storage_t> storage);
    };

} // namespace components::index
