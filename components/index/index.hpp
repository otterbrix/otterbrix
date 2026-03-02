#pragma once

#include "forward.hpp"
#include <actor-zeta.hpp>
#include <components/table/row_version_manager.hpp>
#include <core/pmr.hpp>
#include <functional>

namespace components::index {

    struct index_value_t {
        int64_t row_index;
        uint64_t insert_id{0};
        uint64_t delete_id{table::NOT_DELETED_ID};

        index_value_t() = default;
        explicit index_value_t(int64_t row_index)
            : row_index(row_index)
            , insert_id(0)
            , delete_id(table::NOT_DELETED_ID) {}
        index_value_t(int64_t row_index, uint64_t insert_id, uint64_t delete_id)
            : row_index(row_index)
            , insert_id(insert_id)
            , delete_id(delete_id) {}
    };

    /// Visibility predicate mirroring table MVCC.
    /// txn_id==0 && start_time==0 → "see all committed" (no MVCC filter).
    inline bool index_entry_visible(const index_value_t& e, uint64_t start_time, uint64_t txn_id) {
        if (txn_id == 0 && start_time == 0) {
            return (e.insert_id < table::TRANSACTION_ID_START) &&
                   (e.delete_id == table::NOT_DELETED_ID || e.delete_id >= table::TRANSACTION_ID_START);
        }
        bool inserted = (e.insert_id < start_time) || (e.insert_id == txn_id);
        bool deleted =
            (e.delete_id < start_time && e.delete_id < table::TRANSACTION_ID_START) || (e.delete_id == txn_id);
        return inserted && !deleted;
    }

    class index_t {
    public:
        index_t() = delete;
        index_t(const index_t&) = delete;
        index_t& operator=(const index_t&) = delete;
        using pointer = index_t*;

        virtual ~index_t();

        class iterator_t final {
        public:
            using iterator_category = std::forward_iterator_tag;
            using value_type = index_value_t;
            using difference_type = std::ptrdiff_t;
            using pointer = const index_value_t*;
            using reference = const index_value_t&;

            class iterator_impl_t;

            explicit iterator_t(iterator_impl_t*);
            ~iterator_t();

            iterator_t(const iterator_t& other);
            iterator_t& operator=(const iterator_t& other);

            reference operator*() const;
            pointer operator->() const;
            iterator_t& operator++();
            bool operator==(const iterator_t& other) const;
            bool operator!=(const iterator_t& other) const;

            class iterator_impl_t {
            public:
                virtual ~iterator_impl_t() = default;
                virtual reference value_ref() const = 0;
                virtual iterator_impl_t* next() = 0;
                virtual bool equals(const iterator_impl_t* other) const = 0;
                virtual bool not_equals(const iterator_impl_t* other) const = 0;
                virtual iterator_impl_t* copy() const = 0;
            };

        private:
            iterator_impl_t* impl_;
        };

        using iterator = iterator_t;
        using range = std::pair<iterator, iterator>;

        void insert(value_t, index_value_t);
        void insert(value_t, int64_t row_index);
        void remove(value_t);
        range find(const value_t& value) const;
        range lower_bound(const value_t& value) const;
        range upper_bound(const value_t& value) const;
        iterator cbegin() const;
        iterator cend() const;
        auto keys() -> std::pair<keys_base_storage_t::iterator, keys_base_storage_t::iterator>;
        std::pmr::memory_resource* resource() const noexcept;
        index_type type() const noexcept;
        const std::string& name() const noexcept;

        bool is_disk() const noexcept;
        const actor_zeta::address_t& disk_agent() const noexcept;
        const actor_zeta::address_t& disk_manager() const noexcept;
        void set_disk_agent(actor_zeta::address_t agent, actor_zeta::address_t manager) noexcept;

        std::pmr::vector<int64_t> search(expressions::compare_type compare, const value_t& value) const;
        std::pmr::vector<int64_t>
        search(expressions::compare_type compare, const value_t& value, uint64_t start_time, uint64_t txn_id) const;

        void insert(value_t key, int64_t row_index, uint64_t txn_id);
        void mark_delete(value_t key, int64_t row_index, uint64_t txn_id);
        void commit_insert(uint64_t txn_id, uint64_t commit_id);
        void commit_delete(uint64_t txn_id, uint64_t commit_id);
        void revert_insert(uint64_t txn_id);
        void cleanup_versions(uint64_t lowest_active);

        // Iterate pending entries for disk mirroring (must be called before commit clears them)
        void for_each_pending_insert(uint64_t txn_id, const std::function<void(const value_t&, int64_t)>& fn) const;
        void for_each_pending_delete(uint64_t txn_id, const std::function<void(const value_t&, int64_t)>& fn) const;

        void clean_memory_to_new_elements(std::size_t count) noexcept;

    protected:
        index_t(std::pmr::memory_resource* resource,
                index_type type,
                std::string name,
                const keys_base_storage_t& keys);

        virtual void insert_impl(value_t, index_value_t) = 0;
        virtual void remove_impl(value_t value_key) = 0;
        virtual range find_impl(const value_t& value) const = 0;
        virtual range lower_bound_impl(const value_t& value) const = 0;
        virtual range upper_bound_impl(const value_t& value) const = 0;
        virtual iterator cbegin_impl() const = 0;
        virtual iterator cend_impl() const = 0;

        virtual void insert_txn_impl(value_t key, int64_t row_index, uint64_t txn_id) = 0;
        virtual void mark_delete_impl(value_t key, int64_t row_index, uint64_t txn_id) = 0;
        virtual void commit_insert_impl(uint64_t txn_id, uint64_t commit_id) = 0;
        virtual void commit_delete_impl(uint64_t txn_id, uint64_t commit_id) = 0;
        virtual void revert_insert_impl(uint64_t txn_id) = 0;
        virtual void cleanup_versions_impl(uint64_t lowest_active) = 0;
        virtual void for_each_pending_insert_impl(uint64_t txn_id,
                                                  const std::function<void(const value_t&, int64_t)>& fn) const = 0;
        virtual void for_each_pending_delete_impl(uint64_t txn_id,
                                                  const std::function<void(const value_t&, int64_t)>& fn) const = 0;

        virtual void clean_memory_to_new_elements_impl(std::size_t count) = 0;

    private:
        std::pmr::memory_resource* resource_;
        index_type type_;
        std::string name_;
        keys_base_storage_t keys_;
        actor_zeta::address_t disk_agent_{actor_zeta::address_t::empty_address()};
        actor_zeta::address_t disk_manager_{actor_zeta::address_t::empty_address()};

        friend struct index_engine_t;
    };

    using index_ptr = core::pmr::unique_ptr<index_t>;

} // namespace components::index
