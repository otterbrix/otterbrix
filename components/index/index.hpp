#pragma once

#include "forward.hpp"
#include <actor-zeta.hpp>
#include <components/table/row_version_manager.hpp>
#include <core/pmr.hpp>
#include <cstdint>
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
                // Which concrete implementation stands behind the pointer.
                //
                // equals/not_equals compare two impls' INNER iterators, which is only
                // meaningful when both impls are the same implementation. That check used
                // to be a dynamic_cast — which also answered nullptr on a mismatch, and
                // every call site dereferenced that nullptr without looking.
                //
                // Pure virtual, with no default enumerator: a default would let two
                // unrelated implementations report the same kind and then be cast into
                // each other, which is the silent wrong answer this replaces.
                enum class kind_t : uint8_t
                {
                    ram_single_field,
                    ram_hash_single_field,
                    disk_hash_single_field,
                    // Not a production backend. The fake index in components/index/test
                    // needs an identity of its own instead of borrowing a real one.
                    test_fake
                };

                virtual ~iterator_impl_t() = default;
                [[nodiscard]] virtual kind_t kind() const noexcept = 0;
                virtual reference value_ref() const = 0;
                virtual iterator_impl_t* next() = 0;
                virtual bool equals(const iterator_impl_t* other) const = 0;
                virtual bool not_equals(const iterator_impl_t* other) const = 0;
                virtual iterator_impl_t* copy() const = 0;

            protected:
                // Stops the process where the bug is when `other` is a different
                // implementation, or null: the two inner iterators are then unrelated
                // types, there is no comparison to make, and returning either answer would
                // be a fabricated one. Unconditional — std::abort() runs under NDEBUG too,
                // so this does not evaporate in a release build.
                void abort_unless_same_kind(const iterator_impl_t* other) const;
            };

        private:
            iterator_impl_t* impl_;
        };

        using iterator = iterator_t;
        using range = std::pair<iterator, iterator>;

        void insert(value_t, index_value_t, core::date::timezone_offset_t local_timezone);
        void insert(value_t, int64_t row_index, core::date::timezone_offset_t local_timezone);
        void remove(value_t, core::date::timezone_offset_t local_timezone);
        range find(const value_t& value, core::date::timezone_offset_t local_timezone) const;
        range lower_bound(const value_t& value, core::date::timezone_offset_t local_timezone) const;
        range upper_bound(const value_t& value, core::date::timezone_offset_t local_timezone) const;
        iterator cbegin() const;
        iterator cend() const;
        auto keys() -> std::pair<keys_base_storage_t::iterator, keys_base_storage_t::iterator>;
        std::pmr::memory_resource* resource() const noexcept;
        index_type type() const noexcept;
        // pg_index.indexrelid — the index's ONLY identity below the planner
        // boundary (rule 16). The human-readable name lives in pg_class and is
        // resolved to this oid exactly once, at plan time.
        catalog::oid_t oid() const noexcept;

        // Can this index answer an ORDERED probe -- lower_bound / upper_bound, and with
        // them every predicate except eq? Asked OF the index rather than guessed from its
        // type, for the same reason index_disk_t::has_txn_log() is asked: the answer is a
        // property of the implementation, and a caller that guesses gets it wrong the day
        // a new one appears.
        //
        // It exists because the two hashed implementations used to answer the question
        // with `throw "not supported"` -- a string literal, catchable only as
        // `catch (const char*)`, thrown from inside an actor coroutine whose
        // unhandled_exception() is empty, so the statement reported success over zero
        // rows. They now fail loudly instead, and this is how a caller keeps away from
        // them: manager_index_t consults it before dispatching any read.
        [[nodiscard]] bool supports_ordered_probe() const noexcept;

        // Where do this index's COMMITTED rows live -- in its own memory, or in the
        // b+tree / bitcask store owned by its disk agent, reachable only by message?
        //
        // Asked OF the index for the same reason supports_ordered_probe() is: the answer
        // is a property of the implementation. The caller used to guess it from
        // `is_disk() && type() == hashed`, which is two guesses -- an address that happens
        // to be wired, and an enumerator -- standing in for one fact the class knows.
        // A wrong guess is silent both ways round: reading a disk facade locally answers
        // from an empty structure, and sending an in-memory index's rows through an agent
        // that holds none answers nothing.
        [[nodiscard]] bool reads_through_disk_agent() const noexcept;

        bool is_disk() const noexcept;
        const actor_zeta::address_t& disk_agent() const noexcept;
        const actor_zeta::address_t& disk_manager() const noexcept;
        void set_disk_agent(actor_zeta::address_t agent, actor_zeta::address_t manager) noexcept;

        std::pmr::vector<int64_t> search(expressions::compare_type compare,
                                         const value_t& value,
                                         core::date::timezone_offset_t local_timezone) const;
        std::pmr::vector<int64_t> search(expressions::compare_type compare,
                                         const value_t& value,
                                         uint64_t start_time,
                                         uint64_t txn_id,
                                         core::date::timezone_offset_t local_timezone) const;

        void insert(value_t key, int64_t row_index, uint64_t txn_id, core::date::timezone_offset_t local_timezone);
        void mark_delete(value_t key, int64_t row_index, uint64_t txn_id, core::date::timezone_offset_t local_timezone);
        void commit_insert(uint64_t txn_id, uint64_t commit_id);
        void commit_delete(uint64_t txn_id, uint64_t commit_id);
        void revert_insert(uint64_t txn_id);
        // Mirror of revert_insert for the delete side: discards this txn's
        // PENDING delete bucket and restores each touched entry's delete_id back
        // to NOT_DELETED_ID (mark_delete only stamped delete_id=txn_id; the row
        // itself was never removed, so reverting just un-stamps it). In-memory
        // only — pending deletes never reach disk before commit_delete.
        void revert_delete(uint64_t txn_id);
        void cleanup_versions(uint64_t lowest_active);

        // One pending entry awaiting commit: the indexed key and the storage row it points at.
        struct pending_entry_t {
            value_t key;
            int64_t row_index;
        };
        using pending_entries_t = std::pmr::vector<pending_entry_t>;

        // Pending entries for disk mirroring (must be read before commit clears them).
        //
        // These RETURN the entries rather than taking a callback: the customization point below is
        // virtual, so the callable cannot be a template parameter and would have to be
        // std::function — a forbidden type that also heap-allocates for a capturing lambda. The
        // disk index materializes the entries anyway, since it stores keys encoded and has to
        // decode them to produce a value_t.
        pending_entries_t pending_inserts(uint64_t txn_id) const;
        pending_entries_t pending_deletes(uint64_t txn_id) const;

        // Fold the txn-local half of a lookup into `rows`.
        //
        // For a disk-backed index the two halves of an answer live in two different
        // places. The COMMITTED half is on disk and is read out of the index's own
        // agent, one message, one reply. The UNCOMMITTED half never reaches disk at all
        // (owner decision 16: per-txn buckets, no write-through), so it can only come
        // from here. `rows` arrives holding the disk half and leaves holding both.
        //
        // Which buckets count is the whole question, and the answer is NOT "all of
        // them": an entry of some other in-flight transaction must not appear. The
        // implementation folds in exactly the bucket of `txn_id` plus bucket 0 (already
        // committed, not yet mirrored), then removes what those same two buckets have
        // marked deleted. That replaces a visibility PREDICATE over a merge of every
        // pending transaction with two map lookups, which is also why nothing here
        // needs an insert_id / delete_id stamp.
        //
        // `compare` is the SAME predicate the agent answered, and it is a parameter
        // rather than an assumption because an ORDERED index can be asked all six. A
        // pending row keyed 3 belongs in the answer to `x < 5` and not in the answer to
        // `x = 5`; a merge that only knew how to test equality would silently drop the
        // first, which is a transaction failing to see its own write. A hashed index is
        // only ever asked eq, and its implementation says so.
        //
        // NOT called for an in-memory index: it keeps committed and pending entries in
        // one structure and answers both from find(). The implementations there say so
        // loudly rather than inheriting a no-op.
        void merge_uncommitted_rows(expressions::compare_type compare,
                                    const value_t& key,
                                    uint64_t txn_id,
                                    core::date::timezone_offset_t local_timezone,
                                    std::pmr::vector<int64_t>& rows) const;

        void clean_memory_to_new_elements(std::size_t count) noexcept;

    protected:
        index_t(std::pmr::memory_resource* resource,
                index_type type,
                catalog::oid_t oid,
                const keys_base_storage_t& keys);

        virtual void insert_impl(value_t, index_value_t, core::date::timezone_offset_t local_timezone) = 0;
        virtual void remove_impl(value_t value_key, core::date::timezone_offset_t local_timezone) = 0;
        virtual range find_impl(const value_t& value, core::date::timezone_offset_t local_timezone) const = 0;
        virtual range lower_bound_impl(const value_t& value, core::date::timezone_offset_t local_timezone) const = 0;
        virtual range upper_bound_impl(const value_t& value, core::date::timezone_offset_t local_timezone) const = 0;
        virtual iterator cbegin_impl() const = 0;
        virtual iterator cend_impl() const = 0;

        virtual void insert_txn_impl(value_t key,
                                     int64_t row_index,
                                     uint64_t txn_id,
                                     core::date::timezone_offset_t local_timezone) = 0;
        virtual void mark_delete_impl(value_t key,
                                      int64_t row_index,
                                      uint64_t txn_id,
                                      core::date::timezone_offset_t local_timezone) = 0;
        virtual void commit_insert_impl(uint64_t txn_id, uint64_t commit_id) = 0;
        virtual void commit_delete_impl(uint64_t txn_id, uint64_t commit_id) = 0;
        virtual void revert_insert_impl(uint64_t txn_id) = 0;
        virtual void revert_delete_impl(uint64_t txn_id) = 0;
        virtual void cleanup_versions_impl(uint64_t lowest_active) = 0;
        virtual pending_entries_t pending_inserts_impl(uint64_t txn_id) const = 0;
        virtual pending_entries_t pending_deletes_impl(uint64_t txn_id) const = 0;
        // Pure virtual on purpose (same reasoning as index_disk_t's backend hooks):
        // there is no default to fall into. A do-nothing default would let an index
        // whose committed rows come from a disk agent inherit "nothing pending", and a
        // transaction would stop seeing its own writes with nothing reporting it.
        virtual void merge_uncommitted_rows_impl(expressions::compare_type compare,
                                                 const value_t& key,
                                                 uint64_t txn_id,
                                                 core::date::timezone_offset_t local_timezone,
                                                 std::pmr::vector<int64_t>& rows) const = 0;

        virtual void clean_memory_to_new_elements_impl(std::size_t count) = 0;

    private:
        // Private customization point (NVI): every index states its own answer and the
        // compiler enforces that it does -- there is no default to inherit, because a
        // default `true` would put an unordered index back on the path that crashes and a
        // default `false` would silently strip range predicates off an ordered one.
        [[nodiscard]] virtual bool supports_ordered_probe_impl() const noexcept = 0;

        // Same NVI shape, same reason there is no default: `false` would route a disk
        // facade's read into an in-memory structure it does not have, and `true` would
        // send an in-memory index's read to an agent that holds none of its rows. Both
        // answer zero rows and report success.
        [[nodiscard]] virtual bool reads_through_disk_agent_impl() const noexcept = 0;

        std::pmr::memory_resource* resource_;
        index_type type_;
        catalog::oid_t oid_;
        keys_base_storage_t keys_;
        actor_zeta::address_t disk_agent_{actor_zeta::address_t::empty_address()};
        actor_zeta::address_t disk_manager_{actor_zeta::address_t::empty_address()};

        friend struct index_engine_t;
    };

    using index_ptr = core::pmr::polymorphic_unique_ptr<index_t>;

} // namespace components::index
