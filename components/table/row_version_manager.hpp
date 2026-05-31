#pragma once

#include <cassert>
#include <cstdlib>

#include <components/vector/indexing_vector.hpp>
#include <stdexcept>
#include <vector>

namespace components::table {
    namespace storage {
        struct meta_block_pointer_t;
    } // namespace storage
    class data_table_t;
    class row_version_manager_t;

    struct delete_info;

    static constexpr uint64_t TRANSACTION_ID_START = uint64_t(4611686018427388000);      // 2^62
    static constexpr uint64_t NOT_DELETED_ID = std::numeric_limits<uint64_t>::max() - 1; // 2^64 - 1

    struct transaction_data {
        transaction_data() = default;
        transaction_data(uint64_t id, uint64_t time)
            : transaction_id(id)
            , start_time(time) {}

        uint64_t transaction_id{0};
        uint64_t start_time{0};

        // Block E ProcArray (Pass 9 dec 46): snapshot of MVCC state captured at
        // begin_transaction by transaction_manager_t::take_snapshot().
        //
        // snapshot_horizon = published_horizon_ at capture time. Any commit_id
        // greater than this is "future" relative to the snapshot and not visible.
        //
        // in_flight_snapshot = sorted commit_ids that were allocated by commit()
        // but not yet publish()-published when this txn captured its snapshot.
        // Visibility filter rejects rows whose insert_id is in this set, even if
        // the id is below the horizon — those commits become visible only to
        // snapshots taken after publish().
        //
        // Visibility filter is canonical:
        //   if (id == transaction_id)                   return true;   // self-write
        //   if (id >= TRANSACTION_ID_START)             return false;  // other-txn pending
        //   if (id > snapshot_horizon)                  return false;  // post-snapshot
        //   if (in_flight_snapshot contains id)         return false;  // in-flight at snapshot
        //   return true;
        //
        // Tests / helpers that bypass transaction_manager and construct
        // transaction_data with literal {id, time} must set snapshot_horizon to
        // a value covering the row insert_ids they expect to read (e.g.
        // UINT64_MAX for "see all committed rows", or a specific commit_id when
        // exercising horizon semantics). There is no implicit fallback —
        // constraint #5/6 (no backwards compatibility) forbids it.
        uint64_t snapshot_horizon{0};
        // Per rule 14 (no get_default_resource): the default vector is anchored
        // to null_memory_resource — empty is fine; any allocation aborts. The
        // ProcArray populates this field via move-assignment from a vector built
        // against the transaction's own pmr resource.
        std::pmr::vector<uint64_t> in_flight_snapshot{std::pmr::null_memory_resource()};
    };
    enum class chunk_info_type : uint8_t
    {
        CONSTANT_INFO,
        VECTOR_INFO,
        EMPTY_INFO
    };

    class chunk_info {
    public:
        chunk_info(int64_t start, chunk_info_type type)
            : start(start)
            , type(type) {}
        virtual ~chunk_info() = default;

        int64_t start;
        chunk_info_type type;

        virtual uint64_t indexing_vector(transaction_data transaction,
                                         vector::indexing_vector_t& indexing_vector,
                                         uint64_t max_count) = 0;
        // Block E (BLOCKER 7): committed-scan path now takes a full transaction_data
        // snapshot — same shape as indexing_vector — so DDL resolves respect
        // ProcArray published_horizon + in_flight_snapshot.
        virtual uint64_t committed_indexing_vector(const transaction_data& txn,
                                                   vector::indexing_vector_t& indexing_vector,
                                                   uint64_t max_count) = 0;
        virtual bool fetch(const transaction_data& transaction, int64_t row) = 0;
        virtual void commit_append(uint64_t commit_id, uint64_t start, uint64_t end) = 0;
        virtual uint64_t committed_deleted_count(uint64_t max_count) = 0;
        virtual bool cleanup(uint64_t lowest_transaction, std::unique_ptr<chunk_info>& result) const;

        virtual bool has_deletes() const = 0;

        template<class TARGET>
        TARGET& cast() {
            return reinterpret_cast<TARGET&>(*this);
        }

        template<class TARGET>
        const TARGET& cast() const {
            return reinterpret_cast<const TARGET&>(*this);
        }
    };

    class chunk_constant_info : public chunk_info {
    public:
        static constexpr chunk_info_type TYPE = chunk_info_type::CONSTANT_INFO;

        explicit chunk_constant_info(int64_t start);

        uint64_t insert_id;
        uint64_t delete_id;

        uint64_t indexing_vector(transaction_data transaction,
                                 vector::indexing_vector_t& indexing_vector,
                                 uint64_t max_count) override;
        uint64_t committed_indexing_vector(const transaction_data& txn,
                                           vector::indexing_vector_t& indexing_vector,
                                           uint64_t max_count) override;
        bool fetch(const transaction_data& transaction, int64_t row) override;
        void commit_append(uint64_t commit_id, uint64_t start, uint64_t end) override;
        uint64_t committed_deleted_count(uint64_t max_count) override;
        bool cleanup(uint64_t lowest_transaction, std::unique_ptr<chunk_info>& result) const override;

        bool has_deletes() const override;

    private:
        template<class OP>
        uint64_t templated_indexing_vector(const transaction_data& txn,
                                           vector::indexing_vector_t& indexing_vector,
                                           uint64_t max_count) const;
    };

    class chunk_vector_info : public chunk_info {
    public:
        static constexpr chunk_info_type TYPE = chunk_info_type::VECTOR_INFO;

        explicit chunk_vector_info(int64_t start);

        uint64_t inserted[vector::DEFAULT_VECTOR_CAPACITY];
        uint64_t insert_id;
        bool same_inserted_id;
        uint64_t deleted[vector::DEFAULT_VECTOR_CAPACITY];
        bool any_deleted;

        uint64_t indexing_vector(transaction_data transaction,
                                 vector::indexing_vector_t& indexing_vector,
                                 uint64_t max_count) override;
        uint64_t committed_indexing_vector(const transaction_data& txn,
                                           vector::indexing_vector_t& indexing_vector,
                                           uint64_t max_count) override;
        bool fetch(const transaction_data& transaction, int64_t row) override;
        void commit_append(uint64_t commit_id, uint64_t start, uint64_t end) override;
        bool cleanup(uint64_t lowest_transaction, std::unique_ptr<chunk_info>& result) const override;
        uint64_t committed_deleted_count(uint64_t max_count) override;

        void append(uint64_t start, uint64_t end, uint64_t commit_id);

        uint64_t delete_rows(uint64_t transaction_id, int64_t rows[], uint64_t count);
        void commit_delete(uint64_t commit_id, const delete_info& info);
        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id);

        bool has_deletes() const override;

    private:
        template<class OP>
        uint64_t templated_indexing_vector(const transaction_data& txn,
                                           vector::indexing_vector_t& indexing_vector,
                                           uint64_t max_count) const;
    };

    struct delete_info {
        data_table_t* table;
        row_version_manager_t* version_info;
        uint64_t vector_idx;
        uint64_t count;
        uint64_t base_row;
        bool is_consecutive;

        uint16_t* get_rows() {
            // Invariant: consecutive form has no row array; callers must check is_consecutive first.
            assert(!is_consecutive && "delete_info is consecutive - rows are not accessible");
            if (is_consecutive) {
                std::abort();
            }
            return rows;
        }
        const uint16_t* get_rows() const {
            assert(!is_consecutive && "delete_info is consecutive - rows are not accessible");
            if (is_consecutive) {
                std::abort();
            }
            return rows;
        }

    private:
        uint16_t rows[1] = {};
    };

    class row_version_manager_t {
    public:
        explicit row_version_manager_t(int64_t start) noexcept;

        int64_t start() const { return start_; }
        void set_start(int64_t start);
        uint64_t committed_deleted_count(uint64_t count);

        uint64_t indexing_vector(transaction_data transaction,
                                 uint64_t vector_idx,
                                 vector::indexing_vector_t& indexing_vector,
                                 uint64_t max_count);
        uint64_t committed_indexing_vector(const transaction_data& txn,
                                           uint64_t vector_idx,
                                           vector::indexing_vector_t& indexing_vector,
                                           uint64_t max_count);
        bool fetch(const transaction_data& transaction, uint64_t row);

        void append_version_info(transaction_data transaction,
                                 uint64_t count,
                                 uint64_t row_group_start,
                                 uint64_t row_group_end);
        void commit_append(uint64_t commit_id, uint64_t row_group_start, uint64_t count);
        void revert_append(uint64_t start_row);
        void cleanup_append(uint64_t lowest_active_transaction, uint64_t row_group_start, uint64_t count);

        uint64_t delete_rows(uint64_t vector_idx, uint64_t transaction_id, int64_t rows[], uint64_t count);
        void commit_delete(uint64_t vector_idx, uint64_t commit_id, const delete_info& info);
        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id);

    private:
        chunk_info* get_chunk_info(uint64_t vector_idx);
        chunk_vector_info& vector_info(uint64_t vector_idx);
        void fill_vector_info(uint64_t vector_idx);

        std::mutex version_lock_;
        int64_t start_;
        std::vector<std::unique_ptr<chunk_info>> vector_info_;
        bool has_changes_;
        std::vector<storage::meta_block_pointer_t> storage_pointers_;
    };

} // namespace components::table