#pragma once

#include <cassert>
#include <cstdlib>
#include <limits>

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
        // Full snapshot ctor — used by transaction_t::data() to construct a
        // copy from the transaction's pmr-anchored snapshot vector.
        transaction_data(uint64_t id, uint64_t time, uint64_t horizon, const std::pmr::vector<uint64_t>& in_flight)
            : transaction_id(id)
            , start_time(time)
            , snapshot_horizon(horizon)
            , in_flight_snapshot(in_flight.begin(), in_flight.end()) {}

        uint64_t transaction_id{0};
        uint64_t start_time{0};

        // MVCC snapshot captured at begin_transaction by
        // transaction_manager_t::take_snapshot().
        //
        // snapshot_horizon = published_horizon_ at capture time. A commit_id
        // greater than this committed after the snapshot and is not visible.
        //
        // in_flight_snapshot = sorted commit_ids allocated by commit() but not
        // yet publish()-published when the snapshot was taken. Rows whose
        // insert_id is in this set stay invisible even below the horizon; they
        // become visible only to snapshots taken after publish().
        // (The filter that consumes both fields is use_inserted_version in
        // row_version_manager.cpp.)
        //
        // Default = UINT64_MAX with an empty in_flight_snapshot makes a
        // default-constructed transaction_data mean "see all committed rows".
        // begin_transaction overwrites it for MVCC reads, so isolation holds;
        // catalog scans, recovery and WAL-replay paths that bypass
        // transaction_manager rely on the default for a see-all view.
        uint64_t snapshot_horizon{std::numeric_limits<uint64_t>::max()};
        // Plain std::vector, not pmr: transaction_data is a value type copied
        // and copy/move-assigned across actor boundaries. pmr allocators don't
        // propagate on copy/move assignment, so a pmr vector here would bad_alloc.
        // The snapshot is tiny (<100 ids), so global-heap value semantics is fine.
        std::vector<uint64_t> in_flight_snapshot;
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
        virtual bool fetch(const transaction_data& transaction, int64_t row) = 0;
        virtual void commit_append(uint64_t commit_id, uint64_t start, uint64_t end) = 0;
        virtual uint64_t committed_deleted_count(uint64_t max_count) = 0;
        // True when ANY stamp in [0, max_count) is not yet visible-to-all under
        // `watermark`: a pending txn id (>= TRANSACTION_ID_START) or a committed
        // id > watermark. NOT_DELETED_ID delete slots are "no delete", not stamps.
        // Feeds data_table_t::compact()'s all-or-nothing MVCC safety gate.
        virtual bool has_version_above(uint64_t watermark, uint64_t max_count) const = 0;
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
        bool fetch(const transaction_data& transaction, int64_t row) override;
        void commit_append(uint64_t commit_id, uint64_t start, uint64_t end) override;
        uint64_t committed_deleted_count(uint64_t max_count) override;
        bool has_version_above(uint64_t watermark, uint64_t max_count) const override;
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
        bool fetch(const transaction_data& transaction, int64_t row) override;
        void commit_append(uint64_t commit_id, uint64_t start, uint64_t end) override;
        bool cleanup(uint64_t lowest_transaction, std::unique_ptr<chunk_info>& result) const override;
        uint64_t committed_deleted_count(uint64_t max_count) override;
        bool has_version_above(uint64_t watermark, uint64_t max_count) const override;

        void append(uint64_t start, uint64_t end, uint64_t commit_id);

        uint64_t delete_rows(uint64_t transaction_id, int64_t rows[], uint64_t count);
        void commit_delete(uint64_t commit_id, const delete_info& info);
        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id);
        void revert_all_deletes(uint64_t txn_id);

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
        // True when any stamp in the first `count` rows is above `watermark`
        // (pending txn id or committed id newer than the visible-to-all horizon).
        bool has_version_above(uint64_t watermark, uint64_t count);

        uint64_t indexing_vector(transaction_data transaction,
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
        void revert_all_deletes(uint64_t txn_id);

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