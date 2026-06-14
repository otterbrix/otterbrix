#pragma once

#include "storage.hpp"
#include <algorithm>
#include <cstdlib>
#include <actor-zeta/actor/basic_actor.hpp>
#include <actor-zeta/actor/dispatch.hpp>
#include <actor-zeta/actor/dispatch_traits.hpp>
#include <actor-zeta/scheduler/sharing_scheduler.hpp>
#include <actor-zeta/send.hpp>
#include <actor-zeta/spawn.hpp>
#include <components/table/data_table.hpp>
#include <components/table/table_state.hpp>
#include <components/vector/vector_operations.hpp>
#include <memory_resource>
#include <mutex>

namespace components::storage {

    namespace detail {

        class parallel_scan_worker_t final : public actor_zeta::actor::basic_actor<parallel_scan_worker_t> {
        public:
            parallel_scan_worker_t(std::pmr::memory_resource* resource, table::data_table_t* table)
                : actor_zeta::actor::basic_actor<parallel_scan_worker_t>(resource)
                , resource_(resource)
                , table_(table) {}

            std::pmr::memory_resource* resource() const noexcept { return resource_; }

            actor_zeta::unique_future<std::pmr::vector<vector::data_chunk_t>>
            scan_row_group(std::vector<table::storage_index_t> column_ids,
                           uint64_t row_group_idx,
                           const table::table_filter_t* filter,
                           std::vector<size_t> projected_cols,
                           bool row_ids_only,
                           table::transaction_data txn) {
                std::pmr::vector<vector::data_chunk_t> batches{resource_};
                if (!table_) {
                    co_return std::move(batches);
                }

                auto types = table_->copy_types(resource_);
                const std::vector<size_t>* projected_ptr =
                    row_ids_only ? &projected_cols : (projected_cols.empty() ? nullptr : &projected_cols);
                table_->scan_row_group_batched(row_group_idx,
                                               column_ids,
                                               filter,
                                               types,
                                               projected_ptr,
                                               batches,
                                               txn,
                                               resource_);
                co_return std::move(batches);
            }

            using dispatch_traits = actor_zeta::dispatch_traits<&parallel_scan_worker_t::scan_row_group>;

            actor_zeta::behavior_t behavior(actor_zeta::mailbox::message* msg) {
                if (msg->command() == actor_zeta::msg_id<parallel_scan_worker_t, &parallel_scan_worker_t::scan_row_group>) {
                    co_await actor_zeta::dispatch(this, &parallel_scan_worker_t::scan_row_group, msg);
                }
            }

        private:
            std::pmr::memory_resource* resource_;
            table::data_table_t* table_;
        };

        using parallel_scan_worker_ptr = std::unique_ptr<parallel_scan_worker_t, actor_zeta::pmr::deleter_t>;

    } // namespace detail

    class table_storage_adapter_t final : public storage_t {
    public:
        explicit table_storage_adapter_t(table::data_table_t& table,
                                         std::pmr::memory_resource* resource,
                                         actor_zeta::scheduler::sharing_scheduler* scheduler = nullptr,
                                         std::function<void()>* progress_fn = nullptr)
            : table_(table)
            , resource_(resource)
            , scheduler_(scheduler)
            , progress_fn_(progress_fn) {}

        std::pmr::vector<types::complex_logical_type> types() const override { return table_.copy_types(); }

        const std::vector<table::column_definition_t>& columns() const override { return table_.columns(); }

        size_t column_count() const override { return table_.column_count(); }

        bool has_schema() const override { return !table_.columns().empty(); }

        void adopt_schema(const std::pmr::vector<types::complex_logical_type>& t) override { table_.adopt_schema(t); }

        void overlay_not_null(const std::string& col_name) override { table_.overlay_not_null(col_name); }

        uint64_t total_rows() const override { return table_.row_group()->total_rows(); }

        uint64_t calculate_size() override { return table_.calculate_size(); }

        void scan(vector::data_chunk_t& output, const table::table_filter_t* filter, int64_t limit) override {
            std::vector<table::storage_index_t> column_indices;
            column_indices.reserve(table_.column_count());
            for (size_t i = 0; i < table_.column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
        }

        void scan(vector::data_chunk_t& output,
                  const table::table_filter_t* filter,
                  int64_t limit,
                  table::transaction_data txn) override {
            std::vector<table::storage_index_t> column_indices;
            column_indices.reserve(table_.column_count());
            for (size_t i = 0; i < table_.column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            state.table_state.txn = txn;
            state.local_state.txn = txn;
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
        }

        void scan_projected(vector::data_chunk_t& output,
                            const table::table_filter_t* filter,
                            int limit,
                            const std::vector<size_t>& projected_cols) override {
            std::vector<table::storage_index_t> column_indices;
            column_indices.reserve(projected_cols.size());
            for (size_t idx : projected_cols) {
                if (idx < table_.column_count()) {
                    column_indices.emplace_back(static_cast<int64_t>(idx));
                }
            }
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
        }

        void scan_projected(vector::data_chunk_t& output,
                            const table::table_filter_t* filter,
                            int limit,
                            const std::vector<size_t>& projected_cols,
                            table::transaction_data txn) override {
            std::vector<table::storage_index_t> column_indices;
            column_indices.reserve(projected_cols.size());
            for (size_t idx : projected_cols) {
                if (idx < table_.column_count()) {
                    column_indices.emplace_back(static_cast<int64_t>(idx));
                }
            }
            table::table_scan_state state(resource_);
            table_.initialize_scan(state, column_indices, filter);
            state.table_state.txn = txn;
            state.local_state.txn = txn;
            table_.scan(output, state);
            if (limit >= 0) {
                output.set_cardinality(std::min(output.size(), static_cast<uint64_t>(limit)));
            }
        }

        void scan_batched(std::pmr::vector<vector::data_chunk_t>& batches,
                          const table::table_filter_t* filter,
                          int64_t limit,
                          const std::vector<size_t>* projected_cols,
                          table::transaction_data txn) override {
            auto types = table_.copy_types();
            const bool row_ids_only = projected_cols && projected_cols->empty();
            auto column_indices =
                row_ids_only ? build_row_id_only_scan_column_ids(filter) : build_scan_column_ids(projected_cols);
            auto total_row_groups = table_.row_group()->row_group_tree()->segment_count();

            // Each worker scans distinct row groups with thread-local state and forwards txn,
            // so visibility matches the serial path. Default is serial; parallel is opt-in via
            // OTTERBRIX_PARALLEL_SCAN and capped so it only runs when the pool holds all row groups.
            // Beware concurrent block eviction under memory pressure.
            static constexpr uint64_t PARALLEL_SCAN_MAX_ROW_GROUPS = 1024;
            const bool plain_committed_scan = txn.transaction_id == 0 && txn.start_time == 0;
            const bool parallel_scan_opt_in = std::getenv("OTTERBRIX_PARALLEL_SCAN") != nullptr;
            if (scheduler_ && (plain_committed_scan || parallel_scan_opt_in) && total_row_groups > 1 &&
                total_row_groups <= PARALLEL_SCAN_MAX_ROW_GROUPS && table_.supports_threaded_scan()) {
                auto workers =
                    snapshot_parallel_workers(static_cast<size_t>(std::min<uint64_t>(table_.max_threads(),
                                                                                      total_row_groups)));

                std::vector<actor_zeta::unique_future<std::pmr::vector<vector::data_chunk_t>>> futures;
                futures.reserve(total_row_groups);
                std::vector<size_t> projected_copy = projected_cols ? *projected_cols : std::vector<size_t>{};

                for (uint64_t row_group_idx = 0; row_group_idx < total_row_groups; ++row_group_idx) {
                    auto* worker = workers[row_group_idx % workers.size()];
                    auto [needs_sched, future] = actor_zeta::send(worker,
                                                                  &detail::parallel_scan_worker_t::scan_row_group,
                                                                  column_indices,
                                                                  row_group_idx,
                                                                  filter,
                                                                  projected_copy,
                                                                  row_ids_only,
                                                                  txn);
                    if (needs_sched) {
                        scheduler_->enqueue(worker);
                    }
                    futures.emplace_back(std::move(future));
                }

                for (auto& future : futures) {
                    auto row_group_batches = wait_future(std::move(future));
                    for (auto& batch : row_group_batches) {
                        batches.push_back(std::move(batch));
                    }
                }
            } else {
                table::table_scan_state state(resource_);
                table_.initialize_scan(state, column_indices, filter);
                state.table_state.txn = txn;
                state.local_state.txn = txn;
                table_.scan_batched(types, projected_cols, batches, state, resource_);
            }

            ensure_at_least_one_batch(batches, types, projected_cols);
            apply_limit(batches, limit);
        }

        void fetch(vector::data_chunk_t& output, const vector::vector_t& row_ids, uint64_t count) override {
            table::column_fetch_state state;
            std::vector<table::storage_index_t> column_indices;
            column_indices.reserve(table_.column_count());
            for (size_t i = 0; i < table_.column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            table_.fetch(output, column_indices, row_ids, count, table::transaction_data{0, 0}, state);
        }

        void fetch(vector::data_chunk_t& output,
                   const vector::vector_t& row_ids,
                   uint64_t count,
                   table::transaction_data txn) override {
            table::column_fetch_state state;
            std::vector<table::storage_index_t> column_indices;
            column_indices.reserve(table_.column_count());
            for (size_t i = 0; i < table_.column_count(); i++) {
                column_indices.emplace_back(static_cast<int64_t>(i));
            }
            table_.fetch(output, column_indices, row_ids, count, txn, state);
        }

        void scan_segment(int64_t start,
                          uint64_t count,
                          const std::function<void(vector::data_chunk_t& chunk)>& callback) override {
            table_.scan_table_segment(start, count, callback);
        }

        uint64_t parallel_scan(const std::function<void(vector::data_chunk_t& chunk)>& callback) override {
            std::pmr::vector<vector::data_chunk_t> batches(resource_);
            scan_batched(batches, nullptr, -1, nullptr, table::transaction_data{0, 0});
            uint64_t total_rows = 0;
            for (auto& batch : batches) {
                if (batch.size() == 0) {
                    continue;
                }
                total_rows += batch.size();
                callback(batch);
            }
            return total_rows;
        }

        uint64_t append(vector::data_chunk_t& data) override {
            table::table_append_state append_state(resource_);
            table_.append_lock(append_state);
            table_.initialize_append(append_state);
            auto start_row = static_cast<uint64_t>(append_state.current_row);
            table_.append(data, append_state);
            table_.finalize_append(append_state, table::transaction_data{0, 0});
            return start_row;
        }

        uint64_t append(vector::data_chunk_t& data, table::transaction_data txn) override {
            table::table_append_state append_state(resource_);
            table_.append_lock(append_state);
            table_.initialize_append(append_state);
            auto start_row = static_cast<uint64_t>(append_state.current_row);
            table_.append(data, append_state);
            table_.finalize_append(append_state, txn);
            return start_row;
        }

        bool has_persisted_pax_layout() const override { return table_.has_persisted_pax_layout(); }

        void update(vector::vector_t& row_ids, vector::data_chunk_t& data) override {
            auto update_state = table_.initialize_update({});
            table_.update(*update_state, row_ids, data);
        }

        std::pair<int64_t, uint64_t>
        update(vector::vector_t& row_ids, vector::data_chunk_t& data, table::transaction_data txn) override {
            auto count = static_cast<uint64_t>(data.size());
            if (count == 0)
                return {0, 0};

            // Step 1: Mark old rows as deleted with txn_id
            auto delete_state = table_.initialize_delete({});
            table_.delete_rows(*delete_state, row_ids, count, txn.transaction_id);

            // Step 2: Append new rows with txn version stamps
            table::table_append_state append_state(resource_);
            table_.append_lock(append_state);
            table_.initialize_append(append_state);
            auto start_row = static_cast<int64_t>(append_state.current_row);
            table_.append(data, append_state);
            table_.finalize_append(append_state, txn);

            return {start_row, count};
        }

        uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count) override {
            auto delete_state = table_.initialize_delete({});
            return table_.delete_rows(*delete_state, row_ids, count, 0);
        }

        uint64_t delete_rows(vector::vector_t& row_ids, uint64_t count, uint64_t txn_id) override {
            auto delete_state = table_.initialize_delete({});
            return table_.delete_rows(*delete_state, row_ids, count, txn_id);
        }

        void commit_append(uint64_t commit_id, int64_t row_start, uint64_t count) override {
            table_.commit_append(commit_id, row_start, count);
        }

        void revert_append(int64_t row_start, uint64_t count) override { table_.revert_append(row_start, count); }

        void commit_all_deletes(uint64_t txn_id, uint64_t commit_id) override {
            table_.commit_all_deletes(txn_id, commit_id);
        }

        void revert_all_deletes(uint64_t txn_id) override { table_.revert_all_deletes(txn_id); }

        std::pmr::memory_resource* resource() const override { return resource_; }

        table::data_table_t& table() { return table_; }

        size_t parallel_worker_count() const {
            std::lock_guard<std::mutex> lock(scan_workers_mutex_);
            return scan_workers_.size();
        }

    private:
        std::vector<table::storage_index_t> build_scan_column_ids(const std::vector<size_t>* projected_cols) const {
            std::vector<table::storage_index_t> column_indices;
            if (projected_cols) {
                column_indices.reserve(projected_cols->size());
                for (size_t idx : *projected_cols) {
                    if (idx < table_.column_count()) {
                        column_indices.emplace_back(static_cast<int64_t>(idx));
                    }
                }
            } else {
                column_indices.reserve(table_.column_count());
                for (size_t i = 0; i < table_.column_count(); i++) {
                    column_indices.emplace_back(static_cast<int64_t>(i));
                }
            }
            return column_indices;
        }

        void append_filter_column_ids(const table::table_filter_t* filter,
                                      std::vector<table::storage_index_t>& column_indices) const {
            if (!filter) {
                return;
            }
            if (auto* conjunction = dynamic_cast<const table::conjunction_filter_t*>(filter)) {
                for (const auto& child : conjunction->child_filters) {
                    append_filter_column_ids(child.get(), column_indices);
                }
                return;
            }
            const auto& table_indices = table::table_filter_table_indices(filter);
            if (table_indices.empty()) {
                return;
            }
            const auto column_index = table_indices.front();
            if (column_index >= table_.column_count()) {
                return;
            }
            table::storage_index_t storage_index{column_index};
            if (std::find(column_indices.begin(), column_indices.end(), storage_index) == column_indices.end()) {
                column_indices.push_back(storage_index);
            }
        }

        std::vector<table::storage_index_t> build_row_id_only_scan_column_ids(
            const table::table_filter_t* filter) const {
            std::vector<table::storage_index_t> column_indices;
            append_filter_column_ids(filter, column_indices);
            if (column_indices.empty()) {
                column_indices.emplace_back();
            }
            return column_indices;
        }

        std::vector<detail::parallel_scan_worker_t*> snapshot_parallel_workers(size_t target_count) {
            std::vector<detail::parallel_scan_worker_t*> workers;
            std::lock_guard<std::mutex> lock(scan_workers_mutex_);
            if (scheduler_ && target_count > 0) {
                while (scan_workers_.size() < target_count) {
                    scan_workers_.push_back(
                        actor_zeta::spawn<detail::parallel_scan_worker_t>(std::pmr::new_delete_resource(), &table_));
                }
            }
            workers.reserve(scan_workers_.size());
            for (auto& worker : scan_workers_) {
                workers.push_back(worker.get());
            }
            return workers;
        }

        template<typename T>
        T wait_future(actor_zeta::unique_future<T>&& future) const {
            if (progress_fn_ && *progress_fn_) {
                while (!future.is_ready()) {
                    (*progress_fn_)();
                }
            }
            return std::move(future).get();
        }

        void ensure_at_least_one_batch(std::pmr::vector<vector::data_chunk_t>& batches,
                                       const std::pmr::vector<types::complex_logical_type>& types,
                                       const std::vector<size_t>* projected_cols) const {
            if (!batches.empty()) {
                return;
            }

            if (projected_cols) {
                batches.emplace_back(resource_, types, *projected_cols, vector::DEFAULT_VECTOR_CAPACITY);
            } else {
                batches.emplace_back(resource_, types, vector::DEFAULT_VECTOR_CAPACITY);
            }
            batches.back().set_cardinality(0);
        }

        void apply_limit(std::pmr::vector<vector::data_chunk_t>& batches, int64_t limit) const {
            if (limit < 0) {
                return;
            }

            uint64_t budget = static_cast<uint64_t>(limit);
            size_t keep = 0;
            for (; keep < batches.size(); ++keep) {
                if (batches[keep].size() <= budget) {
                    budget -= batches[keep].size();
                } else {
                    batches[keep].set_cardinality(budget);
                    ++keep;
                    break;
                }
            }
            batches.erase(batches.begin() + static_cast<std::ptrdiff_t>(keep), batches.end());
        }

        table::data_table_t& table_;
        std::pmr::memory_resource* resource_;
        actor_zeta::scheduler::sharing_scheduler* scheduler_;
        std::function<void()>* progress_fn_;
        mutable std::mutex scan_workers_mutex_;
        std::vector<detail::parallel_scan_worker_ptr> scan_workers_;
    };

} // namespace components::storage
