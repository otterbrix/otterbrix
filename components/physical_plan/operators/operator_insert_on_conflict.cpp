#include "operator_insert_on_conflict.hpp"

#include "dml_util.hpp"
#include "operator_raw_data.hpp"

#include <components/context/context.hpp>
#include <components/context/subplan_runner.hpp>
#include <components/vector/vector_operations.hpp>
#include <services/disk/manager_disk.hpp>

#include <unordered_map>
#include <unordered_set>

namespace components::operators {

    operator_insert_on_conflict_t::operator_insert_on_conflict_t(std::pmr::memory_resource* resource,
                                                                 log_t log,
                                                                 catalog::oid_t table_oid,
                                                                 bool has_returning)
        : read_write_operator_t(resource, std::move(log), operator_type::insert_on_conflict)
        , table_oid_(table_oid)
        , has_returning_(has_returning) {}

    void operator_insert_on_conflict_t::set_parts(operator_ptr insert_part,
                                                  operator_unique_constraint_t* check,
                                                  boost::intrusive_ptr<operator_delete> removal) noexcept {
        insert_part_ = std::move(insert_part);
        check_ = check;
        removal_ = std::move(removal);
    }

    void operator_insert_on_conflict_t::set_update(operator_ptr update_part, operator_update* update) noexcept {
        update_part_ = std::move(update_part);
        update_ = update;
    }

    actor_zeta::unique_future<void> operator_insert_on_conflict_t::await_async_and_resume(pipeline::context_t* ctx) {
        auto err = co_await drive_(ctx);
        if (err.contains_error()) {
            set_error(err);
            mark_failed();
            co_return;
        }
        mark_executed();
    }

    actor_zeta::unique_future<core::error_t> operator_insert_on_conflict_t::drive_(pipeline::context_t* ctx) {
        if (!ctx->runner) {
            co_return core::error_t{core::error_code_t::physical_plan_error,
                                    std::pmr::string{"INSERT ... ON CONFLICT: no sub-plan runner", resource_}};
        }
        auto inserted = co_await ctx->runner->run_subplan(insert_part_, ctx);
        if (inserted.has_error()) {
            co_return inserted.error();
        }

        const operator_data_ptr conflicts = check_ ? check_->conflict_rows() : nullptr;
        bool any_conflict = conflicts && conflicts->size() > 0;
        if (any_conflict && update_part_) {
            // One existing row is affected at most once
            std::pmr::unordered_set<int64_t> affected(resource_);
            for (const auto& holder : check_->conflict_holders()) {
                if (holder.written_by_statement || !affected.insert(holder.row_id).second) {
                    co_return core::error_t{
                        core::error_code_t::invalid_constraint,
                        std::pmr::string{"ON CONFLICT DO UPDATE cannot affect a row a second time: two proposed "
                                         "rows have the same conflict key",
                                         resource_}};
                }
            }
        }
        if (any_conflict) {
            removal_->set_children(boost::intrusive_ptr(new operator_raw_data_t(conflicts->chunks())));
            auto removed = co_await ctx->runner->run_subplan(removal_, ctx);
            if (removed.has_error()) {
                co_return removed.error();
            }
        }
        if (any_conflict && update_part_) {
            chunks_vector_t targets(resource_);
            if (auto fetched = co_await fetch_holders_(ctx, conflicts, &targets); fetched.contains_error()) {
                co_return fetched;
            }
            update_->set_children(boost::intrusive_ptr(new operator_raw_data_t(targets)),
                                  boost::intrusive_ptr(new operator_raw_data_t(conflicts->chunks())));
            auto updated = co_await ctx->runner->run_subplan(update_part_, ctx);
            if (updated.has_error()) {
                co_return updated.error();
            }
        }
        compose_output_(insert_part_->output(),
                        conflicts,
                        any_conflict && update_part_ ? update_part_->output() : nullptr);
        co_return core::error_t::no_error();
    }

    actor_zeta::unique_future<core::error_t>
    operator_insert_on_conflict_t::fetch_holders_(pipeline::context_t* ctx,
                                                  const operator_data_ptr& conflicts,
                                                  chunks_vector_t* targets) {
        const auto& holders = check_->conflict_holders();
        vector::vector_t row_ids(resource_, types::logical_type::BIGINT, holders.size());
        for (size_t i = 0; i < holders.size(); ++i) {
            row_ids.data<int64_t>()[i] = holders[i].row_id;
        }
        auto [_fetch, future] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::storage_fetch,
                                                            ctx->session,
                                                            table_oid_,
                                                            std::move(row_ids),
                                                            holders.size(),
                                                            std::vector<size_t>{},
                                                            ctx->txn,
                                                            components::table::fetch_visibility_t::SNAPSHOT,
                                                            int64_t{-1},
                                                            services::disk::k_fetch_epoch_unchecked);
        auto segments_r = co_await std::move(future);
        if (segments_r.has_error()) {
            co_return segments_r.error();
        }
        // The reply pairs rows by id
        auto& segments = segments_r.value();
        std::pmr::vector<types::complex_logical_type> types(resource_);
        for (const auto& segment : segments) {
            if (segment.size() > 0) {
                types = segment.types();
                break;
            }
        }
        struct fetched_row_t {
            std::size_t segment;
            uint64_t row;
        };
        std::pmr::unordered_map<int64_t, fetched_row_t> fetched(resource_);
        for (size_t segment = 0; segment < segments.size(); ++segment) {
            for (uint64_t row = 0; row < segments[segment].size(); ++row) {
                fetched.emplace(segments[segment].row_ids.data<int64_t>()[row], fetched_row_t{segment, row});
            }
        }

        // Each target chunk lines up with its conflict chunk: gathered segment by segment, then put in order.
        size_t holder = 0;
        for (const auto& chunk : conflicts->chunks()) {
            const uint64_t rows_count = chunk.size();
            std::pmr::vector<fetched_row_t> sources(resource_);
            sources.reserve(rows_count);
            for (uint64_t row = 0; row < rows_count; ++row, ++holder) {
                auto found = fetched.find(holders[holder].row_id);
                if (found == fetched.end()) {
                    co_return core::error_t{
                        core::error_code_t::invalid_constraint,
                        std::pmr::string{"ON CONFLICT DO UPDATE: the conflicting row could not be read", resource_}};
                }
                sources.push_back(found->second);
            }
            vector::data_chunk_t gathered(resource_, types, rows_count == 0 ? 1 : rows_count);
            vector::indexing_vector_t order(resource_, rows_count == 0 ? 1 : rows_count);
            uint64_t offset = 0;
            for (size_t segment = 0; segment < segments.size(); ++segment) {
                vector::indexing_vector_t selection(resource_, rows_count == 0 ? 1 : rows_count);
                uint64_t selected = 0;
                for (uint64_t row = 0; row < rows_count; ++row) {
                    if (sources[row].segment == segment) {
                        selection.set_index(selected, sources[row].row);
                        order.set_index(row, offset + selected);
                        ++selected;
                    }
                }
                if (selected == 0) {
                    continue;
                }
                for (size_t column = 0; column < segments[segment].column_count(); ++column) {
                    vector::vector_ops::copy(segments[segment].data[column],
                                             gathered.data[column],
                                             selection,
                                             selected,
                                             0,
                                             offset);
                }
                vector::vector_ops::copy(segments[segment].row_ids, gathered.row_ids, selection, selected, 0, offset);
                offset += selected;
            }
            gathered.set_cardinality(offset);
            vector::data_chunk_t rows(resource_, types, rows_count == 0 ? 1 : rows_count);
            gathered.copy(rows, order, rows_count);
            vector::vector_ops::copy(gathered.row_ids, rows.row_ids, order, rows_count, 0, 0);
            targets->emplace_back(std::move(rows));
        }
        co_return core::error_t::no_error();
    }

    void operator_insert_on_conflict_t::compose_output_(const operator_data_ptr& inserted,
                                                        const operator_data_ptr& removed,
                                                        const operator_data_ptr& updated) {
        uint64_t removed_count = removed ? removed->size() : 0;
        if (!has_returning_) {
            uint64_t inserted_count = inserted ? inserted->size() : 0;
            uint64_t updated_count = updated ? updated->size() : 0;
            uint64_t written = inserted_count - removed_count + updated_count;
            set_output(
                written == 0
                    ? nullptr
                    : make_operator_data(resource_, dml_detail::make_affected_count_chunks(resource_, written, {})));
            return;
        }
        std::pmr::unordered_set<int64_t> removed_ids(resource_);
        if (removed) {
            for (const auto& chunk : removed->chunks()) {
                const auto* ids = chunk.row_ids.data<int64_t>();
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    removed_ids.insert(ids[row]);
                }
            }
        }
        chunks_vector_t kept(resource_);
        if (inserted) {
            for (const auto& chunk : inserted->chunks()) {
                const auto* ids = chunk.row_ids.data<int64_t>();
                vector::indexing_vector_t selection(resource_, chunk.size() == 0 ? 1 : chunk.size());
                uint64_t count = 0;
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    if (removed_ids.count(ids[row]) == 0) {
                        selection.set_index(count++, row);
                    }
                }
                // A chunk left with no rows still carries the RETURNING columns.
                vector::data_chunk_t rows(resource_, chunk.types(), count == 0 ? 1 : count);
                chunk.copy(rows, selection, count);
                kept.emplace_back(std::move(rows));
            }
        }
        if (updated) {
            for (const auto& chunk : updated->chunks()) {
                if (chunk.size() > 0) {
                    kept.emplace_back(chunk.partial_copy(resource_, 0, chunk.size()));
                }
            }
        }
        set_output(make_operator_data(resource_, std::move(kept)));
    }

} // namespace components::operators
