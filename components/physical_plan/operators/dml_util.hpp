#pragma once

#include <algorithm>
#include <cstdint>
#include <memory_resource>

#include <components/context/context.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/table/transaction.hpp>
#include <components/vector/data_chunk.hpp>
#include <core/result_wrapper.hpp>

// Shared per-flush bookkeeping for the bounded DML sinks (insert / update / delete),
// Step 3b-B. The three operators fold each source batch into a bounded buffer in push()
// and the executor drives await_async_and_resume INCREMENTALLY: once per "buffer full"
// during the pump (dml_flush_is_final==false) and once at finalize (==true). Each flush
// runs the operator's DIVERGENT storage op (append vs delete-old+append-new vs delete —
// co_awaited IN the operator, where its shapes live), then calls record_flush() to do the
// COMMON post-storage bookkeeping: record the append range into the unified 3b-A channel
// and, only when a parent constraint sits above the DML, accumulate a persistent copy of
// the just-written rows into constraint_input_ (so the constraint validates the full set
// at finalize). When there is NO parent constraint, constraint_input_ stays null and the
// sink is memory-bounded — the whole point of the incremental flush.
//
// This is the sync half of the "one DML-sink contract" (the divergent async storage op
// stays per-operator); it mirrors the free-function-helper idiom of join_utils.hpp, with
// NO base class and NO std::function (R14).
namespace components::operators::dml_detail {

    // Normalized result of one operator storage op, handed to record_flush().
    struct flush_outcome_t {
        core::error_t error{core::error_t::no_error()};
        bool has_append{false};   // did this flush append rows (insert / update new-rows)?
        int64_t append_start{0};  // storage-assigned start row of the appended range
        uint64_t append_count{0}; // rows appended (0 => nothing recorded)
    };

    // Common post-storage bookkeeping for one flushed slice. Returns the error to
    // propagate (the operator does set_error/mark_failed on a non-ok result).
    //   - table_oid            : the DML target table.
    //   - outcome              : the resolved storage op result (see flush_outcome_t).
    //   - has_parent_constraint: ctx->dml_has_parent_constraint (executor-set).
    //   - constraint_accum     : the operator's constraint_input_ (grown ONLY when a
    //                            parent constraint is present; left null otherwise).
    //   - constraint_rows      : the rows this flush wrote that a parent constraint must
    //                            observe (insert/update: the new rows; delete: the OLD
    //                            about-to-delete rows the fk_cascade reads).
    [[nodiscard]] inline core::error_t record_flush(pipeline::context_t* ctx,
                                                    std::pmr::memory_resource* resource,
                                                    catalog::oid_t table_oid,
                                                    const flush_outcome_t& outcome,
                                                    bool has_parent_constraint,
                                                    operator_data_ptr& constraint_accum,
                                                    const chunks_vector_t& constraint_rows) {
        // Record the append range FIRST, even when the flush errored: a late failure
        // (e.g. a RETURNING projection error on the read-back) surfaces AFTER the rows
        // were physically appended, and only a recorded range lets the failed-statement
        // abort tail (storage_revert_appends) reclaim them.
        if (outcome.has_append && outcome.append_count > 0) {
            ctx->dml_appends.push_back(
                components::table::dml_append_range_t{table_oid, outcome.append_start, outcome.append_count});
        }
        if (outcome.error.contains_error()) {
            return outcome.error;
        }
        if (has_parent_constraint && !constraint_rows.empty()) {
            if (!constraint_accum) {
                constraint_accum = make_operator_data(resource, chunks_vector_t{resource});
            }
            for (const auto& src : constraint_rows) {
                if (src.size() == 0) {
                    continue;
                }
                vector::data_chunk_t dst(resource, src.types(), src.size());
                src.copy(dst, 0);
                constraint_accum->append_chunk(std::move(dst));
            }
        }
        return core::error_t::no_error();
    }

    // Build the "affected-row count" carrier for a no-RETURNING DML result: a run of chunks whose
    // cardinalities SUM to affected_rows (the cursor totals chunk sizes), each capped at
    // DEFAULT_VECTOR_CAPACITY rows so no oversized data_chunk_t is built. insert/update pass an
    // EMPTY col_types (column-less carrier); delete passes the table's storage types. One place
    // instead of three byte-identical copies (3b-B bookkeeping).
    [[nodiscard]] inline chunks_vector_t
    make_affected_count_chunks(std::pmr::memory_resource* resource,
                               uint64_t affected_rows,
                               const std::pmr::vector<types::complex_logical_type>& col_types) {
        const uint64_t cap = vector::DEFAULT_VECTOR_CAPACITY;
        chunks_vector_t batches(resource);
        batches.reserve((affected_rows + cap - 1) / cap);
        for (uint64_t base = 0; base < affected_rows; base += cap) {
            const uint64_t window = std::min<uint64_t>(cap, affected_rows - base);
            vector::data_chunk_t chunk(resource, col_types, window);
            chunk.set_cardinality(window);
            batches.emplace_back(std::move(chunk));
        }
        return batches;
    }

} // namespace components::operators::dml_detail
