#pragma once

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <memory_resource>
#include <vector>

#include <components/context/context.hpp>
#include <components/expressions/cast_expression.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_data.hpp>
#include <components/physical_plan/operators/operator_select.hpp>
#include <components/table/transaction.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>
#include <core/result_wrapper.hpp>

// Shared per-flush bookkeeping for the bounded DML sinks (insert / update / delete).
// The three operators fold each source batch into a bounded buffer in push()
// and the executor drives await_async_and_resume INCREMENTALLY: once per "buffer full"
// during the pump (dml_flush_is_final==false) and once at finalize (==true). Each flush
// runs the operator's DIVERGENT storage op (append vs delete-old+append-new vs delete —
// co_awaited IN the operator, where its shapes live), then calls record_flush() to do the
// COMMON post-storage bookkeeping: record the append range into the unified append channel
// and, only when a parent constraint sits above the DML, accumulate a persistent copy of
// the just-written rows into constraint_input_ (so the constraint validates the full set
// at finalize). When there is NO parent constraint, constraint_input_ stays null and the
// sink is memory-bounded — the whole point of the incremental flush.
//
// This is the sync half of the "one DML-sink contract" (the divergent async storage op
// stays per-operator); it mirrors the free-function-helper idiom of join_utils.hpp, with
// NO base class and NO std::function (R14).
namespace components::operators::dml_detail {

    // The scan's id of `row`: a dictionary-sliced batch keeps it in its first column's indexing, any other batch in
    // row_ids. A batch without columns (a table whose last column was dropped) has only row_ids.
    inline int64_t row_id_of(const vector::data_chunk_t& chunk, uint64_t row) {
        if (!chunk.data.empty() && chunk.data.front().get_vector_type() == vector::vector_type::DICTIONARY) {
            return static_cast<int64_t>(chunk.data.front().indexing().get_index(row));
        }
        return chunk.row_ids.data<int64_t>()[row];
    }

    // The selected rows of `source` as a slice over its columns: no cell is copied. The row ids are left unfilled.
    inline vector::data_chunk_t sliced_rows(std::pmr::memory_resource* resource,
                                            const vector::data_chunk_t& source,
                                            const vector::indexing_vector_t& selection,
                                            uint64_t count) {
        vector::data_chunk_t sliced(resource, std::pmr::vector<types::complex_logical_type>{resource}, count);
        sliced.data.reserve(source.column_count());
        for (const auto& column : source.data) {
            sliced.data.emplace_back(column);
        }
        sliced.slice(selection, count);
        return sliced;
    }

    // Matched rows of `source` as a slice. The row ids are gathered into an owned vector, since the write
    // overwrites them in place.
    inline vector::data_chunk_t slice_matched(std::pmr::memory_resource* resource,
                                              const vector::data_chunk_t& source,
                                              const vector::indexing_vector_t& selection,
                                              uint64_t count) {
        auto sliced = sliced_rows(resource, source, selection, count);
        vector::vector_ops::copy(source.row_ids, sliced.row_ids, selection, count, 0, 0);
        return sliced;
    }

    // Slices keep their whole source buffers alive, so staged and returned rows are slices only while they are at
    // least this share (1 in N) of its capacity; sparser matches are copied out once and the source is released.
    inline constexpr uint64_t slice_min_share = 8;

    inline bool returning_keeps_slice(uint64_t matched, uint64_t source_capacity) {
        return matched * slice_min_share >= source_capacity;
    }

    inline vector::data_chunk_t stage_matched(std::pmr::memory_resource* resource,
                                              const vector::data_chunk_t& source,
                                              const vector::indexing_vector_t& selection,
                                              uint64_t count) {
        if (returning_keeps_slice(count, source.capacity())) {
            return slice_matched(resource, source, selection, count);
        }
        vector::data_chunk_t owned(resource, source.types(), count);
        source.copy(owned, selection, count);
        return owned;
    }

    inline vector::data_chunk_t gather_rows(std::pmr::memory_resource* resource, const vector::data_chunk_t& chunk) {
        vector::data_chunk_t owned(resource, chunk.types(), chunk.size());
        chunk.copy(owned, 0);
        return owned;
    }

    // Does any column of `result` still point into a column of `source`?
    inline bool references_columns_of(const vector::data_chunk_t& result, const vector::data_chunk_t& source) {
        for (const auto& column : result.data) {
            const auto* auxiliary = column.auxiliary().get();
            if (auxiliary == nullptr) {
                continue;
            }
            for (const auto& origin : source.data) {
                if (origin.auxiliary().get() == auxiliary) {
                    return true;
                }
            }
        }
        return false;
    }

    // `chunks` cut into rounds of at most `rows_per_round` rows (0: one round), each round a write message of its
    // own. A chunk that spans two rounds is copied in parts.
    inline std::pmr::vector<chunks_vector_t>
    split_rounds(std::pmr::memory_resource* resource, chunks_vector_t& chunks, uint64_t rows_per_round) {
        std::pmr::vector<chunks_vector_t> rounds{resource};
        uint64_t in_round = 0;
        for (auto& chunk : chunks) {
            const uint64_t rows = chunk.size();
            if (rows == 0) {
                continue;
            }
            uint64_t offset = 0;
            while (offset < rows) {
                if (rounds.empty() || (rows_per_round != 0 && in_round == rows_per_round)) {
                    rounds.emplace_back();
                    in_round = 0;
                }
                const uint64_t take =
                    rows_per_round == 0 ? rows - offset : std::min(rows - offset, rows_per_round - in_round);
                if (take == rows) {
                    rounds.back().push_back(std::move(chunk));
                } else {
                    rounds.back().push_back(chunk.partial_copy(resource, offset, take));
                }
                offset += take;
                in_round += take;
            }
        }
        return rounds;
    }

    // Parallel chunk lists (chunk k of each non-empty list describes the same rows) cut into the same rounds of at
    // most `rows_per_round` rows (0: one round). Round r holds, per list, its chunks of that round (none for an
    // empty list).
    inline std::pmr::vector<std::pmr::vector<chunks_vector_t>>
    split_parallel_rounds(std::pmr::memory_resource* resource,
                          std::initializer_list<chunks_vector_t*> lists,
                          uint64_t rows_per_round) {
        std::pmr::vector<std::pmr::vector<chunks_vector_t>> rounds{resource};
        const auto& primary = **lists.begin();
        auto open_round = [&] {
            auto& round = rounds.emplace_back();
            for (std::size_t list = 0; list < lists.size(); ++list) {
                round.emplace_back();
            }
        };
        uint64_t in_round = 0;
        for (std::size_t index = 0; index < primary.size(); ++index) {
            const uint64_t rows = primary[index].size();
            if (rows == 0) {
                continue;
            }
            uint64_t offset = 0;
            while (offset < rows) {
                if (rounds.empty() || (rows_per_round != 0 && in_round == rows_per_round)) {
                    open_round();
                    in_round = 0;
                }
                const uint64_t take =
                    rows_per_round == 0 ? rows - offset : std::min(rows - offset, rows_per_round - in_round);
                std::size_t list_index = 0;
                for (auto* list : lists) {
                    if (!list->empty()) {
                        assert(list->size() == primary.size() &&
                               "split_parallel_rounds: the lists describe the same rows");
                        auto& chunk = (*list)[index];
                        if (take == rows) {
                            rounds.back()[list_index].push_back(std::move(chunk));
                        } else {
                            rounds.back()[list_index].push_back(chunk.partial_copy(resource, offset, take));
                        }
                    }
                    ++list_index;
                }
                offset += take;
                in_round += take;
            }
        }
        return rounds;
    }

    // The columns of a whole row a scan read, in the scan's order (`columns` empty: the scan read every one). A
    // storage answers every column of a deleted row, while a DELETE's RETURNING is bound to its scan's columns.
    inline vector::data_chunk_t
    as_scanned(std::pmr::memory_resource* resource, vector::data_chunk_t&& row, const std::vector<size_t>& columns) {
        if (columns.empty()) {
            return std::move(row);
        }
        std::pmr::vector<types::complex_logical_type> types(resource);
        types.reserve(columns.size());
        for (const auto column : columns) {
            types.push_back(row.data[column].type());
        }
        vector::data_chunk_t scanned(resource, types, std::max<uint64_t>(row.size(), 1));
        for (size_t index = 0; index < columns.size(); ++index) {
            vector::vector_ops::copy(row.data[columns[index]], scanned.data[index], row.size(), 0, 0);
        }
        scanned.set_cardinality(row.size());
        return scanned;
    }

    // Arithmetic and casts of literals and bound parameters only: what PostgreSQL 18 folds while planning (an
    // immutable expression of constants), so its error comes before the statement runs.
    inline bool is_constant_arithmetic(const expressions::param_storage& param,
                                       const logical_plan::storage_parameters& parameters) {
        if (expressions::is_parameter(param)) {
            return parameters.kind(expressions::as_parameter(param)) != vector::parameter_kind::execution;
        }
        if (!expressions::is_expr(param) || expressions::as_expr(param) == nullptr) {
            return false;
        }
        const auto& expression = expressions::as_expr(param);
        if (expression->group() == expressions::expression_group::cast) {
            return is_constant_arithmetic(static_cast<const expressions::cast_expression_t&>(*expression).child(),
                                          parameters);
        }
        if (expression->group() != expressions::expression_group::scalar) {
            return false;
        }
        const auto& scalar = static_cast<const expressions::scalar_expression_t&>(*expression);
        switch (scalar.type()) {
            case expressions::scalar_type::add:
            case expressions::scalar_type::subtract:
            case expressions::scalar_type::multiply:
            case expressions::scalar_type::divide:
            case expressions::scalar_type::mod:
            case expressions::scalar_type::unary_minus:
                break;
            default:
                return false;
        }
        if (scalar.params().empty()) {
            return false;
        }
        for (const auto& operand : scalar.params()) {
            if (!is_constant_arithmetic(operand, parameters)) {
                return false;
            }
        }
        return true;
    }

    // The RETURNING columns of constants only, computed once before the first write: a division by zero there is
    // the statement's error before any row reaches the storage.
    inline core::error_t check_constant_returning(std::pmr::memory_resource* resource,
                                                  const std::pmr::vector<projected_column_t>& returning,
                                                  const pipeline::context_t& ctx) {
        std::pmr::vector<projected_column_t> constant(resource);
        for (const auto& column : returning) {
            if (is_constant_arithmetic(column.value, ctx.parameters)) {
                constant.push_back(column);
            }
        }
        if (constant.empty()) {
            return core::error_t::no_error();
        }
        vector::data_chunk_t one_row(resource, std::pmr::vector<types::complex_logical_type>{resource}, 1);
        one_row.set_cardinality(1);
        std::unique_ptr<execution_dag::execution_dag_t> graph;
        auto computed =
            evaluate_projection(resource, constant, &one_row, ctx.parameters, ctx.execution_context, &graph);
        return computed.has_error() ? computed.error() : core::error_t::no_error();
    }

    // `count` rows of `chunks` from the row `offset` on, counted across the chunks, copied into one chunk.
    inline vector::data_chunk_t
    rows_at(std::pmr::memory_resource* resource, const chunks_vector_t& chunks, uint64_t offset, uint64_t count) {
        assert(!chunks.empty());
        vector::data_chunk_t out(resource, chunks.front().types(), std::max<uint64_t>(count, 1));
        uint64_t written = 0;
        uint64_t skipped = 0;
        for (const auto& chunk : chunks) {
            if (written == count) {
                break;
            }
            if (skipped + chunk.size() <= offset) {
                skipped += chunk.size();
                continue;
            }
            const uint64_t from = offset > skipped ? offset - skipped : 0;
            const uint64_t take = std::min(chunk.size() - from, count - written);
            for (uint64_t column = 0; column < out.column_count(); ++column) {
                vector::vector_ops::copy(chunk.data[column], out.data[column], from + take, from, written);
            }
            written += take;
            skipped += chunk.size();
        }
        out.set_cardinality(written);
        return out;
    }

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
            int64_t written_row_id = outcome.append_start;
            for (const auto& src : constraint_rows) {
                if (src.size() == 0) {
                    continue;
                }
                vector::data_chunk_t dst(resource, src.types(), src.size());
                src.copy(dst, 0);
                if (outcome.has_append) {
                    auto* row_ids = dst.row_ids.data<int64_t>();
                    for (uint64_t row = 0; row < dst.size(); ++row) {
                        row_ids[row] = written_row_id++;
                    }
                }
                constraint_accum->append_chunk(std::move(dst));
            }
            assert((!outcome.has_append ||
                    written_row_id == outcome.append_start + static_cast<int64_t>(outcome.append_count)) &&
                   "record_flush: the constraint rows are the appended range");
        }
        return core::error_t::no_error();
    }

} // namespace components::operators::dml_detail
