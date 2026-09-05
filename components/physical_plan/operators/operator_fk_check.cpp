#include "operator_fk_check.hpp"

#include "constraint_util.hpp"

#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>
#include <services/disk/manager_disk.hpp>

#include <limits>

namespace components::operators {

    operator_fk_check_t::operator_fk_check_t(std::pmr::memory_resource* resource, log_t log, catalog::fk_info_t fk)
        : read_write_operator_t(resource, log, operator_type::fk_check)
        , fk_(std::move(fk)) {}

    using constraint_detail::resolve_cursor_output;

    actor_zeta::unique_future<void> operator_fk_check_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Resolve the source here directly in await_async_and_resume (the executor
        // marks the root executed after the pump). fk_check validates the DML's
        // constraint_input() snapshot: constraint ops STACK above one DML, so walk DOWN
        // the left_ spine to the DML's snapshot (single canonical source, R6).
        const auto& source = constraint_detail::resolve_constraint_source(left_);
        if (!source || source->size() == 0) {
            // Nothing to validate; still surface the DML result as the cursor.
            output_ = resolve_cursor_output(left_, source);
            mark_executed();
            co_return;
        }
        const auto& in_chunks = source->chunks();
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        const auto& indices = fk_.child_col_indices;
        const std::size_t absent = std::numeric_limits<std::size_t>::max();

        // THE TWO COLUMN LISTS MUST BE THE SAME LENGTH. The keys-chunk below carries one column per
        // child_col_indices entry, while the key column NAMES sent with it are parent_col_names — the two counts
        // have to agree for the parent-side lookup to mean anything, and nothing on the DDL path makes them
        // agree: `FOREIGN KEY (a, b) REFERENCES parent (x)` is accepted verbatim and each list is resolved to
        // attoids independently. Left unchecked, the disk side gets a key chunk it cannot read and answers one
        // empty bucket per key, which this operator reports as "referenced row not found in parent table" — a
        // violation message pointing at data that is perfectly fine. Name the actual defect instead.
        if (indices.size() != fk_.parent_col_names.size()) {
            std::pmr::string what{"FK constraint: foreign key column count mismatch — ", resource_};
            what.append(std::to_string(indices.size()).c_str());
            what.append(" referencing column(s) vs ");
            what.append(std::to_string(fk_.parent_col_names.size()).c_str());
            what.append(" referenced column(s)");
            set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
            mark_failed();
            co_return;
        }

        // EVERY REFERENCING COLUMN MUST HAVE A POSITION IN THE WRITTEN ROW. A different defect from the arity
        // mismatch above — the two lists can agree in length and still name a column this write-set does not
        // expose — and it takes the operator's QUIETEST path: an unresolved position means the row qualifies for
        // no parent lookup, so every row is skipped, the qualifying count stays 0, and 0 is this operator's
        // SUCCESS path — an INSERT/UPDATE accepted with its foreign key checked against nothing at all. There is
        // no reading of an absent column that is a check, so refuse and name the column.
        // (Constant per FK: the positions are the same for every row.)
        if (indices.empty()) {
            set_error(core::error_t{
                core::error_code_t::invalid_constraint,
                std::pmr::string{"FK constraint: no referencing column resolved to a position in the written row",
                                 resource_}});
            mark_failed();
            co_return;
        }
        for (std::size_t i = 0; i < indices.size(); ++i) {
            if (indices[i] != absent) {
                continue;
            }
            std::pmr::string what{"FK constraint: referencing column \"", resource_};
            what.append(i < fk_.child_col_names.size() ? fk_.child_col_names[i].c_str() : "?");
            what.append("\" has no position in the written row");
            set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
            mark_failed();
            co_return;
        }

        // Parent key column names are the same for every row; hoist them once.
        std::pmr::vector<std::string> parent_col_names(resource_);
        parent_col_names.reserve(fk_.parent_col_names.size());
        for (const auto& n : fk_.parent_col_names) {
            parent_col_names.emplace_back(n);
        }

        // Collect the qualifying child rows as a per-chunk SELECTION, preserving the MATCH null policy + error
        // path. Each input chunk's qualifying keys become their own owned keys-chunk, verified by one
        // scan_by_keys call per input chunk (below); scan_by_keys does ONE single-pass hash semi-join scan of the
        // parent per call, so the whole verify is O(child_rows + parent_rows), not O(keys * parent_rows).
        // qualifying[c] = selection into in_chunks[c]; counts[c] = its qualifying count.
        std::pmr::vector<components::vector::indexing_vector_t> qualifying(resource_);
        std::pmr::vector<uint64_t> counts(resource_);
        qualifying.reserve(in_chunks.size());
        counts.reserve(in_chunks.size());
        uint64_t qcount = 0;

        for (const auto& chunk : in_chunks) {
            // The positions come from the plan; the chunk is what was written. A position
            // past the end of this chunk would be read as a cell, so check it once per
            // chunk rather than trusting the two to agree.
            if (chunk.size() > 0) {
                for (std::size_t i = 0; i < indices.size(); ++i) {
                    if (indices[i] < chunk.column_count()) {
                        continue;
                    }
                    std::pmr::string what{"FK constraint: referencing column \"", resource_};
                    what.append(i < fk_.child_col_names.size() ? fk_.child_col_names[i].c_str() : "?");
                    what.append("\" is outside the written row");
                    set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
                    mark_failed();
                    co_return;
                }
            }
            components::vector::indexing_vector_t selection(resource_, chunk.size() == 0 ? 1 : chunk.size());
            uint64_t chunk_count = 0;
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                bool any_null = false;
                bool all_null = true;
                for (std::size_t i = 0; i < indices.size(); ++i) {
                    // Every position resolved (guarded above), so the validity bit of the
                    // written cell is the whole question.
                    if (!chunk.data[indices[i]].validity().row_is_valid(row))
                        any_null = true;
                    else
                        all_null = false;
                }

                if (fk_.matchtype == 'f') {
                    // MATCH FULL: all-NULL → skip; partial-NULL → error; no-NULL → check.
                    if (all_null)
                        continue;
                    if (any_null) {
                        set_error(core::error_t{
                            core::error_code_t::other_error,
                            std::pmr::string{"FK MATCH FULL: partial null in foreign key columns", resource_}});
                        co_return;
                    }
                } else {
                    // MATCH SIMPLE (default): any-NULL → skip.
                    if (any_null)
                        continue;
                }

                selection.set_index(chunk_count, row);
                ++chunk_count;
            }
            qcount += chunk_count;
            qualifying.emplace_back(std::move(selection));
            counts.push_back(chunk_count);
        }

        if (qcount == 0) {
            output_ = resolve_cursor_output(left_, source);
            mark_executed();
            co_return;
        }

        // Verify the qualifying keys against the parent table ONE input chunk at a time. Each input chunk holds
        // <= DEFAULT_VECTOR_CAPACITY rows, so its keys-chunk is bounded; gathering ALL streamed batches into one
        // carrier would overflow the chunk capacity. Each keys-chunk is an OWNED copy (it crosses the mailbox;
        // actors must not share buffers). The per-chunk scans are sequential co_awaits living in this nested
        // operator coroutine (driven by the executor) — no lost-wakeup.
        std::pmr::vector<types::complex_logical_type> key_types(resource_);
        key_types.reserve(indices.size());
        for (auto idx : indices) {
            key_types.push_back(in_chunks.front().data[idx].type());
        }
        for (std::size_t c = 0; c < in_chunks.size(); ++c) {
            if (counts[c] == 0) {
                continue;
            }
            components::vector::data_chunk_t keys(resource_, key_types, counts[c]);
            for (std::size_t j = 0; j < indices.size(); ++j) {
                components::vector::vector_ops::copy(in_chunks[c].data[indices[j]],
                                                     keys.data[j],
                                                     qualifying[c],
                                                     counts[c],
                                                     0,
                                                     0);
            }
            keys.set_cardinality(counts[c]);

            // Parent key column names cross the mailbox per scan, so copy them each time.
            std::pmr::vector<std::string> col_names(resource_);
            col_names.reserve(parent_col_names.size());
            for (const auto& n : parent_col_names) {
                col_names.emplace_back(n);
            }
            auto [_, fut] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::scan_by_keys,
                                                        exec_ctx,
                                                        fk_.parent_table_oid,
                                                        std::move(col_names),
                                                        std::move(keys));
            auto matches_r = co_await std::move(fut);
            if (matches_r.has_error()) {
                // A failed parent-key read is not a miss; treating it as one lets the
                // operation proceed on data that was never read.
                set_error(matches_r.error());
                co_return;
            }
            auto& matches = matches_r.value();

            // Any missing parent (empty match list) is a violation.
            for (std::size_t i = 0; i < matches.size(); ++i) {
                if (matches[i].empty()) {
                    set_error(core::error_t{
                        core::error_code_t::other_error,
                        std::pmr::string{"FK constraint violated: referenced row not found in parent table",
                                         resource_}});
                    co_return;
                }
            }
        }
        output_ = resolve_cursor_output(left_, source);
        mark_executed();
    }

} // namespace components::operators
