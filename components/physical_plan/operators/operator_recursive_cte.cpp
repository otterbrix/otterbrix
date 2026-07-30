#include "operator_recursive_cte.hpp"
#include "operator_data.hpp"

#include <components/context/context.hpp>
#include <components/context/subplan_runner.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/data_chunk.hpp>

#include <unordered_map>
#include <vector>

namespace components::operators {

    namespace {
        // An address into the accumulated `result` chunks (index-based so it survives
        // result's reallocation as it grows).
        struct row_ref_t {
            std::size_t chunk_idx;
            uint64_t row;
        };

        // Whole-row equality via the engine's canonical NULL-aware typed cell equality
        // (the same semantics as GROUP BY / HASH JOIN / DISTINCT).
        bool rows_equal(const vector::data_chunk_t& a, uint64_t ra, const vector::data_chunk_t& b, uint64_t rb) {
            for (size_t c = 0; c < a.column_count(); ++c) {
                if (!vector::cells_equal(a.data[c], ra, b.data[c], rb)) {
                    return false;
                }
            }
            return true;
        }
    } // namespace

    // Upper bound on fixpoint iterations: a clean error instead of an unbounded hang if
    // a recursive term keeps producing rows (cyclic graph data with no terminating filter).
    // Generous so legitimate deep hierarchies are never clipped.
    static constexpr std::size_t kMaxRecursionDepth = 10000;

    operator_recursive_cte_t::operator_recursive_cte_t(std::pmr::memory_resource* resource, log_t log, bool union_all)
        : read_only_operator_t(resource, std::move(log), operator_type::recursive_cte)
        , union_all_(union_all) {}

    void operator_recursive_cte_t::reset_recursive_subtree(const operator_ptr& op) {
        if (!op) {
            return;
        }
        // Generic reuse reset (state_ -> created, output_ -> null) PLUS the streaming
        // bookkeeping rewind (scan re-OPEN, cte_scan re-walk, hash-join index rebuild).
        // prepared_ is left set, so the cte_scan's captured working_set_ pointer survives
        // (no re-create, no dangle).
        op->reset_for_reuse();
        op->reset_pipeline_state();
        reset_recursive_subtree(op->left());
        reset_recursive_subtree(op->right());
    }

    actor_zeta::unique_future<void> operator_recursive_cte_t::await_async_and_resume(pipeline::context_t* ctx) {
        auto err = co_await drive_fixpoint_(ctx);
        if (err.contains_error()) {
            set_error(err);
            mark_failed();
            co_return;
        }
        mark_executed();
        co_return;
    }

    actor_zeta::unique_future<core::error_t> operator_recursive_cte_t::drive_fixpoint_(pipeline::context_t* ctx) {
        auto* res = resource_;

        if (!anchor_) {
            co_return core::error_t::no_error();
        }
        if (!ctx->runner) {
            co_return core::error_t{core::error_code_t::physical_plan_error,
                                    std::pmr::string{"recursive_cte: no sub-plan runner", res}};
        }

        chunks_vector_t result(res);

        // For UNION (DISTINCT): a row is emitted (into `result` AND the next working set) only the first
        // time it is seen across the ENTIRE accumulated result — PostgreSQL recursive-UNION semantics, and
        // the termination guard for cyclic graphs (a revisited node produces no new working-set rows). For
        // UNION ALL every row passes and `seen` stays unused.
        // hash -> the rows already emitted into `result` carrying that hash. Typed hash
        // + cells_equal verify (the canonical dedup), NOT a lossy string key.
        std::pmr::unordered_map<uint64_t, std::pmr::vector<row_ref_t>> seen(res);
        auto emit_rows = [&](const vector::data_chunk_t& chunk, chunks_vector_t& ws_target) {
            if (chunk.size() == 0) {
                return;
            }
            if (union_all_) {
                result.emplace_back(chunk.partial_copy(res, 0, chunk.size()));
                ws_target.emplace_back(chunk.partial_copy(res, 0, chunk.size()));
                return;
            }
            vector::vector_t hash_vec(res, types::logical_type::UBIGINT, chunk.size());
            const_cast<vector::data_chunk_t&>(chunk).hash(hash_vec);
            const auto* hashes = hash_vec.data<uint64_t>();

            std::vector<size_t> keep;
            keep.reserve(chunk.size());
            for (size_t r = 0; r < chunk.size(); ++r) {
                const uint64_t h = hashes[r];
                bool dup = false;
                auto it = seen.find(h);
                if (it != seen.end()) {
                    for (const auto& ref : it->second) {
                        if (rows_equal(result[ref.chunk_idx], ref.row, chunk, r)) {
                            dup = true;
                            break;
                        }
                    }
                }
                // Intra-chunk: a row produced twice within THIS chunk collapses too (the
                // first copy is not in `result` yet, so check the already-kept rows).
                if (!dup) {
                    for (size_t k : keep) {
                        if (hashes[k] == h && rows_equal(chunk, k, chunk, r)) {
                            dup = true;
                            break;
                        }
                    }
                }
                if (!dup) {
                    keep.push_back(r);
                }
            }
            if (keep.empty()) {
                return;
            }
            vector::data_chunk_t filtered(res, chunk.types(), keep.size());
            filtered.set_cardinality(keep.size());
            for (size_t out_r = 0; out_r < keep.size(); ++out_r) {
                for (size_t col = 0; col < chunk.column_count(); ++col) {
                    filtered.set_value(col, out_r, chunk.value(col, keep[out_r]));
                }
            }
            // Register the kept rows against their position in the just-appended result
            // chunk (index-based, so a later result reallocation does not dangle them).
            const std::size_t result_idx = result.size();
            result.emplace_back(filtered.partial_copy(res, 0, filtered.size()));
            for (size_t out_r = 0; out_r < keep.size(); ++out_r) {
                seen[hashes[keep[out_r]]].push_back(row_ref_t{result_idx, out_r});
            }
            ws_target.emplace_back(std::move(filtered));
        };

        // 1. Anchor (the non-recursive term). Seed both `result` and the working set with its rows
        //    (de-duplicated for UNION). run_subplan returns COPIES, so they are ours to own.
        {
            auto anchor_res = co_await ctx->runner->run_subplan(anchor_, ctx);
            if (anchor_res.has_error()) {
                co_return anchor_res.error();
            }
            chunks_vector_t seed(res);
            for (auto& chunk : anchor_res.value()) {
                emit_rows(chunk, seed);
            }
            working_set_ = make_operator_data(res, std::move(seed));
        }

        // 2. Fixpoint: each pass re-runs the recursive term over the CURRENT working set
        //    (the rows the previous pass produced), appends its output to `result` (all rows for
        //    UNION ALL; only rows not yet seen for UNION), and repoints the working set to that
        //    fresh output. Terminate when a pass yields no NEW rows. The recursion-depth guard
        //    still bounds a pathological UNION ALL that never converges.
        if (recursive_) {
            std::size_t depth = 0;
            while (working_set_ && working_set_->size() > 0) {
                if (++depth > kMaxRecursionDepth) {
                    co_return core::error_t{core::error_code_t::physical_plan_error,
                                            std::pmr::string{"recursive_cte: maximum recursion depth exceeded", res}};
                }

                // Fresh run of the recursive-term sub-plan over the repointed working set.
                reset_recursive_subtree(recursive_);
                auto step = co_await ctx->runner->run_subplan(recursive_, ctx);
                if (step.has_error()) {
                    co_return step.error();
                }

                chunks_vector_t next(res);
                for (auto& chunk : step.value()) {
                    emit_rows(chunk, next);
                }
                if (next.empty()) {
                    break;
                }
                working_set_ = make_operator_data(res, std::move(next));
            }
        }

        // Preserve the legacy empty-result schema guard: an empty CTE still emits one
        // schema'd 0-row chunk (so a downstream projection/aggregate sees the columns).
        if (result.empty() && anchor_->output() && !anchor_->output()->chunks().empty()) {
            // A zero-row answer still NAMES its columns: built from the anchor's schema, not
            // from its bare types (M3-B5).
            result.emplace_back(
                vector::make_chunk(res,
                                   vector::clone_schema(res, anchor_->output()->chunks().front().schema()),
                                   0));
        }

        output_ = make_operator_data(res, std::move(result));
        co_return core::error_t::no_error();
    }

} // namespace components::operators
