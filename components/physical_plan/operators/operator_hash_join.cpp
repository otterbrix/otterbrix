#include "operator_hash_join.hpp"
#include "join_utils.hpp"

#include <components/expressions/execution_dag_builder.hpp>
#include <components/types/types.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/vector.hpp>
#include <components/vector/vector_operations.hpp>
#include <core/operations_helper.hpp>

#include <cassert>
#include <cstdint>

namespace components::operators {

    using hash_join_detail::right_index_t;
    using hash_join_detail::row_ref;

    namespace {

        // Confirm a probe row against a candidate build row by a TYPED cell-by-cell
        // comparison over every key column (uniform for single- and multi-column
        // keys). A non-matching column short-circuits to false. Callers have already
        // excluded NULLs on both sides (NULL keys never equi-join), so
        // vector::cells_equal's NULL==NULL semantics are never exercised here.
        bool keys_verify(const vector::data_chunk_t& probe,
                         uint64_t probe_row,
                         const vector::data_chunk_t& build,
                         uint64_t build_row) {
            for (size_t k = 0; k < probe.column_count(); ++k) {
                if (!vector::cells_equal(probe.data[k], probe_row, build.data[k], build_row)) {
                    return false;
                }
            }
            return true;
        }

        // True iff every key cell of `row` is non-NULL — a row with any NULL key (a failed TRY_CAST
        // included) never participates in an equi-join match (build- or probe-side).
        bool keys_all_valid(const vector::data_chunk_t& keys, uint64_t row) {
            for (const auto& key : keys.data) {
                if (!key.validity().row_is_valid(row)) {
                    return false;
                }
            }
            return true;
        }
    } // namespace

    operator_hash_join_t::operator_hash_join_t(std::pmr::memory_resource* resource,
                                               log_t log,
                                               type join_type,
                                               std::pmr::vector<expressions::expression_ptr> probe_keys,
                                               std::pmr::vector<expressions::expression_ptr> build_keys,
                                               bool swapped,
                                               bool single_match)
        : read_only_operator_t(resource, std::move(log), operator_type::hash_join)
        , join_type_(join_type)
        , single_match_(single_match)
        , swapped_(swapped)
        , probe_key_exprs_(std::move(probe_keys), resource_)
        , build_key_exprs_(std::move(build_keys), resource_) {
        assert(probe_key_exprs_.size() == build_key_exprs_.size() && "one build key per probe key");
        key_columns_.reserve(probe_key_exprs_.size());
        for (uint64_t key = 0; key < probe_key_exprs_.size(); ++key) {
            key_columns_.push_back(key);
        }
    }

    std::pmr::string operator_hash_join_t::explain_label_impl() const {
        switch (join_type_) {
            case type::semi:
                return std::pmr::string{"Hash Semi Join", resource_};
            case type::anti:
                return std::pmr::string{"Hash Anti Join", resource_};
            default:
                return std::pmr::string{resource_};
        }
    }

    core::error_t operator_hash_join_t::key_graph_(pipeline::context_t* ctx,
                                                   const std::pmr::vector<expressions::expression_ptr>& keys,
                                                   const vector::data_chunk_t& sample,
                                                   uint64_t capacity,
                                                   std::unique_ptr<execution_dag::execution_dag_t>* graph) {
        auto types = sample.types();
        auto built = std::make_unique<execution_dag::execution_dag_t>(resource_, capacity);
        execution_dag::slot_list_t slots(resource_);
        slots.reserve(keys.size());
        for (const auto& key : keys) {
            auto slot = expressions::build_expression(built.get(), ctx->parameters, key.get(), types);
            if (slot.has_error()) {
                return slot.error();
            }
            built->add_key_slot(slot.value());
            slots.push_back(slot.value());
        }
        built->set_output(slots);
        built->set_parameters(&ctx->parameters);
        if (auto error = built->prepare(); error.contains_error()) {
            return error;
        }
        for (auto slot : slots) {
            if (!vector::vector_ops::is_hashable(built->slot_type(slot))) {
                return core::error_t(core::error_code_t::unimplemented_yet,
                                     std::pmr::string{"hash join: a join key type has no hash", resource_});
            }
        }
        *graph = std::move(built);
        return core::error_t::no_error();
    }

    core::error_t operator_hash_join_t::build_index_(pipeline::context_t* ctx) {
        right_index_.clear();
        build_matched_.clear();
        build_chunk_offsets_.clear();
        build_keys_.clear();
        build_rows_ = 0;
        if (!right_ || !right_->output() || right_->output()->chunks().empty()) {
            return core::error_t::no_error();
        }
        const auto& build_chunks = right_->output()->chunks();

        if (!build_graph_) {
            if (auto error = key_graph_(ctx,
                                        build_key_exprs_,
                                        build_chunks.front(),
                                        vector::DEFAULT_VECTOR_CAPACITY,
                                        &build_graph_);
                error.contains_error()) {
                return error;
            }
        }

        const bool track_matched = (join_type_ == type::right || join_type_ == type::full);

        // Per-chunk start offsets into the flat marker; total = #build rows.
        build_chunk_offsets_.reserve(build_chunks.size());
        uint64_t total = 0;
        for (const auto& B : build_chunks) {
            build_chunk_offsets_.push_back(total);
            total += B.size();
        }
        build_rows_ = total;
        right_index_.reserve(total);
        if (track_matched) {
            build_matched_.assign(total, 0);
        }

        build_keys_.reserve(build_chunks.size());
        for (size_t ci = 0; ci < build_chunks.size(); ++ci) {
            const auto& B = build_chunks[ci];
            assert(B.size() <= vector::DEFAULT_VECTOR_CAPACITY && "a build chunk exceeds the vector capacity");
            auto& keys =
                build_keys_.emplace_back(resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 0);
            if (B.size() == 0) {
                continue;
            }
            if (auto error = build_graph_->process_keys(B, ctx->execution_context); error.contains_error()) {
                return error;
            }
            keys.data.reserve(key_columns_.size());
            for (size_t key = 0; key < key_columns_.size(); ++key) {
                const auto& type = build_graph_->slot_type(build_graph_->key_slots()[key]);
                auto& column = keys.data.emplace_back(resource_, type, false, false, 0);
                build_graph_->take_key_values(key, &column);
            }
            keys.set_capacity(B.size());
            keys.set_cardinality(B.size());
            keys.hash(key_columns_, hashes_);
            const auto* h = hashes_.data<uint64_t>();
            for (uint64_t rj = 0; rj < B.size(); ++rj) {
                // Skip NULL build keys — they never equi-join.
                if (!keys_all_valid(keys, rj)) {
                    continue;
                }
                right_index_.emplace(h[rj], row_ref{static_cast<uint32_t>(ci), static_cast<uint32_t>(rj)});
            }
        }
        return core::error_t::no_error();
    }

    core::error_t operator_hash_join_t::probe_batch_(pipeline::context_t* ctx,
                                                     const vector::data_chunk_t& probe,
                                                     chunks_vector_t& out) {
        const bool left_outer = (join_type_ == type::left || join_type_ == type::full);
        const bool mark_matched = (join_type_ == type::right || join_type_ == type::full);
        const bool keeps_probe = (join_type_ == type::semi || join_type_ == type::anti);

        builder_.set_output(&out);

        const uint64_t n = probe.size();
        if (n == 0) {
            return core::error_t::no_error();
        }
        assert(n <= vector::DEFAULT_VECTOR_CAPACITY && "a probe chunk exceeds the vector capacity");
        // PostgreSQL 18 skips the probe when its hash table is empty, and a right join's table keeps NULL keys.
        if (((join_type_ == type::inner || join_type_ == type::semi) && right_index_.empty()) ||
            (join_type_ == type::right && build_rows_ == 0)) {
            return core::error_t::no_error();
        }
        if (join_type_ == type::anti && right_index_.empty()) {
            vector::data_chunk_t every_row(resource_, probe.types(), n);
            probe.copy(every_row, 0);
            out.push_back(std::move(every_row));
            note_emitted();
            return core::error_t::no_error();
        }
        if (!probe_graph_) {
            if (auto error =
                    key_graph_(ctx, probe_key_exprs_, probe, vector::DEFAULT_VECTOR_CAPACITY, &probe_graph_);
                error.contains_error()) {
                return error;
            }
            for (size_t key = 0; key < key_columns_.size(); ++key) {
                const auto& build_slot = build_graph_->key_slots()[key];
                const auto& probe_slot = probe_graph_->key_slots()[key];
                if (build_graph_->slot_type(build_slot) != probe_graph_->slot_type(probe_slot)) {
                    return core::error_t(
                        core::error_code_t::internal_error,
                        std::pmr::string{"hash join: the two sides of a join key differ in type", resource_});
                }
            }
        }
        if (auto error = probe_graph_->process_keys(probe, ctx->execution_context); error.contains_error()) {
            return error;
        }
        if (probe_keys_.empty()) {
            auto& keys =
                probe_keys_.emplace_back(resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 0);
            keys.data.reserve(key_columns_.size());
            for (size_t key = 0; key < key_columns_.size(); ++key) {
                keys.data.emplace_back(probe_graph_->key_values(key));
            }
        } else {
            for (size_t key = 0; key < key_columns_.size(); ++key) {
                probe_keys_.front().data[key].reference(probe_graph_->key_values(key));
            }
        }
        auto& keys = probe_keys_.front();
        keys.set_capacity(n);
        keys.set_cardinality(n);
        keys.hash(key_columns_, hashes_);
        const auto* h = hashes_.data<uint64_t>();

        const auto& build_chunks = right_->output()->chunks();
        vector::indexing_vector_t kept(resource_, keeps_probe ? n : uint64_t{0});
        uint64_t kept_count = 0;
        for (uint64_t li = 0; li < n; ++li) {
            bool matched = false;
            // A NULL probe key matches nothing (left-outer still emits the row).
            if (keys_all_valid(keys, li)) {
                auto range = right_index_.equal_range(h[li]);
                for (auto it = range.first; it != range.second; ++it) {
                    const row_ref& ref = it->second;
                    // Collision-safe: confirm by a typed key comparison.
                    if (!keys_verify(keys, li, build_keys_[ref.chunk_index], ref.row_index)) {
                        continue;
                    }
                    if (keeps_probe) {
                        matched = true;
                        break;
                    }
                    if (matched && single_match_) {
                        return join_detail::more_than_one_row(resource_);
                    }
                    builder_.emit_matched(probe, li, build_chunks[ref.chunk_index], ref.row_index);
                    matched = true;
                    if (mark_matched) {
                        build_matched_[build_chunk_offsets_[ref.chunk_index] + ref.row_index] = 1;
                    }
                }
            }
            if (keeps_probe) {
                if (matched == (join_type_ == type::semi)) {
                    kept.set_index(kept_count++, li);
                }
                continue;
            }
            if (!matched && left_outer) {
                builder_.emit_left_only(probe, li);
            }
        }
        if (keeps_probe) {
            join_detail::emit_probe_rows(resource_, probe, kept, kept_count, out);
            if (kept_count != 0) {
                note_emitted();
            }
            return core::error_t::no_error();
        }
        builder_.gather();
        return core::error_t::no_error();
    }

    void operator_hash_join_t::emit_unmatched_build_(chunks_vector_t& out) {
        if (join_type_ != type::right && join_type_ != type::full) {
            return;
        }
        if (!right_ || !right_->output()) {
            return;
        }
        const auto& build_chunks = right_->output()->chunks();
        builder_.set_output(&out);
        for (size_t ci = 0; ci < build_chunks.size(); ++ci) {
            const auto& B = build_chunks[ci];
            const uint64_t base = build_chunk_offsets_[ci];
            for (uint64_t rj = 0; rj < B.size(); ++rj) {
                // A NULL-key build row is never marked matched, so it is correctly
                // emitted here (right/full preserve every build row).
                if (build_matched_[base + rj] == 0) {
                    builder_.emit_right_only(B, rj);
                }
            }
        }
    }

    core::error_t
    operator_hash_join_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // Degenerate build side → emit nothing. This covers a truly absent right_
        // AND a build side that materialized ZERO chunks: a single-table filter
        // pushed BELOW the join can empty the build scan, and a disk scan
        // over no rows yields an operator_data_t whose chunks() is empty (not one
        // empty chunk). Either way there is no build schema to lay out and, for an
        // inner/right-probe join, no build rows to match — so compute_join_layout
        // must never be handed a missing front chunk (dereferencing
        // build_chunks.front() on an empty vector is UB: a null data_chunk_t whose
        // types() faulted under -O2). Left NULL-padding is not preserved for this
        // degenerate shape, matching the long-standing absent-right behavior;
        // pushdown never pushes a filter onto an outer join's optional side, so an
        // emptied build here is always an inner join with genuinely zero matches.
        if (!right_ || !right_->output() || right_->output()->chunks().empty()) {
            index_built_ = true;
            if (join_type_ == type::anti || join_type_ == type::semi) {
                res_types_ = input.types();
                layout_ready_ = true;
                if (join_type_ == type::anti && input.size() != 0) {
                    out.push_back(std::move(input));
                    note_emitted();
                }
            }
            return core::error_t::no_error();
        }

        if (!layout_ready_ && (join_type_ == type::semi || join_type_ == type::anti)) {
            res_types_ = input.types();
            layout_ready_ = true;
        }
        if (!layout_ready_) {
            const auto& build_chunks = right_->output()->chunks();
            // Non-empty by the degenerate-build guard above.
            res_types_ = std::pmr::vector<types::complex_logical_type>{resource_};
            join_detail::compute_join_layout(input,
                                             build_chunks.front(),
                                             swapped_,
                                             res_types_,
                                             indices_left_,
                                             indices_right_);
            layout_ready_ = true;
        }
        // As PostgreSQL 18's ExecHashJoin: inner and left joins build only once a probe row exists.
        if (!index_built_) {
            if ((join_type_ == type::inner || join_type_ == type::left || join_type_ == type::semi ||
                 join_type_ == type::anti) &&
                input.size() == 0) {
                return core::error_t::no_error();
            }
            if (auto error = build_index_(ctx); error.contains_error()) {
                return error;
            }
            index_built_ = true;
        }

        return probe_batch_(ctx, input, out);
    }

    core::error_t operator_hash_join_t::finalize(pipeline::context_t* ctx, chunks_vector_t& out) {
        // The builder holds its output chunk ACROSS probe batches, so finalize() is
        // where the last partial chunk is emitted — for EVERY join type, not only the
        // right/full drain below.
        //
        // An inner or left join that never saw a probe row has nothing to emit from the build side, so its
        // index is never built; right and full always build, since every build row is emitted. If push() never
        // ran, res_types_ is empty and nothing is emitted at all.
        if (!index_built_) {
            if (join_type_ == type::right || join_type_ == type::full) {
                if (auto error = build_index_(ctx); error.contains_error()) {
                    return error;
                }
            }
            index_built_ = true;
        }
        if (res_types_.empty()) {
            return core::error_t::no_error();
        }
        if (join_type_ != type::semi && join_type_ != type::anti) {
            emit_unmatched_build_(out);
            builder_.set_output(&out);
            builder_.flush();
            if (builder_.emitted()) {
                note_emitted();
            }
        }
        if (!emitted()) {
            vector::data_chunk_t empty(resource_, res_types_, 0);
            empty.set_cardinality(0);
            out.emplace_back(std::move(empty));
        }
        return core::error_t::no_error();
    }

} // namespace components::operators
