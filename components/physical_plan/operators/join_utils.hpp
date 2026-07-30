#pragma once

#include <components/physical_plan/operators/operator_data.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector_operations.hpp>

#include <algorithm>
#include <unordered_map>
#include <vector>

// Shared building blocks for the join operators. operator_join_t (nested-loop,
// all join types) and operator_hash_join_t (equi-join fast path) produce the
// same output layout and stream rows the same way; the only difference is how
// they decide which (left, right) row pairs match. Everything that is common
// lives here so the two operators stay in sync.
namespace components::operators::join_detail {

    // Placeholder columns (produced by projected scans) have no buffer and no auxiliary.
    // They must be skipped when copying — vector_ops::copy would dereference a null data_.
    inline bool is_placeholder(const vector::vector_t& v) noexcept {
        return v.data() == nullptr && v.auxiliary() == nullptr;
    }

    // The join's output schema: one record per output column, carrying the column's NAME
    // and catalog identity next to its type (M3-B5).
    //
    // A join is the one operator that MERGES two inputs into a single chunk, and the
    // merged chunk records the split nowhere — which side a column came from survives
    // only as its name. A bare `vector<complex_logical_type>` used to carry that name
    // implicitly, inside the type's alias slot, which is exactly the slot this stage
    // takes away; the same list would then arrive at the output chunk naming nothing.
    // Carrying `{attoid, name, type}` says out loud what was being smuggled.
    //
    // column_schema_t is move-only by design (a defaulted copy would take the string's
    // allocator from std::pmr's DEFAULT resource), so a record is never copied out of an
    // input chunk — it is rebuilt against the owner's resource with clone().
    using output_schema_t = std::pmr::vector<vector::column_schema_t>;

    // The chunk one flush emits: built from the joined schema, with each column given
    // BOTH halves of its identity — the name and the attoid — from the schema record
    // rather than from the type it happens to be constructed out of. The type is where a
    // name still lives today, so building from types alone would keep working and stop
    // the day the slot goes; reading the record cannot.
    inline vector::data_chunk_t
    make_output_chunk(std::pmr::memory_resource* resource, const output_schema_t& schema, uint64_t capacity) {
        std::pmr::vector<types::complex_logical_type> types(resource);
        types.reserve(schema.size());
        for (const auto& record : schema) {
            types.push_back(record.type);
        }
        vector::data_chunk_t chunk(resource, types, capacity);
        for (uint64_t i = 0; i < schema.size(); i++) {
            // The guard skips a copy, it does not decide anything: while a name still
            // rides inside the type, the column was born holding it and the store is
            // redundant; once it does not, the guard always fires. vector_t::name() is
            // total — an unnamed column answers with an empty view.
            if (chunk.data[i].name() != std::string_view{schema[i].name}) {
                chunk.set_column_name(i, schema[i].name);
            }
            chunk.set_column_attoid(i, schema[i].attoid);
        }
        return chunk;
    }

    // Computes the joined output schema and the per-side column→output-slot maps.
    //
    // The output schema is assembled in LOGICAL [left, right] order regardless of
    // which physical input the operator materialized as its build side:
    //   * `left_front`  is the operator's PROBE (physical left) front chunk.
    //   * `right_front` is the operator's BUILD (physical right) front chunk.
    //   * `swapped == false` — probe is logical-left, build is logical-right:
    //       output = [probe cols 0..Lw, build cols Lw..Lw+Bw].
    //   * `swapped == true`  — build is logical-left, probe is logical-right (the
    //       build-side-selection heuristic moved the smaller/logical-left table into
    //       the physical build slot): output = [build cols 0..Bw, probe cols Bw..Bw+Lw].
    // `indices_left`/`indices_right` stay pure source→slot maps: indices_left[c] is
    // the output slot for probe column c, indices_right[c] for build column c. The
    // join_builder gathers each source column into its slot, so it never needs to
    // know the orientation — only these maps change.
    //
    // The width invariant (Lw + Bw == total output cols) holds by construction from
    // the same front chunks, so it is never guarded on the hot path.
    //
    // Duplicate (same-aliased) columns across the two sides stay addressable via
    // their table qualifier; USING/NATURAL column-merging is resolved at the logical
    // layer (validate_logical_plan.cpp), not here.
    inline void compute_join_layout(std::pmr::memory_resource* resource,
                                    const vector::data_chunk_t& left_front,
                                    const vector::data_chunk_t& right_front,
                                    bool swapped,
                                    output_schema_t& res_schema,
                                    std::vector<size_t>& indices_left,
                                    std::vector<size_t>& indices_right) {
        const auto& left_schema = left_front.schema();   // probe columns
        const auto& right_schema = right_front.schema(); // build columns
        const size_t left_col_count = left_front.column_count();
        const size_t right_col_count = right_front.column_count();

        indices_left.clear();
        indices_right.clear();
        indices_left.reserve(left_col_count);
        indices_right.reserve(right_col_count);
        res_schema.clear();
        res_schema.reserve(left_col_count + right_col_count);

        if (!swapped) {
            // Probe == logical-left, build == logical-right.
            for (size_t i = 0; i < left_col_count; ++i) {
                indices_left.emplace_back(i);
                res_schema.emplace_back(left_schema[i].clone(resource));
            }
            for (size_t i = 0; i < right_col_count; ++i) {
                indices_right.emplace_back(left_col_count + i);
                res_schema.emplace_back(right_schema[i].clone(resource));
            }
        } else {
            // Build == logical-left, probe == logical-right.
            for (size_t i = 0; i < right_col_count; ++i) {
                indices_right.emplace_back(i);
                res_schema.emplace_back(right_schema[i].clone(resource));
            }
            for (size_t i = 0; i < left_col_count; ++i) {
                indices_left.emplace_back(right_col_count + i);
                res_schema.emplace_back(left_schema[i].clone(resource));
            }
        }
    }

    // Streams join output into a chunks_vector_t where every chunk is
    // ≤ DEFAULT_VECTOR_CAPACITY (1024) rows.
    //
    // Rather than materialize one cell at a time (which allocated a length-(li+1)
    // indexing sequence per cell), the builder BUFFERS the matched source-row
    // indices and flushes each full output chunk with ONE indexed vector_ops::copy
    // (a gather) per (source-chunk, column). The probe (left) side is single-source
    // per builder, so it is one gather per left column over the whole chunk; the
    // build (right) side may span several chunks, so output rows are REORDERED
    // grouped by their build chunk — one gather per (build-chunk, right column).
    //
    // Consequence: the operator's emit ROW order is UNSPECIFIED (downstream
    // sorts/groups absorb it). COLUMN identity stays logical [left, right] — that is
    // an orthogonal axis fixed by compute_join_layout / the indices_* maps.
    //
    // A single builder is used in exactly one of two modes, matching the callers:
    //   * probe mode  — emit_matched / emit_left_only: every buffered row has a valid
    //     left (probe) source; right source is a build chunk (matched) or NULL (left-only).
    //   * drain mode  — emit_right_only only: every row's left source is NULL, right
    //     source is a build chunk.
    // NULL-padding therefore routes to the LOGICAL side via the same source→slot maps.
    class join_builder {
    public:
        join_builder(std::pmr::memory_resource* resource,
                     const output_schema_t& out_schema,
                     const std::vector<size_t>& indices_left,
                     const std::vector<size_t>& indices_right,
                     chunks_vector_t& out_chunks)
            : resource_(resource)
            , out_schema_(out_schema)
            , indices_left_(indices_left)
            , indices_right_(indices_right)
            , out_chunks_(out_chunks)
            , cur_(make_output_chunk(resource, out_schema, vector::DEFAULT_VECTOR_CAPACITY))
            , buf_left_rows_(resource)
            , buf_right_chunks_(resource)
            , buf_right_rows_(resource) {}

        void flush() {
            const uint64_t n = filled_;
            if (n == 0) {
                return;
            }

            // --- Group buffered rows by their build (right) chunk pointer. A NULL
            // right chunk (left-only rows) forms its own group. Rows are emitted into
            // the output grouped by chunk so each right-column gather targets a
            // contiguous range from a single source chunk. ---
            std::pmr::unordered_map<const vector::data_chunk_t*, uint32_t> group_id{resource_};
            std::pmr::vector<const vector::data_chunk_t*> group_chunk{resource_};
            std::pmr::vector<uint32_t> entry_group{resource_};
            entry_group.reserve(n);
            for (uint64_t k = 0; k < n; ++k) {
                const auto* rc = buf_right_chunks_[k];
                auto it = group_id.find(rc);
                uint32_t g;
                if (it == group_id.end()) {
                    g = static_cast<uint32_t>(group_chunk.size());
                    group_id.emplace(rc, g);
                    group_chunk.push_back(rc);
                } else {
                    g = it->second;
                }
                entry_group.push_back(g);
            }
            const uint32_t group_count = static_cast<uint32_t>(group_chunk.size());

            // Counting sort into `order` (output slot → buffer entry index), grouped
            // by build chunk. group_start[g] is the first output slot of group g.
            std::pmr::vector<uint64_t> group_start{resource_};
            group_start.assign(group_count + 1, 0);
            for (uint64_t k = 0; k < n; ++k) {
                ++group_start[entry_group[k] + 1];
            }
            for (uint32_t g = 0; g < group_count; ++g) {
                group_start[g + 1] += group_start[g];
            }
            std::pmr::vector<uint64_t> order{resource_};
            order.assign(n, 0);
            std::pmr::vector<uint64_t> cursor{resource_};
            cursor.assign(group_start.begin(), group_start.end());
            for (uint64_t k = 0; k < n; ++k) {
                order[cursor[entry_group[k]]++] = k;
            }

            // --- Left (probe) columns. Single source, so one gather over [0, n). ---
            if (left_chunk_ != nullptr) {
                vector::indexing_vector_t idx(resource_, n);
                for (uint64_t out = 0; out < n; ++out) {
                    idx.set_index(out, buf_left_rows_[order[out]]);
                }
                for (size_t c = 0; c < left_chunk_->column_count(); ++c) {
                    if (is_placeholder(left_chunk_->data[c])) {
                        continue;
                    }
                    vector::vector_ops::copy(left_chunk_->data[c], cur_.data[indices_left_[c]], idx, n, 0, 0, n);
                }
            } else {
                // Drain mode: every row is right-only → NULL-pad all left columns.
                for (size_t c = 0; c < indices_left_.size(); ++c) {
                    for (uint64_t out = 0; out < n; ++out) {
                        cur_.data[indices_left_[c]].validity().set_invalid(out);
                    }
                }
            }

            // --- Right (build) columns, one gather per (build-chunk, column). ---
            for (uint32_t g = 0; g < group_count; ++g) {
                const auto* rc = group_chunk[g];
                const uint64_t s = group_start[g];
                const uint64_t e = group_start[g + 1];
                const uint64_t count = e - s;
                if (count == 0) {
                    continue;
                }
                if (rc == nullptr) {
                    // left-only rows → NULL-pad all right columns over the range.
                    for (size_t c = 0; c < indices_right_.size(); ++c) {
                        for (uint64_t out = s; out < e; ++out) {
                            cur_.data[indices_right_[c]].validity().set_invalid(out);
                        }
                    }
                } else {
                    vector::indexing_vector_t idx(resource_, count);
                    for (uint64_t i = 0; i < count; ++i) {
                        idx.set_index(i, buf_right_rows_[order[s + i]]);
                    }
                    for (size_t c = 0; c < rc->column_count(); ++c) {
                        if (is_placeholder(rc->data[c])) {
                            continue;
                        }
                        vector::vector_ops::copy(rc->data[c], cur_.data[indices_right_[c]], idx, count, 0, s, count);
                    }
                }
            }

            cur_.set_cardinality(n);
            out_chunks_.emplace_back(std::move(cur_));
            cur_ = make_output_chunk(resource_, out_schema_, vector::DEFAULT_VECTOR_CAPACITY);
            filled_ = 0;
            left_chunk_ = nullptr;
            buf_left_rows_.clear();
            buf_right_chunks_.clear();
            buf_right_rows_.clear();
        }

        void emit_matched(const vector::data_chunk_t& L, uint64_t li, const vector::data_chunk_t& R, uint64_t rj) {
            ensure_space();
            left_chunk_ = &L;
            buf_left_rows_.push_back(li);
            buf_right_chunks_.push_back(&R);
            buf_right_rows_.push_back(rj);
            ++filled_;
        }

        // L row with NULLs on all right-side output columns.
        void emit_left_only(const vector::data_chunk_t& L, uint64_t li) {
            ensure_space();
            left_chunk_ = &L;
            buf_left_rows_.push_back(li);
            buf_right_chunks_.push_back(nullptr);
            buf_right_rows_.push_back(0);
            ++filled_;
        }

        // R row with NULLs on all left-side output columns.
        void emit_right_only(const vector::data_chunk_t& R, uint64_t rj) {
            ensure_space();
            buf_left_rows_.push_back(0);
            buf_right_chunks_.push_back(&R);
            buf_right_rows_.push_back(rj);
            ++filled_;
        }

    private:
        void ensure_space() {
            if (filled_ == vector::DEFAULT_VECTOR_CAPACITY) {
                flush();
            }
        }

        std::pmr::memory_resource* resource_;
        const output_schema_t& out_schema_;
        const std::vector<size_t>& indices_left_;
        const std::vector<size_t>& indices_right_;
        chunks_vector_t& out_chunks_;
        vector::data_chunk_t cur_;
        uint64_t filled_ = 0;

        // Buffered source-row indices for the current (not-yet-flushed) output chunk.
        // `left_chunk_` is the single probe source (nullptr in drain mode); a NULL
        // entry in `buf_right_chunks_` marks a left-only row (NULL right side).
        const vector::data_chunk_t* left_chunk_ = nullptr;
        std::pmr::vector<uint64_t> buf_left_rows_;
        std::pmr::vector<const vector::data_chunk_t*> buf_right_chunks_;
        std::pmr::vector<uint64_t> buf_right_rows_;
    };

    // Eager join output builder for operators whose BUILD (right) side is NOT
    // materialized once for the whole probe but REGENERATED per outer row — namely
    // operator_lateral_join_t, which re-runs its inner sub-plan for every outer row.
    //
    // The lazy join_builder above buffers raw (left_chunk, right_chunk) POINTERS and
    // gathers them at flush(). That is sound ONLY when (i) a single probe chunk feeds
    // the whole build and (ii) every build chunk outlives flush(). LATERAL breaks
    // BOTH invariants: the outer input spans several persistent chunks (so a single
    // overwritten left_chunk_ would mix rows across a flush window), and each inner
    // result is destroyed at the end of its outer-row iteration (so a buffered right
    // pointer would dangle by the post-loop flush → heap-use-after-free). This builder
    // instead COPIES each row's left+right cells into the current output chunk at emit
    // time, while both sources are still alive and correctly identified — no pointer
    // buffering, so neither invariant is needed.
    //
    // Row/column semantics match join_builder: logical [left, right] output layout via
    // the indices_* source→slot maps, NULL-padding the absent side for emit_left_only.
    // It is a distinct strategy for a distinct operator — the two builders never share
    // an instance or a flush path (no runtime lazy/eager toggle).
    class eager_join_builder {
    public:
        eager_join_builder(std::pmr::memory_resource* resource,
                           const output_schema_t& out_schema,
                           const std::vector<size_t>& indices_left,
                           const std::vector<size_t>& indices_right,
                           chunks_vector_t& out_chunks)
            : resource_(resource)
            , out_schema_(out_schema)
            , indices_left_(indices_left)
            , indices_right_(indices_right)
            , out_chunks_(out_chunks)
            , cur_(make_output_chunk(resource, out_schema, vector::DEFAULT_VECTOR_CAPACITY))
            , idx1_(resource, uint64_t{1}) {}

        // Matched (L row li) × (R row rj): copy both source rows into the output now.
        void emit_matched(const vector::data_chunk_t& L, uint64_t li, const vector::data_chunk_t& R, uint64_t rj) {
            copy_row_(L, li, indices_left_);
            copy_row_(R, rj, indices_right_);
            advance_();
        }

        // L row li with NULLs on all right-side output columns.
        void emit_left_only(const vector::data_chunk_t& L, uint64_t li) {
            copy_row_(L, li, indices_left_);
            null_side_(indices_right_);
            advance_();
        }

        void flush() {
            if (filled_ == 0) {
                return;
            }
            cur_.set_cardinality(filled_);
            out_chunks_.emplace_back(std::move(cur_));
            cur_ = make_output_chunk(resource_, out_schema_, vector::DEFAULT_VECTOR_CAPACITY);
            filled_ = 0;
        }

    private:
        // Gather source row `srow` into output row `filled_` via the side's source→slot map.
        void copy_row_(const vector::data_chunk_t& src, uint64_t srow, const std::vector<size_t>& slots) {
            idx1_.set_index(0, srow);
            const size_t cols = src.column_count();
            for (size_t c = 0; c < cols; ++c) {
                if (is_placeholder(src.data[c])) {
                    continue;
                }
                vector::vector_ops::copy(src.data[c], cur_.data[slots[c]], idx1_, 1, 0, filled_, 1);
            }
        }

        void null_side_(const std::vector<size_t>& slots) {
            for (size_t c = 0; c < slots.size(); ++c) {
                cur_.data[slots[c]].validity().set_invalid(filled_);
            }
        }

        void advance_() {
            if (++filled_ == vector::DEFAULT_VECTOR_CAPACITY) {
                flush();
            }
        }

        std::pmr::memory_resource* resource_;
        const output_schema_t& out_schema_;
        const std::vector<size_t>& indices_left_;
        const std::vector<size_t>& indices_right_;
        chunks_vector_t& out_chunks_;
        vector::data_chunk_t cur_;
        vector::indexing_vector_t idx1_;
        uint64_t filled_ = 0;
    };

} // namespace components::operators::join_detail
