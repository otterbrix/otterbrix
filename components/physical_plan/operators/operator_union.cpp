#include "operator_union.hpp"
#include "operator_data.hpp"

#include <components/vector/cell_equal.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/indexing_vector.hpp>
#include <components/vector/vector_operations.hpp>
#include <core/operations_helper.hpp>

namespace components::operators {

    namespace {

        struct row_ref_t {
            const vector::data_chunk_t* chunk;
            uint64_t row;
        };

        // Deep-copy selected rows from src into a new chunk of `types`. Columns are copied per-column so a
        // column whose source type differs from the target (the reconciled NULL-literal UNION branch — a
        // genuine type mismatch is rejected at validation) is materialised as target-typed NULLs instead of
        // copied (data_chunk_t::copy would assert on the type mismatch). Matching columns use the whole-value
        // copy, which handles STRUCT/ARRAY/LIST.
        vector::data_chunk_t copy_rows(const vector::data_chunk_t& src,
                                       const std::pmr::vector<uint64_t>& row_indices,
                                       const std::pmr::vector<types::complex_logical_type>& types,
                                       std::pmr::memory_resource* res) {
            const uint64_t n = row_indices.size();
            vector::indexing_vector_t idx(res, n);
            for (uint64_t i = 0; i < n; ++i) {
                idx.set_index(i, row_indices[i]);
            }
            vector::data_chunk_t out(res, types, n);
            for (size_t c = 0; c < types.size(); ++c) {
                if (src.data[c].type().type() == types[c].type()) {
                    vector::vector_ops::copy(src.data[c], out.data[c], idx, n, 0, 0);
                } else {
                    for (uint64_t i = 0; i < n; ++i) {
                        out.data[c].set_null(i, true);
                    }
                }
            }
            out.set_cardinality(n);
            return out;
        }

    } // namespace

    operator_union_t::operator_union_t(std::pmr::memory_resource* resource, log_t log, bool all)
        : read_only_operator_t(resource, log, operator_type::union_op)
        , all_(all)
        , output_types_(resource) {}

    void operator_union_t::set_output_types(const std::pmr::vector<types::complex_logical_type>& types) {
        output_types_.assign(types.begin(), types.end());
    }

    void operator_union_t::emit_union_(std::pmr::memory_resource* res,
                                       const chunks_vector_t& left_chunks,
                                       const chunks_vector_t& right_chunks,
                                       chunks_vector_t& out_chunks) {
        // Output column types are the validator-stamped union schema (see set_output_types):
        // the reconciliation — including the PostgreSQL NULL-literal rule — happened
        // data-INDEPENDENTLY at validation, so the result type never depends on which rows
        // the tables happen to hold. An unstamped plan (validation leaves error/empty
        // schemas unstamped) degrades to the left side's data-derived types — PostgreSQL
        // UNION uses the first SELECT's column types — with the right side used only when
        // the left produced no chunks at all.
        std::pmr::vector<types::complex_logical_type> types(res);
        if (!output_types_.empty()) {
            types.assign(output_types_.begin(), output_types_.end());
        } else if (!left_chunks.empty() || !right_chunks.empty()) {
            const auto& src = left_chunks.empty() ? right_chunks.front().types() : left_chunks.front().types();
            types.assign(src.begin(), src.end());
        } else {
            return; // nothing to emit and no schema to type an empty result with
        }

        const size_t emitted_before = out_chunks.size();
        auto emit_shape_if_silent = [&]() {
            if (out_chunks.size() == emitted_before) {
                vector::data_chunk_t empty(res, types, 0);
                empty.set_cardinality(0);
                out_chunks.emplace_back(std::move(empty));
            }
        };

        if (all_) {
            auto copy_all = [&](const chunks_vector_t& src_chunks) {
                for (const auto& chunk : src_chunks) {
                    if (chunk.size() == 0) {
                        continue;
                    }
                    std::pmr::vector<uint64_t> all_rows(res);
                    all_rows.resize(chunk.size());
                    for (uint64_t i = 0; i < chunk.size(); ++i) {
                        all_rows[i] = i;
                    }
                    out_chunks.emplace_back(copy_rows(chunk, all_rows, types, res));
                }
            };
            copy_all(left_chunks);
            copy_all(right_chunks);
            emit_shape_if_silent();
            return;
        }

        std::pmr::unordered_map<uint64_t, std::pmr::vector<row_ref_t>> seen(res);

        auto process = [&](const vector::data_chunk_t& chunk) {
            if (chunk.size() == 0) {
                return;
            }
            vector::vector_t hash_vec(res, types::logical_type::UBIGINT, chunk.size());
            const_cast<vector::data_chunk_t&>(chunk).hash(hash_vec);
            const auto* hashes = hash_vec.data<uint64_t>();

            std::pmr::vector<uint64_t> selected(res);
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                const uint64_t h = hashes[row];
                auto it = seen.find(h);
                if (it == seen.end()) {
                    selected.push_back(row);
                    std::pmr::vector<row_ref_t> refs(res);
                    refs.push_back({&chunk, row});
                    seen.emplace(h, std::move(refs));
                } else {
                    bool is_dup = false;
                    for (const auto& ref : it->second) {
                        if (vector::rows_equal(chunk, row, *ref.chunk, ref.row)) {
                            is_dup = true;
                            break;
                        }
                    }
                    if (!is_dup) {
                        selected.push_back(row);
                        it->second.push_back({&chunk, row});
                    }
                }
            }

            if (!selected.empty()) {
                out_chunks.emplace_back(copy_rows(chunk, selected, types, res));
            }
        };

        for (const auto& chunk : left_chunks) {
            process(chunk);
        }
        for (const auto& chunk : right_chunks) {
            process(chunk);
        }
        emit_shape_if_silent();
    }

    core::error_t operator_union_t::push(pipeline::context_t*, vector::data_chunk_t&& input, chunks_vector_t&) {
        // Blocking sink: accumulate the left batch; the right side is not readable until finalize()
        buffered_left_.emplace_back(std::move(input));
        return core::error_t::no_error();
    }

    core::error_t operator_union_t::finalize(pipeline::context_t*, chunks_vector_t& out) {
        chunks_vector_t empty_right(resource_);
        const chunks_vector_t& right_chunks = (right_ && right_->output()) ? right_->output()->chunks() : empty_right;
        emit_union_(resource_, buffered_left_, right_chunks, out);
        return core::error_t::no_error();
    }

} // namespace components::operators
