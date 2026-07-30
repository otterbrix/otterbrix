#include "operator_union.hpp"
#include "operator_data.hpp"

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

        // Physical equality for a single column at given row indices.
        // Handles FLAT vectors directly; falls back to value() for others.
        bool cols_equal(const vector::vector_t& a, uint64_t ra, const vector::vector_t& b, uint64_t rb) {
            using pt = types::physical_type;
            if (a.get_vector_type() == vector::vector_type::FLAT && b.get_vector_type() == vector::vector_type::FLAT) {
                switch (a.type().to_physical_type()) {
                    case pt::BOOL:
                        return a.data<bool>()[ra] == b.data<bool>()[rb];
                    case pt::INT8:
                        return a.data<int8_t>()[ra] == b.data<int8_t>()[rb];
                    case pt::INT16:
                        return a.data<int16_t>()[ra] == b.data<int16_t>()[rb];
                    case pt::INT32:
                        return a.data<int32_t>()[ra] == b.data<int32_t>()[rb];
                    case pt::INT64:
                        return a.data<int64_t>()[ra] == b.data<int64_t>()[rb];
                    case pt::UINT8:
                        return a.data<uint8_t>()[ra] == b.data<uint8_t>()[rb];
                    case pt::UINT16:
                        return a.data<uint16_t>()[ra] == b.data<uint16_t>()[rb];
                    case pt::UINT32:
                        return a.data<uint32_t>()[ra] == b.data<uint32_t>()[rb];
                    case pt::UINT64:
                        return a.data<uint64_t>()[ra] == b.data<uint64_t>()[rb];
                    case pt::FLOAT:
                        return core::is_equals(a.data<float>()[ra], b.data<float>()[rb]);
                    case pt::DOUBLE:
                        return core::is_equals(a.data<double>()[ra], b.data<double>()[rb]);
                    default:
                        break;
                }
            }
            return a.value(ra) == b.value(rb);
        }

        bool rows_equal(const vector::data_chunk_t& a, uint64_t ra, const vector::data_chunk_t& b, uint64_t rb) {
            for (size_t c = 0; c < a.column_count(); ++c) {
                if (!cols_equal(a.data[c], ra, b.data[c], rb)) {
                    return false;
                }
            }
            return true;
        }

        // Deep-copy selected rows from src into a new chunk of `schema`. Columns are copied per-column so a
        // column whose source type differs from the target (the reconciled NULL-literal UNION branch — a
        // genuine type mismatch is rejected at validation) is materialised as target-typed NULLs instead of
        // copied (data_chunk_t::copy would assert on the type mismatch). Matching columns use the whole-value
        // copy, which handles STRUCT/ARRAY/LIST. make_chunk names each output column from the plan-stamped
        // schema, so the union result is named by the plan and not by whichever branch supplied the row.
        vector::data_chunk_t copy_rows(const vector::data_chunk_t& src,
                                       const std::pmr::vector<uint64_t>& row_indices,
                                       const vector::schema_t& schema,
                                       std::pmr::memory_resource* res) {
            const uint64_t n = row_indices.size();
            vector::indexing_vector_t idx(res, n);
            for (uint64_t i = 0; i < n; ++i) {
                idx.set_index(i, row_indices[i]);
            }
            vector::data_chunk_t out = vector::make_chunk(res, schema, n);
            for (size_t c = 0; c < schema.size(); ++c) {
                if (src.data[c].type().type() == schema[c].type.type()) {
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
        , output_schema_(resource) {}

    void operator_union_t::set_output_schema(const vector::schema_t& schema) {
        output_schema_ = vector::clone_schema(resource_, schema);
    }

    core::error_t operator_union_t::emit_union_(std::pmr::memory_resource* res,
                                                const chunks_vector_t& left_chunks,
                                                const chunks_vector_t& right_chunks,
                                                chunks_vector_t& out_chunks) {
        // Output column types are the validator-stamped union schema (see set_output_schema):
        // the reconciliation — including the PostgreSQL NULL-literal rule — happened
        // data-INDEPENDENTLY at validation, so the result type never depends on which rows
        // the tables happen to hold. A union node produces rows, so validate_schema always
        // stamps it and create_plan_union always forwards the stamp; an empty schema here
        // means the plan reached execution unvalidated, which is a hard error rather than
        // something to reconstruct from whichever side happens to have chunks.
        if (output_schema_.empty()) {
            return core::error_t{core::error_code_t::physical_plan_error,
                                 std::pmr::string{"operator_union: no plan-resolved output schema", res}};
        }
        // Re-homed onto the emitting resource: output_schema_ belongs to the operator, the
        // chunks belong to `res` (the left input's resource), and every record the chunks
        // are built from has to be allocated where the chunks are.
        const vector::schema_t schema = vector::clone_schema(res, output_schema_);

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
                    out_chunks.emplace_back(copy_rows(chunk, all_rows, schema, res));
                }
            };
            copy_all(left_chunks);
            copy_all(right_chunks);
            return core::error_t::no_error();
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
                        if (rows_equal(chunk, row, *ref.chunk, ref.row)) {
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
                out_chunks.emplace_back(copy_rows(chunk, selected, schema, res));
            }
        };

        for (const auto& chunk : left_chunks) {
            process(chunk);
        }
        for (const auto& chunk : right_chunks) {
            process(chunk);
        }
        return core::error_t::no_error();
    }

    core::error_t operator_union_t::push(pipeline::context_t*, vector::data_chunk_t&&, chunks_vector_t&) {
        // Both inputs are materialized by separate sub-plans before this operator runs
        // (traverse_plan_ splits the union's left and right children). The streaming
        // pump's left batches are therefore a redundant view of left_->output(), which
        // finalize() reads directly — so push() folds nothing and emits nothing.
        return core::error_t::no_error();
    }

    core::error_t operator_union_t::finalize(pipeline::context_t*, chunks_vector_t& out) {
        // Emit the union of the two MATERIALIZED sides (the emit_union_ core).
        if (!left_ || !left_->output()) {
            return core::error_t::no_error();
        }
        auto* res = left_->output()->resource();
        const auto& left_chunks = left_->output()->chunks();
        chunks_vector_t empty_right(res);
        const chunks_vector_t& right_chunks = (right_ && right_->output()) ? right_->output()->chunks() : empty_right;
        return emit_union_(res, left_chunks, right_chunks, out);
    }

} // namespace components::operators
