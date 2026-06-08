#include "operator_union.hpp"
#include "join_utils.hpp"
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

        // Deep-copy selected rows from src into a new chunk.
        // Uses data_chunk_t::copy which correctly handles STRUCT/ARRAY/LIST columns.
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
            src.copy(out, idx, n, 0);
            return out;
        }

    } // namespace

    operator_union_t::operator_union_t(std::pmr::memory_resource* resource, log_t log, bool all)
        : read_only_operator_t(resource, log, operator_type::union_op)
        , all_(all) {}

    void operator_union_t::on_execute_impl(pipeline::context_t*) {
        if (!left_ || !right_ || !left_->output() || !right_->output()) {
            return;
        }

        auto* res = left_->output()->resource();
        const auto& left_chunks = left_->output()->chunks();
        const auto& right_chunks = right_->output()->chunks();
        const auto& types = left_chunks.empty() ? right_chunks.front().types() : left_chunks.front().types();
        chunks_vector_t out_chunks(res);

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
        } else {
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
                    out_chunks.emplace_back(copy_rows(chunk, selected, types, res));
                }
            };

            for (const auto& chunk : left_chunks) {
                process(chunk);
            }
            for (const auto& chunk : right_chunks) {
                process(chunk);
            }
        }

        if (out_chunks.empty()) {
            out_chunks.emplace_back(res, types, 0);
        }
        output_ = make_operator_data(res, std::move(out_chunks));
    }

} // namespace components::operators