#include "operator_distinct.hpp"

#include <components/vector/cell_equal.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>

namespace components::operators {

    operator_distinct_t::operator_distinct_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::distinct)
        , seen_(resource)
        , retained_(resource)
        , on_keys_(resource) {}

    bool operator_distinct_t::is_duplicate_(const vector::data_chunk_t& chunk, uint64_t row, uint64_t hash) const {
        auto it = seen_.find(hash);
        if (it == seen_.end()) {
            return false;
        }
        for (const auto& ref : it->second) {
            const auto& kept = retained_[ref.chunk_idx];
            bool equal = true;
            // DISTINCT ON compares only the ON-key columns; plain DISTINCT compares the whole row.
            // retained_ holds the full row in both cases, so on_keys_ indices address it directly.
            if (on_keys_.empty()) {
                for (size_t c = 0; c < chunk.column_count(); ++c) {
                    if (!vector::cells_equal(kept.data[c], ref.row, chunk.data[c], row)) {
                        equal = false;
                        break;
                    }
                }
            } else {
                for (size_t c : on_keys_) {
                    if (!vector::cells_equal(kept.data[c], ref.row, chunk.data[c], row)) {
                        equal = false;
                        break;
                    }
                }
            }
            if (equal) {
                return true;
            }
        }
        return false;
    }

    void operator_distinct_t::retain_(const vector::data_chunk_t& chunk,
                                      uint64_t row,
                                      uint64_t hash,
                                      const std::pmr::vector<types::complex_logical_type>& types,
                                      std::pmr::memory_resource* res) {
        if (retained_.empty() || retained_fill_ == vector::DEFAULT_VECTOR_CAPACITY) {
            retained_.emplace_back(res, types, vector::DEFAULT_VECTOR_CAPACITY);
            retained_fill_ = 0;
        }
        auto& dst = retained_.back();
        for (size_t j = 0; j < chunk.column_count(); ++j) {
            dst.set_value(j, retained_fill_, chunk.value(j, row));
        }
        dst.set_cardinality(retained_fill_ + 1);
        seen_[hash].push_back(retained_row_ref_t{retained_.size() - 1, retained_fill_});
        ++retained_fill_;
    }

    void operator_distinct_t::emit_distinct_(std::pmr::memory_resource* res,
                                             const chunks_vector_t& chunks,
                                             chunks_vector_t& out) {
        // Find a schema-carrying chunk for the output column types.
        const vector::data_chunk_t* schema = nullptr;
        for (const auto& c : chunks) {
            if (c.column_count() > 0) {
                schema = &c;
                break;
            }
        }
        if (schema == nullptr) {
            return;
        }
        auto types = schema->types();
        if (shape_.empty()) {
            shape_.assign(types.begin(), types.end());
        }

        vector::data_chunk_t cur(res, types, vector::DEFAULT_VECTOR_CAPACITY);
        uint64_t filled = 0;
        auto flush = [&]() {
            if (filled == 0) {
                return;
            }
            cur.set_cardinality(filled);
            out.emplace_back(std::move(cur));
            note_emitted();
            cur = vector::data_chunk_t(res, types, vector::DEFAULT_VECTOR_CAPACITY);
            filled = 0;
        };

        for (const auto& chunk : chunks) {
            if (chunk.size() == 0) {
                continue;
            }
            // Typed hash of the DISTINCT key. Plain DISTINCT hashes the whole row; DISTINCT ON
            // hashes only the ON-key subset. hash() takes a non-const chunk; it only reads columns.
            vector::vector_t hash_vec(res, types::logical_type::UBIGINT, chunk.size());
            if (on_keys_.empty()) {
                const_cast<vector::data_chunk_t&>(chunk).hash(hash_vec);
            } else {
                std::vector<uint64_t> col_ids(on_keys_.begin(), on_keys_.end());
                const_cast<vector::data_chunk_t&>(chunk).hash(col_ids, hash_vec);
                // An all-CONSTANT ON-column hash can come back non-FLAT; flatten so data<uint64_t>()
                // reads one hash per row (mirrors operator_unique_constraint).
                hash_vec.flatten(chunk.size());
            }
            const auto* hashes = hash_vec.data<uint64_t>();

            for (size_t i = 0; i < chunk.size(); i++) {
                const uint64_t h = hashes[i];
                if (is_duplicate_(chunk, i, h)) {
                    continue;
                }
                // First occurrence: retain it (so later batches dedup against it) and
                // emit it. retain_ registers the row in seen_ BEFORE the next row is
                // examined, so an intra-batch duplicate is caught too.
                retain_(chunk, i, h, types, res);
                if (filled == vector::DEFAULT_VECTOR_CAPACITY) {
                    flush();
                }
                for (size_t j = 0; j < chunk.column_count(); j++) {
                    cur.set_value(j, filled, chunk.value(j, i));
                }
                ++filled;
            }
        }
        flush();
    }

    core::error_t operator_distinct_t::push(pipeline::context_t*, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // Dedup this batch against everything seen so far (seen_/retained_ accumulate),
        // emitting the freshly-unique rows downstream immediately.
        chunks_vector_t one(resource_);
        one.emplace_back(std::move(input));
        emit_distinct_(resource_, one, out);
        return core::error_t::no_error();
    }

    core::error_t operator_distinct_t::finalize(pipeline::context_t*, chunks_vector_t& out) {
        // push() already emitted every distinct row as it arrived; nothing buffered. What may be
        // outstanding is the columns themselves, when no row survived (or none arrived).
        if (emitted() || shape_.empty()) {
            return core::error_t::no_error();
        }
        vector::data_chunk_t empty(resource_, shape_, 0);
        empty.set_cardinality(0);
        out.emplace_back(std::move(empty));
        return core::error_t::no_error();
    }

} // namespace components::operators
