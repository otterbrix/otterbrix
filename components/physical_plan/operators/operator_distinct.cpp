#include "operator_distinct.hpp"

#include <components/vector/data_chunk.hpp>

#include <sstream>

namespace components::operators {

    operator_distinct_t::operator_distinct_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::match) {}

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

        vector::data_chunk_t cur(res, types, vector::DEFAULT_VECTOR_CAPACITY);
        uint64_t filled = 0;
        auto flush = [&]() {
            if (filled == 0) {
                return;
            }
            cur.set_cardinality(filled);
            out.emplace_back(std::move(cur));
            cur = vector::data_chunk_t(res, types, vector::DEFAULT_VECTOR_CAPACITY);
            filled = 0;
        };

        for (const auto& chunk : chunks) {
            for (size_t i = 0; i < chunk.size(); i++) {
                if (seen_.insert(vector::row_identity_key(chunk, i)).second) {
                    if (filled == vector::DEFAULT_VECTOR_CAPACITY) {
                        flush();
                    }
                    for (size_t j = 0; j < chunk.column_count(); j++) {
                        cur.set_value(j, filled, chunk.data[j].value(i));
                    }
                    ++filled;
                }
            }
        }
        flush();
    }

    core::error_t operator_distinct_t::push(pipeline::context_t*, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // Dedup this batch against everything seen so far (push accumulates into
        // seen_), emitting the freshly-unique rows downstream immediately. A single
        // batch fits in one output chunk (≤ DEFAULT_VECTOR_CAPACITY rows), so no
        // chunk retention is needed across pushes.
        chunks_vector_t one(resource_);
        one.emplace_back(std::move(input));
        emit_distinct_(resource_, one, out);
        return core::error_t::no_error();
    }

    core::error_t operator_distinct_t::finalize(pipeline::context_t*, chunks_vector_t&) {
        // push() already emitted every distinct row as it arrived; nothing buffered.
        return core::error_t::no_error();
    }

} // namespace components::operators
