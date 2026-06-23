#include "operator_distinct.hpp"

#include <components/vector/data_chunk.hpp>

#include <sstream>

namespace components::operators {

    namespace {

        // The all-column identity key for one row — IDENTICAL to the legacy distinct
        // key so dedup semantics are unchanged: per-column "<type>:<value>" segments,
        // NULLs as a dedicated marker, '|' separated. Comparison-based identity.
        std::string row_key(const vector::data_chunk_t& chunk, size_t row) {
            std::ostringstream key;
            for (size_t j = 0; j < chunk.column_count(); j++) {
                auto val = chunk.data[j].value(row);
                if (val.is_null()) {
                    key << "\0NULL\0";
                } else {
                    key << static_cast<int>(val.type().type()) << ":";
                    switch (val.type().to_physical_type()) {
                        case types::physical_type::INT8:
                            key << val.value<int8_t>();
                            break;
                        case types::physical_type::INT16:
                            key << val.value<int16_t>();
                            break;
                        case types::physical_type::INT32:
                            key << val.value<int32_t>();
                            break;
                        case types::physical_type::INT64:
                            key << val.value<int64_t>();
                            break;
                        case types::physical_type::UINT8:
                            key << val.value<uint8_t>();
                            break;
                        case types::physical_type::UINT16:
                            key << val.value<uint16_t>();
                            break;
                        case types::physical_type::UINT32:
                            key << val.value<uint32_t>();
                            break;
                        case types::physical_type::UINT64:
                            key << val.value<uint64_t>();
                            break;
                        case types::physical_type::FLOAT:
                            key << val.value<float>();
                            break;
                        case types::physical_type::DOUBLE:
                            key << val.value<double>();
                            break;
                        case types::physical_type::BOOL:
                            key << val.value<bool>();
                            break;
                        case types::physical_type::STRING:
                            key << val.value<std::string_view>();
                            break;
                        default:
                            key << "?";
                            break;
                    }
                }
                key << "|";
            }
            return key.str();
        }

    } // namespace

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
                if (seen_.insert(row_key(chunk, i)).second) {
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

    void operator_distinct_t::on_execute_impl(pipeline::context_t*) {
        // Materialized entry (sourceless sub-plans). Shares emit_distinct_() with the
        // streaming push() path so the result is identical.
        if (!left_ || !left_->output()) {
            return;
        }
        seen_.clear();
        auto* res = left_->output()->resource();
        const auto& chunks = left_->output()->chunks();
        chunks_vector_t out_chunks(res);
        emit_distinct_(res, chunks, out_chunks);

        if (out_chunks.empty()) {
            const auto& types = left_->output()->data_chunk().types();
            out_chunks.emplace_back(res, types, 0);
        }
        output_ = operators::make_operator_data(res, std::move(out_chunks));
    }

} // namespace components::operators
