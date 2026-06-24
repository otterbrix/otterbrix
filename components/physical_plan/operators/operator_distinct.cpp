#include "operator_distinct.hpp"

#include <sstream>
#include <unordered_set>

namespace components::operators {

    operator_distinct_t::operator_distinct_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::match) {}

    namespace {
        // Build a comparison-stable identity key for a single row across all columns.
        std::string row_key(const vector::data_chunk_t& chunk, size_t row) {
            std::ostringstream key;
            for (size_t column = 0; column < chunk.column_count(); column++) {
                auto val = chunk.data[column].value(row);
                if (val.is_null()) {
                    key << "\0NULL\0";
                } else {
                    key << static_cast<int>(val.type().type()) << ":";
                    // Use comparison-based identity — serialize through the value's type
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

    void operator_distinct_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (!left_ || !left_->output()) {
            return;
        }
        auto* resource = left_->output()->resource();
        const auto& in_chunks = left_->output()->chunks();
        std::pmr::vector<types::complex_logical_type> types{resource};
        if (!in_chunks.empty()) {
            types = in_chunks.front().types();
        }

        // Dedup is global across every input chunk: the seen-set persists for the whole
        // pass. Each input chunk is already ≤ DEFAULT_VECTOR_CAPACITY, so the surviving
        // rows of one input chunk form one output chunk of the same bound.
        chunks_vector_t out_chunks(resource);
        out_chunks.reserve(in_chunks.size());
        std::unordered_set<std::string> seen;

        for (const auto& chunk : in_chunks) {
            if (chunk.size() == 0) {
                continue;
            }
            vector::data_chunk_t out_chunk(resource, types, chunk.size());
            size_t count = 0;
            for (size_t i = 0; i < chunk.size(); i++) {
                if (seen.insert(row_key(chunk, i)).second) {
                    for (size_t column = 0; column < chunk.column_count(); column++) {
                        out_chunk.set_value(column, count, chunk.data[column].value(i));
                    }
                    ++count;
                }
            }
            out_chunk.set_cardinality(count);
            if (count > 0) {
                out_chunks.emplace_back(std::move(out_chunk));
            }
        }

        if (out_chunks.empty()) {
            output_ = operators::make_operator_data(resource, types, 0);
        } else {
            output_ = operators::make_operator_data(resource, std::move(out_chunks));
        }
    }

} // namespace components::operators
