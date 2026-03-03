#include "operator_distinct.hpp"

#include <sstream>
#include <unordered_set>

namespace components::operators {

    operator_distinct_t::operator_distinct_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::match) {}

    void operator_distinct_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (!left_ || !left_->output()) {
            return;
        }
        const auto& chunk = left_->output()->data_chunk();
        auto types = chunk.types();
        output_ = operators::make_operator_data(left_->output()->resource(), types, chunk.size());
        auto& out_chunk = output_->data_chunk();

        std::unordered_set<std::string> seen;
        size_t count = 0;

        for (size_t i = 0; i < chunk.size(); i++) {
            // Build a key from all column values
            std::ostringstream key;
            for (size_t j = 0; j < chunk.column_count(); j++) {
                auto val = chunk.data[j].value(i);
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

            if (seen.insert(key.str()).second) {
                for (size_t j = 0; j < chunk.column_count(); j++) {
                    out_chunk.set_value(j, count, chunk.data[j].value(i));
                }
                ++count;
            }
        }
        out_chunk.set_cardinality(count);
    }

} // namespace components::operators
