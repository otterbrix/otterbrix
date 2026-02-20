#include "operator_count.hpp"

#include <sstream>
#include <unordered_set>

namespace components::operators::aggregate {

    constexpr auto key_result_ = "count";

    operator_count_t::operator_count_t(std::pmr::memory_resource* resource, log_t log)
        : operator_aggregate_t(resource, log)
        , distinct_(false)
        , field_(resource) {}

    operator_count_t::operator_count_t(std::pmr::memory_resource* resource, log_t log, bool distinct,
                                       expressions::key_t field)
        : operator_aggregate_t(resource, log)
        , distinct_(distinct)
        , field_(std::move(field)) {}

    types::logical_value_t operator_count_t::aggregate_impl() {
        if (left_ && left_->output()) {
            if (distinct_ && !field_.is_null()) {
                // COUNT(DISTINCT col) — count unique non-null values
                const auto& chunk = left_->output()->data_chunk();
                // Find column index for field_
                size_t col_idx = 0;
                auto types = chunk.types();
                for (size_t i = 0; i < types.size(); i++) {
                    if (core::pmr::operator==(types[i].alias(), field_.storage().front())) {
                        col_idx = i;
                        break;
                    }
                }

                std::unordered_set<std::string> seen;
                for (size_t i = 0; i < chunk.size(); i++) {
                    auto val = chunk.data[col_idx].value(i);
                    if (!val.is_null()) {
                        std::ostringstream key;
                        switch (val.type().to_physical_type()) {
                            case types::physical_type::INT64:
                                key << val.value<int64_t>();
                                break;
                            case types::physical_type::INT32:
                                key << val.value<int32_t>();
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
                            case types::physical_type::STRING:
                                key << val.value<std::string_view>();
                                break;
                            case types::physical_type::BOOL:
                                key << val.value<bool>();
                                break;
                            default:
                                key << i;
                                break;
                        }
                        seen.insert(key.str());
                    }
                }

                auto result = types::logical_value_t(left_->output()->resource(), uint64_t(seen.size()));
                result.set_alias(key_result_);
                return result;
            }
            auto result = types::logical_value_t(left_->output()->resource(), uint64_t(left_->output()->size()));
            result.set_alias(key_result_);
            return result;
        }
        auto result = types::logical_value_t(std::pmr::null_memory_resource(), uint64_t(0));
        result.set_alias(key_result_);
        return result;
    }

    std::string operator_count_t::key_impl() const { return key_result_; }

} // namespace components::operators::aggregate
