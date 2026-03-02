#include "base_statistics.hpp"

#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/vector/vector.hpp>
#include <string_view>

namespace components::table {

    namespace {

        template<typename T>
        void update_numeric_stats(base_statistics_t& stats,
                                  std::pmr::memory_resource* resource,
                                  const vector::vector_t& vec,
                                  uint64_t count) {
            auto data = vec.data<T>();
            const auto& validity = vec.validity();
            bool found_valid = false;
            T local_min{};
            T local_max{};
            uint64_t null_count = 0;

            if (vec.get_vector_type() == vector::vector_type::CONSTANT) {
                // CONSTANT vector: data[0] is the single value for all rows
                if (!validity.row_is_valid(0)) {
                    null_count = count;
                } else {
                    T val = data[0];
                    local_min = val;
                    local_max = val;
                    found_valid = true;
                }
            } else {
                for (uint64_t i = 0; i < count; i++) {
                    if (!validity.row_is_valid(i)) {
                        null_count++;
                        continue;
                    }
                    T val = data[i];
                    if (!found_valid) {
                        local_min = val;
                        local_max = val;
                        found_valid = true;
                    } else {
                        if (val < local_min)
                            local_min = val;
                        if (val > local_max)
                            local_max = val;
                    }
                }
            }

            stats.set_null_count(stats.null_count() + null_count);
            if (found_valid) {
                types::logical_value_t batch_min(resource, local_min);
                types::logical_value_t batch_max(resource, local_max);
                if (!stats.has_stats()) {
                    stats.set_min(std::move(batch_min));
                    stats.set_max(std::move(batch_max));
                } else {
                    if (batch_min < stats.min_value()) {
                        stats.set_min(std::move(batch_min));
                    }
                    if (batch_max > stats.max_value()) {
                        stats.set_max(std::move(batch_max));
                    }
                }
            }
        }

        void update_string_stats(base_statistics_t& stats,
                                 std::pmr::memory_resource* resource,
                                 const vector::vector_t& vec,
                                 uint64_t count) {
            auto data = vec.data<std::string_view>();
            const auto& validity = vec.validity();
            bool found_valid = false;
            std::string local_min;
            std::string local_max;
            uint64_t null_count = 0;

            if (vec.get_vector_type() == vector::vector_type::CONSTANT) {
                // CONSTANT vector: data[0] is the single value for all rows
                if (!validity.row_is_valid(0)) {
                    null_count = count;
                } else {
                    std::string val(data[0]);
                    local_min = val;
                    local_max = val;
                    found_valid = true;
                }
            } else {
                for (uint64_t i = 0; i < count; i++) {
                    if (!validity.row_is_valid(i)) {
                        null_count++;
                        continue;
                    }
                    std::string val(data[i]);
                    if (!found_valid) {
                        local_min = val;
                        local_max = val;
                        found_valid = true;
                    } else {
                        if (val < local_min)
                            local_min = val;
                        if (val > local_max)
                            local_max = val;
                    }
                }
            }

            stats.set_null_count(stats.null_count() + null_count);
            if (found_valid) {
                types::logical_value_t batch_min(resource, local_min);
                types::logical_value_t batch_max(resource, local_max);
                if (!stats.has_stats()) {
                    stats.set_min(std::move(batch_min));
                    stats.set_max(std::move(batch_max));
                } else {
                    if (batch_min < stats.min_value()) {
                        stats.set_min(std::move(batch_min));
                    }
                    if (batch_max > stats.max_value()) {
                        stats.set_max(std::move(batch_max));
                    }
                }
            }
        }

        void serialize_logical_value(const types::logical_value_t& val, storage::metadata_writer_t& writer) {
            auto type = val.type().type();
            switch (type) {
                case types::logical_type::BOOLEAN:
                    writer.write<uint8_t>(val.value<bool>() ? 1 : 0);
                    break;
                case types::logical_type::TINYINT:
                    writer.write<int8_t>(val.value<int8_t>());
                    break;
                case types::logical_type::SMALLINT:
                    writer.write<int16_t>(val.value<int16_t>());
                    break;
                case types::logical_type::INTEGER:
                    writer.write<int32_t>(val.value<int32_t>());
                    break;
                case types::logical_type::BIGINT:
                    writer.write<int64_t>(val.value<int64_t>());
                    break;
                case types::logical_type::UTINYINT:
                    writer.write<uint8_t>(val.value<uint8_t>());
                    break;
                case types::logical_type::USMALLINT:
                    writer.write<uint16_t>(val.value<uint16_t>());
                    break;
                case types::logical_type::UINTEGER:
                    writer.write<uint32_t>(val.value<uint32_t>());
                    break;
                case types::logical_type::UBIGINT:
                    writer.write<uint64_t>(val.value<uint64_t>());
                    break;
                case types::logical_type::FLOAT:
                    writer.write<float>(val.value<float>());
                    break;
                case types::logical_type::DOUBLE:
                    writer.write<double>(val.value<double>());
                    break;
                case types::logical_type::STRING_LITERAL:
                    writer.write_string(std::string(val.value<std::string_view>()));
                    break;
                default:
                    break;
            }
        }

        types::logical_value_t deserialize_logical_value(types::logical_type type,
                                                         std::pmr::memory_resource* resource,
                                                         storage::metadata_reader_t& reader) {
            switch (type) {
                case types::logical_type::BOOLEAN:
                    return types::logical_value_t(resource, reader.read<uint8_t>() != 0);
                case types::logical_type::TINYINT:
                    return types::logical_value_t(resource, reader.read<int8_t>());
                case types::logical_type::SMALLINT:
                    return types::logical_value_t(resource, reader.read<int16_t>());
                case types::logical_type::INTEGER:
                    return types::logical_value_t(resource, reader.read<int32_t>());
                case types::logical_type::BIGINT:
                    return types::logical_value_t(resource, reader.read<int64_t>());
                case types::logical_type::UTINYINT:
                    return types::logical_value_t(resource, reader.read<uint8_t>());
                case types::logical_type::USMALLINT:
                    return types::logical_value_t(resource, reader.read<uint16_t>());
                case types::logical_type::UINTEGER:
                    return types::logical_value_t(resource, reader.read<uint32_t>());
                case types::logical_type::UBIGINT:
                    return types::logical_value_t(resource, reader.read<uint64_t>());
                case types::logical_type::FLOAT:
                    return types::logical_value_t(resource, reader.read<float>());
                case types::logical_type::DOUBLE:
                    return types::logical_value_t(resource, reader.read<double>());
                case types::logical_type::STRING_LITERAL: {
                    auto str = reader.read_string();
                    return types::logical_value_t(resource, str);
                }
                default:
                    return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
            }
        }

        bool type_supports_minmax(types::logical_type type) {
            switch (type) {
                case types::logical_type::BOOLEAN:
                case types::logical_type::TINYINT:
                case types::logical_type::SMALLINT:
                case types::logical_type::INTEGER:
                case types::logical_type::BIGINT:
                case types::logical_type::UTINYINT:
                case types::logical_type::USMALLINT:
                case types::logical_type::UINTEGER:
                case types::logical_type::UBIGINT:
                case types::logical_type::FLOAT:
                case types::logical_type::DOUBLE:
                case types::logical_type::STRING_LITERAL:
                    return true;
                default:
                    return false;
            }
        }

    } // anonymous namespace

    base_statistics_t::base_statistics_t(std::pmr::memory_resource* resource)
        : resource_(resource)
        , type_(types::logical_type::NA)
        , min_(resource, types::complex_logical_type{types::logical_type::NA})
        , max_(resource, types::complex_logical_type{types::logical_type::NA}) {}

    base_statistics_t::base_statistics_t(std::pmr::memory_resource* resource, types::logical_type type)
        : resource_(resource)
        , type_(type)
        , min_(resource, types::complex_logical_type{types::logical_type::NA})
        , max_(resource, types::complex_logical_type{types::logical_type::NA}) {}

    base_statistics_t::base_statistics_t(std::pmr::memory_resource* resource,
                                         types::logical_type type,
                                         types::logical_value_t min_val,
                                         types::logical_value_t max_val,
                                         uint64_t null_count)
        : resource_(resource)
        , type_(type)
        , min_(std::move(min_val))
        , max_(std::move(max_val))
        , null_count_(null_count)
        , has_stats_(true) {}

    base_statistics_t::base_statistics_t(const base_statistics_t& other)
        : resource_(other.resource_)
        , type_(other.type_)
        , min_(other.min_)
        , max_(other.max_)
        , null_count_(other.null_count_)
        , has_stats_(other.has_stats_) {}

    base_statistics_t& base_statistics_t::operator=(const base_statistics_t& other) {
        if (this != &other) {
            resource_ = other.resource_;
            type_ = other.type_;
            min_ = other.min_;
            max_ = other.max_;
            null_count_ = other.null_count_;
            has_stats_ = other.has_stats_;
        }
        return *this;
    }

    void base_statistics_t::set_min(types::logical_value_t val) {
        min_ = std::move(val);
        has_stats_ = true;
    }

    void base_statistics_t::set_max(types::logical_value_t val) {
        max_ = std::move(val);
        has_stats_ = true;
    }

    void base_statistics_t::update(const vector::vector_t& vec, uint64_t count) {
        if (count == 0) {
            return;
        }
        switch (type_) {
            case types::logical_type::BOOLEAN:
                update_numeric_stats<bool>(*this, resource_, vec, count);
                break;
            case types::logical_type::TINYINT:
                update_numeric_stats<int8_t>(*this, resource_, vec, count);
                break;
            case types::logical_type::SMALLINT:
                update_numeric_stats<int16_t>(*this, resource_, vec, count);
                break;
            case types::logical_type::INTEGER:
                update_numeric_stats<int32_t>(*this, resource_, vec, count);
                break;
            case types::logical_type::BIGINT:
                update_numeric_stats<int64_t>(*this, resource_, vec, count);
                break;
            case types::logical_type::UTINYINT:
                update_numeric_stats<uint8_t>(*this, resource_, vec, count);
                break;
            case types::logical_type::USMALLINT:
                update_numeric_stats<uint16_t>(*this, resource_, vec, count);
                break;
            case types::logical_type::UINTEGER:
                update_numeric_stats<uint32_t>(*this, resource_, vec, count);
                break;
            case types::logical_type::UBIGINT:
                update_numeric_stats<uint64_t>(*this, resource_, vec, count);
                break;
            case types::logical_type::FLOAT:
                update_numeric_stats<float>(*this, resource_, vec, count);
                break;
            case types::logical_type::DOUBLE:
                update_numeric_stats<double>(*this, resource_, vec, count);
                break;
            case types::logical_type::STRING_LITERAL:
                update_string_stats(*this, resource_, vec, count);
                break;
            default:
                // unsupported types: just count nulls
                {
                    const auto& validity = vec.validity();
                    uint64_t null_count = 0;
                    if (vec.get_vector_type() == vector::vector_type::CONSTANT) {
                        null_count = validity.row_is_valid(0) ? 0 : count;
                    } else {
                        for (uint64_t i = 0; i < count; i++) {
                            if (!validity.row_is_valid(i)) {
                                null_count++;
                            }
                        }
                    }
                    null_count_ += null_count;
                }
                break;
        }
    }

    void base_statistics_t::merge(const base_statistics_t& other) {
        null_count_ += other.null_count_;
        if (!other.has_stats_) {
            return;
        }
        if (!has_stats_) {
            min_ = other.min_;
            max_ = other.max_;
            has_stats_ = true;
            return;
        }
        if (!other.min_.is_null() && (min_.is_null() || other.min_ < min_)) {
            min_ = other.min_;
        }
        if (!other.max_.is_null() && (max_.is_null() || other.max_ > max_)) {
            max_ = other.max_;
        }
    }

    void base_statistics_t::serialize(storage::metadata_writer_t& writer) const {
        writer.write<uint8_t>(static_cast<uint8_t>(type_));
        writer.write<uint8_t>(has_stats_ ? 1 : 0);
        writer.write<uint64_t>(null_count_);
        bool has_minmax = has_stats_ && type_supports_minmax(type_);
        writer.write<uint8_t>(has_minmax ? 1 : 0);
        if (has_minmax) {
            serialize_logical_value(min_, writer);
            serialize_logical_value(max_, writer);
        }
    }

    base_statistics_t base_statistics_t::deserialize(std::pmr::memory_resource* resource,
                                                     storage::metadata_reader_t& reader) {
        auto type = static_cast<types::logical_type>(reader.read<uint8_t>());
        auto has_stats = reader.read<uint8_t>() != 0;
        auto null_count = reader.read<uint64_t>();

        base_statistics_t result(resource, type);
        result.null_count_ = null_count;
        result.has_stats_ = has_stats;

        auto has_minmax = reader.read<uint8_t>() != 0;
        if (has_minmax) {
            result.min_ = deserialize_logical_value(type, resource, reader);
            result.max_ = deserialize_logical_value(type, resource, reader);
        }

        return result;
    }

} // namespace components::table
