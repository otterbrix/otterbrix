#include "base_statistics.hpp"

#include <cassert>
#include <components/table/storage/metadata_reader.hpp>
#include <components/table/storage/metadata_writer.hpp>
#include <components/vector/vector.hpp>
#include <core/operations_helper.hpp>
#include <string_view>
#include <type_traits>

namespace components::table {

    namespace {

        template<typename T>
        bool less_than(const T& lhs, const T& rhs) {
            if constexpr (std::is_floating_point_v<T>) {
                return core::float_order(lhs, rhs) < 0;
            } else {
                return lhs < rhs;
            }
        }

        template<typename T>
        void update_numeric_stats(base_statistics_t& stats, vector::vector_t& vec, uint64_t offset, uint64_t count) {
            vector::unified_vector_format uvf(vec.resource(), offset + count);
            vec.to_unified_format(offset + count, uvf);
            const auto* data = uvf.get_data<T>();
            const auto& validity = uvf.validity;
            const auto* indexing = uvf.referenced_indexing;
            bool found_valid = false;
            T local_min{};
            T local_max{};
            uint64_t null_count = 0;

            for (uint64_t i = offset; i < offset + count; i++) {
                uint64_t idx = indexing->get_index(i);
                if (!validity.row_is_valid(idx)) {
                    null_count++;
                    continue;
                }
                T val = data[idx];
                if (!found_valid) {
                    local_min = val;
                    local_max = val;
                    found_valid = true;
                } else {
                    if (less_than(val, local_min))
                        local_min = val;
                    if (less_than(local_max, val))
                        local_max = val;
                }
            }

            stats.set_null_count(stats.null_count() + null_count);
            if (found_valid) {
                if (stats.has_bounds()) {
                    if (less_than(stats.min_as<T>(), local_min)) {
                        local_min = stats.min_as<T>();
                    }
                    if (less_than(local_max, stats.max_as<T>())) {
                        local_max = stats.max_as<T>();
                    }
                }
                stats.set_bounds<T>(local_min, local_max);
            }
        }

        void update_string_stats(base_statistics_t& stats, vector::vector_t& vec, uint64_t offset, uint64_t count) {
            vector::unified_vector_format uvf(vec.resource(), offset + count);
            vec.to_unified_format(offset + count, uvf);
            const auto* data = uvf.get_data<std::string_view>();
            const auto& validity = uvf.validity;
            const auto* indexing = uvf.referenced_indexing;
            bool found_valid = false;
            std::string local_min;
            std::string local_max;
            uint64_t null_count = 0;

            for (uint64_t i = offset; i < offset + count; i++) {
                uint64_t idx = indexing->get_index(i);
                if (!validity.row_is_valid(idx)) {
                    null_count++;
                    continue;
                }
                std::string val(data[idx]);
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

            stats.set_null_count(stats.null_count() + null_count);
            if (found_valid) {
                if (stats.has_bounds()) {
                    if (stats.min_string() < local_min) {
                        local_min = std::string(stats.min_string());
                    }
                    if (stats.max_string() > local_max) {
                        local_max = std::string(stats.max_string());
                    }
                }
                stats.set_bounds<std::string_view>(local_min, local_max);
            }
        }

        // DECIMAL zone-map leg: values are SCALED integers (width follows
        // decimal_storage_for_width); min/max are collected in that raw space and carried with the
        // column's own (width, scale).
        template<typename T>
        void
        update_decimal_stats_typed(base_statistics_t& stats, vector::vector_t& vec, uint64_t offset, uint64_t count) {
            vector::unified_vector_format uvf(vec.resource(), offset + count);
            vec.to_unified_format(offset + count, uvf);
            const auto* data = uvf.get_data<T>();
            const auto& validity = uvf.validity;
            const auto* indexing = uvf.referenced_indexing;
            bool found_valid = false;
            T local_min{};
            T local_max{};
            uint64_t null_count = 0;

            for (uint64_t i = offset; i < offset + count; i++) {
                uint64_t idx = indexing->get_index(i);
                if (!validity.row_is_valid(idx)) {
                    null_count++;
                    continue;
                }
                T val = data[idx];
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

            stats.set_null_count(stats.null_count() + null_count);
            if (found_valid) {
                auto batch_min = types::int128_t(local_min);
                auto batch_max = types::int128_t(local_max);
                if (stats.has_bounds()) {
                    if (stats.min_as<types::int128_t>() < batch_min) {
                        batch_min = stats.min_as<types::int128_t>();
                    }
                    if (stats.max_as<types::int128_t>() > batch_max) {
                        batch_max = stats.max_as<types::int128_t>();
                    }
                }
                const auto* ext =
                    reinterpret_cast<const types::decimal_logical_type_extension*>(vec.type().extension());
                stats.set_decimal_bounds(ext->width(), ext->scale(), batch_min, batch_max);
            }
        }

        void update_decimal_stats(base_statistics_t& stats, vector::vector_t& vec, uint64_t offset, uint64_t count) {
            switch (vec.type().to_physical_type()) {
                case types::physical_type::INT16:
                    update_decimal_stats_typed<int16_t>(stats, vec, offset, count);
                    break;
                case types::physical_type::INT32:
                    update_decimal_stats_typed<int32_t>(stats, vec, offset, count);
                    break;
                case types::physical_type::INT64:
                    update_decimal_stats_typed<int64_t>(stats, vec, offset, count);
                    break;
                case types::physical_type::INT128:
                    update_decimal_stats_typed<types::int128_t>(stats, vec, offset, count);
                    break;
                default: {
                    // Impossible storage width for a DECIMAL: count nulls only, no fabricated bounds.
                    const auto& validity = vec.validity();
                    uint64_t null_count = 0;
                    if (vec.get_vector_type() == vector::vector_type::CONSTANT) {
                        null_count = validity.row_is_valid(0) ? 0 : count;
                    } else {
                        for (uint64_t i = offset; i < offset + count; i++) {
                            if (!validity.row_is_valid(i)) {
                                null_count++;
                            }
                        }
                    }
                    stats.set_null_count(stats.null_count() + null_count);
                    break;
                }
            }
        }

        void serialize_bounds(const base_statistics_t& stats, storage::metadata_writer_t& writer) {
            switch (stats.type()) {
                case types::logical_type::BOOLEAN:
                    writer.write<uint8_t>(stats.min_as<bool>() ? 1 : 0);
                    writer.write<uint8_t>(stats.max_as<bool>() ? 1 : 0);
                    break;
                case types::logical_type::TINYINT:
                    writer.write<int8_t>(stats.min_as<int8_t>());
                    writer.write<int8_t>(stats.max_as<int8_t>());
                    break;
                case types::logical_type::SMALLINT:
                    writer.write<int16_t>(stats.min_as<int16_t>());
                    writer.write<int16_t>(stats.max_as<int16_t>());
                    break;
                case types::logical_type::INTEGER:
                    writer.write<int32_t>(stats.min_as<int32_t>());
                    writer.write<int32_t>(stats.max_as<int32_t>());
                    break;
                case types::logical_type::BIGINT:
                    writer.write<int64_t>(stats.min_as<int64_t>());
                    writer.write<int64_t>(stats.max_as<int64_t>());
                    break;
                case types::logical_type::UTINYINT:
                    writer.write<uint8_t>(stats.min_as<uint8_t>());
                    writer.write<uint8_t>(stats.max_as<uint8_t>());
                    break;
                case types::logical_type::USMALLINT:
                    writer.write<uint16_t>(stats.min_as<uint16_t>());
                    writer.write<uint16_t>(stats.max_as<uint16_t>());
                    break;
                case types::logical_type::UINTEGER:
                    writer.write<uint32_t>(stats.min_as<uint32_t>());
                    writer.write<uint32_t>(stats.max_as<uint32_t>());
                    break;
                case types::logical_type::UBIGINT:
                    writer.write<uint64_t>(stats.min_as<uint64_t>());
                    writer.write<uint64_t>(stats.max_as<uint64_t>());
                    break;
                case types::logical_type::FLOAT:
                    writer.write<float>(stats.min_as<float>());
                    writer.write<float>(stats.max_as<float>());
                    break;
                case types::logical_type::DOUBLE:
                    writer.write<double>(stats.min_as<double>());
                    writer.write<double>(stats.max_as<double>());
                    break;
                case types::logical_type::HUGEINT:
                    writer.write<types::int128_t>(stats.min_as<types::int128_t>());
                    writer.write<types::int128_t>(stats.max_as<types::int128_t>());
                    break;
                case types::logical_type::UHUGEINT:
                    writer.write<types::uint128_t>(stats.min_as<types::uint128_t>());
                    writer.write<types::uint128_t>(stats.max_as<types::uint128_t>());
                    break;
                case types::logical_type::DECIMAL:
                    // (width, scale) travel with each bound: the reader has only this stream to
                    // rebuild the column's decimal type from.
                    writer.write<uint8_t>(stats.decimal_width());
                    writer.write<uint8_t>(stats.decimal_scale());
                    writer.write<types::int128_t>(stats.min_as<types::int128_t>());
                    writer.write<uint8_t>(stats.decimal_width());
                    writer.write<uint8_t>(stats.decimal_scale());
                    writer.write<types::int128_t>(stats.max_as<types::int128_t>());
                    break;
                case types::logical_type::STRING_LITERAL:
                    writer.write_string(std::string(stats.min_string()));
                    writer.write_string(std::string(stats.max_string()));
                    break;
                default:
                    break;
            }
        }

        template<typename T>
        void deserialize_pair(base_statistics_t& stats, storage::metadata_reader_t& reader) {
            auto lo = reader.read<T>();
            auto hi = reader.read<T>();
            stats.set_bounds<T>(lo, hi);
        }

        void deserialize_bounds(base_statistics_t& stats,
                                std::pmr::memory_resource* resource,
                                storage::metadata_reader_t& reader) {
            switch (stats.type()) {
                case types::logical_type::BOOLEAN: {
                    auto lo = reader.read<uint8_t>() != 0;
                    auto hi = reader.read<uint8_t>() != 0;
                    stats.set_bounds<bool>(lo, hi);
                    break;
                }
                case types::logical_type::TINYINT:
                    deserialize_pair<int8_t>(stats, reader);
                    break;
                case types::logical_type::SMALLINT:
                    deserialize_pair<int16_t>(stats, reader);
                    break;
                case types::logical_type::INTEGER:
                    deserialize_pair<int32_t>(stats, reader);
                    break;
                case types::logical_type::BIGINT:
                    deserialize_pair<int64_t>(stats, reader);
                    break;
                case types::logical_type::UTINYINT:
                    deserialize_pair<uint8_t>(stats, reader);
                    break;
                case types::logical_type::USMALLINT:
                    deserialize_pair<uint16_t>(stats, reader);
                    break;
                case types::logical_type::UINTEGER:
                    deserialize_pair<uint32_t>(stats, reader);
                    break;
                case types::logical_type::UBIGINT:
                    deserialize_pair<uint64_t>(stats, reader);
                    break;
                case types::logical_type::FLOAT:
                    deserialize_pair<float>(stats, reader);
                    break;
                case types::logical_type::DOUBLE:
                    deserialize_pair<double>(stats, reader);
                    break;
                case types::logical_type::HUGEINT:
                    deserialize_pair<types::int128_t>(stats, reader);
                    break;
                case types::logical_type::UHUGEINT:
                    deserialize_pair<types::uint128_t>(stats, reader);
                    break;
                case types::logical_type::DECIMAL: {
                    auto min_width = reader.read<uint8_t>();
                    auto min_scale = reader.read<uint8_t>();
                    auto min_raw = reader.read<types::int128_t>();
                    auto max_width = reader.read<uint8_t>();
                    auto max_scale = reader.read<uint8_t>();
                    auto max_raw = reader.read<types::int128_t>();
                    // (width, scale) outside range = corrupt stream; no error channel here,
                    // so leave the bounds ABSENT rather than fabricate them.
                    if (types::complex_logical_type::create_decimal(resource, min_width, min_scale).has_error() ||
                        types::complex_logical_type::create_decimal(resource, max_width, max_scale).has_error()) {
                        break;
                    }
                    stats.set_decimal_bounds(min_width, min_scale, min_raw, max_raw);
                    break;
                }
                case types::logical_type::STRING_LITERAL: {
                    auto lo = reader.read_string();
                    auto hi = reader.read_string();
                    stats.set_bounds<std::string_view>(lo, hi);
                    break;
                }
                default:
                    break;
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
                case types::logical_type::HUGEINT:
                case types::logical_type::UHUGEINT:
                case types::logical_type::DECIMAL:
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
        , min_s_(resource)
        , max_s_(resource) {}

    base_statistics_t::base_statistics_t(std::pmr::memory_resource* resource, types::logical_type type)
        : resource_(resource)
        , type_(type)
        , min_s_(resource)
        , max_s_(resource) {}

    base_statistics_t::base_statistics_t(std::pmr::memory_resource* resource,
                                         types::logical_type type,
                                         uint64_t null_count)
        : resource_(resource)
        , type_(type)
        , min_s_(resource)
        , max_s_(resource)
        , null_count_(null_count)
        , has_stats_(true) {}

    base_statistics_t::base_statistics_t(const base_statistics_t& other)
        : resource_(other.resource_)
        , type_(other.type_)
        , min_i_(other.min_i_)
        , max_i_(other.max_i_)
        , min_u_(other.min_u_)
        , max_u_(other.max_u_)
        , min_d_(other.min_d_)
        , max_d_(other.max_d_)
        , min_s_(other.min_s_, other.resource_)
        , max_s_(other.max_s_, other.resource_)
        , decimal_width_(other.decimal_width_)
        , decimal_scale_(other.decimal_scale_)
        , null_count_(other.null_count_)
        , has_stats_(other.has_stats_)
        , has_bounds_(other.has_bounds_) {}

    base_statistics_t& base_statistics_t::operator=(const base_statistics_t& other) {
        if (this != &other) {
            type_ = other.type_;
            min_i_ = other.min_i_;
            max_i_ = other.max_i_;
            min_u_ = other.min_u_;
            max_u_ = other.max_u_;
            min_d_ = other.min_d_;
            max_d_ = other.max_d_;
            min_s_.assign(other.min_s_);
            max_s_.assign(other.max_s_);
            decimal_width_ = other.decimal_width_;
            decimal_scale_ = other.decimal_scale_;
            null_count_ = other.null_count_;
            has_stats_ = other.has_stats_;
            has_bounds_ = other.has_bounds_;
        }
        return *this;
    }

    // resource_ stays this object's own, like the allocator of min_s_/max_s_ under pmr assignment.
    base_statistics_t& base_statistics_t::operator=(base_statistics_t&& other) {
        if (this != &other) {
            type_ = other.type_;
            min_i_ = other.min_i_;
            max_i_ = other.max_i_;
            min_u_ = other.min_u_;
            max_u_ = other.max_u_;
            min_d_ = other.min_d_;
            max_d_ = other.max_d_;
            min_s_ = std::move(other.min_s_);
            max_s_ = std::move(other.max_s_);
            decimal_width_ = other.decimal_width_;
            decimal_scale_ = other.decimal_scale_;
            null_count_ = other.null_count_;
            has_stats_ = other.has_stats_;
            has_bounds_ = other.has_bounds_;
        }
        return *this;
    }

    void base_statistics_t::set_decimal_bounds(uint8_t width,
                                               uint8_t scale,
                                               types::int128_t min_raw,
                                               types::int128_t max_raw) {
        assert(type_ == types::logical_type::DECIMAL);
        decimal_width_ = width;
        decimal_scale_ = scale;
        min_i_ = min_raw;
        max_i_ = max_raw;
        has_bounds_ = true;
        has_stats_ = true;
    }

    void base_statistics_t::update(vector::vector_t& vec, uint64_t offset, uint64_t count) {
        if (count == 0) {
            return;
        }
        switch (type_) {
            case types::logical_type::BOOLEAN:
                update_numeric_stats<bool>(*this, vec, offset, count);
                break;
            case types::logical_type::TINYINT:
                update_numeric_stats<int8_t>(*this, vec, offset, count);
                break;
            case types::logical_type::SMALLINT:
                update_numeric_stats<int16_t>(*this, vec, offset, count);
                break;
            case types::logical_type::INTEGER:
                update_numeric_stats<int32_t>(*this, vec, offset, count);
                break;
            case types::logical_type::BIGINT:
                update_numeric_stats<int64_t>(*this, vec, offset, count);
                break;
            case types::logical_type::UTINYINT:
                update_numeric_stats<uint8_t>(*this, vec, offset, count);
                break;
            case types::logical_type::USMALLINT:
                update_numeric_stats<uint16_t>(*this, vec, offset, count);
                break;
            case types::logical_type::UINTEGER:
                update_numeric_stats<uint32_t>(*this, vec, offset, count);
                break;
            case types::logical_type::UBIGINT:
                update_numeric_stats<uint64_t>(*this, vec, offset, count);
                break;
            case types::logical_type::FLOAT:
                update_numeric_stats<float>(*this, vec, offset, count);
                break;
            case types::logical_type::DOUBLE:
                update_numeric_stats<double>(*this, vec, offset, count);
                break;
            case types::logical_type::HUGEINT:
                update_numeric_stats<types::int128_t>(*this, vec, offset, count);
                break;
            case types::logical_type::UHUGEINT:
                update_numeric_stats<types::uint128_t>(*this, vec, offset, count);
                break;
            case types::logical_type::DECIMAL:
                update_decimal_stats(*this, vec, offset, count);
                break;
            case types::logical_type::STRING_LITERAL:
                update_string_stats(*this, vec, offset, count);
                break;
            default:
                // unsupported types: just count nulls
                {
                    const auto& validity = vec.validity();
                    uint64_t null_count = 0;
                    if (vec.get_vector_type() == vector::vector_type::CONSTANT) {
                        null_count = validity.row_is_valid(0) ? 0 : count;
                    } else {
                        for (uint64_t i = offset; i < offset + count; i++) {
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
            min_i_ = other.min_i_;
            max_i_ = other.max_i_;
            min_u_ = other.min_u_;
            max_u_ = other.max_u_;
            min_d_ = other.min_d_;
            max_d_ = other.max_d_;
            min_s_.assign(other.min_s_);
            max_s_.assign(other.max_s_);
            decimal_width_ = other.decimal_width_;
            decimal_scale_ = other.decimal_scale_;
            has_bounds_ = other.has_bounds_;
            has_stats_ = true;
            return;
        }
        if (!other.has_bounds_) {
            return;
        }
        if (!has_bounds_) {
            min_i_ = other.min_i_;
            max_i_ = other.max_i_;
            min_u_ = other.min_u_;
            max_u_ = other.max_u_;
            min_d_ = other.min_d_;
            max_d_ = other.max_d_;
            min_s_.assign(other.min_s_);
            max_s_.assign(other.max_s_);
            decimal_width_ = other.decimal_width_;
            decimal_scale_ = other.decimal_scale_;
            has_bounds_ = true;
            return;
        }
        switch (slot_of_type(type_)) {
            case slot::signed_int:
                if (other.min_i_ < min_i_) {
                    min_i_ = other.min_i_;
                }
                if (other.max_i_ > max_i_) {
                    max_i_ = other.max_i_;
                }
                break;
            case slot::unsigned_int:
                if (other.min_u_ < min_u_) {
                    min_u_ = other.min_u_;
                }
                if (other.max_u_ > max_u_) {
                    max_u_ = other.max_u_;
                }
                break;
            case slot::real:
                if (other.min_d_ < min_d_) {
                    min_d_ = other.min_d_;
                }
                if (other.max_d_ > max_d_) {
                    max_d_ = other.max_d_;
                }
                break;
            case slot::str:
                if (other.min_s_ < min_s_) {
                    min_s_.assign(other.min_s_);
                }
                if (other.max_s_ > max_s_) {
                    max_s_.assign(other.max_s_);
                }
                break;
            case slot::none:
                break;
        }
    }

    void base_statistics_t::serialize(storage::metadata_writer_t& writer) const {
        writer.write<uint8_t>(static_cast<uint8_t>(type_));
        writer.write<uint8_t>(has_stats_ ? 1 : 0);
        writer.write<uint64_t>(null_count_);
        bool has_minmax = has_stats_ && has_bounds_ && type_supports_minmax(type_);
        writer.write<uint8_t>(has_minmax ? 1 : 0);
        if (has_minmax) {
            serialize_bounds(*this, writer);
        }
    }

    base_statistics_t base_statistics_t::deserialize(std::pmr::memory_resource* resource,
                                                     storage::metadata_reader_t& reader) {
        auto type = static_cast<types::logical_type>(reader.read<uint8_t>());
        auto has_stats = reader.read<uint8_t>() != 0;
        auto null_count = reader.read<uint64_t>();

        base_statistics_t result(resource, type);
        result.null_count_ = null_count;

        auto has_minmax = reader.read<uint8_t>() != 0;
        if (has_minmax) {
            deserialize_bounds(result, resource, reader);
        }
        result.has_stats_ = has_stats;

        return result;
    }

} // namespace components::table
