#pragma once

#include <components/types/logical_value.hpp>

#include <cstring>
#include <memory_resource>
#include <stdexcept>
#include <string>

namespace components::index::codec {

    using logical_value_t = components::types::logical_value_t;
    using logical_type_t = components::types::logical_type;
    using physical_type_t = components::types::physical_type;

    template<typename T>
    inline void append_le(std::pmr::string& out, T v) {
        unsigned char bytes[sizeof(T)];
        std::memcpy(bytes, &v, sizeof(T));
        out.append(reinterpret_cast<const char*>(bytes), sizeof(T));
    }

    template<typename T>
    inline T read_le(const std::pmr::string& in, size_t& pos) {
        if (pos + sizeof(T) > in.size()) {
            throw std::runtime_error("logical value codec: short read");
        }
        T v{};
        std::memcpy(&v, in.data() + pos, sizeof(T));
        pos += sizeof(T);
        return v;
    }

    template<typename T>
    inline T read_le_ptr(const uint8_t* p) {
        T v{};
        std::memcpy(&v, p, sizeof(T));
        return v;
    }

    template<typename T>
    inline void write_le_ptr(uint8_t* p, T v) {
        std::memcpy(p, &v, sizeof(T));
    }

    template<typename AppendFn>
    inline void append_decimal_payload(AppendFn&& append, const logical_value_t& key) {
        const auto* decimal =
            reinterpret_cast<const components::types::decimal_logical_type_extension*>(key.type().extension());
        append(decimal->width());
        append(decimal->scale());
        switch (decimal->stored_as()) {
            case physical_type_t::INT16:
                append(key.value<int16_t>());
                break;
            case physical_type_t::INT32:
                append(key.value<int32_t>());
                break;
            case physical_type_t::INT64:
                append(key.value<int64_t>());
                break;
            case physical_type_t::INT128:
                append(key.value<components::types::int128_t>());
                break;
            default:
                throw std::runtime_error("logical value codec: unsupported DECIMAL physical storage");
        }
    }

    template<typename ReadFn>
    inline logical_value_t read_decimal_payload(std::pmr::memory_resource* resource, ReadFn&& read) {
        const auto width = read.template operator()<uint8_t>();
        const auto scale = read.template operator()<uint8_t>();
        const auto decimal_type = components::types::complex_logical_type::create_decimal(width, scale);
        switch (decimal_type.to_physical_type()) {
            case physical_type_t::INT16:
                return logical_value_t::create_decimal(resource, decimal_type, read.template operator()<int16_t>());
            case physical_type_t::INT32:
                return logical_value_t::create_decimal(resource, decimal_type, read.template operator()<int32_t>());
            case physical_type_t::INT64:
                return logical_value_t::create_decimal(resource, decimal_type, read.template operator()<int64_t>());
            case physical_type_t::INT128:
                return logical_value_t::create_decimal(resource,
                                                       decimal_type,
                                                       read.template operator()<components::types::int128_t>());
            default:
                throw std::runtime_error("logical value codec: unsupported DECIMAL physical storage during decode");
        }
    }

    inline void append_logical_value(std::pmr::string& out, const logical_value_t& key) {
        const auto logical = key.type().type();
        append_le<uint8_t>(out, static_cast<uint8_t>(logical));
        if (logical == logical_type_t::DECIMAL) {
            append_decimal_payload([&out]<typename T>(T v) { append_le<T>(out, v); }, key);
            return;
        }

        switch (key.type().to_physical_type()) {
            case physical_type_t::NA:
                break;
            case physical_type_t::BOOL:
                append_le<uint8_t>(out, key.value<bool>() ? 1 : 0);
                break;
            case physical_type_t::INT8:
                append_le<int8_t>(out, key.value<int8_t>());
                break;
            case physical_type_t::UINT8:
                append_le<uint8_t>(out, key.value<uint8_t>());
                break;
            case physical_type_t::INT16:
                append_le<int16_t>(out, key.value<int16_t>());
                break;
            case physical_type_t::UINT16:
                append_le<uint16_t>(out, key.value<uint16_t>());
                break;
            case physical_type_t::INT32:
                append_le<int32_t>(out, key.value<int32_t>());
                break;
            case physical_type_t::UINT32:
                append_le<uint32_t>(out, key.value<uint32_t>());
                break;
            case physical_type_t::INT64:
                append_le<int64_t>(out, key.value<int64_t>());
                break;
            case physical_type_t::UINT64:
                append_le<uint64_t>(out, key.value<uint64_t>());
                break;
            case physical_type_t::FLOAT:
                append_le<float>(out, key.value<float>());
                break;
            case physical_type_t::DOUBLE:
                append_le<double>(out, key.value<double>());
                break;
            case physical_type_t::STRING: {
                auto s = key.value<std::string_view>();
                append_le<uint32_t>(out, static_cast<uint32_t>(s.size()));
                out.append(s.data(), s.size());
                break;
            }
            default:
                throw std::runtime_error("logical value codec: unsupported physical key type");
        }
    }

    inline logical_value_t
    read_logical_value(std::pmr::memory_resource* resource, const std::pmr::string& in, size_t& pos) {
        const auto logical = static_cast<logical_type_t>(read_le<uint8_t>(in, pos));
        if (logical == logical_type_t::DECIMAL) {
            return read_decimal_payload(resource, [&in, &pos]<typename T>() { return read_le<T>(in, pos); });
        }
        const auto physical = components::types::to_physical_type(logical);

        switch (physical) {
            case physical_type_t::NA:
                return logical_value_t(resource, components::types::complex_logical_type{logical_type_t::NA});
            case physical_type_t::BOOL:
                if (logical != logical_type_t::BOOLEAN) {
                    throw std::runtime_error("logical value codec: unsupported BOOL logical key type during decode");
                }
                return logical_value_t(resource, read_le<uint8_t>(in, pos) != 0);
            case physical_type_t::INT8:
                if (logical != logical_type_t::TINYINT) {
                    throw std::runtime_error("logical value codec: unsupported INT8 logical key type during decode");
                }
                return logical_value_t(resource, read_le<int8_t>(in, pos));
            case physical_type_t::UINT8:
                if (logical != logical_type_t::UTINYINT) {
                    throw std::runtime_error("logical value codec: unsupported UINT8 logical key type during decode");
                }
                return logical_value_t(resource, read_le<uint8_t>(in, pos));
            case physical_type_t::INT16:
                if (logical != logical_type_t::SMALLINT) {
                    throw std::runtime_error("logical value codec: unsupported INT16 logical key type during decode");
                }
                return logical_value_t(resource, read_le<int16_t>(in, pos));
            case physical_type_t::UINT16:
                if (logical != logical_type_t::USMALLINT) {
                    throw std::runtime_error("logical value codec: unsupported UINT16 logical key type during decode");
                }
                return logical_value_t(resource, read_le<uint16_t>(in, pos));
            case physical_type_t::INT32: {
                const auto v = read_le<int32_t>(in, pos);
                if (logical == logical_type_t::DATE) {
                    return logical_value_t(resource, core::date::date_t{core::date::days{v}});
                }
                if (logical != logical_type_t::INTEGER) {
                    throw std::runtime_error("logical value codec: unsupported INT32 logical key type during decode");
                }
                return logical_value_t(resource, v);
            }
            case physical_type_t::UINT32:
                if (logical != logical_type_t::UINTEGER) {
                    throw std::runtime_error("logical value codec: unsupported UINT32 logical key type during decode");
                }
                return logical_value_t(resource, read_le<uint32_t>(in, pos));
            case physical_type_t::INT64: {
                const auto v = read_le<int64_t>(in, pos);
                switch (logical) {
                    case logical_type_t::BIGINT:
                        return logical_value_t(resource, v);
                    case logical_type_t::TIME:
                        return logical_value_t(resource, core::date::time_t{core::date::microseconds{v}});
                    case logical_type_t::TIMESTAMP:
                        return logical_value_t(resource, core::date::timestamp_t{core::date::microseconds{v}});
                    case logical_type_t::TIMESTAMP_TZ:
                        return logical_value_t(resource, core::date::timestamptz_t{core::date::microseconds{v}});
                    default:
                        throw std::runtime_error(
                            "logical value codec: unsupported INT64 logical key type during decode");
                }
            }
            case physical_type_t::UINT64:
                if (logical != logical_type_t::UBIGINT) {
                    throw std::runtime_error("logical value codec: unsupported UINT64 logical key type during decode");
                }
                return logical_value_t(resource, read_le<uint64_t>(in, pos));
            case physical_type_t::FLOAT:
                if (logical != logical_type_t::FLOAT) {
                    throw std::runtime_error("logical value codec: unsupported FLOAT logical key type during decode");
                }
                return logical_value_t(resource, read_le<float>(in, pos));
            case physical_type_t::DOUBLE:
                if (logical != logical_type_t::DOUBLE) {
                    throw std::runtime_error("logical value codec: unsupported DOUBLE logical key type during decode");
                }
                return logical_value_t(resource, read_le<double>(in, pos));
            case physical_type_t::STRING: {
                if (logical != logical_type_t::STRING_LITERAL) {
                    throw std::runtime_error("logical value codec: unsupported STRING logical key type during decode");
                }
                const auto n = read_le<uint32_t>(in, pos);
                if (pos + n > in.size()) {
                    throw std::runtime_error("logical value codec: string overrun");
                }
                std::pmr::string s(in.data() + pos, n, resource);
                pos += n;
                return logical_value_t(resource, std::move(s));
            }
            default:
                throw std::runtime_error("logical value codec: unsupported physical key type during decode");
        }
    }

    inline std::string encode_disk_hash_key(const logical_value_t& key) {
        auto append_raw = [](std::string& out, const void* data, size_t size) {
            out.append(reinterpret_cast<const char*>(data), size);
        };
        auto append_le_std = [&](auto v, std::string& out) {
            using T = decltype(v);
            unsigned char bytes[sizeof(T)];
            std::memcpy(bytes, &v, sizeof(T));
            append_raw(out, bytes, sizeof(T));
        };

        std::string out;
        out.reserve(32);

        const auto logical = key.type().type();
        append_le_std(static_cast<uint8_t>(logical), out);
        if (logical == logical_type_t::DECIMAL) {
            append_decimal_payload([&out, &append_le_std]<typename T>(T v) { append_le_std(v, out); }, key);
            return out;
        }

        switch (key.type().to_physical_type()) {
            case physical_type_t::NA:
                break;
            case physical_type_t::BOOL:
                append_le_std(static_cast<uint8_t>(key.value<bool>() ? 1 : 0), out);
                break;
            case physical_type_t::INT8:
                append_le_std(key.value<int8_t>(), out);
                break;
            case physical_type_t::UINT8:
                append_le_std(key.value<uint8_t>(), out);
                break;
            case physical_type_t::INT16:
                append_le_std(key.value<int16_t>(), out);
                break;
            case physical_type_t::UINT16:
                append_le_std(key.value<uint16_t>(), out);
                break;
            case physical_type_t::INT32:
                append_le_std(key.value<int32_t>(), out);
                break;
            case physical_type_t::UINT32:
                append_le_std(key.value<uint32_t>(), out);
                break;
            case physical_type_t::INT64:
                append_le_std(key.value<int64_t>(), out);
                break;
            case physical_type_t::UINT64:
                append_le_std(key.value<uint64_t>(), out);
                break;
            case physical_type_t::FLOAT:
                append_le_std(key.value<float>(), out);
                break;
            case physical_type_t::DOUBLE:
                append_le_std(key.value<double>(), out);
                break;
            case physical_type_t::STRING: {
                auto sv = key.value<std::string_view>();
                append_le_std(static_cast<uint32_t>(sv.size()), out);
                append_raw(out, sv.data(), sv.size());
                break;
            }
            default:
                throw std::runtime_error("disk hash key codec: unsupported physical key type");
        }
        return out;
    }

} // namespace components::index::codec
