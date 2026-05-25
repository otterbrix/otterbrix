#pragma once

#include <components/types/logical_value.hpp>

#include <cstring>
#include <memory_resource>
#include <stdexcept>
#include <string>

namespace components::index::codec {

    using logical_value_t = components::types::logical_value_t;
    using logical_type_t = components::types::logical_type;

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

    inline void append_logical_value(std::pmr::string& out, const logical_value_t& key) {
        const auto t = key.type().type();
        append_le<uint8_t>(out, static_cast<uint8_t>(t));
        switch (t) {
            case logical_type_t::NA:
                break;
            case logical_type_t::BOOLEAN:
                append_le<uint8_t>(out, key.value<bool>() ? 1 : 0);
                break;
            case logical_type_t::TINYINT:
                append_le<int8_t>(out, key.value<int8_t>());
                break;
            case logical_type_t::UTINYINT:
                append_le<uint8_t>(out, key.value<uint8_t>());
                break;
            case logical_type_t::SMALLINT:
                append_le<int16_t>(out, key.value<int16_t>());
                break;
            case logical_type_t::USMALLINT:
                append_le<uint16_t>(out, key.value<uint16_t>());
                break;
            case logical_type_t::INTEGER:
                append_le<int32_t>(out, key.value<int32_t>());
                break;
            case logical_type_t::UINTEGER:
                append_le<uint32_t>(out, key.value<uint32_t>());
                break;
            case logical_type_t::BIGINT:
                append_le<int64_t>(out, key.value<int64_t>());
                break;
            case logical_type_t::UBIGINT:
                append_le<uint64_t>(out, key.value<uint64_t>());
                break;
            case logical_type_t::FLOAT:
                append_le<float>(out, key.value<float>());
                break;
            case logical_type_t::DOUBLE:
                append_le<double>(out, key.value<double>());
                break;
            case logical_type_t::STRING_LITERAL: {
                auto s = key.value<std::string_view>();
                append_le<uint32_t>(out, static_cast<uint32_t>(s.size()));
                out.append(s.data(), s.size());
                break;
            }
            default:
                throw std::runtime_error("logical value codec: unsupported key type");
        }
    }

    inline logical_value_t read_logical_value(std::pmr::memory_resource* resource, const std::pmr::string& in, size_t& pos) {
        const auto t = static_cast<logical_type_t>(read_le<uint8_t>(in, pos));
        switch (t) {
            case logical_type_t::NA:
                return logical_value_t(resource, components::types::complex_logical_type{logical_type_t::NA});
            case logical_type_t::BOOLEAN:
                return logical_value_t(resource, read_le<uint8_t>(in, pos) != 0);
            case logical_type_t::TINYINT:
                return logical_value_t(resource, read_le<int8_t>(in, pos));
            case logical_type_t::UTINYINT:
                return logical_value_t(resource, read_le<uint8_t>(in, pos));
            case logical_type_t::SMALLINT:
                return logical_value_t(resource, read_le<int16_t>(in, pos));
            case logical_type_t::USMALLINT:
                return logical_value_t(resource, read_le<uint16_t>(in, pos));
            case logical_type_t::INTEGER:
                return logical_value_t(resource, read_le<int32_t>(in, pos));
            case logical_type_t::UINTEGER:
                return logical_value_t(resource, read_le<uint32_t>(in, pos));
            case logical_type_t::BIGINT:
                return logical_value_t(resource, read_le<int64_t>(in, pos));
            case logical_type_t::UBIGINT:
                return logical_value_t(resource, read_le<uint64_t>(in, pos));
            case logical_type_t::FLOAT:
                return logical_value_t(resource, read_le<float>(in, pos));
            case logical_type_t::DOUBLE:
                return logical_value_t(resource, read_le<double>(in, pos));
            case logical_type_t::STRING_LITERAL: {
                const auto n = read_le<uint32_t>(in, pos);
                if (pos + n > in.size()) {
                    throw std::runtime_error("logical value codec: string overrun");
                }
                std::pmr::string s(in.data() + pos, n, resource);
                pos += n;
                return logical_value_t(resource, std::move(s));
            }
            default:
                throw std::runtime_error("logical value codec: unsupported key type during decode");
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

        const auto t = key.type().type();
        append_le_std(static_cast<uint8_t>(t), out);
        switch (t) {
            case logical_type_t::NA:
                break;
            case logical_type_t::BOOLEAN:
                append_le_std(static_cast<uint8_t>(key.value<bool>() ? 1 : 0), out);
                break;
            case logical_type_t::TINYINT:
                append_le_std(key.value<int8_t>(), out);
                break;
            case logical_type_t::UTINYINT:
                append_le_std(key.value<uint8_t>(), out);
                break;
            case logical_type_t::SMALLINT:
                append_le_std(key.value<int16_t>(), out);
                break;
            case logical_type_t::USMALLINT:
                append_le_std(key.value<uint16_t>(), out);
                break;
            case logical_type_t::INTEGER:
                append_le_std(key.value<int32_t>(), out);
                break;
            case logical_type_t::UINTEGER:
                append_le_std(key.value<uint32_t>(), out);
                break;
            case logical_type_t::BIGINT:
                append_le_std(key.value<int64_t>(), out);
                break;
            case logical_type_t::UBIGINT:
                append_le_std(key.value<uint64_t>(), out);
                break;
            case logical_type_t::FLOAT:
                append_le_std(key.value<float>(), out);
                break;
            case logical_type_t::DOUBLE:
                append_le_std(key.value<double>(), out);
                break;
            case logical_type_t::STRING_LITERAL: {
                auto sv = key.value<std::string_view>();
                append_le_std(static_cast<uint32_t>(sv.size()), out);
                append_raw(out, sv.data(), sv.size());
                break;
            }
            default:
                throw std::runtime_error("disk hash key codec: unsupported key type");
        }
        return out;
    }

} // namespace components::index::codec
