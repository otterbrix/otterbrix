#pragma once

#include <components/types/physical_value.hpp>
#include <components/types/types.hpp>

#include <cassert>
#include <cstring>
#include <memory_resource>
#include <string>
#include <vector>

namespace components::index::codec {

    using logical_type_t = components::types::logical_type;
    using physical_type_t = components::types::physical_type;

    // CREATE INDEX vets key types before this file sees them; only the decode side reads untrusted disk bytes.
    inline constexpr bool is_representable_index_key_type(logical_type_t type, bool ordered) {
        switch (type) {
            case logical_type_t::BOOLEAN:
            case logical_type_t::TINYINT:
            case logical_type_t::UTINYINT:
            case logical_type_t::SMALLINT:
            case logical_type_t::USMALLINT:
            case logical_type_t::INTEGER:
            case logical_type_t::UINTEGER:
            case logical_type_t::BIGINT:
            case logical_type_t::UBIGINT:
            case logical_type_t::FLOAT:
            case logical_type_t::DOUBLE:
            case logical_type_t::STRING_LITERAL:
            case logical_type_t::DATE:
            case logical_type_t::TIME:
            case logical_type_t::TIMESTAMP:
            case logical_type_t::TIMESTAMP_TZ:
                return true;
            case logical_type_t::DECIMAL:
                return !ordered;
            default:
                return false;
        }
    }

    template<typename T>
    inline void append_le(std::pmr::string& out, T v) {
        unsigned char bytes[sizeof(T)];
        std::memcpy(bytes, &v, sizeof(T));
        out.append(reinterpret_cast<const char*>(bytes), sizeof(T));
    }

    // Must not throw -- an exception here unwinds into an actor coroutine whose unhandled_exception() aborts.
    template<typename T>
    inline T read_le(const std::pmr::string& in, size_t& pos, bool* ok = nullptr) {
        if (pos > in.size() || in.size() - pos < sizeof(T)) {
            if (ok != nullptr) {
                *ok = false;
            }
            return T{};
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

    // Raw-buffer twin of read_le: an assert alone would vanish under NDEBUG and let memcpy read past the end.
    template<typename T>
    inline T read_le_raw(const char* data, size_t size, size_t& pos, bool* ok = nullptr) {
        if (pos > size || size - pos < sizeof(T)) {
            if (ok != nullptr) {
                *ok = false;
            }
            return T{};
        }
        T v{};
        std::memcpy(&v, data + pos, sizeof(T));
        pos += sizeof(T);
        return v;
    }
    inline components::types::physical_value
    read_tagged_value(const char* data, size_t size, size_t& pos, bool* ok = nullptr) {
        auto refuse = [ok]() {
            if (ok != nullptr) {
                *ok = false;
            }
            return components::types::physical_value();
        };
        bool read_ok = true;
        const auto logical = static_cast<logical_type_t>(read_le_raw<uint8_t>(data, size, pos, &read_ok));
        if (!read_ok) {
            return refuse();
        }
        // physical_value carries no width/scale, so honouring DECIMAL here would silently misread the
        // scaled payload as plain INT64; ordered indexes already refuse DECIMAL at CREATE INDEX.
        if (logical == logical_type_t::DECIMAL) {
            return refuse();
        }
        const auto physical = components::types::to_physical_type(logical);

        auto scalar = [&]<typename T>() {
            const auto v = read_le_raw<T>(data, size, pos, &read_ok);
            if (!read_ok) {
                return refuse();
            }
            return components::types::physical_value(v);
        };

        switch (physical) {
            case physical_type_t::NA:
                return components::types::physical_value();
            case physical_type_t::BOOL: {
                const auto v = read_le_raw<uint8_t>(data, size, pos, &read_ok);
                if (!read_ok) {
                    return refuse();
                }
                return components::types::physical_value(v != 0);
            }
            case physical_type_t::INT8:
                return scalar.template operator()<int8_t>();
            case physical_type_t::UINT8:
                return scalar.template operator()<uint8_t>();
            case physical_type_t::INT16:
                return scalar.template operator()<int16_t>();
            case physical_type_t::UINT16:
                return scalar.template operator()<uint16_t>();
            case physical_type_t::INT32:
                return scalar.template operator()<int32_t>();
            case physical_type_t::UINT32:
                return scalar.template operator()<uint32_t>();
            case physical_type_t::INT64:
                return scalar.template operator()<int64_t>();
            case physical_type_t::UINT64:
                return scalar.template operator()<uint64_t>();
            case physical_type_t::FLOAT:
                return scalar.template operator()<float>();
            case physical_type_t::DOUBLE:
                return scalar.template operator()<double>();
            case physical_type_t::STRING: {
                const auto n = read_le_raw<uint32_t>(data, size, pos, &read_ok);
                if (!read_ok) {
                    return refuse();
                }
                if (n > size - pos) {
                    return refuse();
                }
                components::types::physical_value pv(data + pos, static_cast<uint32_t>(n));
                pos += n;
                return pv;
            }
            default:
                return refuse();
        }
    }
    inline void skip_tagged_value(const char* data, size_t size, size_t& pos, bool* ok = nullptr) {
        auto refuse = [ok]() {
            if (ok != nullptr) {
                *ok = false;
            }
        };
        auto advance = [&](size_t n) {
            if (pos > size || size - pos < n) {
                refuse();
                return false;
            }
            pos += n;
            return true;
        };
        bool read_ok = true;
        const auto logical = static_cast<logical_type_t>(read_le_raw<uint8_t>(data, size, pos, &read_ok));
        if (!read_ok) {
            refuse();
            return;
        }
        if (logical == logical_type_t::DECIMAL) {
            const auto width = read_le_raw<uint8_t>(data, size, pos, &read_ok);
            read_le_raw<uint8_t>(data, size, pos, &read_ok);
            if (!read_ok) {
                refuse();
                return;
            }
            // Only the width matters for a skip, so this asks the storage table directly rather than
            // building a type via create_decimal, whose error path a corrupt stored byte could reach.
            switch (components::types::decimal_storage_for_width(width)) {
                case physical_type_t::INT16:
                    advance(sizeof(int16_t));
                    return;
                case physical_type_t::INT32:
                    advance(sizeof(int32_t));
                    return;
                case physical_type_t::INT64:
                    advance(sizeof(int64_t));
                    return;
                case physical_type_t::INT128:
                    advance(sizeof(components::types::int128_t));
                    return;
                default:
                    refuse();
                    return;
            }
        }
        const auto physical = components::types::to_physical_type(logical);
        switch (physical) {
            case physical_type_t::NA:
                return;
            case physical_type_t::BOOL:
            case physical_type_t::INT8:
            case physical_type_t::UINT8:
                advance(sizeof(uint8_t));
                return;
            case physical_type_t::INT16:
            case physical_type_t::UINT16:
                advance(sizeof(uint16_t));
                return;
            case physical_type_t::INT32:
            case physical_type_t::UINT32:
                advance(sizeof(uint32_t));
                return;
            case physical_type_t::INT64:
            case physical_type_t::UINT64:
                advance(sizeof(uint64_t));
                return;
            case physical_type_t::FLOAT:
                advance(sizeof(float));
                return;
            case physical_type_t::DOUBLE:
                advance(sizeof(double));
                return;
            case physical_type_t::STRING: {
                const auto n = read_le_raw<uint32_t>(data, size, pos, &read_ok);
                if (!read_ok) {
                    refuse();
                    return;
                }
                advance(n);
                return;
            }
            default:
                refuse();
                return;
        }
    }

} // namespace components::index::codec
