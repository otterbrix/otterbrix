#pragma once

#include <components/types/logical_value.hpp>
#include <components/types/physical_value.hpp>

#include <cassert>
#include <cstring>
#include <memory_resource>
#include <string>
#include <vector>

namespace components::index::codec {

    using logical_value_t = components::types::logical_value_t;
    using logical_type_t = components::types::logical_type;
    using physical_type_t = components::types::physical_type;

    // CREATE INDEX vets key types before this file sees them (services/dispatcher/validate_logical_plan.cpp),
    // so the encode-side `default:` arms never fire on user data; the decode side reads untrusted bytes off
    // disk instead, so nothing in this file aborts on either side — a corrupt payload is reported via `ok`.
    // Ordered (b+tree) indexes round-trip keys through physical_value, which carries no DECIMAL tag, so
    // DECIMAL is refused when `ordered` is true; hashed indexes accept it via append_decimal_payload.
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

    // Must not throw: an exception here unwinds into an actor coroutine whose unhandled_exception() aborts,
    // so a corrupt key must be reported through `ok`, not thrown. Bounds check is written to avoid overflow
    // in `pos + sizeof(T)` for a `pos` already past the end.
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

    // Reports rather than asserts: both encoders that call this can be handed a DECIMAL decoded
    // off disk (see encode_disk_hash_key), not only one this process built.
    template<typename AppendFn>
    [[nodiscard]] inline bool append_decimal_payload(AppendFn&& append, const logical_value_t& key) {
        const auto* decimal =
            reinterpret_cast<const components::types::decimal_logical_type_extension*>(key.type().extension());
        append(decimal->width());
        append(decimal->scale());
        switch (decimal->stored_as()) {
            case physical_type_t::INT16:
                append(key.value<int16_t>());
                return true;
            case physical_type_t::INT32:
                append(key.value<int32_t>());
                return true;
            case physical_type_t::INT64:
                append(key.value<int64_t>());
                return true;
            case physical_type_t::INT128:
                append(key.value<components::types::int128_t>());
                return true;
            default:
                return false;
        }
    }

    // width/scale are stored bytes; an unrepresentable pair is data, reported through `ok` as NA.
    template<typename ReadFn>
    inline logical_value_t read_decimal_payload(std::pmr::memory_resource* resource, ReadFn&& read, bool* ok) {
        const auto width = read.template operator()<uint8_t>();
        const auto scale = read.template operator()<uint8_t>();
        auto refuse = [resource, ok]() {
            if (ok != nullptr) {
                *ok = false;
            }
            return logical_value_t(resource, components::types::complex_logical_type{logical_type_t::NA});
        };
        // A flipped bit in a stored width (e.g. 18 -> 50) must not assert: an ordinary
        // NUMERIC(18,2) key would kill the process.
        auto decimal_result = components::types::complex_logical_type::create_decimal(resource, width, scale);
        if (decimal_result.has_error()) {
            return refuse();
        }
        const auto decimal_type = std::move(decimal_result.value());
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
                // Invariant, not data: create_decimal already vetted (width, scale) to one of the four above.
                assert(false && "logical value codec: unsupported DECIMAL physical storage during decode");
                return refuse();
        }
    }

    // A refusal leaves `out` holding only the tag byte — not a usable key. The `default:` arm must not
    // abort: bitcask's merge relocation (services/index/bitcask_index_disk.cpp) can hand this a value
    // decoded off disk.
    inline void append_logical_value(std::pmr::string& out, const logical_value_t& key, bool* ok = nullptr) {
        const auto refuse = [ok]() {
            if (ok != nullptr) {
                *ok = false;
            }
        };
        const auto logical = key.type().type();
        append_le<uint8_t>(out, static_cast<uint8_t>(logical));
        if (logical == logical_type_t::DECIMAL) {
            if (!append_decimal_payload([&out]<typename T>(T v) { append_le<T>(out, v); }, key)) {
                refuse();
            }
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
                // Unreachable from a vetted column, but reachable with a value decoded off disk
                // (merge relocation) — an assert can't tell those apart, so this refuses instead.
                refuse();
                return;
        }
    }

    // `in` is an untrusted stored key payload (b+tree leaf or bitcask segment, no checksum).
    // Nothing here aborts; a refusal answers NA and sets `*ok` to false.
    inline logical_value_t read_logical_value(std::pmr::memory_resource* resource,
                                              const std::pmr::string& in,
                                              size_t& pos,
                                              bool* ok = nullptr) {
        auto refuse = [resource, ok]() {
            if (ok != nullptr) {
                *ok = false;
            }
            return logical_value_t(resource, components::types::complex_logical_type{logical_type_t::NA});
        };
        bool read_ok = true;
        const auto logical = static_cast<logical_type_t>(read_le<uint8_t>(in, pos, &read_ok));
        if (!read_ok) {
            return refuse();
        }
        if (logical == logical_type_t::DECIMAL) {
            auto decoded = read_decimal_payload(
                resource,
                [&in, &pos, &read_ok]<typename T>() { return read_le<T>(in, pos, &read_ok); },
                &read_ok);
            if (!read_ok) {
                return refuse();
            }
            return decoded;
        }
        // Most arms below guard a DERIVATION (physical width implies one logical type) and keep a Debug
        // assert but still refuse rather than abort. INT32 (shared by INTEGER/DATE/ENUM), the STRING
        // length, and the outer `default:` carry no assert — ordinary stored bytes can reach them.
        const auto physical = components::types::to_physical_type(logical);

        auto value_or_refuse = [&]<typename T>(auto&& build) {
            const auto v = read_le<T>(in, pos, &read_ok);
            if (!read_ok) {
                return refuse();
            }
            return build(v);
        };

        switch (physical) {
            case physical_type_t::NA:
                return logical_value_t(resource, components::types::complex_logical_type{logical_type_t::NA});
            case physical_type_t::BOOL:
                if (logical != logical_type_t::BOOLEAN) {
                    assert(false && "logical value codec: unsupported BOOL logical key type during decode");
                    return refuse();
                }
                return value_or_refuse.template operator()<uint8_t>(
                    [&](uint8_t v) { return logical_value_t(resource, v != 0); });
            case physical_type_t::INT8:
                if (logical != logical_type_t::TINYINT) {
                    assert(false && "logical value codec: unsupported INT8 logical key type during decode");
                    return refuse();
                }
                return value_or_refuse.template operator()<int8_t>(
                    [&](int8_t v) { return logical_value_t(resource, v); });
            case physical_type_t::UINT8:
                if (logical != logical_type_t::UTINYINT) {
                    assert(false && "logical value codec: unsupported UINT8 logical key type during decode");
                    return refuse();
                }
                return value_or_refuse.template operator()<uint8_t>(
                    [&](uint8_t v) { return logical_value_t(resource, v); });
            case physical_type_t::INT16:
                if (logical != logical_type_t::SMALLINT) {
                    assert(false && "logical value codec: unsupported INT16 logical key type during decode");
                    return refuse();
                }
                return value_or_refuse.template operator()<int16_t>(
                    [&](int16_t v) { return logical_value_t(resource, v); });
            case physical_type_t::UINT16:
                if (logical != logical_type_t::USMALLINT) {
                    assert(false && "logical value codec: unsupported UINT16 logical key type during decode");
                    return refuse();
                }
                return value_or_refuse.template operator()<uint16_t>(
                    [&](uint16_t v) { return logical_value_t(resource, v); });
            case physical_type_t::INT32: {
                // Data, not a derivation: ENUM also maps to INT32, and this codec cannot read it
                // (no entry list carried in the key).
                if (logical != logical_type_t::DATE && logical != logical_type_t::INTEGER) {
                    return refuse();
                }
                const auto v = read_le<int32_t>(in, pos, &read_ok);
                if (!read_ok) {
                    return refuse();
                }
                if (logical == logical_type_t::DATE) {
                    return logical_value_t(resource, core::date::date_t{core::date::days{v}});
                }
                return logical_value_t(resource, v);
            }
            case physical_type_t::UINT32:
                if (logical != logical_type_t::UINTEGER) {
                    assert(false && "logical value codec: unsupported UINT32 logical key type during decode");
                    return refuse();
                }
                return value_or_refuse.template operator()<uint32_t>(
                    [&](uint32_t v) { return logical_value_t(resource, v); });
            case physical_type_t::INT64: {
                const auto v = read_le<int64_t>(in, pos, &read_ok);
                if (!read_ok) {
                    return refuse();
                }
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
                        // Derivation: INT64 comes only from these four and DECIMAL, which already left above.
                        assert(false && "logical value codec: unsupported INT64 logical key type during decode");
                        return refuse();
                }
            }
            case physical_type_t::UINT64:
                if (logical != logical_type_t::UBIGINT) {
                    assert(false && "logical value codec: unsupported UINT64 logical key type during decode");
                    return refuse();
                }
                return value_or_refuse.template operator()<uint64_t>(
                    [&](uint64_t v) { return logical_value_t(resource, v); });
            case physical_type_t::FLOAT:
                if (logical != logical_type_t::FLOAT) {
                    assert(false && "logical value codec: unsupported FLOAT logical key type during decode");
                    return refuse();
                }
                return value_or_refuse.template operator()<float>(
                    [&](float v) { return logical_value_t(resource, v); });
            case physical_type_t::DOUBLE:
                if (logical != logical_type_t::DOUBLE) {
                    assert(false && "logical value codec: unsupported DOUBLE logical key type during decode");
                    return refuse();
                }
                return value_or_refuse.template operator()<double>(
                    [&](double v) { return logical_value_t(resource, v); });
            case physical_type_t::STRING: {
                if (logical != logical_type_t::STRING_LITERAL) {
                    assert(false && "logical value codec: unsupported STRING logical key type during decode");
                    return refuse();
                }
                const auto n = read_le<uint32_t>(in, pos, &read_ok);
                if (!read_ok) {
                    return refuse();
                }
                // Data: the length is four stored bytes; a flipped high bit could claim gigabytes for a
                // tiny record, so the comparison below is written to not overflow.
                if (n > in.size() - pos) {
                    return refuse();
                }
                std::pmr::string s(in.data() + pos, n, resource);
                pos += n;
                return logical_value_t(resource, std::move(s));
            }
            default:
                // Data: a tag whose width has no reader, or one the type table maps to nothing.
                return refuse();
        }
    }
    // Runs on the path that opens a database: bitcask_index_disk_t::load_from_disk hands this a
    // value decoded off disk via key_bytes_for_hash()/normalize_hash_key() (services/index/bitcask_index_disk.cpp).
    // The `default:` arm must not abort — it reports through `ok` instead, and every caller
    // reachable with disk-decoded bytes passes the flag and refuses the whole operation.
    inline std::string encode_disk_hash_key(const logical_value_t& key, bool* ok = nullptr) {
        const auto refuse = [ok]() {
            if (ok != nullptr) {
                *ok = false;
            }
        };
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
            if (!append_decimal_payload([&out, &append_le_std]<typename T>(T v) { append_le_std(v, out); }, key)) {
                refuse();
            }
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
                // No assert here, unlike the decode-side derivation guards: a stored tag byte can
                // steer this arm through the rebuild loop above, and assert(false) aborts in Debug.
                refuse();
                break;
        }
        return out;
    }

    // Raw-buffer twin of read_le: an assert alone would vanish under NDEBUG and let memcpy read past
    // the end of the record. `ok` reports a short read; a caller that ignores it gets T{} and an unmoved `pos`.
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
    // Ordered (b+tree) decoder; same untrusted-input contract as read_logical_value — refuses
    // via `ok`, never aborts.
    inline components::types::physical_value
    read_logical_value_as_view(const char* data, size_t size, size_t& pos, bool* ok = nullptr) {
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
        // physical_value carries no width/scale, so honouring DECIMAL here would silently compare
        // the scaled payload as plain INT64. Ordered indexes refuse DECIMAL at CREATE INDEX, so
        // seeing one here is corrupt data.
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
                // physical_value here is a VIEW into `data`; an unchecked length would publish a
                // view past the end that every later comparison reads through.
                if (n > size - pos) {
                    return refuse();
                }
                components::types::physical_value pv(data + pos, static_cast<uint32_t>(n));
                pos += n;
                return pv;
            }
            default:
                // DATA: an unmapped tag byte, or a width this decoder has no arm for.
                return refuse();
        }
    }
    // Steps `pos` past one value without decoding it (services::index::id_of reaches the row id
    // after the key). A `pos` left past the end would turn the next read out-of-bounds, so every
    // step is bounds-checked.
    inline void skip_logical_value(const char* data, size_t size, size_t& pos, bool* ok = nullptr) {
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
            // Only the width matters for a skip, so ask the storage table directly instead of
            // building a type via create_decimal, whose error path a flipped stored byte (18 -> 50)
            // would otherwise reach.
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
                // DATA: an unmapped tag byte, or a width this codec cannot size.
                refuse();
                return;
        }
    }
    // TYPE-DIRECTED VALUE CODEC — persists a column DEFAULT (pg_attribute.attdefspec; see
    // components/catalog/system_table_schemas.hpp). Unlike the key codec above: a value may be NULL (one
    // presence byte), and since the reader already knows the type (atttypspec), the payload carries no
    // width/scale — only a per-value logical tag, written at EVERY nesting level, as a CHECK against the
    // caller's type. Without it, a same-width type divergence (e.g. a BIGINT default read against a DOUBLE
    // column) would decode silently instead of leaving trailing bytes.
    // Second line of defence, not a replacement: the ALTER-time check
    // (components/catalog/alter_column_validators.cpp, validate_default_value_type) still refuses the
    // divergence before it reaches disk; this codec only catches one that already did.
    // Nothing here aborts — a short or type-mismatched payload is data, reported through `ok`/`false`.

    // Scalars are exactly the hashed-index key set; nested types are encodable when every leaf is.
    inline bool is_encodable_value_type(const components::types::complex_logical_type& type) {
        switch (type.type()) {
            case logical_type_t::ARRAY:
            case logical_type_t::LIST:
                return is_encodable_value_type(type.child_type());
            case logical_type_t::STRUCT: {
                if (type.child_types().empty()) {
                    return false;
                }
                for (const auto& field : type.child_types()) {
                    if (!is_encodable_value_type(field)) {
                        return false;
                    }
                }
                return true;
            }
            default:
                return is_representable_index_key_type(type.type(), /*ordered=*/false);
        }
    }

    // DECIMAL storage width taken from the TYPE, never the stream. INVALID if unsupported.
    inline physical_type_t decimal_storage_of(const components::types::complex_logical_type& type) {
        const auto physical = type.to_physical_type();
        switch (physical) {
            case physical_type_t::INT16:
            case physical_type_t::INT32:
            case physical_type_t::INT64:
            case physical_type_t::INT128:
                return physical;
            default:
                return physical_type_t::INVALID;
        }
    }

    inline bool append_typed_value(std::pmr::string& out, const logical_value_t& value) {
        const auto& type = value.type();
        if (value.is_null()) {
            append_le<uint8_t>(out, 0);
            return true;
        }
        append_le<uint8_t>(out, 1);
        append_le<uint8_t>(out, static_cast<uint8_t>(type.type()));
        switch (type.type()) {
            case logical_type_t::ARRAY:
            case logical_type_t::LIST: {
                const auto& children = value.children();
                append_le<uint32_t>(out, static_cast<uint32_t>(children.size()));
                for (const auto& child : children) {
                    if (!append_typed_value(out, child)) {
                        return false;
                    }
                }
                return true;
            }
            case logical_type_t::STRUCT: {
                const auto& children = value.children();
                if (children.size() != type.child_types().size()) {
                    return false;
                }
                for (const auto& child : children) {
                    if (!append_typed_value(out, child)) {
                        return false;
                    }
                }
                return true;
            }
            case logical_type_t::DECIMAL:
                switch (decimal_storage_of(type)) {
                    case physical_type_t::INT16:
                        append_le<int16_t>(out, value.value<int16_t>());
                        return true;
                    case physical_type_t::INT32:
                        append_le<int32_t>(out, value.value<int32_t>());
                        return true;
                    case physical_type_t::INT64:
                        append_le<int64_t>(out, value.value<int64_t>());
                        return true;
                    case physical_type_t::INT128:
                        append_le<components::types::int128_t>(out, value.value<components::types::int128_t>());
                        return true;
                    default:
                        return false;
                }
            case logical_type_t::BOOLEAN:
                append_le<uint8_t>(out, value.value<bool>() ? 1 : 0);
                return true;
            case logical_type_t::TINYINT:
                append_le<int8_t>(out, value.value<int8_t>());
                return true;
            case logical_type_t::UTINYINT:
                append_le<uint8_t>(out, value.value<uint8_t>());
                return true;
            case logical_type_t::SMALLINT:
                append_le<int16_t>(out, value.value<int16_t>());
                return true;
            case logical_type_t::USMALLINT:
                append_le<uint16_t>(out, value.value<uint16_t>());
                return true;
            case logical_type_t::INTEGER:
            case logical_type_t::DATE:
                append_le<int32_t>(out, value.value<int32_t>());
                return true;
            case logical_type_t::UINTEGER:
                append_le<uint32_t>(out, value.value<uint32_t>());
                return true;
            case logical_type_t::BIGINT:
            case logical_type_t::TIME:
            case logical_type_t::TIMESTAMP:
            case logical_type_t::TIMESTAMP_TZ:
                append_le<int64_t>(out, value.value<int64_t>());
                return true;
            case logical_type_t::UBIGINT:
                append_le<uint64_t>(out, value.value<uint64_t>());
                return true;
            case logical_type_t::FLOAT:
                append_le<float>(out, value.value<float>());
                return true;
            case logical_type_t::DOUBLE:
                append_le<double>(out, value.value<double>());
                return true;
            case logical_type_t::STRING_LITERAL: {
                const auto s = value.value<std::string_view>();
                append_le<uint32_t>(out, static_cast<uint32_t>(s.size()));
                out.append(s.data(), s.size());
                return true;
            }
            default:
                // No encoding for this type. Reported, never dropped.
                return false;
        }
    }

    inline logical_value_t read_typed_value(std::pmr::memory_resource* resource,
                                            const components::types::complex_logical_type& type,
                                            const std::pmr::string& in,
                                            size_t& pos,
                                            bool& ok) {
        const auto fail = [&]() {
            ok = false;
            return logical_value_t(resource, components::types::complex_logical_type{logical_type_t::NA});
        };
        bool read_ok = true;
        const auto present = read_le<uint8_t>(in, pos, &read_ok);
        if (!read_ok || present > 1) {
            return fail();
        }
        if (present == 0) {
            // NULL is NA-typed here (is_null() IS type()==NA), so it carries no tag: the
            // caller already holds the type alongside; "present==0" says only "this is NULL".
            return logical_value_t(resource, components::types::complex_logical_type{logical_type_t::NA});
        }
        // Checked BEFORE any payload byte is read, so a divergence never half-consumes the
        // stream. This catches a SAME-WIDTH divergence; `pos != payload.size()` only sees a
        // WIDTH one.
        const auto stored_tag = read_le<uint8_t>(in, pos, &read_ok);
        if (!read_ok || stored_tag != static_cast<uint8_t>(type.type())) {
            return fail();
        }
        switch (type.type()) {
            case logical_type_t::ARRAY:
            case logical_type_t::LIST: {
                const auto count = read_le<uint32_t>(in, pos, &read_ok);
                if (!read_ok || count > in.size()) { // one element costs >=1 byte
                    return fail();
                }
                std::vector<logical_value_t> children;
                children.reserve(count);
                for (uint32_t i = 0; i < count; ++i) {
                    children.push_back(read_typed_value(resource, type.child_type(), in, pos, ok));
                    if (!ok) {
                        return fail();
                    }
                }
                return type.type() == logical_type_t::ARRAY
                           ? logical_value_t::create_array(resource, type.child_type(), children)
                           : logical_value_t::create_list(resource, type.child_type(), children);
            }
            case logical_type_t::STRUCT: {
                std::vector<logical_value_t> fields;
                fields.reserve(type.child_types().size());
                for (const auto& field_type : type.child_types()) {
                    fields.push_back(read_typed_value(resource, field_type, in, pos, ok));
                    if (!ok) {
                        return fail();
                    }
                }
                return logical_value_t::create_struct(resource, type, fields);
            }
            case logical_type_t::DECIMAL: {
                switch (decimal_storage_of(type)) {
                    case physical_type_t::INT16: {
                        const auto v = read_le<int16_t>(in, pos, &read_ok);
                        return read_ok ? logical_value_t::create_decimal(resource, type, static_cast<int64_t>(v))
                                       : fail();
                    }
                    case physical_type_t::INT32: {
                        const auto v = read_le<int32_t>(in, pos, &read_ok);
                        return read_ok ? logical_value_t::create_decimal(resource, type, static_cast<int64_t>(v))
                                       : fail();
                    }
                    case physical_type_t::INT64: {
                        const auto v = read_le<int64_t>(in, pos, &read_ok);
                        return read_ok ? logical_value_t::create_decimal(resource, type, v) : fail();
                    }
                    case physical_type_t::INT128: {
                        const auto v = read_le<components::types::int128_t>(in, pos, &read_ok);
                        return read_ok ? logical_value_t::create_decimal(resource, type, v) : fail();
                    }
                    default:
                        return fail();
                }
            }
            case logical_type_t::BOOLEAN: {
                const auto v = read_le<uint8_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, v != 0) : fail();
            }
            case logical_type_t::TINYINT: {
                const auto v = read_le<int8_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, v) : fail();
            }
            case logical_type_t::UTINYINT: {
                const auto v = read_le<uint8_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, v) : fail();
            }
            case logical_type_t::SMALLINT: {
                const auto v = read_le<int16_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, v) : fail();
            }
            case logical_type_t::USMALLINT: {
                const auto v = read_le<uint16_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, v) : fail();
            }
            case logical_type_t::INTEGER: {
                const auto v = read_le<int32_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, v) : fail();
            }
            case logical_type_t::DATE: {
                const auto v = read_le<int32_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, core::date::date_t{core::date::days{v}}) : fail();
            }
            case logical_type_t::UINTEGER: {
                const auto v = read_le<uint32_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, v) : fail();
            }
            case logical_type_t::BIGINT: {
                const auto v = read_le<int64_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, v) : fail();
            }
            case logical_type_t::TIME: {
                const auto v = read_le<int64_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, core::date::time_t{core::date::microseconds{v}}) : fail();
            }
            case logical_type_t::TIMESTAMP: {
                const auto v = read_le<int64_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, core::date::timestamp_t{core::date::microseconds{v}})
                               : fail();
            }
            case logical_type_t::TIMESTAMP_TZ: {
                const auto v = read_le<int64_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, core::date::timestamptz_t{core::date::microseconds{v}})
                               : fail();
            }
            case logical_type_t::UBIGINT: {
                const auto v = read_le<uint64_t>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, v) : fail();
            }
            case logical_type_t::FLOAT: {
                const auto v = read_le<float>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, v) : fail();
            }
            case logical_type_t::DOUBLE: {
                const auto v = read_le<double>(in, pos, &read_ok);
                return read_ok ? logical_value_t(resource, v) : fail();
            }
            case logical_type_t::STRING_LITERAL: {
                const auto n = read_le<uint32_t>(in, pos, &read_ok);
                // Same non-overflowing shape as read_le/read_le_raw, for the same reason.
                if (!read_ok || pos > in.size() || in.size() - pos < n) {
                    return fail();
                }
                std::pmr::string s(in.data() + pos, n, resource);
                pos += n;
                return logical_value_t(resource, std::move(s));
            }
            default:
                return fail();
        }
    }

} // namespace components::index::codec
