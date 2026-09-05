#pragma once

#include <components/types/logical_value.hpp>
#include <components/types/physical_value.hpp>

#include <cassert>
#include <cstdlib>
#include <cstring>
#include <memory_resource>
#include <stdexcept>
#include <string>
#include <vector>

namespace components::index::codec {

    using logical_value_t = components::types::logical_value_t;
    using logical_type_t = components::types::logical_type;
    using physical_type_t = components::types::physical_type;

    // THE index key-type list — the single authority on which logical key types an index can
    // carry. The CREATE INDEX gate (services/dispatcher/validate_logical_plan.cpp,
    // node_type::create_index_t) refuses every other type with index_create_fail BEFORE the
    // statement executes, so no unrepresentable key ever reaches an encoder from user data.
    // That gate is what makes every `default:` arm in this file — and in the ordered probe
    // encoder services::index::convert() (services/index/btree_index_disk.cpp) and its decode
    // mirror reverse_convert() (services/index/manager_index.cpp) — unreachable from user
    // data, which is why those arms are hard assert+abort. When adding a type, extend the
    // encoder switches FIRST and this function LAST; the other order re-opens the hole this
    // gate closed.
    //
    // `ordered` picks the encoder family the index kind uses:
    //   * ordered (b+tree) keys additionally round-trip through physical_value —
    //     services::index::convert() encodes probes and read_logical_value_as_view() decodes
    //     stored keys for in-tree comparison. physical_value carries no DECIMAL tag
    //     (width/scale would be lost), so DECIMAL is refused for ordered indexes;
    //   * hashed (bitcask / disk-hash) keys use only the logical codec below, which
    //     round-trips DECIMAL via append_decimal_payload, so hashed indexes accept it.
    //
    // DATE / TIME / TIMESTAMP / TIMESTAMP_TZ are physically INT32/INT64 raw counters; both
    // encoder families order and equal-compare them exactly like the column does.
    // Refused (both kinds): HUGEINT/UHUGEINT/UUID (16-byte payloads the codec has no arm
    // for), INTERVAL/TIME_TZ (physically STRUCT), BLOB/BIT, and every nested type.
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

    // `ok` reports a SHORT READ — a truncated or corrupt payload, which is data, not a bug.
    // It used to throw, and this codec runs on every disk-index key: an exception here unwinds
    // into an actor coroutine whose unhandled_exception() is empty, so a corrupt key became a
    // hang instead of an error. Callers that pass no flag keep the old shape and get T{}.
    template<typename T>
    inline T read_le(const std::pmr::string& in, size_t& pos, bool* ok = nullptr) {
        if (pos + sizeof(T) > in.size()) {
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
                assert(false && "logical value codec: unsupported DECIMAL physical storage");
                std::abort(); // NDEBUG drops the assert; without this control continues into the next case
        }
    }

    template<typename ReadFn>
    inline logical_value_t read_decimal_payload(std::pmr::memory_resource* resource, ReadFn&& read) {
        const auto width = read.template operator()<uint8_t>();
        const auto scale = read.template operator()<uint8_t>();
        // width/scale arrive from STORED BYTES. create_decimal used to reach a throw for
        // width > 38 from right here, and an exception on this path unwinds into an actor
        // coroutine with an empty unhandled_exception() — a corrupt key became a HANG.
        // The refusal now has the same shape as every other refusal in this file.
        auto decimal_result = components::types::complex_logical_type::create_decimal(width, scale);
        if (decimal_result.has_error()) {
            assert(false && "logical value codec: DECIMAL width/scale out of range during decode");
            std::abort(); // NDEBUG drops the assert; without this control continues with no type
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
                assert(false && "logical value codec: unsupported DECIMAL physical storage during decode");
                std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                // NDEBUG compiles the assert out; without this the function falls off the end,
                // which is undefined behaviour in exactly the build users ship.
                std::abort();
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
                assert(false && "logical value codec: unsupported physical key type");
                std::abort(); // NDEBUG drops the assert; without this control continues into the next case
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
                    assert(false && "logical value codec: unsupported BOOL logical key type during decode");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                }
                return logical_value_t(resource, read_le<uint8_t>(in, pos) != 0);
            case physical_type_t::INT8:
                if (logical != logical_type_t::TINYINT) {
                    assert(false && "logical value codec: unsupported INT8 logical key type during decode");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                }
                return logical_value_t(resource, read_le<int8_t>(in, pos));
            case physical_type_t::UINT8:
                if (logical != logical_type_t::UTINYINT) {
                    assert(false && "logical value codec: unsupported UINT8 logical key type during decode");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                }
                return logical_value_t(resource, read_le<uint8_t>(in, pos));
            case physical_type_t::INT16:
                if (logical != logical_type_t::SMALLINT) {
                    assert(false && "logical value codec: unsupported INT16 logical key type during decode");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                }
                return logical_value_t(resource, read_le<int16_t>(in, pos));
            case physical_type_t::UINT16:
                if (logical != logical_type_t::USMALLINT) {
                    assert(false && "logical value codec: unsupported UINT16 logical key type during decode");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                }
                return logical_value_t(resource, read_le<uint16_t>(in, pos));
            case physical_type_t::INT32: {
                const auto v = read_le<int32_t>(in, pos);
                if (logical == logical_type_t::DATE) {
                    return logical_value_t(resource, core::date::date_t{core::date::days{v}});
                }
                if (logical != logical_type_t::INTEGER) {
                    assert(false && "logical value codec: unsupported INT32 logical key type during decode");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                }
                return logical_value_t(resource, v);
            }
            case physical_type_t::UINT32:
                if (logical != logical_type_t::UINTEGER) {
                    assert(false && "logical value codec: unsupported UINT32 logical key type during decode");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
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
                        assert(false && "logical value codec: unsupported INT64 logical key type during decode");
                        std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                }
            }
            case physical_type_t::UINT64:
                if (logical != logical_type_t::UBIGINT) {
                    assert(false && "logical value codec: unsupported UINT64 logical key type during decode");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                }
                return logical_value_t(resource, read_le<uint64_t>(in, pos));
            case physical_type_t::FLOAT:
                if (logical != logical_type_t::FLOAT) {
                    assert(false && "logical value codec: unsupported FLOAT logical key type during decode");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                }
                return logical_value_t(resource, read_le<float>(in, pos));
            case physical_type_t::DOUBLE:
                if (logical != logical_type_t::DOUBLE) {
                    assert(false && "logical value codec: unsupported DOUBLE logical key type during decode");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                }
                return logical_value_t(resource, read_le<double>(in, pos));
            case physical_type_t::STRING: {
                if (logical != logical_type_t::STRING_LITERAL) {
                    assert(false && "logical value codec: unsupported STRING logical key type during decode");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                }
                const auto n = read_le<uint32_t>(in, pos);
                if (pos + n > in.size()) {
                    // Corrupt payload, same class as a short read: reported as an NA value
                    // rather than thrown, because this runs inside an actor coroutine.
                    assert(false && "logical value codec: string overrun");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                    return logical_value_t(resource, components::types::complex_logical_type{logical_type_t::NA});
                }
                std::pmr::string s(in.data() + pos, n, resource);
                pos += n;
                return logical_value_t(resource, std::move(s));
            }
            default:
                assert(false && "logical value codec: unsupported physical key type during decode");
                std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                // NDEBUG compiles the assert out; without this the function falls off the end,
                // which is undefined behaviour in exactly the build users ship.
                std::abort();
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
                assert(false && "disk hash key codec: unsupported physical key type");
                std::abort(); // NDEBUG drops the assert; without this control continues into the next case
        }
        return out;
    }

    template<typename T>
    inline T read_le_raw(const char* data, [[maybe_unused]] size_t size, size_t& pos) {
        assert(pos + sizeof(T) <= size);
        T v{};
        std::memcpy(&v, data + pos, sizeof(T));
        pos += sizeof(T);
        return v;
    }

    inline components::types::physical_value read_logical_value_as_view(const char* data, size_t size, size_t& pos) {
        const auto logical = static_cast<logical_type_t>(read_le_raw<uint8_t>(data, size, pos));
        assert(logical != logical_type_t::DECIMAL && "DECIMAL not supported in physical_value");
        const auto physical = components::types::to_physical_type(logical);

        switch (physical) {
            case physical_type_t::NA:
                return components::types::physical_value();
            case physical_type_t::BOOL:
                return components::types::physical_value(read_le_raw<uint8_t>(data, size, pos) != 0);
            case physical_type_t::INT8:
                return components::types::physical_value(read_le_raw<int8_t>(data, size, pos));
            case physical_type_t::UINT8:
                return components::types::physical_value(read_le_raw<uint8_t>(data, size, pos));
            case physical_type_t::INT16:
                return components::types::physical_value(read_le_raw<int16_t>(data, size, pos));
            case physical_type_t::UINT16:
                return components::types::physical_value(read_le_raw<uint16_t>(data, size, pos));
            case physical_type_t::INT32:
                return components::types::physical_value(read_le_raw<int32_t>(data, size, pos));
            case physical_type_t::UINT32:
                return components::types::physical_value(read_le_raw<uint32_t>(data, size, pos));
            case physical_type_t::INT64:
                return components::types::physical_value(read_le_raw<int64_t>(data, size, pos));
            case physical_type_t::UINT64:
                return components::types::physical_value(read_le_raw<uint64_t>(data, size, pos));
            case physical_type_t::FLOAT:
                return components::types::physical_value(read_le_raw<float>(data, size, pos));
            case physical_type_t::DOUBLE:
                return components::types::physical_value(read_le_raw<double>(data, size, pos));
            case physical_type_t::STRING: {
                const auto n = read_le_raw<uint32_t>(data, size, pos);
                assert(pos + n <= size);
                components::types::physical_value pv(data + pos, static_cast<uint32_t>(n));
                pos += n;
                return pv;
            }
            default:
                assert(false && "read_logical_value_as_view: unsupported physical type");
                std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                return components::types::physical_value();
        }
    }

    inline void skip_logical_value(const char* data, size_t size, size_t& pos) {
        const auto logical = static_cast<logical_type_t>(read_le_raw<uint8_t>(data, size, pos));
        if (logical == logical_type_t::DECIMAL) {
            const auto width = read_le_raw<uint8_t>(data, size, pos);
            read_le_raw<uint8_t>(data, size, pos);
            // Only the payload WIDTH matters for a skip, so ask the storage table directly
            // instead of building a type: an out-of-window width answers INVALID and falls
            // into the default arm below, where it used to reach create_decimal's throw.
            switch (components::types::decimal_storage_for_width(width)) {
                case physical_type_t::INT16:
                    pos += sizeof(int16_t);
                    break;
                case physical_type_t::INT32:
                    pos += sizeof(int32_t);
                    break;
                case physical_type_t::INT64:
                    pos += sizeof(int64_t);
                    break;
                case physical_type_t::INT128:
                    pos += sizeof(components::types::int128_t);
                    break;
                default:
                    assert(false && "skip_logical_value: unsupported DECIMAL storage");
                    std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                    break;
            }
            return;
        }
        const auto physical = components::types::to_physical_type(logical);
        switch (physical) {
            case physical_type_t::NA:
                break;
            case physical_type_t::BOOL:
                pos += sizeof(uint8_t);
                break;
            case physical_type_t::INT8:
                pos += sizeof(int8_t);
                break;
            case physical_type_t::UINT8:
                pos += sizeof(uint8_t);
                break;
            case physical_type_t::INT16:
                pos += sizeof(int16_t);
                break;
            case physical_type_t::UINT16:
                pos += sizeof(uint16_t);
                break;
            case physical_type_t::INT32:
                pos += sizeof(int32_t);
                break;
            case physical_type_t::UINT32:
                pos += sizeof(uint32_t);
                break;
            case physical_type_t::INT64:
                pos += sizeof(int64_t);
                break;
            case physical_type_t::UINT64:
                pos += sizeof(uint64_t);
                break;
            case physical_type_t::FLOAT:
                pos += sizeof(float);
                break;
            case physical_type_t::DOUBLE:
                pos += sizeof(double);
                break;
            case physical_type_t::STRING: {
                const auto n = read_le_raw<uint32_t>(data, size, pos);
                assert(pos + n <= size);
                pos += n;
                break;
            }
            default:
                assert(false && "skip_logical_value: unsupported physical type");
                std::abort(); // NDEBUG drops the assert; without this control continues into the next case
                break;
        }
    }

    // -----------------------------------------------------------------------------
    // TYPE-DIRECTED VALUE CODEC — the form a column DEFAULT is persisted in
    // (pg_attribute.attdefspec; see components/catalog/system_table_schemas.hpp).
    //
    // Same file, same primitives and the same payload bytes as the key encoders
    // above. Two differences, both forced by what a DEFAULT is and an index key is
    // not:
    //   * a DEFAULT may be NULL, so every value carries a one-byte presence flag.
    //     Index keys are never NULL — the constraint layers skip NULL keys — so the
    //     key encoders have no place to put one;
    //   * the reader ALREADY knows the type: pg_attribute.atttypspec sits in the
    //     next column. Nothing type-describing goes into the stream — no logical
    //     tag, and no DECIMAL width/scale. That is exactly what lets a NESTED value
    //     (ARRAY / LIST / STRUCT) round-trip here without carrying a schema, which
    //     the self-describing key form cannot do.
    //
    // Nothing in this section aborts. Its input is a catalog row, so a short or
    // inconsistent payload is DATA, not a broken invariant: it is reported through
    // `ok` and the caller raises data_corruption. Symmetrically, a value whose type
    // has no encoding is reported as `false` rather than silently dropped — the
    // caller turns that into an error at CREATE TABLE / ALTER SET DEFAULT.

    // The types this codec can carry. Scalars are exactly the hashed-index key set
    // (that predicate is the single authority and gates the same payload writers);
    // nested types are carried element-wise, so they are encodable exactly when
    // every leaf is.
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

    // Raw scaled-integer width of a DECIMAL, taken from the TYPE (never from the
    // stream, which carries no width). Returns physical_type_t::INVALID for a
    // width the engine has no storage for.
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
                // No encoding for this type. Reported, never dropped (rule 6).
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
            // NULL in this engine is NA-typed (logical_value_t::is_null() IS
            // type()==NA), so a null value cannot also carry the column's type. The
            // caller keeps the type alongside — that is the whole point of a
            // type-directed codec — and "present==0" says only: this one is NULL.
            return logical_value_t(resource, components::types::complex_logical_type{logical_type_t::NA});
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
                if (!read_ok || pos + n > in.size()) {
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
