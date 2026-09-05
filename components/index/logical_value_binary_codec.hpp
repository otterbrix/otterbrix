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

    // THE index key-type list — the single authority on which logical key types an index can carry. The
    // CREATE INDEX gate (services/dispatcher/validate_logical_plan.cpp, node_type::create_index_t) refuses
    // every other type with index_create_fail BEFORE the statement executes, so no unrepresentable key
    // reaches an encoder from user data — which is what makes every `default:` arm on the ENCODE side of this
    // file, and in services::index::convert() (services/index/btree_index_disk.cpp), unreachable from user
    // data. When adding a type, extend the encoder switches FIRST and this function LAST; the other order
    // re-opens the hole the gate closed. The ordered family's decode mirror is read_logical_value_as_view()
    // here, reached through services::index::item_key_getter / id_of (services/index/btree_record_codec.hpp).
    //
    // THE DECODE SIDE GETS NO GUARANTEE FROM THE GATE, and that is the whole difference. An encoder is handed
    // a value this process built for a vetted column — EXCEPT on the paths named at encode_disk_hash_key,
    // where a value decoded off this very disk goes straight back into an encoder. A decoder is handed BYTES
    // OFF A DISK with no checksum over the key payload, so an unhandled arm there is DATA: the stored tag byte
    // is dense over about thirty of its 256 values, and one flipped bit in an ordinary BIGINT key (14) names
    // HUGEINT (15), whose physical width this codec has no reader for. Aborting on that does not make the
    // engine loud, it makes the DATABASE UNOPENABLE — in release builds too, since the process that dies is
    // the host of an embedded engine. So THERE IS NO std::abort() ANYWHERE IN THIS FILE, on either side.
    //
    // TWELVE `assert(false)` GUARDS REMAIN on the decode side — eleven in read_logical_value, one in
    // read_decimal_payload — and an assert IS an abort in a Debug build, so the claim that keeps them honest
    // has to be narrow: they guard a DERIVATION, not the input. `physical` comes from `logical` through
    // to_physical_type(), and for each of those arms exactly one logical type maps to that width, so a guard
    // can fire only if the derivation table gains an entry and this switch does not. THAT CLAIM IS CHECKED,
    // not asserted: components/index/test/test_logical_value_binary_codec.cpp walks all 256 tag bytes through
    // all three decode entry points in a Debug build, where a guard that IS reachable takes the test binary
    // down. The three arms stored bytes really do steer — INT32 (shared with ENUM), the STRING length, and the
    // outer `default:` — carry no assert at all.
    //
    // Every decode entry point takes a `bool* ok`, answers a corrupt payload with NA (or a default
    // physical_value) and reports it through that flag, which is only ever set to false — so a caller
    // initialises it to true and checks once at the end. A caller that passes no flag still gets the benign
    // value instead of a dead process, which keeps a corrupt index a BAD ANSWER TO ONE QUERY and leaves DROP
    // INDEX able to take the object off the books.
    //
    // `ordered` picks the encoder family the index kind uses: ordered (b+tree) keys additionally round-trip
    // through physical_value — services::index::convert() encodes probes, read_logical_value_as_view() decodes
    // stored keys for in-tree comparison — and physical_value carries no DECIMAL tag (width/scale would be
    // lost), so DECIMAL is refused for ordered indexes. Hashed (bitcask / disk-hash) keys use only the logical
    // codec below, which round-trips DECIMAL via append_decimal_payload, so hashed indexes accept it.
    //
    // DATE / TIME / TIMESTAMP / TIMESTAMP_TZ are physically INT32/INT64 raw counters; both encoder families
    // order and equal-compare them exactly like the column does. Refused (both kinds): HUGEINT/UHUGEINT/UUID
    // (16-byte payloads the codec has no arm for), INTERVAL/TIME_TZ (physically STRUCT), BLOB/BIT, and every
    // nested type.
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

    // `ok` reports a SHORT READ — a truncated or corrupt payload, which is data, not a bug. IT MUST NOT
    // THROW: this codec runs on every disk-index key, and an exception here unwinds into an actor coroutine
    // whose unhandled_exception() only asserts, so a corrupt key becomes a SIGABRT in Debug and a hang under
    // NDEBUG. Callers that pass no flag get T{}. THE BOUNDS TEST IS THE SAME ONE read_le_raw USES, and it has
    // to be: `pos + sizeof(T)` is a size_t addition that wraps, so a `pos` already past the end (which a
    // caller that ignored a previous refusal can hold) would answer "in range" and memcpy from `in.data() + pos`.
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

    // Answers whether the payload was written. The extension was built by create_decimal,
    // which refuses every width outside the window, so the storage is one of the four below
    // by construction — but "by construction" is not the same as "by this process": both
    // encoders that call this can be handed a DECIMAL that came off the disk a moment ago
    // (see encode_disk_hash_key), so it reports rather than aborts.
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

    // DECODE SIDE — width and scale are STORED BYTES. `read` reports its own short reads
    // through the caller's flag; this function reports an unrepresentable (width, scale)
    // through `ok` and answers NA. Neither is a bug in this file, so neither aborts.
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
        // width/scale arrive from STORED BYTES, so an out-of-window width is DATA and must not
        // throw or assert here: width 18 is 0b010010 and one flipped bit makes it 50, so an
        // ordinary NUMERIC(18,2) key would kill the process. It refuses like everything else
        // in this file.
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
                // INVARIANT, not data: create_decimal just vetted (width, scale), so the
                // storage is one of the four above. Loud in a Debug build, and still a
                // refusal in the build users ship rather than a dead process.
                assert(false && "logical value codec: unsupported DECIMAL physical storage during decode");
                return refuse();
        }
    }

    // ENCODE ENTRY POINT for the b+tree leaf record and the bitcask segment payload. `ok` has the shape every
    // other refusal in this file has and is only ever set to false. A refusal leaves `out` holding the tag
    // byte and no payload, which is NOT a usable key — the caller must not store it. The `default:` arm must
    // not abort, for the reason spelled out at encode_disk_hash_key: bitcask's merge relocation
    // (services/index/bitcask_index_disk.cpp, serialize_payload over a key that came back out of
    // read_rows_at) hands this function a value DECODED OFF THE DISK.
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
                // Our bug on the query-side callers (the CREATE INDEX gate vetted the column
                // type), DATA on the merge-relocation caller — and an assert cannot tell the
                // two apart, it can only abort for both. Same reasoning as encode_disk_hash_key
                // below: no assert, because a stored byte can reach this arm.
                refuse();
                return;
        }
    }

    // DECODE ENTRY POINT. `in` is a stored key payload — a b+tree leaf record or a bitcask
    // segment value — and neither file carries a checksum over it, so every byte read below
    // is untrusted. Nothing here aborts; see the note above is_representable_index_key_type
    // for why. A refusal answers NA and sets `*ok` to false, leaving `pos` wherever the bad
    // byte was found; the caller decides whether that fails the query or is skipped.
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
        // `physical` is DERIVED from `logical` through one table, so the arms below that check "this
        // physical width came from an unexpected logical type" are guarding a DERIVATION, not the input:
        // physical BOOL comes only from BOOLEAN, INT8 only from TINYINT, and so on. Those keep a Debug
        // assert — they fire only if the table gains an entry and this switch does not — but still refuse
        // rather than abort, because the cost of being wrong about that is a database nobody can open.
        //
        // THREE ARMS ARE NOT LIKE THAT and carry no assert at all, because ordinary stored bytes reach them:
        // INT32 is shared by INTEGER, DATE *and ENUM*; the STRING length is four bytes off the disk; and the
        // outer `default:` catches every tag the table maps to a width this codec has no reader for (INT128,
        // UINT128, STRUCT, LIST, ARRAY, BIT) plus the ~226 byte values that map to nothing at all.
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
                // DATA, not a derivation: ENUM maps to INT32 as well, and this codec has no
                // reading for it — the entry list is not carried in the key.
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
                        // Derivation: INT64 comes from these four and from DECIMAL, and DECIMAL
                        // left through the branch at the top of the function.
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
                // DATA: the length is four stored bytes, so a flipped high bit claims gigabytes
                // of a six-byte record. Written so it cannot overflow on the way to the test.
                if (n > in.size() - pos) {
                    return refuse();
                }
                std::pmr::string s(in.data() + pos, n, resource);
                pos += n;
                return logical_value_t(resource, std::move(s));
            }
            default:
                // DATA: every tag whose width this codec has no reader for, and every byte the
                // type table maps to nothing.
                return refuse();
        }
    }
    // THE HASHED FAMILY'S KEY BYTES, and the one encoder in this file that runs ON THE PATH THAT OPENS A
    // DATABASE: bitcask_index_disk_t::load_from_disk walks every segment record, deserialize_payload()s it,
    // and hands the RESULT — a value decoded off the disk — to key_bytes_for_hash() -> normalize_hash_key()
    // -> here (services/index/bitcask_index_disk.cpp, the rebuild loop and the merge relocation). So the
    // `default:` arm must not abort: "the encoder is handed a value this process built and the gate vetted"
    // is false for that caller, and being wrong about it costs a DATABASE NOBODY CAN OPEN, in release builds
    // too. It reports through `ok` instead, returning the tag byte alone — not a usable hash key, and every
    // caller reachable with disk-decoded bytes passes the flag and refuses the whole operation.
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
                // NO ASSERT HERE, unlike the derivation guards on the decode side. Those keep
                // one because no value of the stored tag byte can steer them; a stored tag byte
                // steers THIS arm, through the rebuild loop named above, and an assert(false)
                // is an abort in a Debug build.
                refuse();
                break;
        }
        return out;
    }

    // The RAW-BUFFER TWIN of read_le, and the same rule holds: `data`/`size` are one stored record, so a short
    // buffer is DATA. An assert alone would not do — it vanishes under NDEBUG, the build users ship, leaving
    // the memcpy to read PAST THE END of the record and hand the caller whatever was next in the arena. `ok`
    // reports the short read; a caller that passes no flag gets T{} and an UNMOVED `pos`, so a partly-decoded
    // record cannot walk itself further off the end.
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
    // The ordered (b+tree) family's decoder: the tree hands itself its own stored bytes
    // through this call on every comparison. Same untrusted input as read_logical_value and
    // the same contract — a corrupt record answers a NA physical_value and reports it through
    // `ok`, and nothing here aborts.
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
        // DATA: a DECIMAL tag is a legal byte that this decoder cannot honour, because
        // physical_value carries no width/scale and would silently compare the SCALED payload
        // as a plain INT64 — an assert would vanish under NDEBUG and let exactly that happen.
        // Ordered indexes refuse DECIMAL keys at CREATE INDEX, so seeing one here means the
        // bytes are not what they claim.
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
                // The returned physical_value is a VIEW into `data`. An unchecked length here
                // would not merely mis-decode: it would publish a view PAST THE END of the
                // record that every later comparison then reads through.
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
    // Steps `pos` over one stored value without decoding it — services::index::id_of uses
    // it to reach the row id that follows the key in a b+tree record. That makes a wrong
    // answer here worse than a wrong value: a `pos` left past the end of the record turns the
    // NEXT read into an out-of-bounds one. So every step is bounds-checked, `pos` never leaves
    // the record, and a corrupt payload reports through `ok` instead of aborting.
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
            // Only the payload WIDTH matters for a skip, so ask the storage table directly
            // instead of building a type: an out-of-window width answers INVALID and falls into
            // the default arm below, which refuses. Building the type instead would reach
            // create_decimal's error path over a STORED BYTE — one flipped bit turns 18 into 50.
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
    // -----------------------------------------------------------------------------
    // TYPE-DIRECTED VALUE CODEC — the form a column DEFAULT is persisted in (pg_attribute.attdefspec; see
    // components/catalog/system_table_schemas.hpp). Same primitives and payload bytes as the key encoders
    // above, with two differences forced by what a DEFAULT is and an index key is not:
    //   * a DEFAULT may be NULL, so every value carries a one-byte presence flag. Index keys are never NULL
    //     (the constraint layers skip NULL keys), so the key encoders have no place to put one;
    //   * the reader ALREADY knows the type — pg_attribute.atttypspec sits in the next column — and the
    //     payload SHAPE is derived from it, so no DECIMAL width/scale goes into the stream. That is exactly
    //     what lets a NESTED value (ARRAY / LIST / STRUCT) round-trip here without carrying a schema, which
    //     the self-describing key form cannot do. What the stream DOES carry is one logical tag byte per
    //     present value, and ONLY as a check.
    //
    // WHY THAT TAG BYTE IS THERE, AND MUST NOT BE LOST. Deriving the shape from the caller's type and storing
    // nothing about it makes a type divergence visible only when it changes the WIDTH. A BIGINT default read
    // back against an INTEGER column was caught — four bytes are left over and decode_default_spec refuses on
    // `pos != payload.size()` — but the SAME bytes read against UBIGINT, TIME, TIMESTAMP, TIMESTAMP_TZ or
    // DOUBLE decoded into a perfectly valid value of that type, silently and identically under NDEBUG. Over
    // the sixteen scalars this codec carries that accepted 50 of the 240 ordered wrong-type pairs, the loudest
    // being BIGINT/DOUBLE — a bit-pattern reinterpretation, not a relabelling. The tag closes all 50; the
    // whole matrix is walked, pair by pair, by services/dispatcher/tests/test_wave_exec_dispatcher.cpp,
    // "attdefspec_type_tag_refuses_every_wrong_type_pair".
    //
    // IT IS WRITTEN AT EVERY LEVEL, NOT JUST THE OUTERMOST, and the extra byte per leaf is what that buys: the
    // divergence a DEFAULT can carry is per-leaf. A STRUCT<BIGINT, ...> payload read against
    // STRUCT<TIMESTAMP, ...> is the same same-width swap one level down, and an outer-only tag would see two
    // STRUCTs, agree, and let the field underneath reinterpret. A NULL carries no tag at all — presence 0 ends
    // the value, and NULL is NA-typed here, so there is no type for a tag to agree with.
    //
    // THIS MAKES THE CODEC THE FIRST LINE OF DEFENCE, NOT THE ONLY ONE. The type-equality refusal in the
    // operator layer — components/catalog/alter_column_validators.cpp, validate_default_value_type, called
    // from components/physical_plan/operators/operator_alter_column_add.cpp — STILL CANNOT BE DROPPED: it
    // refuses at ALTER time, before the first catalog write, so the divergence never reaches disk and the
    // statement fails with "default value type mismatch", while the tag guarantees only that a divergence
    // which somehow DID reach disk is refused on the way back out, as data_corruption. Drop the validator and
    // a rejected statement becomes a persisted row nobody can read. The ALTER path GREW that DEFAULT-to-column
    // cast on 2026-09-05 (services/collection/executor.cpp calls the same convert_column_defaults CREATE TABLE
    // does; pinned by "alter_add_column_default_is_coerced_like_create_table" in the same test file), and the
    // refusal kept standing beside it: the cast handles a divergence an ASSIGNMENT cast can carry, the
    // validator and this tag handle the one it cannot.
    //
    // The tag costs one byte per present value in the persisted attdefspec bytes; the format carries no
    // version and no length that is computed anywhere else, so nothing outside this file needed adjusting.
    //
    // Nothing in this section aborts. Its input is a catalog row, so a short or inconsistent payload is DATA,
    // not a broken invariant: it is reported through `ok` and the caller raises data_corruption. A value whose
    // type has no encoding is likewise reported as `false` rather than silently dropped — the caller turns
    // that into an error at CREATE TABLE / ALTER SET DEFAULT.

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
        // The value's own logical tag, written for EVERY present value and at EVERY
        // nesting level. It is a CHECK and never a source: read_typed_value compares it
        // and decodes nothing from it, so the payload shape still comes from the type the
        // caller holds. logical_type is a uint8_t enum, so the cast loses nothing.
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
            // type-directed codec — and "present==0" says only: this one is NULL. A NULL
            // therefore carries NO tag, and must not: NA is not a type to agree with.
            return logical_value_t(resource, components::types::complex_logical_type{logical_type_t::NA});
        }
        // The tag the writer stored, against the type this read was handed. Compared
        // BEFORE a single payload byte is touched, so a divergence costs nothing and can
        // never half-consume the stream. THIS is what catches a SAME-WIDTH divergence;
        // the caller's `pos != payload.size()` can only ever see a WIDTH one.
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
