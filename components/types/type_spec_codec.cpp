#include "type_spec_codec.hpp"

#include "logical_value.hpp"

#include <cstring>
#include <string>

namespace components::types {

    namespace {

        constexpr uint8_t FLAG_HAS_ALIAS = 0x01;
        constexpr uint8_t FLAG_KNOWN_MASK = FLAG_HAS_ALIAS;

        // Nesting deeper than this cannot come from a legitimate column type; a corrupt
        // buffer could otherwise drive unbounded recursion (stack overflow instead of an
        // error). Matches nothing the SQL surface can produce — real types are < 10 deep.
        //
        // THE WINDOW IS SHARED. encode_one carries the same `depth` counter and applies
        // the same predicate, because a writer that accepts more than the reader does
        // manufactures a file that cannot be opened. Nesting is not hypothetical: every
        // `CREATE TYPE t_n AS (a t_{n-1})` INLINES t_{n-1} whole, so a chain of CREATE
        // TYPE statements walks the depth up one level at a time until a checkpoint
        // succeeds and the next load fails forever.
        constexpr uint32_t MAX_SPEC_DEPTH = 64;

        // ---- encode helpers -------------------------------------------------------

        void put_bytes(std::pmr::vector<std::byte>& out, const void* data, size_t size) {
            const auto* p = static_cast<const std::byte*>(data);
            out.insert(out.end(), p, p + size);
        }

        void put_u8(std::pmr::vector<std::byte>& out, uint8_t v) { out.push_back(std::byte{v}); }

        template<typename T>
        void put_pod(std::pmr::vector<std::byte>& out, T v) {
            static_assert(std::is_trivially_copyable_v<T>);
            put_bytes(out, &v, sizeof(T));
        }

        void put_str(std::pmr::vector<std::byte>& out, const std::string& s) {
            put_pod<uint32_t>(out, static_cast<uint32_t>(s.size()));
            put_bytes(out, s.data(), s.size());
        }

        core::error_t encode_error(std::pmr::memory_resource* resource, const char* what) {
            return core::error_t(core::error_code_t::schema_error, std::pmr::string(what, resource));
        }

        // ---- decode helpers -------------------------------------------------------

        struct spec_reader_t {
            const std::byte* pos;
            const std::byte* end;
            bool ok{true};

            uint64_t remaining() const { return static_cast<uint64_t>(end - pos); }

            template<typename T>
            T pod() {
                static_assert(std::is_trivially_copyable_v<T>);
                T v{};
                if (!ok || remaining() < sizeof(T)) {
                    ok = false;
                    return v;
                }
                std::memcpy(&v, pos, sizeof(T));
                pos += sizeof(T);
                return v;
            }

            uint8_t u8() { return pod<uint8_t>(); }

            std::string str() {
                auto len = pod<uint32_t>();
                if (!ok || remaining() < len) {
                    ok = false;
                    return {};
                }
                std::string s(reinterpret_cast<const char*>(pos), len);
                pos += len;
                return s;
            }
        };

        core::error_t corrupt(std::pmr::memory_resource* resource, const char* what) {
            return core::error_t(core::error_code_t::data_corruption, std::pmr::string(what, resource));
        }

        bool is_plain_scalar(logical_type t) {
            switch (t) {
                case logical_type::NA:
                case logical_type::ANY:
                case logical_type::BOOLEAN:
                case logical_type::TINYINT:
                case logical_type::SMALLINT:
                case logical_type::INTEGER:
                case logical_type::BIGINT:
                case logical_type::HUGEINT:
                case logical_type::DATE:
                case logical_type::TIME:
                case logical_type::TIME_TZ:
                case logical_type::TIMESTAMP:
                case logical_type::TIMESTAMP_TZ:
                case logical_type::INTERVAL:
                case logical_type::FLOAT:
                case logical_type::DOUBLE:
                case logical_type::BLOB:
                case logical_type::UTINYINT:
                case logical_type::USMALLINT:
                case logical_type::UINTEGER:
                case logical_type::UBIGINT:
                case logical_type::UHUGEINT:
                case logical_type::BIT:
                case logical_type::STRING_LITERAL:
                case logical_type::INTEGER_LITERAL:
                case logical_type::POINTER:
                case logical_type::VALIDITY:
                case logical_type::UUID:
                    return true;
                default:
                    return false;
            }
        }

        // Fetches the extension and verifies it is the kind the logical type implies. A
        // mismatch (e.g. a DECIMAL whose extension is GENERIC because set_alias ran on a
        // bare type) is exactly the in-memory corruption this codec exists to prevent on
        // disk — refuse to persist it.
        const logical_type_extension* checked_extension(const complex_logical_type& t,
                                                        logical_type_extension::extension_type expected) {
            const auto* ext = t.extension();
            if (ext == nullptr || ext->type() != expected) {
                return nullptr;
            }
            return ext;
        }

        core::result_wrapper_t<bool>
        encode_one(const complex_logical_type& type, std::pmr::vector<std::byte>& out, uint32_t depth) {
            auto* resource = out.get_allocator().resource();

            // The write side validates EXACTLY the window the read side accepts. Refusing
            // here costs a failed DDL or a failed checkpoint, both of which leave the
            // database open; letting the bytes through costs a database that never opens
            // again. Same predicate, same constant, same depth accounting as decode_one.
            if (depth > MAX_SPEC_DEPTH) {
                return encode_error(resource, "type spec encode: nesting exceeds the format depth limit");
            }

            put_u8(out, static_cast<uint8_t>(type.type()));
            const bool aliased = type.has_alias();
            put_u8(out, aliased ? FLAG_HAS_ALIAS : 0);
            if (aliased) {
                put_str(out, type.alias());
            }

            if (is_plain_scalar(type.type())) {
                return true;
            }

            switch (type.type()) {
                case logical_type::DECIMAL: {
                    const auto* ext = checked_extension(type, logical_type_extension::extension_type::DECIMAL);
                    if (ext == nullptr) {
                        return encode_error(resource, "type spec encode: DECIMAL without a decimal extension");
                    }
                    const auto* dec = static_cast<const decimal_logical_type_extension*>(ext);
                    // Mirror of the decode-side window check below, and the reason it can
                    // never drift: both call is_valid_decimal_spec. create_decimal refuses
                    // an out-of-window pair outright, so reaching this is already an
                    // in-memory type nobody could have built through the factory — refuse
                    // to make it durable rather than write bytes decode_one will reject.
                    if (!is_valid_decimal_spec(dec->width(), dec->scale())) {
                        return encode_error(resource, "type spec encode: DECIMAL width/scale out of range");
                    }
                    put_u8(out, dec->width());
                    put_u8(out, dec->scale());
                    return true;
                }
                case logical_type::LIST: {
                    const auto* ext = checked_extension(type, logical_type_extension::extension_type::LIST);
                    if (ext == nullptr) {
                        return encode_error(resource, "type spec encode: LIST without a list extension");
                    }
                    const auto* list = static_cast<const list_logical_type_extension*>(ext);
                    put_pod<uint64_t>(out, list->field_id());
                    put_u8(out, list->required() ? 1 : 0);
                    return encode_one(list->node(), out, depth + 1);
                }
                case logical_type::ARRAY: {
                    const auto* ext = checked_extension(type, logical_type_extension::extension_type::ARRAY);
                    if (ext == nullptr) {
                        return encode_error(resource, "type spec encode: ARRAY without an array extension");
                    }
                    const auto* arr = static_cast<const array_logical_type_extension*>(ext);
                    put_pod<uint64_t>(out, static_cast<uint64_t>(arr->size()));
                    return encode_one(arr->internal_type(), out, depth + 1);
                }
                case logical_type::MAP: {
                    const auto* ext = checked_extension(type, logical_type_extension::extension_type::MAP);
                    if (ext == nullptr) {
                        return encode_error(resource, "type spec encode: MAP without a map extension");
                    }
                    const auto* map = static_cast<const map_logical_type_extension*>(ext);
                    put_pod<uint64_t>(out, map->key_id());
                    put_pod<uint64_t>(out, map->value_id());
                    put_u8(out, map->value_required() ? 1 : 0);
                    auto key_res = encode_one(map->key(), out, depth + 1);
                    if (key_res.has_error()) {
                        return key_res;
                    }
                    return encode_one(map->value(), out, depth + 1);
                }
                case logical_type::STRUCT: {
                    const auto* ext = checked_extension(type, logical_type_extension::extension_type::STRUCT);
                    if (ext == nullptr) {
                        return encode_error(resource, "type spec encode: STRUCT without a struct extension");
                    }
                    const auto* strct = static_cast<const struct_logical_type_extension*>(ext);
                    put_str(out, strct->type_name());
                    const auto& fields = strct->child_types();
                    put_pod<uint32_t>(out, static_cast<uint32_t>(fields.size()));
                    for (const auto& field : fields) {
                        auto field_res = encode_one(field, out, depth + 1);
                        if (field_res.has_error()) {
                            return field_res;
                        }
                    }
                    return true;
                }
                case logical_type::UNION: {
                    // UNION reuses struct_logical_type_extension; child 0 is the hidden
                    // UTINYINT tag create_union() prepends — it is NOT persisted.
                    const auto* ext = checked_extension(type, logical_type_extension::extension_type::STRUCT);
                    if (ext == nullptr) {
                        return encode_error(resource, "type spec encode: UNION without a struct extension");
                    }
                    const auto* strct = static_cast<const struct_logical_type_extension*>(ext);
                    const auto& children = strct->child_types();
                    if (children.empty() || children.front().type() != logical_type::UTINYINT) {
                        return encode_error(resource, "type spec encode: UNION without the hidden tag child");
                    }
                    put_pod<uint32_t>(out, static_cast<uint32_t>(children.size() - 1));
                    for (size_t i = 1; i < children.size(); ++i) {
                        auto member_res = encode_one(children[i], out, depth + 1);
                        if (member_res.has_error()) {
                            return member_res;
                        }
                    }
                    return true;
                }
                case logical_type::ENUM: {
                    const auto* ext = checked_extension(type, logical_type_extension::extension_type::ENUM);
                    if (ext == nullptr) {
                        return encode_error(resource, "type spec encode: ENUM without an enum extension");
                    }
                    const auto* enm = static_cast<const enum_logical_type_extension*>(ext);
                    put_str(out, enm->type_name());
                    const auto& entries = enm->entries();
                    put_pod<uint32_t>(out, static_cast<uint32_t>(entries.size()));
                    for (const auto& entry : entries) {
                        const auto& entry_type = entry.type();
                        put_str(out, entry_type.has_alias() ? entry_type.alias() : std::string{});
                        put_pod<int32_t>(out, entry.value<int32_t>());
                    }
                    return true;
                }
                case logical_type::VARIANT: {
                    // The internal struct layout is fixed; create_variant() rebuilds it
                    // bit-identically on decode, so no payload is stored.
                    if (checked_extension(type, logical_type_extension::extension_type::STRUCT) == nullptr) {
                        return encode_error(resource, "type spec encode: VARIANT without its internal struct");
                    }
                    return true;
                }
                case logical_type::UNKNOWN: {
                    const auto* ext = checked_extension(type, logical_type_extension::extension_type::UNKNOWN);
                    put_u8(out, ext != nullptr ? 1 : 0);
                    if (ext != nullptr) {
                        put_str(out, static_cast<const unknown_logical_type_extension*>(ext)->type_name());
                    }
                    return true;
                }
                default:
                    // USER / TABLE / FUNCTION / LAMBDA / INVALID and anything future: these
                    // never describe stored data — refusing beats inventing a byte for them.
                    return encode_error(resource, "type spec encode: type cannot be persisted");
            }
        }

        core::result_wrapper_t<complex_logical_type>
        decode_one(std::pmr::memory_resource* resource, spec_reader_t& in, uint32_t depth) {
            if (depth > MAX_SPEC_DEPTH) {
                return corrupt(resource, "type spec decode: nesting exceeds the format depth limit");
            }

            const auto raw_type = in.u8();
            const auto flags = in.u8();
            if (!in.ok) {
                return corrupt(resource, "type spec decode: truncated spec header");
            }
            if ((flags & ~FLAG_KNOWN_MASK) != 0) {
                return corrupt(resource, "type spec decode: unknown flag bits");
            }

            std::string alias;
            if ((flags & FLAG_HAS_ALIAS) != 0) {
                alias = in.str();
                if (!in.ok) {
                    return corrupt(resource, "type spec decode: truncated alias");
                }
                if (alias.empty()) {
                    return corrupt(resource, "type spec decode: alias flag set but alias empty");
                }
            }

            const auto type = static_cast<logical_type>(raw_type);
            if (is_plain_scalar(type)) {
                return complex_logical_type(type, std::move(alias));
            }

            switch (type) {
                case logical_type::DECIMAL: {
                    const auto width = in.u8();
                    const auto scale = in.u8();
                    if (!in.ok) {
                        return corrupt(resource, "type spec decode: truncated DECIMAL payload");
                    }
                    // Validate BEFORE create_decimal so the failure is reported as what it
                    // is — corrupt DISK BYTES, not a bad argument. create_decimal refuses
                    // the same pair; this arm only decides which error code the caller sees.
                    if (!is_valid_decimal_spec(width, scale)) {
                        return corrupt(resource, "type spec decode: DECIMAL width/scale out of range");
                    }
                    return complex_logical_type::create_decimal(width, scale, std::move(alias));
                }
                case logical_type::LIST: {
                    const auto field_id = in.pod<uint64_t>();
                    const auto required = in.u8();
                    if (!in.ok || required > 1) {
                        return corrupt(resource, "type spec decode: bad LIST payload");
                    }
                    auto child = decode_one(resource, in, depth + 1);
                    if (child.has_error()) {
                        return child;
                    }
                    return complex_logical_type(
                        logical_type::LIST,
                        std::make_unique<list_logical_type_extension>(field_id, std::move(child.value()), required != 0),
                        std::move(alias));
                }
                case logical_type::ARRAY: {
                    const auto size = in.pod<uint64_t>();
                    if (!in.ok) {
                        return corrupt(resource, "type spec decode: truncated ARRAY payload");
                    }
                    auto child = decode_one(resource, in, depth + 1);
                    if (child.has_error()) {
                        return child;
                    }
                    return complex_logical_type::create_array(child.value(), size, std::move(alias));
                }
                case logical_type::MAP: {
                    const auto key_id = in.pod<uint64_t>();
                    const auto value_id = in.pod<uint64_t>();
                    const auto value_required = in.u8();
                    if (!in.ok || value_required > 1) {
                        return corrupt(resource, "type spec decode: bad MAP payload");
                    }
                    auto key = decode_one(resource, in, depth + 1);
                    if (key.has_error()) {
                        return key;
                    }
                    auto value = decode_one(resource, in, depth + 1);
                    if (value.has_error()) {
                        return value;
                    }
                    return complex_logical_type(logical_type::MAP,
                                                std::make_unique<map_logical_type_extension>(resource,
                                                                                             key_id,
                                                                                             key.value(),
                                                                                             value_id,
                                                                                             value.value(),
                                                                                             value_required != 0),
                                                std::move(alias));
                }
                case logical_type::STRUCT: {
                    auto type_name = in.str();
                    const auto field_count = in.pod<uint32_t>();
                    if (!in.ok) {
                        return corrupt(resource, "type spec decode: truncated STRUCT payload");
                    }
                    // Each field spec is >= 2 bytes; a count beyond that is a lying header.
                    if (field_count > in.remaining() / 2) {
                        return corrupt(resource, "type spec decode: STRUCT field count exceeds buffer");
                    }
                    std::pmr::vector<complex_logical_type> fields(resource);
                    fields.reserve(field_count);
                    for (uint32_t i = 0; i < field_count; ++i) {
                        auto field = decode_one(resource, in, depth + 1);
                        if (field.has_error()) {
                            return field;
                        }
                        fields.push_back(std::move(field.value()));
                    }
                    return complex_logical_type::create_struct(std::move(type_name), fields, std::move(alias));
                }
                case logical_type::UNION: {
                    const auto member_count = in.pod<uint32_t>();
                    if (!in.ok) {
                        return corrupt(resource, "type spec decode: truncated UNION payload");
                    }
                    if (member_count > in.remaining() / 2) {
                        return corrupt(resource, "type spec decode: UNION member count exceeds buffer");
                    }
                    std::pmr::vector<complex_logical_type> members(resource);
                    members.reserve(member_count);
                    for (uint32_t i = 0; i < member_count; ++i) {
                        auto member = decode_one(resource, in, depth + 1);
                        if (member.has_error()) {
                            return member;
                        }
                        members.push_back(std::move(member.value()));
                    }
                    return complex_logical_type::create_union(std::move(members), std::move(alias));
                }
                case logical_type::ENUM: {
                    auto type_name = in.str();
                    const auto entry_count = in.pod<uint32_t>();
                    if (!in.ok) {
                        return corrupt(resource, "type spec decode: truncated ENUM payload");
                    }
                    // Each entry is >= 8 bytes (empty label + i32).
                    if (entry_count > in.remaining() / 8) {
                        return corrupt(resource, "type spec decode: ENUM entry count exceeds buffer");
                    }
                    std::vector<logical_value_t> entries;
                    entries.reserve(entry_count);
                    for (uint32_t i = 0; i < entry_count; ++i) {
                        auto label = in.str();
                        const auto value = in.pod<int32_t>();
                        if (!in.ok) {
                            return corrupt(resource, "type spec decode: truncated ENUM entry");
                        }
                        logical_value_t entry(resource, value);
                        entry.set_alias(label);
                        entries.push_back(std::move(entry));
                    }
                    return complex_logical_type::create_enum(std::move(type_name), std::move(entries), std::move(alias));
                }
                case logical_type::VARIANT:
                    return complex_logical_type::create_variant(resource, std::move(alias));
                case logical_type::UNKNOWN: {
                    const auto has_type_name = in.u8();
                    if (!in.ok || has_type_name > 1) {
                        return corrupt(resource, "type spec decode: bad UNKNOWN payload");
                    }
                    if (has_type_name == 0) {
                        return complex_logical_type(logical_type::UNKNOWN, std::move(alias));
                    }
                    auto type_name = in.str();
                    if (!in.ok) {
                        return corrupt(resource, "type spec decode: truncated UNKNOWN type name");
                    }
                    return complex_logical_type::create_unknown(std::move(type_name), std::move(alias));
                }
                default:
                    return corrupt(resource, "type spec decode: unrecognized logical_type byte");
            }
        }

    } // namespace

    core::result_wrapper_t<bool> encode_type_spec(const complex_logical_type& type, std::pmr::vector<std::byte>& out) {
        return encode_one(type, out, 0);
    }

    core::result_wrapper_t<complex_logical_type>
    decode_type_spec(std::pmr::memory_resource* resource, const std::byte* data, uint64_t size) {
        spec_reader_t reader{data, data + size};
        auto result = decode_one(resource, reader, 0);
        if (result.has_error()) {
            return result;
        }
        if (reader.pos != reader.end) {
            return corrupt(resource, "type spec decode: trailing bytes after the type spec");
        }
        return result;
    }

} // namespace components::types
