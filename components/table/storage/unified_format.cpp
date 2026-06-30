#include "unified_format.hpp"

#include <components/vector/data_chunk.hpp>
#include <components/vector/vector.hpp>
#include <components/vector/vector_buffer.hpp>
#include <components/types/types.hpp>
#include <absl/crc/crc32c.h>

#include <cstring>

namespace components::table::storage {

namespace {

constexpr char MAGIC_OTSC[] = "OTSC1.0";
constexpr uint32_t VERSION = 1;
constexpr size_t HEADER_SIZE = 64;
constexpr size_t TRAILER_SIZE = 16;

// Auxiliary-blob kind discriminator written inside each column frame.
enum class aux_kind : uint8_t {
    NONE = 0,        // flat fixed-width column (INT/DOUBLE/BOOL/...)
    STRING_HEAP = 1, // STRING_LITERAL: per-row length-prefixed char blob
    LIST_CHILD = 2,  // LIST/MAP: one recursive child column frame
    ARRAY_CHILD = 3, // ARRAY: one recursive child column frame (cap = row*stride)
    STRUCT_CHILDREN = 4, // STRUCT: N recursive child column frames
};

// ===== little-endian integer helpers =========================================

template<typename T>
void write_le(std::byte* dest, T value) {
    for (size_t i = 0; i < sizeof(T); ++i) {
        dest[i] = static_cast<std::byte>(value >> (i * 8));
    }
}

template<typename T>
T read_le(const std::byte* src) {
    T value = 0;
    for (size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<T>(src[i]) << (i * 8);
    }
    return value;
}

uint64_t calculate_row_group_count(uint64_t row_count) {
    return (row_count + DEFAULT_ROW_GROUP_SIZE - 1) / DEFAULT_ROW_GROUP_SIZE;
}

inline void put_u8(std::byte*& p, uint8_t v) { write_le<uint8_t>(p, v); p += 1; }
inline void put_u16(std::byte*& p, uint16_t v) { write_le<uint16_t>(p, v); p += 2; }
inline void put_u32(std::byte*& p, uint32_t v) { write_le<uint32_t>(p, v); p += 4; }
inline void put_u64(std::byte*& p, uint64_t v) { write_le<uint64_t>(p, v); p += 8; }
inline uint8_t get_u8(const std::byte*& p) { uint8_t v = read_le<uint8_t>(p); p += 1; return v; }
inline uint16_t get_u16(const std::byte*& p) { uint16_t v = read_le<uint16_t>(p); p += 2; return v; }
inline uint32_t get_u32(const std::byte*& p) { uint32_t v = read_le<uint32_t>(p); p += 4; return v; }
inline uint64_t get_u64(const std::byte*& p) { uint64_t v = read_le<uint64_t>(p); p += 8; return v; }

// True iff at least `n` bytes remain in [p, end). Used on the read path so a
// corrupt/hostile frame can never drive a read past the end of the buffer
// (returns false -> deserialize bails with ok=false instead of an OOB read).
inline bool can_read(const std::byte* p, const std::byte* end, size_t n) {
    return p <= end && static_cast<size_t>(end - p) >= n;
}

// ===== recursive type-tree codec ============================================
// logical_type alone does not carry the child type for LIST/ARRAY/MAP or the
// field list for STRUCT (those live in the extension). We serialize the full
// type tree so the reader can rebuild a complex_logical_type whose child_type()
// / child_types() are valid -- otherwise LIST/ARRAY/STRUCT vectors would
// construct a child vector with an extension-less type and crash on the first
// child_type() dereference.

uint64_t estimate_type_bytes(const components::types::complex_logical_type& type);

uint64_t estimate_type_bytes(const components::types::complex_logical_type& type) {
    using components::types::logical_type;
    uint64_t bytes = 1; // logical_type tag
    switch (type.type()) {
        case logical_type::LIST:
            bytes += estimate_type_bytes(type.child_type());
            break;
        case logical_type::ARRAY:
            bytes += sizeof(uint64_t); // array size
            bytes += estimate_type_bytes(type.child_type());
            break;
        case logical_type::MAP: {
            const auto* ext = static_cast<const components::types::map_logical_type_extension*>(
                type.extension());
            bytes += estimate_type_bytes(ext->key());
            bytes += estimate_type_bytes(ext->value());
            break;
        }
        case logical_type::STRUCT: {
            const auto& fields = type.child_types();
            bytes += sizeof(uint32_t);
            for (const auto& field : fields) {
                bytes += sizeof(uint16_t) + field.alias().size();
                bytes += estimate_type_bytes(field);
            }
            break;
        }
        case logical_type::DECIMAL:
            bytes += 2; // width, scale
            break;
        default:
            break;
    }
    return bytes;
}

void write_type(std::byte*& ptr, const components::types::complex_logical_type& type) {
    using components::types::logical_type;
    put_u8(ptr, static_cast<uint8_t>(type.type()));
    switch (type.type()) {
        case logical_type::LIST:
            write_type(ptr, type.child_type());
            break;
        case logical_type::ARRAY:
            put_u64(ptr, static_cast<const components::types::array_logical_type_extension*>(
                              type.extension())->size());
            write_type(ptr, type.child_type());
            break;
        case logical_type::MAP: {
            const auto* ext = static_cast<const components::types::map_logical_type_extension*>(
                type.extension());
            write_type(ptr, ext->key());
            write_type(ptr, ext->value());
            break;
        }
        case logical_type::STRUCT: {
            const auto& fields = type.child_types();
            put_u32(ptr, static_cast<uint32_t>(fields.size()));
            for (const auto& field : fields) {
                const std::string& name = field.alias();
                put_u16(ptr, static_cast<uint16_t>(name.size()));
                if (!name.empty()) {
                    std::memcpy(ptr, name.data(), name.size());
                    ptr += name.size();
                }
                write_type(ptr, field);
            }
            break;
        }
        case logical_type::DECIMAL: {
            const auto* ext = static_cast<const components::types::decimal_logical_type_extension*>(
                type.extension());
            put_u8(ptr, ext->width());
            put_u8(ptr, ext->scale());
            break;
        }
        default:
            break;
    }
}

components::types::complex_logical_type
read_type(const std::byte*& ptr, std::pmr::memory_resource* resource) {
    using components::types::complex_logical_type;
    using components::types::logical_type;
    auto tag = static_cast<logical_type>(get_u8(ptr));
    switch (tag) {
        case logical_type::LIST:
            return complex_logical_type::create_list(read_type(ptr, resource));
        case logical_type::ARRAY: {
            uint64_t size = get_u64(ptr);
            return complex_logical_type::create_array(read_type(ptr, resource), size);
        }
        case logical_type::MAP: {
            auto key = read_type(ptr, resource);
            auto value = read_type(ptr, resource);
            return complex_logical_type::create_map(resource, key, value);
        }
        case logical_type::STRUCT: {
            uint32_t field_count = get_u32(ptr);
            std::pmr::vector<complex_logical_type> fields(resource);
            fields.reserve(field_count);
            for (uint32_t f = 0; f < field_count; ++f) {
                uint16_t name_len = get_u16(ptr);
                std::string name;
                if (name_len > 0) {
                    name.assign(reinterpret_cast<const char*>(ptr), name_len);
                    ptr += name_len;
                }
                auto field_type = read_type(ptr, resource);
                if (!name.empty()) {
                    field_type.set_alias(name);
                }
                fields.emplace_back(std::move(field_type));
            }
            return complex_logical_type::create_struct("struct", fields);
        }
        case logical_type::DECIMAL: {
            uint8_t width = get_u8(ptr);
            uint8_t scale = get_u8(ptr);
            return complex_logical_type::create_decimal(width, scale);
        }
        default:
            return complex_logical_type(tag);
    }
}

// A column type round-trips faithfully only if every node of its type tree is a
// case that the frame writer/reader (and write_type/read_type) handle without
// loss. Types that reach the `default` branch are NOT round-trippable:
//   - MAP/UNION/VARIANT: their physical layout (or child semantics) is not
//     reconstructed by the direct child-vector recursion below.
//   - INTERVAL/TIME_TZ (physical STRUCT): write_type/read_type do not carry the
//     fixed child layout these rely on, so a reload would lose the payload.
//   - ENUM: write_type/read_type drop the dictionary, so reloaded codes cannot
//     be resolved to names.
//   - any other (NA/UUID/BIT/...): no frame encoding exists.
// serialize_unified rejects such columns up front (ok=false) so a spill of an
// unsupported type fails loudly rather than corrupting data (R6).
bool codec_can_serialize(const components::types::complex_logical_type& type) {
    using components::types::logical_type;
    switch (type.type()) {
        case logical_type::BOOLEAN:
        case logical_type::TINYINT:
        case logical_type::UTINYINT:
        case logical_type::SMALLINT:
        case logical_type::USMALLINT:
        case logical_type::INTEGER:
        case logical_type::UINTEGER:
        case logical_type::BIGINT:
        case logical_type::UBIGINT:
        case logical_type::HUGEINT:
        case logical_type::UHUGEINT:
        case logical_type::FLOAT:
        case logical_type::DOUBLE:
        case logical_type::DATE:
        case logical_type::TIME:
        case logical_type::TIMESTAMP:
        case logical_type::TIMESTAMP_TZ:
        case logical_type::DECIMAL:
        case logical_type::STRING_LITERAL:
        case logical_type::BLOB:
            return true;
        case logical_type::LIST:
        case logical_type::ARRAY:
            return codec_can_serialize(type.child_type());
        case logical_type::STRUCT: {
            for (const auto& field : type.child_types()) {
                if (!codec_can_serialize(field)) {
                    return false;
                }
            }
            return true;
        }
        default:
            // MAP/UNION/VARIANT/INTERVAL/TIME_TZ/ENUM/unknown: not faithfully
            // serializable by this codec (see comment above).
            return false;
    }
}

// Human-readable name for a column's top-level logical type, used only to make
// the serialize_unified conversion_failure message descriptive (it names the
// offending column's type). Covers the types this codec rejects plus a numeric
// fallback so a never-before-seen tag is still reported.
const char* logical_type_name(components::types::logical_type t) {
    using components::types::logical_type;
    switch (t) {
        case logical_type::INTERVAL: return "INTERVAL";
        case logical_type::TIME_TZ:  return "TIME_TZ";
        case logical_type::ENUM:     return "ENUM";
        case logical_type::MAP:      return "MAP";
        case logical_type::UNION:    return "UNION";
        case logical_type::VARIANT:  return "VARIANT";
        case logical_type::UUID:     return "UUID";
        case logical_type::BIT:      return "BIT";
        case logical_type::NA:       return "NA";
        default:                     return "unsupported";
    }
}

// ===== recursive frame estimate / write / read ==============================
//
// A column frame is fully self-describing and length-prefixed:
//
//   [uint32 frame_body_size]               -- bytes that follow (excl. self)
//   [uint8  physical_type] [uint8 aux_kind]
//   [uint64 row_count]                     -- rows carried by THIS vector
//   [flat data: type.size() * row_count]   -- only when type.size() > 0
//   [null mask: entry_count*8 bytes]       -- one uint64 per 64 rows (bit=row)
//   [aux blob]                             -- see aux_kind
//
// STRING aux: per row [uint32 len][len chars].
// LIST/MAP aux:    [uint64 child_n][one recursive child frame of child_n elems].
//   The list_entry_t offsets/lengths live in the flat-data block above.
// ARRAY aux:       [one recursive child frame of row_count*array_size elems].
// STRUCT aux:      [N recursive child frames, one per field, each row_count rows].
//   Each child column is serialized DIRECTLY via its child vector (vec.entry() /
//   vec.entries()); every nesting level carries its own null mask, so validity is
//   reconstructed per level with no per-row value round-trip.

uint64_t estimate_vector_bytes(const components::vector::vector_t& vec, uint64_t row_count);

uint64_t estimate_string_heap(const components::vector::vector_t& vec, uint64_t row_count) {
    uint64_t bytes = 0;
    auto* slots = reinterpret_cast<const std::string_view*>(vec.data());
    for (uint64_t i = 0; i < row_count; ++i) {
        bytes += sizeof(uint32_t);
        if (vec.validity().row_is_valid(i)) {
            bytes += slots[i].size();
        }
    }
    return bytes;
}

uint64_t estimate_vector_bytes(const components::vector::vector_t& vec, uint64_t row_count) {
    uint64_t bytes = sizeof(uint32_t); // frame_body_size
    bytes += 1 + 1 + 8;                // phys, kind, row_count

    auto phys = vec.type().to_physical_type();
    bytes += vec.type().size() * row_count;          // flat data
    bytes += ((row_count + 63) / 64) * sizeof(uint64_t); // null mask

    switch (phys) {
        case components::types::physical_type::STRING:
            bytes += estimate_string_heap(vec, row_count);
            break;
        case components::types::physical_type::LIST:
            // [u64 child_n] + one recursive child frame over the populated child
            // elements (estimate_vector_bytes already charges that frame's header,
            // flat data and null mask).
            bytes += sizeof(uint64_t);
            bytes += estimate_vector_bytes(vec.entry(), vec.size());
            break;
        case components::types::physical_type::ARRAY: {
            // Fixed stride: one recursive child frame over row_count*array_size
            // dense elements.
            uint64_t array_size = static_cast<const components::types::array_logical_type_extension*>(
                                      vec.type().extension())
                                      ->size();
            bytes += estimate_vector_bytes(vec.entry(), row_count * array_size);
            break;
        }
        case components::types::physical_type::STRUCT:
            // One recursive child frame per field, each carrying row_count rows.
            for (const auto& field : vec.entries()) {
                bytes += estimate_vector_bytes(*field, row_count);
            }
            break;
        default:
            break;
    }
    return bytes;
}

void write_string_heap(std::byte*& ptr,
                       const components::vector::vector_t& vec,
                       uint64_t row_count) {
    auto* slots = reinterpret_cast<const std::string_view*>(vec.data());
    for (uint64_t i = 0; i < row_count; ++i) {
        uint32_t len = 0;
        const char* chars = "";
        if (vec.validity().row_is_valid(i)) {
            std::string_view sv = slots[i];
            len = static_cast<uint32_t>(sv.size());
            chars = sv.data();
        }
        put_u32(ptr, len);
        if (len > 0) {
            std::memcpy(ptr, chars, len);
            ptr += len;
        }
    }
}

bool write_vector_frame(std::byte*& ptr,
                        std::pmr::memory_resource* resource,
                        const components::vector::vector_t& vec,
                        uint64_t row_count) {
    std::byte* size_field = ptr;
    ptr += sizeof(uint32_t);
    std::byte* body_start = ptr;

    auto phys = vec.type().to_physical_type();
    aux_kind kind = aux_kind::NONE;
    switch (phys) {
        case components::types::physical_type::STRING: kind = aux_kind::STRING_HEAP; break;
        case components::types::physical_type::LIST:   kind = aux_kind::LIST_CHILD; break;
        case components::types::physical_type::ARRAY:  kind = aux_kind::ARRAY_CHILD; break;
        case components::types::physical_type::STRUCT: kind = aux_kind::STRUCT_CHILDREN; break;
        default: break;
    }

    put_u8(ptr, static_cast<uint8_t>(phys));
    put_u8(ptr, static_cast<uint8_t>(kind));
    put_u64(ptr, row_count);

    auto type_size = vec.type().size();
    if (type_size > 0 && vec.data() != nullptr) {
        std::memcpy(ptr, vec.data(), type_size * row_count);
        ptr += type_size * row_count;
    }

    // Null mask: one uint64 entry per 64 rows, bit i = row i validity.
    {
        uint64_t entry_count = (row_count + 63) / 64;
        size_t mask_bytes = entry_count * sizeof(uint64_t);
        std::memset(ptr, 0, mask_bytes);
        auto* out = reinterpret_cast<uint8_t*>(ptr);
        for (uint64_t i = 0; i < row_count; ++i) {
            if (vec.validity().row_is_valid(i)) {
                out[i / 8] |= static_cast<uint8_t>(uint8_t(1) << (i % 8));
            }
        }
        ptr += mask_bytes;
    }

    switch (kind) {
        case aux_kind::STRING_HEAP:
            write_string_heap(ptr, vec, row_count);
            break;
        case aux_kind::LIST_CHILD: {
            // LIST/MAP: the list_entry_t offsets/lengths are already in the
            // flat-data block; serialize the populated child elements as ONE
            // dense child frame (R1: direct child-vector recursion).
            uint64_t child_n = vec.size();
            put_u64(ptr, child_n);
            if (!write_vector_frame(ptr, resource, vec.entry(), child_n)) {
                return false;
            }
            break;
        }
        case aux_kind::ARRAY_CHILD: {
            // ARRAY: fixed stride (carried by the type), so the child holds
            // row_count*array_size dense elements -- written as one child frame.
            uint64_t array_size = static_cast<const components::types::array_logical_type_extension*>(
                                      vec.type().extension())
                                      ->size();
            if (!write_vector_frame(ptr, resource, vec.entry(), row_count * array_size)) {
                return false;
            }
            break;
        }
        case aux_kind::STRUCT_CHILDREN:
            // STRUCT: one child frame per field, each carrying row_count rows.
            for (const auto& field : vec.entries()) {
                if (!write_vector_frame(ptr, resource, *field, row_count)) {
                    return false;
                }
            }
            break;
        default:
            break;
    }

    uint32_t body_size = static_cast<uint32_t>(ptr - body_start);
    write_le<uint32_t>(size_field, body_size);
    return true;
}

// Reconstruct a vector from a frame. out_vec is already constructed with the
// correct type/capacity; we fill data_, validity_, and auxiliary_. `buffer_end`
// is the absolute end of the input so a corrupt body_size / aux length can never
// read past the buffer (returns false instead of an OOB read).
bool read_vector_frame(const std::byte*& ptr,
                       const std::byte* buffer_end,
                       std::pmr::memory_resource* resource,
                       components::types::complex_logical_type type,
                       components::vector::vector_t& out_vec,
                       uint64_t row_count) {
    // Fixed frame prefix: body_size + phys + kind + row_count.
    if (!can_read(ptr, buffer_end, sizeof(uint32_t) + 1 + 1 + sizeof(uint64_t))) {
        return false;
    }
    uint32_t body_size = get_u32(ptr);
    // The body must fit inside the remaining buffer; a corrupt (huge) body_size
    // would otherwise let later reads run off the end.
    if (!can_read(ptr, buffer_end, body_size)) {
        return false;
    }
    const std::byte* body_end = ptr + body_size;

    auto phys = static_cast<components::types::physical_type>(get_u8(ptr));
    auto kind = static_cast<aux_kind>(get_u8(ptr));
    uint64_t frame_row_count = get_u64(ptr);
    if (frame_row_count != row_count) {
        return false; // malformed
    }
    // The frame's physical type must match the column type we are rebuilding;
    // a mismatch means a corrupt/desynced frame.
    if (phys != type.to_physical_type()) {
        return false;
    }

    // Grow buffers if the frame exceeds the default capacity.
    if (row_count > components::vector::DEFAULT_VECTOR_CAPACITY) {
        out_vec.resize(components::vector::DEFAULT_VECTOR_CAPACITY, row_count);
    }

    // Flat data.
    auto type_size = type.size();
    if (type_size > 0 && out_vec.data() != nullptr) {
        if (!can_read(ptr, body_end, type_size * row_count)) {
            return false;
        }
        std::memcpy(out_vec.data(), ptr, type_size * row_count);
        ptr += type_size * row_count;
    }

    // Null mask. vector_t::validity's set_valid(row) has an off-by-entry bug
    // (it assigns to validity_mask_[row] instead of the bit), so we write the
    // raw uint64 entries directly after forcing the mask to materialize.
    {
        uint64_t entry_count = (row_count + 63) / 64;
        size_t mask_bytes = entry_count * sizeof(uint64_t);
        if (!can_read(ptr, body_end, mask_bytes)) {
            return false;
        }
        const std::byte* mask = ptr;
        ptr += mask_bytes;

        bool any_invalid = false;
        for (uint64_t i = 0; i < row_count; ++i) {
            if ((mask[i / 8] & static_cast<std::byte>(uint8_t(1) << (i % 8))) == std::byte{0}) {
                any_invalid = true;
                break;
            }
        }
        if (any_invalid) {
            out_vec.validity().set_all_invalid(row_count);
            std::memcpy(out_vec.validity().data(), mask, mask_bytes);
        }
        // else: the ctor's un-materialized (all-valid) mask is correct.
    }

    switch (kind) {
        case aux_kind::STRING_HEAP: {
            auto aux = out_vec.auxiliary();
            auto* str_aux = static_cast<components::vector::string_vector_buffer_t*>(aux.get());
            auto* slots = reinterpret_cast<std::string_view*>(out_vec.data());
            for (uint64_t i = 0; i < row_count; ++i) {
                if (!can_read(ptr, body_end, sizeof(uint32_t))) {
                    return false;
                }
                uint32_t len = get_u32(ptr);
                if (len == 0) {
                    continue; // null/empty: validity already set, slot stays default
                }
                if (!can_read(ptr, body_end, len)) {
                    return false; // corrupt length: would read past the frame body
                }
                void* inserted = str_aux->insert(
                    const_cast<void*>(static_cast<const void*>(ptr)), len);
                ptr += len;
                slots[i] = std::string_view(static_cast<const char*>(inserted), len);
            }
            break;
        }
        case aux_kind::LIST_CHILD: {
            // LIST/MAP: the list_entry_t offsets/lengths were memcpy'd into
            // out_vec.data() by the flat block above. Read the populated child
            // element count, grow the child to fit (the count can exceed
            // DEFAULT_VECTOR_CAPACITY -- a row of 1024 lists can carry far more
            // elements than rows), then fill the single child frame in place.
            if (!can_read(ptr, body_end, sizeof(uint64_t))) {
                return false;
            }
            uint64_t child_n = get_u64(ptr);
            out_vec.reserve(child_n);
            out_vec.set_list_size(child_n);
            if (!read_vector_frame(ptr, body_end, resource, type.child_type(), out_vec.entry(), child_n)) {
                return false;
            }
            break;
        }
        case aux_kind::ARRAY_CHILD: {
            // ARRAY: fixed stride; the child frame holds row_count*array_size
            // dense elements. The child was sized at construction to
            // capacity*array_size, so no reserve is needed here.
            uint64_t array_size = static_cast<const components::types::array_logical_type_extension*>(
                                      type.extension())
                                      ->size();
            if (!read_vector_frame(ptr,
                                   body_end,
                                   resource,
                                   type.child_type(),
                                   out_vec.entry(),
                                   row_count * array_size)) {
                return false;
            }
            break;
        }
        case aux_kind::STRUCT_CHILDREN: {
            // STRUCT: one child frame per field, each carrying row_count rows;
            // every field child was allocated at construction.
            const auto& field_types = type.child_types();
            auto& fields = out_vec.entries();
            if (fields.size() != field_types.size()) {
                return false;
            }
            for (size_t f = 0; f < field_types.size(); ++f) {
                if (!read_vector_frame(ptr, body_end, resource, field_types[f], *fields[f], row_count)) {
                    return false;
                }
            }
            break;
        }
        default:
            break;
    }

    ptr = body_end; // robust forward-progress even if aux layout diverged
    return true;
}

} // anonymous namespace

// ===== public API ============================================================

core::error_t serialize_unified(components::vector::data_chunk_t& chunk,
                      file_buffer_t& buffer,
                      const unified_format_header& header) {
    auto* err_resource = chunk.resource();

    // Fail loudly (R6) if any column carries a type the codec cannot faithfully
    // round-trip (MAP/UNION/VARIANT/INTERVAL/TIME_TZ/ENUM/...). Writing such a
    // column would silently drop data or desync the reader; refusing keeps the
    // write/read paths symmetric.
    {
        auto types_to_check = chunk.types();
        for (size_t ci = 0; ci < types_to_check.size(); ++ci) {
            if (!codec_can_serialize(types_to_check[ci])) {
                std::pmr::string msg(err_resource);
                msg += "serialize_unified: column ";
                msg += std::to_string(ci);
                msg += " has type ";
                msg += logical_type_name(types_to_check[ci].type());
                msg += " which the spill codec cannot faithfully round-trip";
                return core::error_t(core::error_code_t::conversion_failure, std::move(msg));
            }
        }
    }

    // Flatten each column so data()/entry()/entries() are row-dense before the
    // frame writer copies them. The flat/string/nested paths all assume FLAT
    // layout; flatten() recurses into child vectors and is a no-op for vectors
    // that are already FLAT. Doing this BEFORE estimate_unified_size keeps the
    // size estimate consistent with the bytes the write loop actually emits.
    for (auto& vec : chunk.data) {
        if (vec.get_vector_type() != components::vector::vector_type::FLAT) {
            vec.flatten(chunk.size());
        }
    }

    uint64_t required_size = estimate_unified_size(chunk, header);
    buffer.resize(required_size);

    std::byte* base_ptr = buffer.internal_buffer();
    std::byte* ptr = base_ptr;

    // ===== Header (64 bytes) =================================================
    std::memcpy(ptr, MAGIC_OTSC, 8);
    ptr += 8;

    write_le<uint32_t>(ptr, VERSION);    ptr += 4;
    write_le<uint32_t>(ptr, 0);          ptr += 4; // reserved0
    write_le<uint64_t>(ptr, header.snapshot_horizon);       ptr += 8;
    write_le<uint64_t>(ptr, header.min_visible_commit_id);  ptr += 8;
    write_le<uint64_t>(ptr, header.max_visible_commit_id);  ptr += 8;
    write_le<uint32_t>(ptr, header.table_oid);     ptr += 4;
    write_le<uint32_t>(ptr, header.column_count);  ptr += 4;
    write_le<uint64_t>(ptr, header.row_count);     ptr += 8;
    write_le<uint64_t>(ptr, header.row_group_count); ptr += 8;

    // ===== Table metadata (per column) =======================================
    // Each column's full type tree (including child types for LIST/ARRAY/
    // STRUCT/MAP) is serialized so the reader can rebuild complex_logical_types
    // whose child_type()/child_types() are valid.
    auto column_types = chunk.types();
    for (const auto& type : column_types) {
        write_type(ptr, type);
    }

    // ===== MVCC metadata per row group =======================================
    uint64_t row_count = chunk.size();
    uint64_t row_group_count = calculate_row_group_count(row_count);
    for (uint64_t rg = 0; rg < row_group_count; ++rg) {
        uint64_t row_start = rg * DEFAULT_ROW_GROUP_SIZE;
        uint64_t remaining = row_count - row_start;
        uint32_t tuple_count = static_cast<uint32_t>(
            std::min<uint64_t>(DEFAULT_ROW_GROUP_SIZE, remaining));

        write_le<uint64_t>(ptr, row_start); ptr += 8;
        write_le<uint32_t>(ptr, tuple_count); ptr += 4;
        uint32_t flags = static_cast<uint32_t>(mvcc_flags::HAS_CONSTANT_INSERT_ID) |
                         static_cast<uint32_t>(mvcc_flags::HAS_CONSTANT_DELETE_ID);
        write_le<uint32_t>(ptr, flags); ptr += 4;
        write_le<uint64_t>(ptr, header.snapshot_horizon); ptr += 8; // constant insert_id
        write_le<uint64_t>(ptr, NOT_DELETED_ID);            ptr += 8; // constant delete_id
    }

    // ===== Column data frames (recursive, self-describing) ===================
    // Each frame carries its own null mask and is length-prefixed, so the reader
    // can advance frame-by-frame with no shared null-mask region and no guessed
    // chunk sizes.
    auto* resource = chunk.resource();
    for (auto& vec : chunk.data) {
        if (!write_vector_frame(ptr, resource, vec, row_count)) {
            // An unsupported value reached the frame writer despite the
            // codec_can_serialize gate above. Surface it as an I/O-class
            // serialize failure rather than emitting a truncated buffer.
            return core::error_t(core::error_code_t::io_error,
                                 std::pmr::string("serialize_unified: failed to write column frame", err_resource));
        }
    }

    // ===== Trailer ===========================================================
    size_t total_size = static_cast<size_t>(ptr - base_ptr);
    write_le<uint64_t>(ptr, static_cast<uint64_t>(total_size));
    ptr += 8;
    auto checksum = absl::ComputeCrc32c({reinterpret_cast<const char*>(base_ptr), total_size});
    write_le<uint32_t>(ptr, static_cast<uint32_t>(checksum));
    ptr += 4;
    write_le<uint32_t>(ptr, 0); // reserved
    ptr += 4;

    buffer.size() = total_size + TRAILER_SIZE;
    return core::error_t::no_error();
}

core::result_wrapper_t<components::vector::data_chunk_t>
deserialize_unified(file_buffer_t& buffer,
                    std::pmr::memory_resource* resource,
                    unified_format_header& header) {
    const std::byte* base_ptr = buffer.internal_buffer();
    const std::byte* ptr = base_ptr;
    const std::byte* buffer_end = base_ptr + buffer.size();

    if (buffer.size() < HEADER_SIZE + TRAILER_SIZE) {
        return core::error_t(core::error_code_t::data_corruption,
                             std::pmr::string("deserialize_unified: buffer smaller than header+trailer", resource));
    }

    // ===== Header ============================================================
    char magic[8];
    std::memcpy(magic, ptr, 8);
    if (std::memcmp(magic, MAGIC_OTSC, 8) != 0) {
        return core::error_t(core::error_code_t::data_corruption,
                             std::pmr::string("deserialize_unified: bad magic (not an OTSC1.0 frame)", resource));
    }
    ptr += 8;

    uint32_t version = read_le<uint32_t>(ptr); ptr += 4;
    if (version != VERSION) {
        return core::error_t(core::error_code_t::data_corruption,
                             std::pmr::string("deserialize_unified: unsupported format version", resource));
    }
    ptr += 4; // reserved0
    header.snapshot_horizon = read_le<uint64_t>(ptr);      ptr += 8;
    header.min_visible_commit_id = read_le<uint64_t>(ptr); ptr += 8;
    header.max_visible_commit_id = read_le<uint64_t>(ptr); ptr += 8;
    header.table_oid = read_le<uint32_t>(ptr);    ptr += 4;
    header.column_count = read_le<uint32_t>(ptr); ptr += 4;
    header.row_count = read_le<uint64_t>(ptr);    ptr += 8;
    header.row_group_count = read_le<uint64_t>(ptr); ptr += 8;

    // ===== Table metadata ====================================================
    std::pmr::vector<types::complex_logical_type> column_types(resource);
    column_types.reserve(header.column_count);
    for (uint32_t i = 0; i < header.column_count; ++i) {
        if (!can_read(ptr, buffer_end, 1)) { // at least the type tag
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string("deserialize_unified: column type metadata out of bounds", resource));
        }
        column_types.emplace_back(read_type(ptr, resource));
        if (ptr > buffer_end) { // read_type recursion ran past the buffer
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string("deserialize_unified: column type tree ran past buffer", resource));
        }
    }

    // ===== MVCC metadata per row group =======================================
    // Validate the per-group tuple counts sum to the header row_count.
    uint64_t mvcc_tuple_total = 0;
    for (uint64_t rg = 0; rg < header.row_group_count; ++rg) {
        if (!can_read(ptr, buffer_end, 8 + 4 + 4)) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string("deserialize_unified: MVCC row-group metadata out of bounds", resource));
        }
        ptr += 8; // row_start
        uint32_t tuple_count = read_le<uint32_t>(ptr); ptr += 4;
        mvcc_tuple_total += tuple_count;
        uint32_t mvcc_flags_val = read_le<uint32_t>(ptr); ptr += 4;
        if (mvcc_flags_val & static_cast<uint32_t>(mvcc_flags::HAS_CONSTANT_INSERT_ID)) {
            if (!can_read(ptr, buffer_end, 8)) {
                return core::error_t(core::error_code_t::data_corruption,
                                     std::pmr::string("deserialize_unified: MVCC insert_id out of bounds", resource));
            }
            ptr += 8;
        }
        if (mvcc_flags_val & static_cast<uint32_t>(mvcc_flags::HAS_CONSTANT_DELETE_ID)) {
            if (!can_read(ptr, buffer_end, 8)) {
                return core::error_t(core::error_code_t::data_corruption,
                                     std::pmr::string("deserialize_unified: MVCC delete_id out of bounds", resource));
            }
            ptr += 8;
        }
    }
    if (mvcc_tuple_total != header.row_count) {
        return core::error_t(core::error_code_t::data_corruption,
                             std::pmr::string("deserialize_unified: MVCC tuple count does not match row_count", resource));
    }

    // ===== Column data frames ================================================
    std::vector<components::vector::vector_t> vectors;
    vectors.reserve(header.column_count);
    for (uint32_t i = 0; i < header.column_count; ++i) {
        components::vector::vector_t vec(resource, column_types[i], header.row_count);
        if (!read_vector_frame(ptr, buffer_end, resource, column_types[i], vec, header.row_count)) {
            return core::error_t(core::error_code_t::data_corruption,
                                 std::pmr::string("deserialize_unified: corrupt or out-of-bounds column frame", resource));
        }
        vectors.push_back(std::move(vec));
    }

    // ===== Trailer ===========================================================
    // After the frames, ptr sits at the trailer. Bound the untrusted total_size
    // BEFORE feeding it to ComputeCrc32c: a corrupt/huge value would otherwise
    // drive the CRC read off the end of the buffer.
    const std::byte* trailer_start = ptr;
    if (!can_read(ptr, buffer_end, TRAILER_SIZE)) {
        return core::error_t(core::error_code_t::data_corruption,
                             std::pmr::string("deserialize_unified: trailer out of bounds", resource));
    }
    uint64_t total_size = read_le<uint64_t>(ptr); ptr += 8;
    uint32_t checksum = read_le<uint32_t>(ptr);   ptr += 4;
    ptr += 4; // reserved

    // total_size is the byte offset of the trailer; it must equal where parsing
    // actually landed and must leave room for the trailer inside the buffer.
    if (total_size != static_cast<uint64_t>(trailer_start - base_ptr) ||
        total_size > buffer.size() - TRAILER_SIZE) {
        return core::error_t(core::error_code_t::data_corruption,
                             std::pmr::string("deserialize_unified: trailer total_size out of bounds", resource));
    }

    auto calculated = absl::ComputeCrc32c({reinterpret_cast<const char*>(base_ptr),
                                            static_cast<size_t>(total_size)});
    if (checksum != static_cast<uint32_t>(calculated)) {
        return core::error_t(core::error_code_t::data_corruption,
                             std::pmr::string("deserialize_unified: CRC32C checksum mismatch", resource));
    }

    components::vector::data_chunk_t chunk(resource, column_types, header.row_count);
    chunk.data = std::move(vectors);
    chunk.set_cardinality(header.row_count);

    return chunk;
}

uint64_t estimate_unified_size(components::vector::data_chunk_t& chunk,
                               const unified_format_header& header) {
    // Exact upper bound: sum the recursive per-column frame estimate (which
    // already includes flat data + null mask + auxiliary) plus the fixed
    // header/metadata/MVCC/trailer. Keeping this tight avoids handing the pmr
    // pool oversized allocations.
    uint64_t size = HEADER_SIZE;
    size += header.row_group_count * 32;         // MVCC
    size += TRAILER_SIZE;

    for (auto& vec : chunk.data) {
        size += estimate_type_bytes(vec.type());     // table metadata (type tree)
        size += estimate_vector_bytes(vec, header.row_count);
    }
    return size;
}

} // namespace components::table::storage
