#pragma once

#include "types.hpp"

#include <core/result_wrapper.hpp>

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <vector>

namespace components::types {

    // Binary codec for a FULL complex_logical_type spec — the persistent form of a column
    // type. Round-trips everything a bare one-byte logical_type tag loses: DECIMAL
    // width/scale, LIST/ARRAY/MAP/STRUCT/UNION child types, ENUM entries, and every alias
    // (recursively). Lives in components::types so components::table can persist column
    // types without linking otterbrix::catalog (that link would be a dependency cycle).
    //
    // Byte layout (native-endian like the rest of the metadata stream, recursive):
    //   spec := u8  logical_type            // types::logical_type numeric value
    //           u8  flags                   // bit0: has_alias; other bits must be 0
    //           [str alias]                 // present iff bit0; str := u32 length + bytes
    //           payload(logical_type)
    //   payload:
    //     DECIMAL : u8 width, u8 scale
    //     LIST    : u64 field_id, u8 required, spec child
    //     ARRAY   : u64 size, spec child
    //     MAP     : u64 key_id, u64 value_id, u8 value_required, spec key, spec value
    //     STRUCT  : str type_name, u32 field_count, field_count * spec
    //               (field aliases ride inside each field's own spec)
    //     UNION   : u32 member_count, member_count * spec
    //               (the hidden UTINYINT tag is NOT stored; create_union re-adds it)
    //     ENUM    : str type_name, u32 entry_count, entry_count * (str label, i32 value)
    //     UNKNOWN : u8 has_type_name, [str type_name]
    //     VARIANT : (none — the fixed internal struct is rebuilt by create_variant)
    //     scalars : (none)
    //
    // Decoding is fail-loud (rule 6): an unrecognized logical_type byte, unknown flag
    // bits, a truncated buffer, an invalid DECIMAL width/scale, over-deep nesting or
    // trailing bytes are data_corruption errors — never a guessed type.

    // Appends the spec for `type` to `out`. Fails (schema_error) for types that cannot be
    // persisted — FUNCTION/LAMBDA/TABLE/USER/INVALID, or a composite type whose extension
    // is missing/mismatched; `out` is left in an unspecified state on error.
    //
    // THE WRITER VALIDATES THE READER'S WINDOW. Every value-range refusal decode_type_spec
    // makes has its mirror here — the DECIMAL width/scale window (is_valid_decimal_spec)
    // and the nesting depth limit — because a refusal on the way IN costs a failed
    // statement or a failed checkpoint, while bytes that only the reader refuses cost a
    // database that never opens again. The remaining decode refusals are structural
    // (truncation, lying counts, trailing bytes, unknown flag bits): a stream this encoder
    // produced cannot exhibit them, so they need no mirror.
    [[nodiscard]] core::result_wrapper_t<bool> encode_type_spec(const complex_logical_type& type,
                                                                std::pmr::vector<std::byte>& out);

    // Parses exactly one spec from [data, data + size); consuming less than the whole
    // buffer is a data_corruption error.
    [[nodiscard]] core::result_wrapper_t<complex_logical_type>
    decode_type_spec(std::pmr::memory_resource* resource, const std::byte* data, uint64_t size);

} // namespace components::types
