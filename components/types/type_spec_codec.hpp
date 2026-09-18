#pragma once

#include "types.hpp"

#include <core/result_wrapper.hpp>

#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <vector>

namespace components::types {

    // Binary codec for a persisted complex_logical_type; lives here (not catalog) to avoid a table->catalog link cycle.
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
    // Decoding is fail-loud: any malformed byte is a data_corruption error, never a guessed type.

    // schema_error for unpersistable types (FUNCTION/LAMBDA/TABLE/USER/INVALID, or a mismatched
    // extension); `out` is left unspecified on error.
    [[nodiscard]] core::result_wrapper_t<bool> encode_type_spec(const complex_logical_type& type,
                                                                std::pmr::vector<std::byte>& out);

    // Parses exactly one spec from [data, data + size); leftover bytes are a data_corruption error.
    [[nodiscard]] core::result_wrapper_t<complex_logical_type>
    decode_type_spec(std::pmr::memory_resource* resource, const std::byte* data, uint64_t size);

} // namespace components::types
