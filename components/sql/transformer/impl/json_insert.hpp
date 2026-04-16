#pragma once

#include <components/types/logical_value.hpp>
#include <memory_resource>
#include <string>
#include <string_view>
#include <vector>

namespace components::sql::transform {

    // One leaf from a flattened JSON object. `path` is the dotted path from the root
    // (e.g. "a.b.c"). `value` is the typed leaf value. NULLs are NOT emitted — missing
    // or null JSON fields simply don't appear in the field list.
    struct json_field_t {
        std::pmr::string path;
        types::logical_value_t value;
    };

    // Parses a single JSON object string and returns its flattened leaves.
    // - Nested objects recurse with dotted paths.
    // - Arrays are serialized back to JSON text (stored as strings).
    // - Scalars are mapped: int→BIGINT, uint→UBIGINT, double→DOUBLE, bool→BOOLEAN,
    //   string→STRING_LITERAL. nulls are skipped.
    // Throws std::runtime_error on parse failure or non-object root.
    std::vector<json_field_t> flatten_json_object(std::pmr::memory_resource* resource,
                                                  std::string_view json_str);

} // namespace components::sql::transform
