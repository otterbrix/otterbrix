#pragma once

#include "types.hpp"

#include <functional>
#include <string_view>

namespace components::types {

    // Walk a complex_logical_type and visit every embedded UDT reference
    // (STRUCT / ENUM / UNKNOWN with non-empty type_name). Recurses into
    // STRUCT children; ENUM stores entries in its own extension and is not
    // recursed into; ARRAY / LIST / MAP carry singular child_type and are
    // not recursed (see commented note inside the impl for the memory
    // safety reason — child_types() does an unchecked cast on primitives).
    inline void walk_user_type_refs(const complex_logical_type& type,
                                    const std::function<void(std::string_view)>& visit) {
        if (type.type() == logical_type::STRUCT || type.type() == logical_type::ENUM ||
            type.type() == logical_type::UNKNOWN) {
            auto name = type.type_name();
            if (!name.empty()) {
                visit(name);
            }
        }
        if (type.type() == logical_type::STRUCT) {
            for (const auto& child : type.child_types()) {
                walk_user_type_refs(child, visit);
            }
        }
    }

} // namespace components::types