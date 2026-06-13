#pragma once

#include <components/table/column_definition.hpp>
#include <components/cursor/cursor.hpp>

#include <core/types/string.hpp>
#include <core/types/vector.hpp>

namespace otterbrix {
        
    string Show(components::cursor::cursor_t_ptr cursor,
            const vector<components::table::column_definition_t>& col_defs);

} // namespace otterbrix

