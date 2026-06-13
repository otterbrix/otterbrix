#pragma once

#include <components/table/column_definition.hpp>
#include <components/cursor/cursor.hpp>
#include <string>
#include <vector>


namespace otterbrix {
        
    std::string Show(components::cursor::cursor_t_ptr cursor,
            const std::vector<components::table::column_definition_t>& col_defs);

} // namespace otterbrix

