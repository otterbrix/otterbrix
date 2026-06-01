#include "box_render.hpp"

#include <util/util.hpp>
#include <components/types/types.hpp>
#include <tabulate/table.hpp>

#include <stdexcept>


namespace otterbrix {

    using namespace tabulate;
    using components::types::logical_type;

    string Show(components::cursor::cursor_t_ptr cursor,
            const vector<components::table::column_definition_t>& col_defs) {
        Table box;
        Table::Row_t head;
        for (const auto& col_def : col_defs) {
            head.push_back(col_def.name());
        }
        box.add_row(head);
        box.column(0).format().font_align(FontAlign::center);

        int idx = 0;
        while (idx < 100 && cursor->has_next()) {
            cursor->advance();
            Table::Row_t row;

            for (idx_t col = 0; col < col_defs.size(); col++) {
                auto val = cursor->value(col);
                row.push_back(util::LogicalValueToString(val));
            }

            box.add_row(row);
            idx++;
        }
        return box.str();
    }
} // namespace otterbrix
