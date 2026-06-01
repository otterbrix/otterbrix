#include "box_render.hpp"

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
        while (idx < 100) {//cursor->has_next()) {
            auto value = cursor->next();
            Table::Row_t row;
            
            for (const auto& col_def : col_defs) {
                const auto& json_pointer = col_def.name();
            	switch (col_def.type().type()) {
            	case logical_type::BOOLEAN:
            		row.push_back(to_string(value->get_bool(json_pointer))); 
                    break;
            	case logical_type::TINYINT:
            		row.push_back(to_string(value->get_tinyint(json_pointer)));
                    break;
            	case logical_type::SMALLINT:
            		row.push_back(to_string(value->get_smallint(json_pointer)));
                    break;
            	case logical_type::INTEGER:
            		row.push_back(to_string(value->get_int(json_pointer)));
                    break;
            	case logical_type::BIGINT:
            		row.push_back(to_string(value->get_long(json_pointer)));
                    break;
            	case logical_type::UTINYINT:
            		row.push_back(to_string(value->get_utinyint(json_pointer)));
                    break;
            	case logical_type::USMALLINT:
            		row.push_back(to_string(value->get_usmallint(json_pointer)));
                    break;
            	case logical_type::UINTEGER:
            		row.push_back(to_string(value->get_uint(json_pointer)));
                    break;
                case logical_type::UBIGINT:
            		row.push_back(to_string(value->get_ulong(json_pointer)));
                    break;
            	case logical_type::FLOAT:
            		row.push_back(to_string(value->get_float(json_pointer)));
                    break;
            	case logical_type::DOUBLE:
            		row.push_back(to_string(value->get_double(json_pointer)));
                    break;
                default:
                    throw std::runtime_error("Could\'t convert document::value to box cell");
                }
            }
            
            box.add_row(row);
//            box.column(idx+1).format().font_align(FontAlign::right);
            idx++;
        }
        return box.str();
    }
} // namespace otterbrix
