#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace components::table {

    enum class table_constraint_type : uint8_t {
        PRIMARY_KEY,
        UNIQUE,
        CHECK
    };

    struct table_constraint_t {
        table_constraint_type type;
        std::vector<std::string> columns;
        std::string check_expression; // only for CHECK (raw SQL text)
    };

} // namespace components::table
