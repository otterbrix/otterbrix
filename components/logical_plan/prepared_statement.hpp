#pragma once

#include <components/types/types.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace components::logical_plan {

    // A session's prepared statement as the dispatcher keeps it (PostgreSQL 18 PreparedStatement): the body as it was
    // written, the types of its $n and the columns PREPARE described. Plain std containers: it rides messages and the
    // session context, which are copied.
    struct prepared_statement_t {
        std::string text;
        std::vector<types::complex_logical_type> parameter_types;
        std::vector<types::complex_logical_type> columns;
    };

    // What a PREPARE or a DEALLOCATE ran: the dispatcher applies it once the statement succeeded.
    struct prepared_change_t {
        enum class kind_t : uint8_t
        {
            none,
            store,
            drop,
            drop_all
        };
        kind_t kind{kind_t::none};
        std::string name;
        prepared_statement_t statement;
    };

} // namespace components::logical_plan
