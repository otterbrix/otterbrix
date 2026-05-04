#pragma once

#include <components/base/collection_full_name.hpp>
#include <components/session/session.hpp>
#include <components/table/row_version_manager.hpp>
#include <core/date/date_types.hpp>

namespace components {

    struct execution_context_t {
        session::session_id_t session;
        table::transaction_data txn{0, 0};
        core::date::timezone_offset_t session_tz{};
        collection_full_name_t name;
    };

} // namespace components
