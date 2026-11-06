#pragma once

#include <cstdint>

namespace components::session {

    enum class transaction_status_t : uint8_t
    {
        idle,
        in_transaction,
        failed
    };

} // namespace components::session
