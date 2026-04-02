#pragma once

#include <cstdint>

namespace services::wal {

    enum class wal_sync_mode : uint8_t {
        OFF = 0,    // no fsync, no disk write
        NORMAL = 1, // fsync at checkpoint only
        FULL = 2    // fsync at every commit
    };

} // namespace services::wal
