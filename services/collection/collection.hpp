#pragma once

#include <memory>
#include <unordered_map>

#include <core/pmr.hpp>

#include <components/cursor/cursor.hpp>
#include <components/log/log.hpp>
#include <components/session/session.hpp>

#include "forward.hpp"

namespace services::collection {

    using cursor_storage_t = std::pmr::unordered_map<session_id_t, components::cursor::cursor_t>;

    namespace executor {
        class executor_t;
    }

} // namespace services::collection
