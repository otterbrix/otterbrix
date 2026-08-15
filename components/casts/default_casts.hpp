#pragma once

#include <components/casts/cast_registry.hpp>

namespace components::casts {

    // Populates a registry with the built-in casts.
    // Does not check whether registry is already initialized
    void register_default_casts(cast_registry_t& registry);

} // namespace components::casts