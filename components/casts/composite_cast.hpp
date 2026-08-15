#pragma once

#include <components/casts/cast_entry.hpp>
#include <components/casts/cast_function.hpp>

#include <optional>

namespace components::casts {

    class cast_registry_t;

    [[nodiscard]] cast_t leaf_closure(cast_function_t fn);

    // Builds a cast for a possibly-nested type pair -- ARRAY / LIST / MAP / STRUCT containers over leaves
    //! This only BUILDS: whether the pair is reachable at all, and at what coercion level, is
    //! cast_registry_t::lookup()'s decision. Callers go through resolve(), never here directly.
    [[nodiscard]] std::optional<cast_t> build_cast(const cast_registry_t& registry,
                                                   const types::complex_logical_type& source,
                                                   const types::complex_logical_type& target,
                                                   cast_type allowed);

} // namespace components::casts
