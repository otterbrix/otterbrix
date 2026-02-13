#pragma once

#include <components/vector/vector.hpp>

namespace components::operators::aggregate::impl {

    types::logical_value_t sum(const vector::vector_t& v, size_t count);
    types::logical_value_t min(const vector::vector_t& v, size_t count);
    types::logical_value_t max(const vector::vector_t& v, size_t count);

} // namespace components::operators::aggregate::impl