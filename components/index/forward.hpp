#pragma once

#include <vector>

#include <components/cursor/cursor.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_create_index.hpp>
#include <components/types/logical_value.hpp>

namespace components::index {

    using components::logical_plan::index_type;
    using key_t = expressions::key_t;
    using components::logical_plan::keys_base_storage_t;
    using id_index = uint32_t;
    using value_t = types::logical_value_t;
    using query_t = expressions::compare_expression_ptr;
    using result_set_t = cursor::cursor_t;

} // namespace components::index