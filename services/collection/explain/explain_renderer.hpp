#pragma once

#include <memory_resource>

#include <components/cursor/cursor.hpp>

#include "explain_plan.hpp"

namespace services::collection {

    // An EXPLAIN renderer maps the engine-neutral IR to a result cursor. A raw function pointer
    // (POD, trivially copyable → safe to fan out per-executor) — NOT std::function.
    // `analyze` is true for EXPLAIN ANALYZE (per-loop stats available on the IR nodes).
    using explain_render_fn = components::cursor::cursor_t_ptr (*)(std::pmr::memory_resource* mr,
                                                                   const explain_plan_node& root,
                                                                   bool analyze);

    // Built-in default renderer: PostgreSQL-style indented "QUERY PLAN" tree.
    components::cursor::cursor_t_ptr
    render_postgres(std::pmr::memory_resource* mr, const explain_plan_node& root, bool analyze);

} // namespace services::collection
