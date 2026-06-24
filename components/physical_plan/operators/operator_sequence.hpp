#pragma once

#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // The ONLY operator_sequence_t ever constructed is the childless, no-op
    // fallback at the tail of create_plan_sequence (real DDL/DML sequences are
    // flattened into left_/right_ chains there) — so it is always a no-op leaf
    // with nothing to drive. It is a trivial SINK: a sourceless sink root with no
    // left child, which execute_pipeline drives through a no-op finalize (no
    // needs_async_finalize), so it never forces a plan onto the legacy
    // on_execute path.
    class operator_sequence_t final : public read_write_operator_t {
    public:
        operator_sequence_t(std::pmr::memory_resource* resource, log_t log);
    };

} // namespace components::operators