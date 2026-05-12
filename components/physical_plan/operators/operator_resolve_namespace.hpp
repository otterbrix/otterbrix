#pragma once

#include <components/physical_plan/operators/operator.hpp>

#include <string>

namespace components::operators {

    // Phase 13 T2 — leaf operator that scans pg_namespace by nspname and emits
    // the resolved namespace_oid as a single-row data_chunk.
    //
    // Output chunk schema:
    //   col 0: UINTEGER  — namespace_oid (oid_t / uint32_t). One row when
    //                       the namespace exists; zero rows when it doesn't.
    //
    // The actual storage scan is performed by manager_disk_t::read_rows_by_key
    // (pure storage primitive — no catalog_view shortcut), so this operator
    // composes cleanly into pipelines that resolve names without going through
    // the dispatcher's cached catalog snapshot.
    class operator_resolve_namespace_t final : public read_write_operator_t {
    public:
        operator_resolve_namespace_t(std::pmr::memory_resource* resource,
                                      log_t                       log,
                                      std::string                 name);

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::string name_;
    };

} // namespace components::operators
