#pragma once

#include <components/catalog/results/ddl_result.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <string>

namespace components::operators {

#ifdef DEV_MODE
    // Test seam: while armed, the pg_proc/pg_depend purge refuses as an unreadable catalog would.
    void dev_set_unregister_udf_purge_refusal(bool refuse) noexcept;
#endif

    // Operator implementation of manager_dispatcher_t::unregister_udf: drops the pg_proc rows of the overload like
    // DROP drops an object (a view calling it refuses RESTRICT, CASCADE drops the view) — the rows of its
    // signatures when the dispatcher's master registry (ctx->function_registry) holds it, else the rows the inputs
    // match, which a previous process left. The dispatcher drops a held overload from the master once this succeeds.
    class operator_unregister_udf_t final : public read_only_operator_t {
    public:
        operator_unregister_udf_t(std::pmr::memory_resource* resource,
                                  log_t log,
                                  std::string function_name,
                                  std::pmr::vector<types::complex_logical_type> inputs,
                                  components::catalog::drop_behavior_t behavior);

        // Sourceless SINK leaf (no data pipeline, no children): the registry
        // existence-check and the pg_proc/pg_depend purge run in
        // await_async_and_resume. The dispatcher drives this operator's async
        // finalize directly (a single await_async_and_resume).
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::string function_name_;
        std::pmr::vector<types::complex_logical_type> inputs_;
        components::catalog::drop_behavior_t behavior_;
    };

} // namespace components::operators
