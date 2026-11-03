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

    // Drops the pg_proc rows of the overload like DROP drops an object (a view calling it refuses RESTRICT, CASCADE
    // drops the view): the rows of its signatures when the statement's registry (ctx->function_registry) holds it,
    // else the rows the inputs match, which a previous process left.
    class operator_unregister_udf_t final : public read_only_operator_t {
    public:
        operator_unregister_udf_t(std::pmr::memory_resource* resource,
                                  log_t log,
                                  std::string function_name,
                                  std::pmr::vector<types::complex_logical_type> inputs,
                                  components::catalog::drop_behavior_t behavior,
                                  std::uint32_t schema_oid);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::string function_name_;
        std::pmr::vector<types::complex_logical_type> inputs_;
        components::catalog::drop_behavior_t behavior_;
        std::uint32_t schema_oid_;
    };

} // namespace components::operators
