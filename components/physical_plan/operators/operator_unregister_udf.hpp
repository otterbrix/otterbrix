#pragma once

#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <string>

namespace components::operators {

    // Operator implementation of manager_dispatcher_t::unregister_udf.
    //
    // Steps:
    //   1. Probe function_registry_t::get_default() for an overload of
    //      `function_name` whose signature matches `inputs`. Bail with
    //      success_=false if no match exists.
    //   2. Drop the matching overload from the default registry so subsequent
    //      validate_logical_plan calls cannot see it.
    //   3. Resolve every pg_proc row sharing this name (across all namespaces),
    //      delete the pg_proc row + pg_depend rows referencing it.
    class operator_unregister_udf_t final : public read_only_operator_t {
    public:
        operator_unregister_udf_t(std::pmr::memory_resource* resource,
                                  log_t log,
                                  std::string function_name,
                                  std::pmr::vector<types::complex_logical_type> inputs);

        bool success() const noexcept { return success_; }

    private:
        void on_execute_impl(pipeline::context_t* ctx) override;
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        std::string function_name_;
        std::pmr::vector<types::complex_logical_type> inputs_;
        bool success_{false};
    };

} // namespace components::operators
