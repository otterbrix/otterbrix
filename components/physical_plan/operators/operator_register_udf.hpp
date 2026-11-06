#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/compute/function.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <core/result_wrapper.hpp>

#include <actor-zeta/detail/future.hpp>

#include <memory_resource>

namespace components::operators {

    // Writes pg_proc only, in the statement's transaction; the registries take the function when it commits.
    class operator_register_udf_t final : public read_only_operator_t {
    public:
        // `uid`: the one the dispatcher reserved, the prouid of the rows; `schema_oid`: the schema of the
        // registering session's place, where the rows go.
        operator_register_udf_t(std::pmr::memory_resource* resource,
                                log_t log,
                                components::compute::function_ptr function,
                                components::compute::function_uid uid,
                                components::catalog::oid_t schema_oid);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        components::catalog::oid_t schema_oid_;
        components::compute::function_ptr function_;
        components::compute::function_uid uid_;
    };

} // namespace components::operators
