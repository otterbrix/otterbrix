#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <actor-zeta/detail/future.hpp>

#include <memory_resource>

namespace components::operators {

    // Operator half of manager_dispatcher_t::register_cast. The per-executor
    // registry fan-out is driven by the dispatcher (it owns the executor
    // addresses + scheduler); this operator only allocates the cast OID and
    // writes the pg_cast + pg_depend rows so the cast survives restart.
    class operator_register_cast_t final : public read_only_operator_t {
    public:
        operator_register_cast_t(std::pmr::memory_resource* resource,
                                 log_t log,
                                 catalog::oid_t source_type_oid,
                                 catalog::oid_t target_type_oid);

        bool success() const noexcept { return success_; }

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        catalog::oid_t source_type_oid_;
        catalog::oid_t target_type_oid_;
        bool success_{false};
    };

    // Operator half of manager_dispatcher_t::unregister_cast. Resolves the cast
    // OID from pg_cast by its (source, target) pair, then deletes the pg_cast row
    // and its pg_depend edges. The registry removal is driven by the dispatcher.
    class operator_unregister_cast_t final : public read_only_operator_t {
    public:
        operator_unregister_cast_t(std::pmr::memory_resource* resource,
                                   log_t log,
                                   catalog::oid_t source_type_oid,
                                   catalog::oid_t target_type_oid);

        bool success() const noexcept { return success_; }

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        catalog::oid_t source_type_oid_;
        catalog::oid_t target_type_oid_;
        bool success_{false};
    };

} // namespace components::operators