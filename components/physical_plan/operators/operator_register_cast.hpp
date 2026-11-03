#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <actor-zeta/detail/future.hpp>

#include <memory_resource>

namespace components::operators {

    // Allocates the cast's oid and writes its pg_cast + pg_depend rows in the statement's transaction.
    class operator_register_cast_t final : public read_only_operator_t {
    public:
        operator_register_cast_t(std::pmr::memory_resource* resource,
                                 log_t log,
                                 catalog::oid_t source_type_oid,
                                 catalog::oid_t target_type_oid);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        catalog::oid_t source_type_oid_;
        catalog::oid_t target_type_oid_;
    };

    // Finds the cast's oid in pg_cast by its (source, target) pair, then deletes the pg_cast row and its pg_depend
    // edges in the statement's transaction.
    class operator_unregister_cast_t final : public read_only_operator_t {
    public:
        operator_unregister_cast_t(std::pmr::memory_resource* resource,
                                   log_t log,
                                   catalog::oid_t source_type_oid,
                                   catalog::oid_t target_type_oid);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        catalog::oid_t source_type_oid_;
        catalog::oid_t target_type_oid_;
    };

} // namespace components::operators