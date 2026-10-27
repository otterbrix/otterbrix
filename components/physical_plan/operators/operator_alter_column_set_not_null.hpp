#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    // The NOT NULL a PRIMARY KEY added by ALTER brings to its column `attoid` (PostgreSQL 18 sets attnotnull): the
    // disk refuses it while a row holds NULL there, and stamps it with the transaction; pg_attribute's row is
    // rewritten with attnotnull. A ROLLBACK reverts the stamp with the transaction's other column stamps.
    class operator_alter_column_set_not_null_t final : public read_write_operator_t {
    public:
        operator_alter_column_set_not_null_t(std::pmr::memory_resource* resource,
                                             log_t log,
                                             components::catalog::oid_t table_oid,
                                             components::catalog::oid_t attoid);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::catalog::oid_t table_oid_;
        components::catalog::oid_t attoid_;
    };

} // namespace components::operators
