#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/context/execution_context.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/types.hpp>

#include <memory_resource>
#include <string>

namespace components::logical_plan {
    class node_catalog_resolve_t;
} // namespace components::logical_plan

namespace components::operators {

    struct foreign_server_row_t {
        components::catalog::oid_t oid{components::catalog::INVALID_OID};
        std::string type;
    };

    // The pg_foreign_server row named `name`; a miss answers INVALID_OID.
    actor_zeta::unique_future<core::result_wrapper_t<foreign_server_row_t>>
    read_foreign_server(std::pmr::memory_resource* resource,
                        actor_zeta::address_t disk_address,
                        components::execution_context_t exec_ctx,
                        std::string name);

    // The server's namespace for a remote (db, schema); a miss answers INVALID_OID.
    actor_zeta::unique_future<core::result_wrapper_t<components::catalog::oid_t>>
    read_foreign_namespace(std::pmr::memory_resource* resource,
                           actor_zeta::address_t disk_address,
                           components::execution_context_t exec_ctx,
                           components::catalog::oid_t server_oid,
                           std::string remote_db,
                           std::string remote_schema);

    // Stamps server_oid and server_type on every entry of a kind()==server resolve node from pg_foreign_server by
    // srvname (the entry's relname). A missing server leaves INVALID_OID; the statement that needs it refuses.
    class operator_resolve_server_t final : public read_write_operator_t {
    public:
        operator_resolve_server_t(std::pmr::memory_resource* resource,
                                  log_t log,
                                  components::logical_plan::node_catalog_resolve_t* node);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        components::logical_plan::node_catalog_resolve_t* node_;
        std::pmr::vector<components::types::complex_logical_type> output_schema_;
    };

} // namespace components::operators
