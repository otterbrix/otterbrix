#include "operator_set_timezone.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/session_catalog.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    operator_set_timezone_t::operator_set_timezone_t(std::pmr::memory_resource* resource,
                                                     log_t log,
                                                     std::pmr::string timezone_name)
        : read_write_operator_t(resource, std::move(log), operator_type::set_timezone)
        , timezone_name_(std::move(timezone_name)) {}

    void operator_set_timezone_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // Validate the timezone name first (set_timezone returns error_t on an
        // unknown/unparseable identifier) so an invalid value never reaches disk.
        components::catalog::session_catalog_t local_cat;
        auto err =
            local_cat.set_timezone(this->resource(), std::string_view{timezone_name_.data(), timezone_name_.size()});
        if (err.contains_error()) {
            set_error(std::move(err));
            mark_failed();
            return;
        }
        async_wait();
    }

    actor_zeta::unique_future<void> operator_set_timezone_t::await_async_and_resume(pipeline::context_t* ctx) {
        // IN_MEMORY-only deployment (no disk actor): no pg_settings persistence,
        // so skip the catalog write.
        if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
            mark_executed();
            co_return;
        }

        const auto* settings_def = components::catalog::find_system_table("pg_settings");
        if (settings_def == nullptr) {
            mark_executed();
            co_return;
        }

        std::pmr::vector<components::types::complex_logical_type> types(this->resource());
        for (const auto& col : settings_def->columns) {
            types.push_back(col.type());
        }
        components::vector::data_chunk_t row(this->resource(), types, 1);
        row.set_cardinality(1);
        row.set_value(0, 0, components::types::logical_value_t(this->resource(), std::string{"TimeZone"}));
        row.set_value(1,
                      0,
                      components::types::logical_value_t(this->resource(),
                                                         std::string{timezone_name_.data(), timezone_name_.size()}));

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->session_tz};
        auto [_u, uf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::append_pg_catalog_row,
                                         exec_ctx,
                                         components::catalog::well_known_oid::pg_settings_table,
                                         std::move(row));
        // Record the append range so the executor's commit tail publishes (and,
        // on error, reverts) this pg_settings row through the unified DML path.
        // append_pg_catalog_row returns count==0 for the txn-less (transaction_id
        // == 0) case, mirroring operator_primitive_write's recording guard.
        auto rng = co_await std::move(uf);
        if (rng.count > 0)
            ctx->pg_catalog_appends.push_back(std::move(rng));
        mark_executed();
    }

} // namespace components::operators