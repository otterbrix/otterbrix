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

    actor_zeta::unique_future<void> operator_set_timezone_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Validate the timezone name first (set_timezone returns error_t on an
        // unknown/unparseable identifier) so an invalid value never reaches disk.
        {
            components::catalog::session_catalog_t local_cat;
            auto err = local_cat.set_timezone(this->resource(),
                                              std::string_view{timezone_name_.data(), timezone_name_.size()});
            if (err.contains_error()) {
                set_error(std::move(err));
                mark_failed();
                co_return;
            }
        }

        // The pg_settings row is what makes the setting outlive the process; with
        // no disk actor to write it, SET TIME ZONE has validated a name and
        // persisted nothing, which a silent mark_executed() would report as done.
        // No production topology wires an executor without the disk actor
        // (base_spaces spawns it unconditionally), so the refusal costs nothing
        // where it cannot fire — same convention as the cast operators.
        if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
            set_error(core::error_t{
                core::error_code_t::physical_plan_error,
                std::pmr::string{"set_timezone: no disk actor is wired — the pg_settings row cannot be "
                                 "written, so the setting would not survive this process",
                                 this->resource()}});
            mark_failed();
            co_return;
        }

        const auto* settings_def =
            components::catalog::find_system_table(components::catalog::well_known_oid::pg_settings_table);
        // pg_settings is a well-known compiled-in table; a registry that cannot
        // name it is not a topology, and "succeed without writing" is the same
        // silent lie the empty-address branch above refuses to tell.
        if (settings_def == nullptr) {
            set_error(core::error_t{
                core::error_code_t::physical_plan_error,
                std::pmr::string{"set_timezone: the pg_settings schema is missing from the system-table "
                                 "registry — the setting cannot be persisted",
                                 this->resource()}});
            mark_failed();
            co_return;
        }

        std::pmr::vector<components::types::complex_logical_type> types(this->resource());
        for (const auto& col : settings_def->columns) {
            types.push_back(col.type());
        }
        components::vector::data_chunk_t row(this->resource(), types, 1);
        row.set_cardinality(1);
        row.set_value(0, 0, std::string_view("TimeZone"));
        row.set_value(1, 0, std::string_view(timezone_name_.data(), timezone_name_.size()));

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->execution_context.timezone_offset};
        auto [_u, uf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::append_pg_catalog_row,
                                         exec_ctx,
                                         components::catalog::well_known_oid::pg_settings_table,
                                         std::move(row));
        // Record the append range so the executor's commit tail publishes (and,
        // on error, reverts) this pg_settings row through the unified DML path.
        // append_pg_catalog_row returns count==0 for the direct-write (transaction_id
        // == 0) case, mirroring operator_insert's catalog-branch recording guard, and an
        // ERROR when the row could not be written at all — which SET TIME ZONE must not
        // report as a successful setting change.
        auto rng_r = co_await std::move(uf);
        if (rng_r.has_error()) {
            set_error(rng_r.error());
            mark_failed();
            co_return;
        }
        if (rng_r.value().count > 0)
            ctx->pg_catalog_appends.push_back(std::move(rng_r.value()));
        mark_executed();
    }

} // namespace components::operators