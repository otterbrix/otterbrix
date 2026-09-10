#include "operator_set_setting.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    operator_set_setting_t::operator_set_setting_t(std::pmr::memory_resource* resource,
                                                   log_t log,
                                                   catalog::setting_id setting,
                                                   std::pmr::string value)
        : read_write_operator_t(resource, std::move(log), operator_type::set_setting)
        , setting_(setting)
        , value_(std::move(value)) {}

    actor_zeta::unique_future<void> operator_set_setting_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Canonicalise first
        auto canonical = components::catalog::canonical_setting_value(setting_,
                                                                      std::string_view{value_.data(), value_.size()},
                                                                      this->resource());
        if (canonical.has_error()) {
            set_error(canonical.error());
            mark_failed();
            co_return;
        }

        const auto& setting_def = components::catalog::find_setting_by_id(setting_);

        if (setting_ == catalog::setting_id::decimal_width || setting_ == catalog::setting_id::decimal_scale) {
            const auto pending = catalog::unsigned_setting_value(canonical.value());
            const auto width =
                setting_ == catalog::setting_id::decimal_width ? pending : ctx->execution_context.decimal_width;
            const auto scale =
                setting_ == catalog::setting_id::decimal_scale ? pending : ctx->execution_context.decimal_scale;
            if (auto err = catalog::validate_decimal_defaults(width, scale, this->resource()); err.contains_error()) {
                set_error(std::move(err));
                mark_failed();
                co_return;
            }
        }

        if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
            set_error(core::error_t{
                core::error_code_t::physical_plan_error,
                std::pmr::string{"set " + std::string(setting_def.sql_name) +
                                     ": no disk actor is wired — the pg_settings row cannot be "
                                     "written, so the setting would not survive this process",
                                 this->resource()}});
            mark_failed();
            co_return;
        }

        const auto* settings_def =
            components::catalog::find_system_table(components::catalog::well_known_oid::pg_settings_table);
        if (settings_def == nullptr) {
            set_error(core::error_t{
                core::error_code_t::physical_plan_error,
                std::pmr::string{"set " + std::string(setting_def.sql_name) +
                                     ": the pg_settings schema is missing from the system-table "
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
        row.set_value(0, 0, setting_def.catalog_name);
        row.set_value(1, 0, std::string_view(canonical.value().data(), canonical.value().size()));

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->execution_context.timezone_offset};
        auto [_u, uf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::append_pg_catalog_row,
                                                    exec_ctx,
                                                    components::catalog::well_known_oid::pg_settings_table,
                                                    std::move(row));
        auto rng_r = co_await std::move(uf);
        if (rng_r.has_error()) {
            set_error(rng_r.error());
            mark_failed();
            co_return;
        }
        if (rng_r.value().count > 0)
            ctx->pg_catalog_appends.push_back(std::move(rng_r.value()));
        ctx->applied_setting = setting_;
        ctx->applied_setting_value.assign(canonical.value().data(), canonical.value().size());
        mark_executed();
    }

} // namespace components::operators
