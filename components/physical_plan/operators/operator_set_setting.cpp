#include "operator_set_setting.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/session_catalog.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/context/execution_context.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    operator_set_setting_t::operator_set_setting_t(std::pmr::memory_resource* resource,
                                                   log_t log,
                                                   catalog::setting_scope_t scope,
                                                   const std::pmr::vector<catalog::setting_assignment_t>& assignments)
        : read_write_operator_t(resource, std::move(log), operator_type::set_setting)
        , scope_(scope)
        , assignments_(resource) {
        assignments_.reserve(assignments.size());
        for (const auto& assignment : assignments) {
            assignments_.push_back(assignment.is_default
                                       ? catalog::setting_assignment_t::to_default(assignment.id, resource)
                                       : catalog::setting_assignment_t{assignment.id, assignment.value, resource});
        }
    }

    actor_zeta::unique_future<void> operator_set_setting_t::await_async_and_resume(pipeline::context_t* ctx) {
        std::pmr::vector<catalog::setting_assignment_t> applied{this->resource()};
        applied.reserve(assignments_.size());
        for (const auto& assignment : assignments_) {
            // A session's default is whatever the defaults are when it reads them; pg_settings stores a value.
            if (assignment.is_default && scope_ != catalog::setting_scope_t::system) {
                applied.push_back(catalog::setting_assignment_t::to_default(assignment.id, this->resource()));
                continue;
            }
            const auto built_in =
                catalog::current_setting_value(catalog::session_catalog_t{}, assignment.id, this->resource());
            const std::string_view written =
                assignment.is_default ? std::string_view{built_in} : std::string_view{assignment.value};
            auto value = catalog::canonical_setting_value(assignment.id, written, this->resource());
            if (value.has_error()) {
                set_error(value.error());
                mark_failed();
                co_return;
            }
            applied.emplace_back(assignment.id, value.value(), this->resource());
        }

        if (scope_ == catalog::setting_scope_t::system) {
            if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
                set_error(core::error_t{core::error_code_t::internal_error,
                                        std::pmr::string{"ALTER SYSTEM: no disk actor is wired, so pg_settings "
                                                         "cannot be written",
                                                         this->resource()}});
                mark_failed();
                co_return;
            }
            const auto* settings_def =
                components::catalog::find_system_table(components::catalog::well_known_oid::pg_settings_table);
            if (settings_def == nullptr) {
                set_error(core::error_t{core::error_code_t::internal_error,
                                        std::pmr::string{"ALTER SYSTEM: the pg_settings schema is missing from the "
                                                         "system-table registry",
                                                         this->resource()}});
                mark_failed();
                co_return;
            }
            std::pmr::vector<components::types::complex_logical_type> types(this->resource());
            for (const auto& col : settings_def->columns) {
                types.push_back(col.type());
            }
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->execution_context.timezone_offset};
            for (const auto& assignment : applied) {
                components::vector::data_chunk_t row(this->resource(), types, 1);
                row.set_cardinality(1);
                row.set_value(0, 0, catalog::find_setting_by_id(assignment.id).catalog_name);
                row.set_value(1, 0, std::string_view(assignment.value));
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
                if (rng_r.value().count > 0) {
                    ctx->pg_catalog_appends.push_back(std::move(rng_r.value()));
                }
            }
        }
        ctx->applied_setting_scope = scope_;
        ctx->applied_settings = std::move(applied);
        mark_executed();
    }

} // namespace components::operators
