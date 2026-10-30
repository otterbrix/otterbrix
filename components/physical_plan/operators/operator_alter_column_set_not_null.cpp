#include "operator_alter_column_set_not_null.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <string>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_alter_column_set_not_null_t::operator_alter_column_set_not_null_t(std::pmr::memory_resource* resource,
                                                                               log_t log,
                                                                               catalog::oid_t table_oid,
                                                                               catalog::oid_t attoid)
        : read_write_operator_t(resource, std::move(log), operator_type::alter_column_set_not_null)
        , table_oid_(table_oid)
        , attoid_(attoid) {}

    actor_zeta::unique_future<void>
    operator_alter_column_set_not_null_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        constexpr catalog::oid_t pg_attr = catalog::well_known_oid::pg_attribute_table;
        namespace col = catalog::pg_attribute_col;

        std::pmr::vector<std::uint64_t> keys(resource_);
        keys.emplace_back(col::attrelid);
        auto [_pa, paf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                      &services::disk::manager_disk_t::read_chunks_by_key,
                                                      exec_ctx,
                                                      pg_attr,
                                                      std::move(keys),
                                                      components::operators::make_key_chunk(resource_, table_oid_),
                                                      std::pmr::vector<std::uint64_t>{resource_});
        auto rows = co_await std::move(paf);
        if (rows.has_error()) {
            set_error(rows.error());
            co_return;
        }
        const vector::data_chunk_t* found_chunk = nullptr;
        uint64_t found_row = 0;
        for (const auto& chunk : rows.value()) {
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (!chunk.is_null(col::attoid, i) &&
                    static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(col::attoid, i)) == attoid_ &&
                    (chunk.is_null(col::attisdropped, i) || !chunk.get_value<bool>(col::attisdropped, i))) {
                    found_chunk = &chunk;
                    found_row = i;
                }
            }
        }
        if (found_chunk == nullptr) {
            std::string what = "the PRIMARY KEY names column attoid " + std::to_string(attoid_) + ", which table oid " +
                               std::to_string(table_oid_) + " does not have";
            set_error(core::error_t{core::error_code_t::internal_error, std::pmr::string{std::move(what), resource_}});
            co_return;
        }
        const auto& row = *found_chunk;
        const auto i = found_row;
        if (!row.is_null(col::attnotnull, i) && row.get_value<bool>(col::attnotnull, i)) {
            mark_executed();
            co_return;
        }

        auto [_s, sf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::storage_set_not_null,
                                                    exec_ctx,
                                                    table_oid_,
                                                    attoid_);
        if (auto refused = co_await std::move(sf); refused.contains_error()) {
            set_error(refused);
            co_return;
        }
        ctx->pg_attribute_commit_id_backfills.push_back(components::pg_attribute_commit_id_backfill_t{
            attoid_,
            components::pg_attribute_commit_id_backfill_t::kind_t::not_null_at,
            table_oid_,
            std::string{},
            std::string{},
            components::types::complex_logical_type{}});

        const auto text_of = [&row, i](uint64_t column) {
            return row.is_null(column, i) ? std::string{} : std::string{row.get_value<std::string_view>(column, i)};
        };
        auto rewritten = catalog::build_pg_attribute_row(
            resource_,
            attoid_,
            table_oid_,
            text_of(col::attname),
            row.is_null(col::atttypid, i) ? catalog::INVALID_OID
                                          : static_cast<catalog::oid_t>(row.get_value<std::uint32_t>(col::atttypid, i)),
            row.is_null(col::attnum, i) ? 0 : row.get_value<std::int32_t>(col::attnum, i),
            /*not_null=*/true,
            !row.is_null(col::atthasdefault, i) && row.get_value<bool>(col::atthasdefault, i),
            /*is_dropped=*/false,
            text_of(col::atttypspec),
            text_of(col::attdefspec),
            row.is_null(col::added_at_commit_id, i) ? 0 : row.get_value<std::int64_t>(col::added_at_commit_id, i),
            /*dropped_at_commit_id=*/0);

        auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                                    exec_ctx,
                                                    pg_attr,
                                                    static_cast<std::int64_t>(col::attoid),
                                                    attoid_);
        co_await std::move(df);
        if (ctx->txn.transaction_id != 0) {
            ctx->pg_catalog_delete_tables.insert(pg_attr);
        }
        auto [_w, wf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::append_pg_catalog_row,
                                                    exec_ctx,
                                                    pg_attr,
                                                    std::move(rewritten));
        auto appended = co_await std::move(wf);
        if (appended.has_error()) {
            set_error(appended.error());
            mark_failed();
            co_return;
        }
        ctx->pg_catalog_appends.push_back(std::move(appended.value()));
        mark_executed();
    }

} // namespace components::operators
