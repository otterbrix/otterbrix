#include "operator_alter_column_rename.hpp"

#include <set>
#include <vector>

#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_alter_column_rename_t::operator_alter_column_rename_t(
        std::pmr::memory_resource* resource,
        log_t                       log,
        catalog::oid_t              table_oid,
        std::string                 old_name,
        std::string                 new_name)
        : read_write_operator_t(resource, std::move(log), operator_type::alter_column_rename)
        , table_oid_(table_oid)
        , old_name_(std::move(old_name))
        , new_name_(std::move(new_name)) {}

    void operator_alter_column_rename_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        async_wait();
    }

    actor_zeta::unique_future<void>
    operator_alter_column_rename_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        const collection_full_name_t pg_attr{"pg_catalog", "main", "pg_attribute"};

        // Step 1: read all pg_attribute rows for this table; locate the live row matching old_name.
        components::types::logical_value_t toid_lv(resource_, table_oid_);
        auto [_pa, paf] = actor_zeta::send(
            ctx->disk_address,
            &services::disk::manager_disk_t::read_rows_by_key,
            exec_ctx, pg_attr,
            std::vector<std::string>{"attrelid"},
            std::vector<components::types::logical_value_t>{toid_lv});
        auto attr_rows = co_await std::move(paf);

        catalog::oid_t attoid = catalog::INVALID_OID;
        std::int32_t   attnum = 0;
        catalog::oid_t atttypid = catalog::INVALID_OID;
        bool att_not_null = false, att_has_default = false;
        std::string att_typspec, att_defspec;
        for (const auto& row : attr_rows) {
            if (row.size() < 10 || row[2].is_null()) continue;
            if (row[2].value<std::string_view>() != old_name_) continue;
            if (!row[7].is_null() && row[7].value<bool>()) continue; // dropped
            attoid          = static_cast<catalog::oid_t>(row[0].value<std::uint32_t>());
            atttypid        = row[3].is_null() ? catalog::INVALID_OID
                                               : static_cast<catalog::oid_t>(row[3].value<std::uint32_t>());
            attnum          = row[4].is_null() ? 0 : row[4].value<std::int32_t>();
            att_not_null    = !row[5].is_null() && row[5].value<bool>();
            att_has_default = !row[6].is_null() && row[6].value<bool>();
            if (!row[8].is_null()) att_typspec = std::string(row[8].value<std::string_view>());
            if (!row[9].is_null()) att_defspec = std::string(row[9].value<std::string_view>());
            break;
        }

        if (attoid != catalog::INVALID_OID) {
            // Step 2: delete the original pg_attribute row.
            auto [_d, df] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::delete_pg_catalog_rows,
                exec_ctx, pg_attr, std::int64_t{0}, attoid);
            co_await std::move(df);
            if (ctx->txn.transaction_id != 0) ctx->pg_catalog_delete_tables.insert(pg_attr);

            // Step 3: append a fresh row reusing attoid/attnum/atttypid with the new name.
            auto new_row = catalog::build_pg_attribute_row(
                resource_, attoid, table_oid_, new_name_,
                atttypid, attnum, att_not_null, att_has_default,
                /*is_dropped=*/false, att_typspec, att_defspec);
            auto [_w, wf] = actor_zeta::send(
                ctx->disk_address,
                &services::disk::manager_disk_t::append_pg_catalog_row,
                exec_ctx, pg_attr, std::move(new_row));
            auto rng = co_await std::move(wf);
            if (rng.count > 0) ctx->pg_catalog_appends.push_back(std::move(rng));
        }

        mark_executed();
    }

} // namespace components::operators
