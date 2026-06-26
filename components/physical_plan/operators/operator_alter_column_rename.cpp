#include "operator_alter_column_rename.hpp"

#include <set>
#include <vector>

#include "alter_validators.hpp"

#include <components/catalog/alter_column_validators.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_alter_column_rename_t::operator_alter_column_rename_t(std::pmr::memory_resource* resource,
                                                                   log_t log,
                                                                   catalog::oid_t table_oid,
                                                                   catalog::oid_t attoid,
                                                                   std::string old_name,
                                                                   std::string new_name)
        : read_write_operator_t(resource, std::move(log), operator_type::alter_column_rename)
        , table_oid_(table_oid)
        , attoid_(attoid)
        , old_name_(std::move(old_name))
        , new_name_(std::move(new_name)) {}

    void operator_alter_column_rename_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_alter_column_rename_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        constexpr catalog::oid_t pg_attr = catalog::well_known_oid::pg_attribute_table;

        // Reject new_name_ if it collides with a column visible to this snapshot.
        auto vc_fut = alter_validators::visible_column_names(resource_, ctx->disk_address, exec_ctx, table_oid_);
        auto visible_column_names = co_await std::move(vc_fut);
        auto ec_dup = components::catalog::alter_column_validators::validate_column_not_duplicate(resource_,
                                                                                                  visible_column_names,
                                                                                                  new_name_);
        if (ec_dup.contains_error()) {
            set_error(std::move(ec_dup));
            co_return;
        }

        // attoid_ is pre-stamped by enrich_logical_plan; INVALID means the
        // resolver couldn't find the column, so no-op.
        if (attoid_ == catalog::INVALID_OID) {
            mark_executed();
            co_return;
        }

        // keyed single-row read of the live pg_attribute row by attoid.
        std::pmr::vector<std::string> pa_keys(resource_);
        pa_keys.emplace_back("attoid");
        auto [_pa, paf] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::read_chunks_by_key,
                                           exec_ctx,
                                           pg_attr,
                                           std::move(pa_keys),
                                           components::operators::make_key_chunk(resource_, attoid_));
        std::pmr::vector<components::vector::data_chunk_t> attr_batches = co_await std::move(paf);

        catalog::oid_t attoid = catalog::INVALID_OID;
        std::int32_t attnum = 0;
        catalog::oid_t atttypid = catalog::INVALID_OID;
        bool att_not_null = false, att_has_default = false;
        std::string att_typspec, att_defspec;
        // Captured so the re-appended row keeps the same added_at_commit_id:
        // RENAME is identity-preserving, so added_at MUST NOT change.
        std::int64_t att_added_at_commit_id = 0;
        for (auto& chunk : attr_batches) {
            if (chunk.column_count() < 10)
                continue;
            bool found = false;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i))
                    continue;
                if (!chunk.is_null(7, i) && chunk.get_value<bool>(7, i))
                    continue; // dropped
                attoid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                atttypid = chunk.is_null(3, i) ? catalog::INVALID_OID
                                               : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(3, i));
                attnum = chunk.is_null(4, i) ? 0 : chunk.get_value<std::int32_t>(4, i);
                att_not_null = chunk.is_null(5, i) ? false : chunk.get_value<bool>(5, i);
                att_has_default = chunk.is_null(6, i) ? false : chunk.get_value<bool>(6, i);
                if (!chunk.is_null(8, i))
                    att_typspec = std::string(chunk.get_value<std::string_view>(8, i));
                if (!chunk.is_null(9, i))
                    att_defspec = std::string(chunk.get_value<std::string_view>(9, i));
                // Column 10 = added_at_commit_id. Rows written before the MVCC
                // commit_id columns landed have only 10 columns; tolerate a missing
                // slot as 0.
                if (chunk.column_count() > 10) {
                    if (!chunk.is_null(10, i))
                        att_added_at_commit_id = chunk.get_value<std::int64_t>(10, i);
                }
                found = true;
                break;
            }
            if (found)
                break;
        }

        if (attoid != catalog::INVALID_OID) {
            auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                             exec_ctx,
                                             pg_attr,
                                             std::int64_t{0},
                                             attoid);
            co_await std::move(df);
            if (ctx->txn.transaction_id != 0)
                ctx->pg_catalog_delete_tables.insert(pg_attr);

            // Re-append a fresh row reusing attoid/attnum/atttypid with the new
            // name. Identity-preserving: keep the captured added_at_commit_id,
            // dropped_at stays 0 (still live). A captured 0 (CREATEd column, or
            // ALTERed-but-not-yet-backfilled) is also correct — RENAME never
            // widens visibility, and no commit_id backfill marker is emitted.
            auto new_row = catalog::build_pg_attribute_row(resource_,
                                                           attoid,
                                                           table_oid_,
                                                           new_name_,
                                                           atttypid,
                                                           attnum,
                                                           att_not_null,
                                                           att_has_default,
                                                           /*is_dropped=*/false,
                                                           att_typspec,
                                                           att_defspec,
                                                           /*added_at_commit_id=*/att_added_at_commit_id,
                                                           /*dropped_at_commit_id=*/0);
            auto [_w, wf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::append_pg_catalog_row,
                                             exec_ctx,
                                             pg_attr,
                                             std::move(new_row));
            auto rng = co_await std::move(wf);
            if (rng.count > 0)
                ctx->pg_catalog_appends.push_back(std::move(rng));
        }

        mark_executed();
    }

} // namespace components::operators
