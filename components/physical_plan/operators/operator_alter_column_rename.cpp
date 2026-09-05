#include "operator_alter_column_rename.hpp"

#include <vector>

#include "alter_validators.hpp"

#include <components/catalog/alter_column_validators.hpp>
#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/helpers.hpp>
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

    actor_zeta::unique_future<void> operator_alter_column_rename_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        constexpr catalog::oid_t pg_attr = catalog::well_known_oid::pg_attribute_table;

        // Reject new_name_ if it collides with a column visible to this snapshot.
        auto vc_fut = alter_validators::visible_column_names(resource_, ctx->disk_address, exec_ctx, table_oid_);
        auto visible_column_names_r = co_await std::move(vc_fut);
        if (visible_column_names_r.has_error()) {
            // The duplicate-column check below cannot run on a read that failed;
            // passing an empty list would silently approve the ALTER.
            set_error(visible_column_names_r.error());
            co_return;
        }
        auto& visible_column_names = visible_column_names_r.value();
        auto ec_dup = components::catalog::alter_column_validators::validate_column_not_duplicate(resource_,
                                                                                                  visible_column_names,
                                                                                                  new_name_);
        if (ec_dup.contains_error()) {
            set_error(std::move(ec_dup));
            co_return;
        }

        // Matches the column BY NAME, not attoid_: nothing stamps node_alter_column_t's attoid, so keying on it
        // made RENAME COLUMN report success having renamed nothing (same defect DROP COLUMN had; gated by
        // test_alter_rename_column.cpp). attoid_ is now only a cross-check when a caller does stamp it.
        if (old_name_.empty()) {
            mark_executed();
            co_return;
        }

        std::pmr::vector<std::uint64_t> pa_keys(resource_);
        pa_keys.emplace_back(catalog::pg_attribute_col::attrelid);
        auto [_pa, paf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                      &services::disk::manager_disk_t::read_chunks_by_key,
                                                      exec_ctx,
                                                      pg_attr,
                                                      std::move(pa_keys),
                                                      components::operators::make_key_chunk(resource_, table_oid_),
                                                      std::pmr::vector<std::uint64_t>{resource_});
        auto attr_batches_r = co_await std::move(paf);
        if (attr_batches_r.has_error()) {
            // A failed pg_attribute read is not a miss; treating it as one lets the
            // operation proceed on data that was never read.
            set_error(attr_batches_r.error());
            co_return;
        }
        auto& attr_batches = attr_batches_r.value();

        catalog::oid_t attoid = catalog::INVALID_OID;
        std::int32_t attnum = 0;
        catalog::oid_t atttypid = catalog::INVALID_OID;
        bool att_not_null = false, att_has_default = false;
        std::string att_typspec, att_defspec;
        // Captured so the re-appended row keeps the same added_at_commit_id:
        // RENAME is identity-preserving, so added_at MUST NOT change.
        std::int64_t att_added_at_commit_id = 0;
        for (auto& chunk : attr_batches) {
            // Narrower than the columns read below is a genuine schema mismatch, not a legacy-row miss (every
            // row this build writes has all pg_attribute columns) — refuse rather than silently under-read.
            if (chunk.column_count() <= catalog::pg_attribute_col::added_at_commit_id) {
                std::string msg = "alter_column_rename: pg_attribute answered with ";
                msg += std::to_string(chunk.column_count());
                msg += " column(s), fewer than the ";
                msg += std::to_string(static_cast<std::size_t>(catalog::pg_attribute_col::added_at_commit_id) + 1);
                msg += " this build reads — the column cannot be resolved";
                set_error(core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource_}});
                co_return;
            }
            bool found = false;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i))
                    continue;
                if (!chunk.is_null(7, i) && chunk.get_value<bool>(7, i))
                    continue; // dropped
                if (chunk.is_null(2, i))
                    continue;
                // get_value<string_view>, not chunk.value(): the latter's logical_value_t is a
                // temporary the view would outlive.
                const auto attname_cell = chunk.get_value<std::string_view>(2, i);
                if (attname_cell != old_name_)
                    continue;
                const auto row_attoid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                if (attoid_ != catalog::INVALID_OID && row_attoid != attoid_)
                    continue; // a stamped identity must match the row it names
                attoid = row_attoid;
                atttypid = chunk.is_null(3, i) ? catalog::INVALID_OID
                                               : static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(3, i));
                attnum = chunk.is_null(4, i) ? 0 : chunk.get_value<std::int32_t>(4, i);
                att_not_null = chunk.is_null(5, i) ? false : chunk.get_value<bool>(5, i);
                att_has_default = chunk.is_null(6, i) ? false : chunk.get_value<bool>(6, i);
                if (!chunk.is_null(8, i))
                    att_typspec = std::string(chunk.get_value<std::string_view>(8, i));
                if (!chunk.is_null(9, i))
                    att_defspec = std::string(chunk.get_value<std::string_view>(9, i));
                // Column 10 = added_at_commit_id. A NULL cell is a pre-backfill row; captured 0 is correct
                // (see the re-append note below).
                if (!chunk.is_null(10, i))
                    att_added_at_commit_id = chunk.get_value<std::int64_t>(10, i);
                found = true;
                break;
            }
            if (found)
                break;
        }

        if (attoid == catalog::INVALID_OID) {
            // Refused, not reported as a rename of nothing (RENAME COLUMN has no IF EXISTS). The pg_class read
            // below picks the wording: a document table (relkind='g') gets "not implemented for document
            // tables" — its storage column is bound by name via a TYPE ALIAS that rename_column doesn't
            // update, so renaming just the catalog would leave the field unreadable under both names.
            std::pmr::vector<std::uint64_t> cl_keys(resource_);
            cl_keys.emplace_back(catalog::pg_class_col::oid);
            auto [_cl, clf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                          &services::disk::manager_disk_t::read_chunks_by_key,
                                                          exec_ctx,
                                                          catalog::well_known_oid::pg_class_table,
                                                          std::move(cl_keys),
                                                          components::operators::make_key_chunk(resource_, table_oid_),
                                                          std::pmr::vector<std::uint64_t>{resource_});
            auto cls_batches_r = co_await std::move(clf);
            if (cls_batches_r.has_error()) {
                set_error(cls_batches_r.error());
                co_return;
            }
            auto rel_id = alter_validators::relation_identity_of(cls_batches_r.value());
            std::string rel = std::move(rel_id.relname);
            if (rel.empty()) {
                rel = "oid ";
                rel += std::to_string(table_oid_);
            }
            if (rel_id.relkind == catalog::relkind::computed) {
                std::string msg = "RENAME COLUMN \"";
                msg += old_name_;
                msg += "\" of \"";
                msg += rel;
                msg += "\": not implemented for document tables";
                set_error(
                    core::error_t{core::error_code_t::unimplemented_yet, std::pmr::string{std::move(msg), resource_}});
                mark_executed();
                co_return;
            }
            std::string msg = "column \"";
            msg += old_name_;
            msg += "\" of relation \"";
            msg += rel;
            msg += "\" does not exist";
            set_error(core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }

        auto [_d, df] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                                    exec_ctx,
                                                    pg_attr,
                                                    std::int64_t{0},
                                                    attoid);
        co_await std::move(df);
        if (ctx->txn.transaction_id != 0)
            ctx->pg_catalog_delete_tables.insert(pg_attr);

        // Re-append reusing attoid/attnum/atttypid with the new name; keeps the captured added_at_commit_id
        // (including 0 — RENAME never widens visibility) and no commit_id backfill marker is emitted.
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
        auto [_w, wf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                    &services::disk::manager_disk_t::append_pg_catalog_row,
                                                    exec_ctx,
                                                    pg_attr,
                                                    std::move(new_row));
        auto rng_r = co_await std::move(wf);
        if (rng_r.has_error()) {
            // Same half-renamed state as the zero-row case below, with the reason attached.
            set_error(rng_r.error());
            mark_failed();
            co_return;
        }
        auto rng = std::move(rng_r.value());
        // A 0-row append would leave the column half-renamed (invisible under either name, no MVCC marker
        // for recovery) — hard error, not a mark_executed() lie (same shape as the drop operator's tombstone append).
        if (rng.count == 0) {
            std::string msg = "operator_alter_column_rename: renamed row append produced no rows for attoid ";
            msg += std::to_string(attoid);
            set_error(core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }
        ctx->pg_catalog_appends.push_back(std::move(rng));

        // Arms a commit-time storage rename rather than renaming inline: an inline rename couldn't be undone by
        // ROLLBACK, and a reverted catalog with storage already renamed would be misread as a drop by
        // manager_disk_t::rearm_dropped_column_blocks_sync (keyed on attoid, not name).
        ctx->pg_attribute_commit_id_backfills.push_back(components::pg_attribute_commit_id_backfill_t{
            attoid,
            components::pg_attribute_commit_id_backfill_t::kind_t::storage_rename,
            table_oid_,
            old_name_,
            new_name_,
            // added_column_type belongs to the added_at kind; a RENAME creates no column.
            components::types::complex_logical_type{}});

        mark_executed();
    }

} // namespace components::operators
