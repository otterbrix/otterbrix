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

        // Keyed read of the table's live pg_attribute rows, then match the column BY NAME.
        //
        // This used to key on attoid_ alone and no-op when it was INVALID_OID, on the stated
        // premise that enrich_logical_plan had pre-stamped it. Nothing ever did:
        // node_alter_column_t::set_attoid has no callers anywhere in the pipeline, so attoid_ was
        // INVALID on every execution and ALTER TABLE RENAME COLUMN returned success having
        // written nothing — the column kept its old name and the statement lied. This is the
        // sibling of the DROP COLUMN defect B3c1 fixed by the same move, and the note pinning it
        // sat in integration/cpp/test/test_multi_database_isolation.cpp.
        //
        // Resolving by (attrelid, attname) is also what planner.cpp::rewrite_alter_table's own
        // comment always claimed the ALTER operators do. attoid_ stays a CROSS-CHECK: when a
        // caller does stamp it, the row must be that row.
        if (old_name_.empty()) {
            mark_executed();
            co_return;
        }

        std::pmr::vector<std::uint64_t> pa_keys(resource_);
        pa_keys.emplace_back(catalog::pg_attribute_col::attrelid);
        auto [_pa, paf] = actor_zeta::send(ctx->disk_address,
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
            // A CHUNK NARROWER THAN THE READS BELOW IS A DIFFERENT ANSWER, NOT A MISS.
            // The read was issued with an empty projection ("all columns"), so the
            // reply's width is the width of the pg_attribute storage itself, and every
            // row this build writes has all 12 columns (build_pg_attribute_row). The
            // old guard both skipped narrow chunks in silence AND read the
            // added_at_commit_id slot only when present, "tolerating" its absence as 0
            // — a pre-MVCC compatibility this engine does not keep. The threshold is
            // the largest ordinal read below: added_at_commit_id (10).
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
                // get_value<string_view> (NOT chunk.value(), whose logical_value_t is a
                // temporary the view would outlive) — this one points into the chunk's own
                // string buffer, alive for the whole comparison.
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
                // Column 10 = added_at_commit_id — width guaranteed by the chunk
                // guard above. A NULL cell is a pre-backfill row (commit_id patched
                // post-commit); its captured 0 is correct, see the re-append note.
                if (!chunk.is_null(10, i))
                    att_added_at_commit_id = chunk.get_value<std::int64_t>(10, i);
                found = true;
                break;
            }
            if (found)
                break;
        }

        if (attoid == catalog::INVALID_OID) {
            // The column to rename is not there. This whole body used to sit INSIDE an
            // `if (attoid != INVALID_OID)` with mark_executed() unconditionally after it,
            // so a miss reported SUCCESS having renamed nothing — the exact silent success
            // its DROP sibling had, and the reason the pipeline's oldest ALTER bug survived
            // two rewrites. PostgreSQL refuses it: `column "x" of relation "y" does not
            // exist`. There is no IF EXISTS form of RENAME COLUMN to honour — the grammar
            // has none — so this refusal is unconditional.
            //
            // WHICH true sentence, though, depends on the table. A relkind='g' (document)
            // table keeps its columns in pg_computed_column and has NO pg_attribute row
            // for ANY of them, so the read above misses on every column of one — and
            // "column b does not exist" about a column the user can SELECT is true only
            // about the wrong catalog. The pg_class read below is on the refusal path
            // anyway, for the relation's name; its row carries relkind too, so the
            // refusal picks its wording from the same row instead of guessing. Both
            // kinds are refused — the loudness is not keyed on the table, only the
            // sentence is.
            //
            // The document sentence says "not implemented" and not "does not exist"
            // because that is what it is. The catalog half of a document rename is
            // straightforward (pg_computed_column is versioned: a refcount=0 tombstone
            // under the old name plus a live row carrying the same attoid under the new
            // one, which the resolver picks up — measured on this branch). The STORAGE
            // half cannot be completed: a relkind='g' column is bound to its physical
            // column by the storage column's TYPE ALIAS (operator_resolve_table matches
            // the resolved rows against data_table_t::copy_types()), and
            // data_table_t::rename_column updates column_definition_t::name_ while
            // column_definition_t::set_name leaves type_ alone — so the alias keeps the
            // OLD name and the storage still reports success. Renaming would therefore
            // leave the catalog naming a column no storage column answers to: the bind
            // fails, and the field disappears from SELECT under BOTH names with its data
            // unreadable. That is worse than the silent success this change removes, so
            // it is refused until components/table carries the name into the alias.
            std::pmr::vector<std::uint64_t> cl_keys(resource_);
            cl_keys.emplace_back(catalog::pg_class_col::oid);
            auto [_cl, clf] = actor_zeta::send(ctx->disk_address,
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
        auto rng_r = co_await std::move(wf);
        if (rng_r.has_error()) {
            // Same half-renamed state as the zero-row case below, with the reason attached.
            set_error(rng_r.error());
            mark_failed();
            co_return;
        }
        auto rng = std::move(rng_r.value());
        // The live row is already deleted above. A 0-row append would leave the column
        // half-renamed — invisible to resolve_table under either name, and with no MVCC
        // marker for recovery — so this is a hard error rather than a mark_executed() lie
        // (same shape as operator_alter_column_drop_t's tombstone append).
        if (rng.count == 0) {
            std::string msg = "operator_alter_column_rename: renamed row append produced no rows for attoid ";
            msg += std::to_string(attoid);
            set_error(core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }
        ctx->pg_catalog_appends.push_back(std::move(rng));

        // ARM THE STORAGE HALF.
        //
        // The storage keeps its own copy of every column's name, and parts of the write
        // path address columns by it (the append's column expansion, drop_storage_column).
        // Leaving that copy on the old name would not be inert: the very next INSERT would
        // expand its chunk against a name the catalog no longer uses.
        //
        // What this marker is NOT is the thing that keeps the column alive across a
        // restart. manager_disk_t::rearm_dropped_column_blocks_sync (B3c2) used to
        // reconcile storage columns against pg_attribute BY NAME and read a storage-only
        // name as a DROP, so a catalog-only rename cost the column and its data at the next
        // start. That walk now compares pg_attribute.attoid (RN-oid), which a rename does
        // not move, and repairs the stale storage name from the catalog instead.
        //
        // WHY THE MARKER AND NOT A SEND FROM HERE — the same ordering B3c1 established for
        // the DROP's physical release, in the ROLLBACK direction. The row appended above
        // carries insert_id == this txn_id: an explicit ROLLBACK reverts it
        // (storage_revert_appends) and a crash before the commit marker discards it. The
        // storage rename is not undone by either. Renaming from here would therefore let a
        // reverted ALTER leave the storage under the new name while the catalog went back
        // to the old one — and that divergence is exactly what the bootstrap walk reads as
        // a drop. So this operator only MARKS, and operator_commit_transaction_t performs
        // the rename after the WAL commit marker and the publish barrier, in the same block
        // that performs a committed DROP COLUMN's release.
        //
        // The marker rides pg_attribute_commit_id_backfill_t because it has exactly that
        // lifetime (drained at commit, discarded whole by txn_abort_drain_t). Its
        // storage_rename kind patches no commit_id column — RENAME preserves added_at — and
        // the commit operator keeps it out of the batch that does.
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
