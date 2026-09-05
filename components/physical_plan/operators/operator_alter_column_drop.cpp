#include "operator_alter_column_drop.hpp"

#include "alter_validators.hpp"

#include <components/catalog/alter_column_validators.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/helpers.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_alter_column_drop_t::operator_alter_column_drop_t(std::pmr::memory_resource* resource,
                                                               log_t log,
                                                               catalog::oid_t table_oid,
                                                               catalog::oid_t namespace_oid,
                                                               std::string column_name,
                                                               catalog::oid_t attoid,
                                                               catalog::drop_behavior_t behavior,
                                                               bool missing_ok)
        // Tagged as alter_column_drop (catch-all read_write_operator_t — same
        // convention as the sibling alter_column_add / alter_column_rename
        // operators).
        : read_write_operator_t(resource, std::move(log), operator_type::alter_column_drop)
        , table_oid_(table_oid)
        , namespace_oid_(namespace_oid)
        , column_name_(std::move(column_name))
        , attoid_(attoid)
        , behavior_(behavior)
        , missing_ok_(missing_ok) {}

    actor_zeta::unique_future<void> operator_alter_column_drop_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        constexpr catalog::oid_t pg_attr_oid = catalog::well_known_oid::pg_attribute_table;
        constexpr catalog::oid_t pg_dep_oid = catalog::well_known_oid::pg_depend_table;
        constexpr catalog::oid_t pg_idx_oid = catalog::well_known_oid::pg_index_table;
        constexpr catalog::oid_t pg_class_oid = catalog::well_known_oid::pg_class_table;
        constexpr catalog::oid_t pg_con_oid = catalog::well_known_oid::pg_constraint_table;

        // Keyed read of the table's live pg_attribute rows, then match the column BY NAME.
        //
        // This used to key on attoid_ alone and no-op when it was INVALID_OID, on the stated
        // premise that enrich_logical_plan had pre-stamped it. Nothing ever did:
        // node_alter_column_t::set_attoid has no callers anywhere in the pipeline, so attoid_
        // was INVALID on every execution and ALTER TABLE DROP COLUMN returned success having
        // written nothing at all — no tombstone, no dependent scrub, no storage release. The
        // sibling defect — RENAME COLUMN, same cause — was fixed by the same move; its gate is
        // integration/cpp/test/test_alter_rename_column.cpp.
        // Resolving by (attrelid, attname) is also what planner.cpp::rewrite_alter_table's own
        // comment always said this operator does — "looks up the attoid by (table_oid,
        // column_name) at execution time" — so this makes the code agree with its contract
        // rather than inventing a new one. attoid_ stays a CROSS-CHECK: when a caller does
        // stamp it, the row must be that row.
        if (column_name_.empty()) {
            mark_executed();
            co_return;
        }

        std::pmr::vector<std::uint64_t> pa_keys(resource_);
        pa_keys.emplace_back(catalog::pg_attribute_col::attrelid);
        auto [_pa, paf] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::read_chunks_by_key,
                                           exec_ctx,
                                           pg_attr_oid,
                                           std::move(pa_keys),
                                           components::operators::make_key_chunk(resource_, table_oid_),
                                           std::pmr::vector<std::uint64_t>{resource_});
        auto attr_batches_r = co_await std::move(paf);
        if (attr_batches_r.has_error()) {
            // A failed catalog read is not a miss; treating it as one lets the
            // operation proceed on data that was never read (same below for pg_depend).
            set_error(attr_batches_r.error());
            co_return;
        }
        auto& attr_batches = attr_batches_r.value();

        catalog::oid_t attoid = catalog::INVALID_OID;
        std::int32_t attnum = 0;
        catalog::oid_t atttypid = catalog::INVALID_OID;
        bool att_not_null = false, att_has_default = false;
        std::string att_typspec, att_defspec;
        for (auto& chunk : attr_batches) {
            if (chunk.column_count() < 10)
                continue;
            bool found = false;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i))
                    continue;
                if (!chunk.is_null(7, i) && chunk.get_value<bool>(7, i))
                    continue; // already dropped
                if (chunk.is_null(2, i))
                    continue;
                // get_value<string_view> (NOT chunk.value(), whose logical_value_t is a
                // temporary the view would outlive) — this one points into the chunk's own
                // string buffer, which is alive for the whole comparison below.
                const auto attname_cell = chunk.get_value<std::string_view>(2, i);
                if (attname_cell != column_name_)
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
                found = true;
                break;
            }
            if (found)
                break;
        }
        if (attoid == catalog::INVALID_OID) {
            // The column is not there — either never was, or a tombstone already hides it.
            //
            // This used to be "no-op, no error": mark_executed() and out, so
            // `ALTER TABLE t DROP COLUMN nosuchcol` reported SUCCESS having written no
            // tombstone, scrubbed no dependent and released no storage. PostgreSQL refuses
            // it (`column "x" of relation "y" does not exist`), and the silence cost more
            // than a wasted statement: a migration that drops a column and then reads the
            // table under its new shape got a green ALTER and a schema that never changed,
            // with nothing between the two to say which half lied.
            //
            // WHY IT WAS DELIBERATE, AND WHAT MAKES IT SAFE TO REVERSE NOW. B3c1 left it
            // silent because a relkind='g' (document) table keeps its columns in
            // pg_computed_column and has NO pg_attribute row, so EVERY column of one misses
            // the read above — a loud refusal here would have refused legal drops on every
            // document table. That is no longer this operator's problem to dodge: the
            // planner routes a relkind='g' DROP COLUMN to
            // operator_computed_field_unregister_t (rewrite_alter_table), which refuses a
            // missing field the same way against the catalog that actually holds it. The
            // answer is the same for both table shapes; only the catalog consulted differs.
            //
            // IF EXISTS is the one form PostgreSQL lets pass, and it is now carried on the
            // node instead of being assumed — so accepting the miss is the caller's explicit
            // request, not this operator's guess.
            if (missing_ok_) {
                mark_executed();
                co_return;
            }
            // Name the relation, not just the column: in a script that alters several
            // tables, "column x does not exist" does not say which one was missing it.
            // pg_class read on the refusal path only, same as the blocking branch below.
            std::pmr::vector<std::uint64_t> cl_keys(resource_);
            cl_keys.emplace_back(catalog::pg_class_col::oid);
            auto [_cl, clf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::read_chunks_by_key,
                                               exec_ctx,
                                               pg_class_oid,
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
            // KEEP THE MESSAGE SHORT — under about 120 bytes. A longer error string
            // built from this operator's resource_ comes back corrupted (doubled, or with
            // a size that makes reading it throw std::length_error) before the executor
            // has even copied it into a cursor. That is a separate, pre-existing defect —
            // reproduced on this branch, NOT fixed here, and unrelated to the cursor-side
            // lifetime bug that test_cursor_error_lifetime.cpp covers. The pre-existing
            // FK-blocking refusal further down already sits at that edge.
            std::string msg = "column \"";
            msg += column_name_;
            msg += "\" of relation \"";
            msg += rel;
            msg += "\" does not exist; use DROP COLUMN IF EXISTS to ignore it";
            set_error(core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }

        // read pg_depend for refclassid=pg_attribute, refobjid=attoid.
        std::pmr::vector<std::uint64_t> pd_keys(resource_);
        pd_keys.emplace_back(catalog::pg_depend_col::refclassid);
        pd_keys.emplace_back(catalog::pg_depend_col::refobjid);
        auto [_pd, pdf] = actor_zeta::send(
            ctx->disk_address,
            &services::disk::manager_disk_t::read_chunks_by_key,
            exec_ctx,
            pg_dep_oid,
            std::move(pd_keys),
            components::operators::make_key_chunk(resource_, catalog::well_known_oid::pg_attribute_table, attoid),
            std::pmr::vector<std::uint64_t>{resource_});
        auto dep_batches_r = co_await std::move(pdf);
        if (dep_batches_r.has_error()) {
            set_error(dep_batches_r.error());
            co_return;
        }
        auto& dep_batches = dep_batches_r.value();

        std::size_t dep_row_count = 0;
        for (const auto& chunk : dep_batches) dep_row_count += chunk.size();

        // ABORT-on-error gate: validate dependents BEFORE the first mutating
        // delete/append below, so a rejected DROP leaves the catalog untouched.
        //
        // `blocking` is the subset this operator may NOT cascade over: a constraint
        // that depends on this column through a 'n' (normal) edge. Only one writer
        // emits that shape — build_create_constraint_writes, for the confkey columns
        // of a FOREIGN KEY, i.e. the PARENT columns a constraint on ANOTHER table
        // references. Everything else reaching here is 'i' (internal): an index or a
        // constraint whose own key column this is, which cannot outlive the column and
        // is therefore dropped with it, below.
        std::pmr::vector<std::pair<int, catalog::oid_t>> dependents{resource_};
        dependents.reserve(dep_row_count);
        std::pmr::vector<catalog::oid_t> blocking{resource_};
        for (auto& chunk : dep_batches) {
            if (chunk.column_count() < 2)
                continue;
            const bool has_deptype = chunk.column_count() > catalog::pg_depend_col::deptype;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i) || chunk.is_null(1, i))
                    continue;
                const auto dep_cls = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                const auto dep_oid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));
                dependents.emplace_back(static_cast<int>(dep_cls), dep_oid);
                if (dep_cls != catalog::well_known_oid::pg_constraint_table || !has_deptype ||
                    chunk.is_null(catalog::pg_depend_col::deptype, i))
                    continue;
                const auto deptype_cell = chunk.get_value<std::string_view>(catalog::pg_depend_col::deptype, i);
                if (!deptype_cell.empty() && deptype_cell[0] == 'n')
                    blocking.push_back(dep_oid);
            }
        }
        if (!blocking.empty()) {
            // Rule 6: name the cause, not the symptom. Without this the statement was
            // ACCEPTED and the damage surfaced later, in a different table, as
            // "keyed read: table has no column <name>" on every subsequent insert.
            // Resolve the blocking constraint's name and owning table so the message
            // names objects the user can act on. Both reads are on the refusal path
            // only, so the accepted path pays nothing for them.
            std::string con_name;
            catalog::oid_t con_relid = catalog::INVALID_OID;
            std::pmr::vector<std::uint64_t> pc_keys(resource_);
            pc_keys.emplace_back(catalog::pg_constraint_col::oid);
            auto [_pc, pcf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::read_chunks_by_key,
                                               exec_ctx,
                                               pg_con_oid,
                                               std::move(pc_keys),
                                               components::operators::make_key_chunk(resource_, blocking.front()),
                                               std::pmr::vector<std::uint64_t>{resource_});
            auto con_batches_r = co_await std::move(pcf);
            if (con_batches_r.has_error()) {
                set_error(con_batches_r.error());
                co_return;
            }
            for (auto& chunk : con_batches_r.value()) {
                if (chunk.size() == 0 || chunk.column_count() <= catalog::pg_constraint_col::conrelid)
                    continue;
                if (!chunk.is_null(catalog::pg_constraint_col::conname, 0))
                    con_name.assign(chunk.get_value<std::string_view>(catalog::pg_constraint_col::conname, 0));
                if (!chunk.is_null(catalog::pg_constraint_col::conrelid, 0))
                    con_relid = static_cast<catalog::oid_t>(
                        chunk.get_value<std::uint32_t>(catalog::pg_constraint_col::conrelid, 0));
                break;
            }
            std::string con_table;
            if (con_relid != catalog::INVALID_OID) {
                std::pmr::vector<std::uint64_t> cl_keys(resource_);
                cl_keys.emplace_back(catalog::pg_class_col::oid);
                auto [_cl, clf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::read_chunks_by_key,
                                                   exec_ctx,
                                                   pg_class_oid,
                                                   std::move(cl_keys),
                                                   components::operators::make_key_chunk(resource_, con_relid),
                                                   std::pmr::vector<std::uint64_t>{resource_});
                auto cls_batches_r = co_await std::move(clf);
                if (cls_batches_r.has_error()) {
                    set_error(cls_batches_r.error());
                    co_return;
                }
                for (auto& chunk : cls_batches_r.value()) {
                    if (chunk.size() == 0 || chunk.column_count() <= catalog::pg_class_col::relname)
                        continue;
                    if (!chunk.is_null(catalog::pg_class_col::relname, 0))
                        con_table.assign(chunk.get_value<std::string_view>(catalog::pg_class_col::relname, 0));
                    break;
                }
            }
            // A blocking edge with no readable constraint row still blocks: fall back
            // to the oid in the text, never to letting the drop through.
            if (con_name.empty()) {
                con_name = "oid ";
                con_name += std::to_string(blocking.front());
            }
            if (con_table.empty()) {
                con_table = "oid ";
                con_table += std::to_string(con_relid);
            }
            std::string msg = "cannot drop column \"";
            msg += column_name_;
            msg += "\": foreign key constraint \"";
            msg += con_name;
            msg += "\" on table \"";
            msg += con_table;
            msg += "\" references it; drop that constraint or that table first";
            set_error(core::error_t{core::error_code_t::schema_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }

        // for RESTRICT, abort if any non-internal dep exists. For CASCADE,
        // drop each dependent object.
        if (behavior_ == catalog::drop_behavior_t::restrict_) {
            if (!dependents.empty()) {
                set_error(
                    core::error_t{core::error_code_t::other_error,
                                  std::pmr::string{"DROP COLUMN RESTRICT: column has dependent objects", resource_}});
                mark_executed();
                co_return;
            }
        }

        // Collect every dependent-scrub delete across all dep_rows into one
        // batched call. dep_batches was already awaited above, so no spec here
        // depends on an intervening read; only the sends are hoisted.
        std::pmr::vector<services::disk::pg_catalog_delete_spec_t> dep_specs(resource_);
        dep_specs.reserve(dep_row_count * 4);
        for (auto& chunk : dep_batches) {
            if (chunk.column_count() < 2)
                continue;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(0, i) || chunk.is_null(1, i))
                    continue;
                const auto dep_cls = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(0, i));
                const auto dep_oid = static_cast<catalog::oid_t>(chunk.get_value<std::uint32_t>(1, i));
                if (dep_cls == catalog::well_known_oid::pg_class_table) {
                    // Dependent index: scrub pg_index (by indexrelid=oid_col_idx 0),
                    // pg_depend.objid (idx 1), pg_depend.refobjid (idx 3), pg_class.oid.
                    dep_specs.push_back({pg_idx_oid, std::int64_t{0}, dep_oid});
                    dep_specs.push_back({pg_dep_oid, std::int64_t{1}, dep_oid});
                    dep_specs.push_back({pg_dep_oid, std::int64_t{3}, dep_oid});
                    dep_specs.push_back({pg_class_oid, std::int64_t{0}, dep_oid});
                    if (ctx->txn.transaction_id != 0) {
                        ctx->pg_catalog_delete_tables.insert(pg_idx_oid);
                        ctx->pg_catalog_delete_tables.insert(pg_dep_oid);
                        ctx->pg_catalog_delete_tables.insert(pg_class_oid);
                    }
                } else if (dep_cls == catalog::well_known_oid::pg_constraint_table) {
                    // Dependent constraint: scrub pg_constraint + pg_depend rows.
                    dep_specs.push_back({pg_con_oid, std::int64_t{0}, dep_oid});
                    dep_specs.push_back({pg_dep_oid, std::int64_t{1}, dep_oid});
                    dep_specs.push_back({pg_dep_oid, std::int64_t{3}, dep_oid});
                    if (ctx->txn.transaction_id != 0) {
                        ctx->pg_catalog_delete_tables.insert(pg_con_oid);
                        ctx->pg_catalog_delete_tables.insert(pg_dep_oid);
                    }
                }
            }
        }
        if (!dep_specs.empty()) {
            auto [_dep, depf] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                                 exec_ctx,
                                                 std::move(dep_specs));
            co_await std::move(depf);
        }

        // soft-delete the column: drop original pg_attribute row,
        // then append a tombstone with attisdropped=true. The tombstone keeps
        // attnum so existing rows on disk that reference this slot remain
        // self-describing for MVCC visibility.
        auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                         exec_ctx,
                                         pg_attr_oid,
                                         std::int64_t{0},
                                         attoid);
        co_await std::move(df);
        if (ctx->txn.transaction_id != 0)
            ctx->pg_catalog_delete_tables.insert(pg_attr_oid);

        // dropped_at_commit_id is placeholder-0; a backfill marker (below) patches
        // it post-commit, since the commit_id isn't allocated until COMMIT
        // (see pg_catalog_swap.hpp). The tombstone's MVCC insert_id is still the
        // executing txn_id.
        auto tombstone = catalog::build_pg_attribute_row(resource_,
                                                         attoid,
                                                         table_oid_,
                                                         column_name_,
                                                         atttypid,
                                                         attnum,
                                                         att_not_null,
                                                         att_has_default,
                                                         /*is_dropped=*/true,
                                                         att_typspec,
                                                         att_defspec,
                                                         /*added_at_commit_id=*/0,
                                                         /*dropped_at_commit_id=*/0);
        auto [_w, wf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::append_pg_catalog_row,
                                         exec_ctx,
                                         pg_attr_oid,
                                         std::move(tombstone));
        auto rng_r = co_await std::move(wf);
        if (rng_r.has_error()) {
            // Same half-applied state as the zero-row case below, with the reason attached.
            set_error(rng_r.error());
            mark_failed();
            co_return;
        }
        auto rng = std::move(rng_r.value());
        // The live row is already deleted above. A 0-row tombstone append leaves
        // the column half-applied (invisible to resolve_table, no MVCC marker for
        // recovery), so surface a hard error instead of letting mark_executed() lie.
        if (rng.count == 0) {
            std::string msg = "operator_alter_column_drop: tombstone append produced no rows for attoid ";
            msg += std::to_string(attoid);
            set_error(core::error_t{core::error_code_t::other_error, std::pmr::string{std::move(msg), resource_}});
            mark_executed();
            co_return;
        }
        ctx->pg_catalog_appends.push_back(std::move(rng));
        // Backfill dropped_at_commit_id on the tombstone, keyed by attoid (same
        // attoid as the live row — identity-preserving tombstone) — AND, B3c1, name the
        // physical column the commit has to release once that tombstone is committed.
        //
        // ORDER, and why the release is NOT sent from here. The storage-side drop is a
        // rebuild: it forgets the column and destroys the object that knows which blocks it
        // sat on, so it cannot be undone. The tombstone above is not durable yet — it is a
        // pg_attribute row carrying insert_id == this txn_id, which an explicit ROLLBACK
        // reverts (storage_revert_appends) and a crash before the commit marker discards.
        // Dropping the column here would therefore let the physical drop become durable (the
        // next checkpoint of THIS table writes a root without the column) while the tombstone
        // never does — catalog says the column exists, storage no longer has it. So this
        // operator only MARKS the drop, exactly as operator_dynamic_cascade_delete_t only
        // MARKS a dropped table, and operator_commit_transaction_t performs it after the WAL
        // commit marker and the publish barrier, in the same block that physically tears down
        // a committed DROP TABLE.
        //
        // (Adding one more cross-actor await here would have been safe in itself: this is an
        // operator, driven by executor_t::execute_pipeline inside the executor actor's own
        // coroutine, not an actor mailbox handler. The standing proof is the chain already
        // above: pg_attribute read, pg_depend read, dependent scrub, live-row delete,
        // tombstone append — five sequential cross-actor awaits in one body, in production
        // today. The one-await-per-handler rule bites on methods dispatched from a behavior()
        // switch, and this method appears in none. Ordering, not the await rule, is what moves
        // the release to commit time.)
        ctx->pg_attribute_commit_id_backfills.push_back(components::pg_attribute_commit_id_backfill_t{
            attoid,
            components::pg_attribute_commit_id_backfill_t::kind_t::dropped_at,
            table_oid_,
            column_name_,
            // rename_to_attname is the storage_rename kind's field; a DROP names no new name,
            // and added_column_type is the added_at kind's — a DROP creates no column either.
            std::string{},
            components::types::complex_logical_type{}});

        // Note: drop_column on a relkind='g' (computing) table is routed to
        // operator_computed_field_unregister_t in planner.cpp::rewrite_alter_table,
        // which clears matching pg_computed_column rows. This branch handles
        // regular (relkind='r') tables only — which is also why the physical release marked
        // above is safe to arm unconditionally here: the relkind='g' storage, whose
        // mid-pipeline drop_column once broke the re-INSERT path (see the note at the end of
        // operator_computed_field_unregister.cpp), never reaches this operator.

        mark_executed();
    }

} // namespace components::operators
