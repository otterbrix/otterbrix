#include "operator_computed_field_register.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_computed_field_register_t::operator_computed_field_register_t(
        std::pmr::memory_resource* resource,
        log_t log,
        catalog::oid_t table_oid,
        std::vector<components::table::column_definition_t> columns)
        : read_write_operator_t(resource, std::move(log), operator_type::computed_field_register)
        , table_oid_(table_oid)
        , columns_(std::move(columns)) {}

    void operator_computed_field_register_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // Propagate left's output up so the executor's cursor-build path
        // (executor.cpp switch on plan->type()) sees the original DML chunk
        // even though we wrap it: sequence_t(insert, computed_field_register)
        // is lowered to a left-chain with register as the OUTER root. Without
        // this propagation, cur->size() would return 0 for relkind='g' INSERT.
        if (left_ && left_->output()) {
            output_ = left_->output();
        }
        async_wait();
    }

    actor_zeta::unique_future<void>
    operator_computed_field_register_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Concurrent INSERTs registering the same field can produce
        // duplicate (relid, attname) rows in pg_computed_column. MVCC
        // isolation hides uncommitted writes from other sessions, so each
        // session sees the field as unregistered and proceeds to allocate a
        // fresh attoid + append a register row. After both commit,
        // pg_computed_column holds two rows for the same (relid, attname)
        // with different attoids.
        //
        // Tolerance path (current):
        //   - Resolver picks max(attversion); ties broken by lowest attoid.
        //   - VACUUM aggressive eventually GCs stale (refcount=0) versions,
        //     so duplicates are short-lived.
        //   - Storage-side add_column (schema-extension) is idempotent: the
        //     second concurrent INSERT either no-ops (column already
        //     extended) or fails benignly — caller path is unaffected.
        //
        // TODO Strict-serialization path (deferred until benchmark proves
        // the race causes user-visible problems): introduce per-table_oid
        // lock via a disk-actor message pair held across the
        // read/classify/allocate/append/depend sequence below.
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        constexpr catalog::oid_t pg_computed_column = catalog::well_known_oid::pg_computed_column_table;
        constexpr catalog::oid_t pg_type = catalog::well_known_oid::pg_type_table;
        constexpr catalog::oid_t pg_depend = catalog::well_known_oid::pg_depend_table;

        const types::logical_value_t toid_lv(resource_, table_oid_);

        for (const auto& col : columns_) {
            // Step 1: read existing pg_computed_column rows for (relid, attname).
            // pg_computed_column layout: 0=relid 1=attoid 2=attname
            // 3=atttypid 4=atttypspec 5=attversion 6=attrefcount.
            types::logical_value_t name_lv(resource_, std::string(col.name()));
            std::pmr::vector<std::string> r_keys(resource_);
            r_keys.emplace_back("relid");
            r_keys.emplace_back("attname");
            std::pmr::vector<types::logical_value_t> r_vals(resource_);
            r_vals.emplace_back(toid_lv);
            r_vals.emplace_back(name_lv);
            auto [_r, rf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::read_rows_by_key,
                                             exec_ctx,
                                             pg_computed_column,
                                             std::move(r_keys),
                                             std::move(r_vals));
            auto rows = co_await std::move(rf);

            std::int64_t max_version = -1;
            catalog::oid_t latest_atttypid = catalog::INVALID_OID;
            std::string latest_atttypspec;
            std::int64_t latest_refcount = 0;
            for (const auto& row : rows) {
                if (row.size() < 7)
                    continue;
                if (row[5].is_null())
                    continue;
                const auto v = row[5].value<std::int64_t>();
                if (v > max_version) {
                    max_version = v;
                    latest_atttypid = row[3].is_null() ? catalog::INVALID_OID
                                                       : static_cast<catalog::oid_t>(row[3].value<std::uint32_t>());
                    latest_atttypspec =
                        row[4].is_null() ? std::string{} : std::string(row[4].value<std::string_view>());
                    latest_refcount = row[6].is_null() ? 0 : row[6].value<std::int64_t>();
                }
            }

            // Resolve the column's atttypid (mirrors operator_alter_column_add).
            // Builtin types map directly; composite/UNKNOWN fall back to a
            // pg_type.typname lookup.
            catalog::oid_t atttypid = (col.atttypid() != catalog::INVALID_OID)
                                          ? col.atttypid()
                                          : catalog::builtin_type_to_oid(col.type().type());
            if (atttypid == catalog::INVALID_OID) {
                std::string lookup;
                const auto lt = col.type().type();
                if (lt == types::logical_type::UNKNOWN) {
                    lookup = col.type().type_name();
                } else if (lt == types::logical_type::DECIMAL) {
                    lookup = "numeric";
                } else {
                    lookup = std::string{catalog::logical_type_to_pg_name(lt)};
                }
                if (!lookup.empty()) {
                    types::logical_value_t lookup_lv(resource_, std::move(lookup));
                    std::pmr::vector<std::string> t_keys(resource_);
                    t_keys.emplace_back("typname");
                    std::pmr::vector<types::logical_value_t> t_vals(resource_);
                    t_vals.emplace_back(lookup_lv);
                    auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                                     &services::disk::manager_disk_t::read_rows_by_key,
                                                     exec_ctx,
                                                     pg_type,
                                                     std::move(t_keys),
                                                     std::move(t_vals));
                    auto type_rows = co_await std::move(tf);
                    if (!type_rows.empty() && !type_rows[0].empty() && !type_rows[0][0].is_null()) {
                        atttypid = static_cast<catalog::oid_t>(type_rows[0][0].value<std::uint32_t>());
                    }
                }
            }

            // Encode complex types into atttypspec so resolve_table for
            // relkind='g' can reconstruct ARRAY/STRUCT/UNION/DECIMAL etc.
            // exactly. Builtin scalars leave atttypspec empty — atttypid
            // alone reconstructs them via oid_to_builtin_type.
            std::string atttypspec;
            if (atttypid == catalog::INVALID_OID && col.type().type() != types::logical_type::UNKNOWN) {
                atttypspec = catalog::encode_type_spec(col.type());
            }

            // Step 2: classify and decide.
            // If latest row is a tombstone (refcount<=0), the column was
            // DROP'd. Treat re-INSERT as is_new so a fresh attoid + bumped
            // attversion are written and operator_computed_field_register
            // doesn't short-circuit on stale type info.
            const bool is_new = (max_version < 0) || (latest_refcount <= 0);
            const bool same_type = !is_new && latest_atttypid == atttypid && latest_atttypspec == atttypspec &&
                                   (latest_atttypid != catalog::INVALID_OID || !atttypspec.empty());
            if (same_type) {
                // No-op: column already registered with the same type. The
                // simplified binary refcount model does not bump on every
                // INSERT.
                continue;
            }

            // Step 3: allocate a fresh attoid for the new (or evolved) column row.
            auto [_oa, oaf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::allocate_oids_batch,
                                               std::size_t{1});
            auto oid_batch = co_await std::move(oaf);
            if (oid_batch.empty()) {
                set_error(core::error_t{
                    core::error_code_t::other_error,
                    std::pmr::string{"computed_field_register: oid allocation returned empty batch", resource_}});
                mark_executed();
                co_return;
            }
            const catalog::oid_t attoid = oid_batch[0];

            // Always bump above the existing max — version=0 only when no prior
            // rows exist. Re-register after a tombstone (is_new=true via
            // latest_refcount<=0 branch) MUST write a version higher than the
            // tombstone, otherwise readers picking max(attversion) per attname
            // pick the tombstone (rc<=0) instead of the new live row, and the
            // column appears dropped even though it was re-inserted.
            // dynamic_schema_re_add_after_drop pins this.
            const std::int64_t new_version = (max_version < 0) ? std::int64_t{0} : (max_version + 1);

            auto cc_row = catalog::build_pg_computed_column_row(resource_,
                                                                table_oid_,
                                                                attoid,
                                                                std::string(col.name()),
                                                                atttypid,
                                                                new_version,
                                                                /*attrefcount=*/std::int64_t{1},
                                                                atttypspec);
            auto [_w, wf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::append_pg_catalog_row,
                                             exec_ctx,
                                             pg_computed_column,
                                             std::move(cc_row));
            if (auto rng = co_await std::move(wf); rng.count > 0) {
                ctx->pg_catalog_appends.push_back(std::move(rng));
            }

            // Emit pg_depend rows so the dynamic computed-column mirrors
            // the static ALTER ADD COLUMN dependency graph:
            //   1) (pg_computed_column, attoid) → (pg_type, atttypid) 'n'
            //      lets DROP TYPE refuse to drop a type still used by a dynamic
            //      column (relkind='g').
            //   2) (pg_computed_column, attoid) → (pg_class, table_oid) 'n'
            //      lets DROP TABLE cascade sweep dynamic-column rows alongside
            //      the parent. Existing cascade in operator_dynamic_cascade_delete
            //      already discovers these via the pg_depend reverse index, so
            //      no extra cascade wiring is needed here.
            // Unregister side intentionally does NOT remove these rows: the
            // parent DROP TABLE cascade or namespace VACUUM will sweep them
            // later, and a stale pg_depend row to a still-live oid is harmless
            // (refcount=0 columns simply remain undiscoverable via attname).
            if (atttypid != catalog::INVALID_OID) {
                auto dep_row =
                    catalog::build_pg_depend_row(resource_,
                                                 catalog::well_known_oid::pg_computed_column_table, // classid
                                                 attoid,                                            // objid
                                                 catalog::well_known_oid::pg_type_table,            // refclassid
                                                 atttypid,                                          // refobjid
                                                 /*deptype=*/'n');
                auto [_dt, dtf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::append_pg_catalog_row,
                                                   exec_ctx,
                                                   pg_depend,
                                                   std::move(dep_row));
                if (auto rng = co_await std::move(dtf); rng.count > 0) {
                    ctx->pg_catalog_appends.push_back(std::move(rng));
                }
            }
            {
                auto dep_row =
                    catalog::build_pg_depend_row(resource_,
                                                 catalog::well_known_oid::pg_computed_column_table, // classid
                                                 attoid,                                            // objid
                                                 catalog::well_known_oid::pg_class_table,           // refclassid
                                                 table_oid_,                                        // refobjid
                                                 /*deptype=*/'n');
                auto [_dc, dcf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::append_pg_catalog_row,
                                                   exec_ctx,
                                                   pg_depend,
                                                   std::move(dep_row));
                if (auto rng = co_await std::move(dcf); rng.count > 0) {
                    ctx->pg_catalog_appends.push_back(std::move(rng));
                }
            }
        }

        mark_executed();
    }

} // namespace components::operators
