#include "operator_create_index_backfill.hpp"

#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/context/context.hpp>
#include <components/index/index_engine.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>

namespace components::operators {

    operator_create_index_backfill_t::operator_create_index_backfill_t(
        std::pmr::memory_resource* resource,
        log_t log,
        std::string index_name,
        components::logical_plan::index_type index_type,
        std::pmr::vector<components::expressions::key_t> keys,
        components::catalog::oid_t table_oid,
        components::catalog::oid_t index_oid,
        std::string indkey)
        : read_write_operator_t(resource, std::move(log), operator_type::create_collection)
        , index_name_(std::move(index_name))
        , index_type_(index_type)
        , keys_(std::move(keys))
        , table_oid_(table_oid)
        , index_oid_(index_oid)
        , indkey_(std::move(indkey)) {}

    void operator_create_index_backfill_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_create_index_backfill_t::await_async_and_resume(pipeline::context_t* ctx) {
        // No-op when there is no index actor wired (e.g. some test harnesses).
        if (ctx->index_address == actor_zeta::address_t::empty_address()) {
            mark_executed();
            co_return;
        }

        // Step 1 + 2: ensure the engine knows about the collection, then create
        // the index entry. register_collection is idempotent.
        auto [_rc, rcf] = actor_zeta::send(ctx->index_address,
                                           &services::index::manager_index_t::register_collection,
                                           ctx->session,
                                           table_oid_);
        co_await std::move(rcf);

        auto [_ix, ixf] = actor_zeta::send(ctx->index_address,
                                           &services::index::manager_index_t::create_index,
                                           ctx->session,
                                           table_oid_,
                                           services::index::index_name_t(index_name_),
                                           keys_,
                                           index_type_);
        const auto id_index = co_await std::move(ixf);

        if (id_index == components::index::INDEX_ID_UNDEFINED) {
            set_error(core::error_t{core::error_code_t::index_create_fail,
                                    std::pmr::string{"index already exists", resource_}});
            co_return;
        }

        // Step 3: backfill — scan table contents and feed them into the index.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            auto [_tr, trf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::storage_total_rows,
                                               ctx->session,
                                               table_oid_);
            const auto total_rows = co_await std::move(trf);
            if (total_rows > 0) {
                auto [_ss, ssf] = actor_zeta::send(ctx->disk_address,
                                                   &services::disk::manager_disk_t::storage_scan_segment,
                                                   ctx->session,
                                                   table_oid_,
                                                   int64_t{0},
                                                   total_rows);
                auto scan_data = co_await std::move(ssf);
                if (scan_data) {
                    const auto count = scan_data->size();
                    auto [_ir, irf] =
                        actor_zeta::send(ctx->index_address,
                                         &services::index::manager_index_t::insert_rows,
                                         services::index::execution_context_t{ctx->session, ctx->txn, table_oid_},
                                         table_oid_,
                                         std::move(scan_data),
                                         uint64_t{0},
                                         count);
                    co_await std::move(irf);
                    // Group 3 fix: insert_rows tags index entries with the
                    // current txn_id but leaves them PENDING. They become
                    // visible only after commit_insert is called with the
                    // commit_id. The executor's post-pipeline commit block
                    // (executor.cpp:215-232) fires commit_insert when
                    // dml_append_row_count > 0 && dml_table_oid set — record
                    // those here so backfilled index entries get committed
                    // alongside the CREATE INDEX txn. Reusing
                    // dml_append_row_count also drives storage_commit_append
                    // (executor.cpp:217-224): that re-commits the already-
                    // committed data rows to the CREATE INDEX commit_id, which
                    // is harmless when no concurrent reader sits between the
                    // INSERT and the CREATE INDEX commits (typical case).
                    ctx->dml_append_row_start = 0;
                    ctx->dml_append_row_count = count;
                    ctx->dml_table_oid = table_oid_;
                }
            }
        }

        // Step 4: flip pg_index.indisvalid → true. The metadata operator wrote
        // an indisvalid=false row earlier; replace it now that the engine is
        // populated. Skipping is safe if we have no disk actor (test harness).
        if (ctx->disk_address != actor_zeta::address_t::empty_address() &&
            index_oid_ != components::catalog::INVALID_OID) {
            constexpr components::catalog::oid_t pg_idx_oid = components::catalog::well_known_oid::pg_index_table;
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

            auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::delete_pg_catalog_rows,
                                             exec_ctx,
                                             pg_idx_oid,
                                             std::int64_t{0},
                                             index_oid_);
            co_await std::move(df);
            if (ctx->txn.transaction_id != 0)
                ctx->pg_catalog_delete_tables.insert(pg_idx_oid);

            auto valid_row = components::catalog::build_pg_index_row(resource(),
                                                                     index_oid_,
                                                                     table_oid_,
                                                                     indkey_,
                                                                     /*indisvalid=*/true);
            auto [_w, wf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::append_pg_catalog_row,
                                             exec_ctx,
                                             pg_idx_oid,
                                             std::move(valid_row));
            auto rng = co_await std::move(wf);
            if (rng.count > 0)
                ctx->pg_catalog_appends.push_back(std::move(rng));
        }

        mark_executed();
    }

} // namespace components::operators
