#include "operator_register_cast.hpp"

#include "single_oid_round.hpp"

#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <utility>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_register_cast_t::operator_register_cast_t(std::pmr::memory_resource* resource,
                                                       log_t log,
                                                       catalog::oid_t source_type_oid,
                                                       catalog::oid_t target_type_oid)
        : read_only_operator_t(resource, std::move(log), operator_type::register_cast)
        , source_type_oid_(source_type_oid)
        , target_type_oid_(target_type_oid) {}

    actor_zeta::unique_future<void> operator_register_cast_t::await_async_and_resume(pipeline::context_t* ctx) {
        success_ = false;

        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

            auto [_oa, oaf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::allocate_oids_batch,
                                               std::size_t{1});
            auto allocated = co_await std::move(oaf);
            // The identity is minted BEFORE the pg_cast row is built. A round that delivered
            // nothing used to be consumed anyway: allocate() answers INVALID_OID, and the
            // pg_cast row went out stamped with it — a durable cast with no identity, reported
            // as a successful CREATE CAST. find_cast_oid then reads that 0 back as "there is
            // no such cast", so the row is unreachable AND undeletable.
            catalog::oid_t cast_oid = catalog::INVALID_OID;
            if (auto ec_oid = single_oid_from_round(resource_, std::move(allocated), "register_cast", cast_oid);
                ec_oid.contains_error()) {
                set_error(std::move(ec_oid));
                mark_failed();
                co_return;
            }

            auto writes = catalog::build_create_cast_writes(resource_, cast_oid, source_type_oid_, target_type_oid_);
            std::pmr::vector<actor_zeta::unique_future<core::result_wrapper_t<components::pg_catalog_append_range_t>>>
                write_futures(resource_);
            write_futures.reserve(writes.size());
            for (auto& w : writes) {
                auto [_w, wf] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::append_pg_catalog_row,
                                                 exec_ctx,
                                                 w.table_oid,
                                                 std::move(w.row));
                write_futures.push_back(std::move(wf));
            }
            // Drain all, first error wins: an unwritten pg_cast row is a cast that does not
            // exist, and CREATE CAST must say so instead of reporting success.
            core::error_t append_error = core::error_t::no_error();
            for (auto& wf : write_futures) {
                auto rng_r = co_await std::move(wf);
                if (rng_r.has_error()) {
                    if (!append_error.contains_error()) {
                        append_error = rng_r.error();
                    }
                    continue;
                }
                if (rng_r.value().count > 0) {
                    ctx->pg_catalog_appends.push_back(std::move(rng_r.value()));
                }
            }
            if (append_error.contains_error()) {
                set_error(std::move(append_error));
                mark_failed();
                co_return;
            }
        }

        success_ = true;
        output_ = nullptr;
        mark_executed();
    }

    operator_unregister_cast_t::operator_unregister_cast_t(std::pmr::memory_resource* resource,
                                                           log_t log,
                                                           catalog::oid_t source_type_oid,
                                                           catalog::oid_t target_type_oid)
        : read_only_operator_t(resource, std::move(log), operator_type::unregister_cast)
        , source_type_oid_(source_type_oid)
        , target_type_oid_(target_type_oid) {}

    actor_zeta::unique_future<void> operator_unregister_cast_t::await_async_and_resume(pipeline::context_t* ctx) {
        success_ = false;

        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

            auto [_rc, rcf] = actor_zeta::send(ctx->disk_address,
                                               &services::disk::manager_disk_t::find_cast_oid,
                                               exec_ctx,
                                               source_type_oid_,
                                               target_type_oid_);
            auto cast_oid_r = co_await std::move(rcf);
            if (cast_oid_r.has_error()) {
                // "the pg_cast read failed" and "there is no such pg_cast row" are different
                // accidents; the INVALID_OID branch below is only allowed to speak for the
                // second one.
                set_error(cast_oid_r.error());
                mark_failed();
                co_return;
            }
            const catalog::oid_t cast_oid = cast_oid_r.value();
            if (cast_oid == catalog::INVALID_OID) {
                // "there is no pg_cast row to delete" is a different accident from "the delete
                // failed", and DROP CAST has to be able to say which one happened.
                set_error(core::error_t{core::error_code_t::do_not_exists,
                                        std::pmr::string{"unregister_cast: no pg_cast row exists for this "
                                                         "(source, target) pair",
                                                         resource_}});
                mark_failed();
                co_return;
            }

            constexpr catalog::oid_t pg_cast_coll = catalog::well_known_oid::pg_cast_table;
            constexpr catalog::oid_t pg_depend_coll = catalog::well_known_oid::pg_depend_table;
            std::pmr::vector<services::disk::pg_catalog_delete_spec_t> specs(resource_);
            specs.push_back({pg_cast_coll, std::int64_t{0}, cast_oid});   // pg_cast.oid
            specs.push_back({pg_depend_coll, std::int64_t{1}, cast_oid}); // pg_depend.objid
            if (ctx->txn.transaction_id != 0) {
                ctx->pg_catalog_delete_tables.insert(pg_cast_coll);
                ctx->pg_catalog_delete_tables.insert(pg_depend_coll);
            }
            auto [_d, df] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::delete_pg_catalog_rows_many,
                                             exec_ctx,
                                             std::move(specs));
            co_await std::move(df);
        }

        success_ = true;
        output_ = nullptr;
        mark_executed();
    }

} // namespace components::operators