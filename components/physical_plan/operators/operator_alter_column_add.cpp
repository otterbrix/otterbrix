#include "operator_alter_column_add.hpp"

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

    operator_alter_column_add_t::operator_alter_column_add_t(std::pmr::memory_resource* resource,
                                                             log_t log,
                                                             catalog::oid_t table_oid,
                                                             components::table::column_definition_t column)
        : read_write_operator_t(resource, std::move(log), operator_type::alter_column_add)
        , table_oid_(table_oid)
        , column_(std::move(column)) {}

    void operator_alter_column_add_t::on_execute_impl(pipeline::context_t* /*ctx*/) { async_wait(); }

    actor_zeta::unique_future<void> operator_alter_column_add_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Pre-execute validation: any failure co_returns an error cursor BEFORE
        // the first catalog mutation below, so a rejected ALTER leaves no trace.
        auto vc_fut = alter_validators::visible_column_names(resource_, ctx->disk_address, exec_ctx, table_oid_);
        auto visible_column_names = co_await std::move(vc_fut);
        auto ec_dup =
            components::catalog::alter_column_validators::validate_column_not_duplicate(resource_,
                                                                                        visible_column_names,
                                                                                        std::string(column_.name()));
        if (ec_dup.contains_error()) {
            set_error(std::move(ec_dup));
            co_return;
        }

        auto ec_type =
            components::catalog::alter_column_validators::validate_default_value_type(resource_,
                                                                                      column_.type(),
                                                                                      column_.default_value_opt());
        if (ec_type.contains_error()) {
            set_error(std::move(ec_type));
            co_return;
        }

        auto ec_eval = components::catalog::alter_column_validators::validate_default_value_evaluatable(
            resource_,
            column_.default_value_opt());
        if (ec_eval.contains_error()) {
            set_error(std::move(ec_eval));
            co_return;
        }

        // scan pg_attribute for max(attnum) for this table.
        constexpr catalog::oid_t pg_attr_oid = catalog::well_known_oid::pg_attribute_table;
        components::types::logical_value_t toid_lv(resource_, table_oid_);
        std::pmr::vector<std::string> pa_keys(resource_);
        pa_keys.emplace_back("attrelid");
        std::pmr::vector<components::types::logical_value_t> pa_vals(resource_);
        pa_vals.emplace_back(toid_lv);
        auto [_pa, paf] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::read_chunks_by_key,
                                           exec_ctx,
                                           pg_attr_oid,
                                           std::move(pa_keys),
                                           components::operators::make_key_chunk(resource_, std::move(pa_vals)));
        std::pmr::vector<components::vector::data_chunk_t> attr_batches = co_await std::move(paf);
        std::int32_t next_attnum = 1;
        for (auto& chunk : attr_batches) {
            if (chunk.column_count() < 5)
                continue;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                auto attnum_cell = chunk.value(4, i);
                if (attnum_cell.is_null())
                    continue;
                auto n = attnum_cell.value<std::int32_t>();
                if (n >= next_attnum)
                    next_attnum = n + 1;
            }
        }

        auto [_oa, oaf] =
            actor_zeta::send(ctx->disk_address, &services::disk::manager_disk_t::allocate_oids_batch, std::size_t{1});
        catalog::oid_batch_t att_batch;
        att_batch.oids = co_await std::move(oaf);
        const catalog::oid_t attoid = att_batch.allocate();

        const std::string typspec = catalog::encode_type_spec(column_.type());
        const std::string defspec =
            column_.has_default_value() ? catalog::encode_default_spec(column_.default_value()) : std::string{};
        const catalog::oid_t atttypid = (column_.atttypid() != catalog::INVALID_OID)
                                            ? column_.atttypid()
                                            : catalog::builtin_type_to_oid(column_.type().type());
        // commit_id columns are placeholder-0; a backfill marker (below) patches
        // them post-commit, since the commit_id isn't allocated until COMMIT
        // (see pg_catalog_swap.hpp). The row's own MVCC insert_id is still the
        // executing txn_id, so even pre-backfill it filters correctly.
        auto att_row = catalog::build_pg_attribute_row(resource_,
                                                       attoid,
                                                       table_oid_,
                                                       std::string(column_.name()),
                                                       atttypid,
                                                       next_attnum,
                                                       column_.is_not_null(),
                                                       column_.has_default_value(),
                                                       /*is_dropped=*/false,
                                                       typspec,
                                                       defspec,
                                                       /*added_at_commit_id=*/0,
                                                       /*dropped_at_commit_id=*/0);
        auto [_w, wf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::append_pg_catalog_row,
                                         exec_ctx,
                                         pg_attr_oid,
                                         std::move(att_row));
        auto rng = co_await std::move(wf);
        if (rng.count > 0) {
            ctx->pg_catalog_appends.push_back(std::move(rng));
            // Backfill added_at_commit_id on this row, keyed by attoid.
            ctx->pg_attribute_commit_id_backfills.push_back(components::pg_attribute_commit_id_backfill_t{
                attoid,
                components::pg_attribute_commit_id_backfill_t::kind_t::added_at});
        }

        // resolve_table rebuilds columns from pg_attribute on each call, so
        // subsequent statements see the new column. A DML in the same txn
        // would need a fresh resolve to refresh its plan-tree metadata.
        mark_executed();
    }

} // namespace components::operators
