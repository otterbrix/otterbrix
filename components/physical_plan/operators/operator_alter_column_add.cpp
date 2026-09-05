#include "operator_alter_column_add.hpp"

#include <vector>

#include "alter_validators.hpp"

#include <components/catalog/alter_column_validators.hpp>
#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/helpers.hpp>
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

    actor_zeta::unique_future<void> operator_alter_column_add_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Pre-execute validation: any failure co_returns an error cursor BEFORE
        // the first catalog mutation below, so a rejected ALTER leaves no trace.
        auto vc_fut = alter_validators::visible_column_names(resource_, ctx->disk_address, exec_ctx, table_oid_);
        auto visible_column_names_r = co_await std::move(vc_fut);
        if (visible_column_names_r.has_error()) {
            // The duplicate-column check below cannot run on a read that failed;
            // passing an empty list would silently approve the ALTER.
            set_error(visible_column_names_r.error());
            co_return;
        }
        auto& visible_column_names = visible_column_names_r.value();
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
            // A failed pg_attribute read is not a miss; treating it as one lets the
            // operation proceed on data that was never read.
            set_error(attr_batches_r.error());
            co_return;
        }
        auto& attr_batches = attr_batches_r.value();
        std::int32_t next_attnum = 1;
        for (auto& chunk : attr_batches) {
            if (chunk.column_count() < 5)
                continue;
            for (uint64_t i = 0; i < chunk.size(); ++i) {
                if (chunk.is_null(4, i))
                    continue;
                auto n = chunk.get_value<std::int32_t>(4, i);
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
        std::string defspec;
        if (column_.has_default_value()) {
            // Rule 6: a DEFAULT that cannot be persisted fails the ALTER here, before any
            // catalog row is written — never a column that claims a default it has lost.
            if (auto ec_spec = catalog::encode_default_spec(resource_, column_.default_value(), defspec);
                ec_spec.contains_error()) {
                set_error(std::move(ec_spec));
                co_return;
            }
        }
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
                components::pg_attribute_commit_id_backfill_t::kind_t::added_at,
                // RN-oid: an added_at marker DOES carry a second half now — not a release and
                // not a rename, but the delivery of this column's IDENTITY to the storage that
                // will materialise it. ALTER TABLE ADD COLUMN writes pg_attribute and stops;
                // the storage column appears later, out of the first INSERT that carries it,
                // on an agent that cannot read pg_attribute. Naming the table and the attname
                // here is what lets manager_disk_t::update_pg_attribute_commit_id_fields park
                // this attoid on the owning agent, so the materialised column is born with it
                // instead of with a 0 the bootstrap reconciliation would have to refuse.
                table_oid_,
                std::string(column_.name()),
                std::string{},
                // ...and the column's TYPE with it. Until an INSERT materialises the column, the
                // storage still has to ANSWER it — pg_attribute shows it from this statement on,
                // so `SELECT <it>` is a legal query with a legal answer (NULL in every existing
                // row) and the agent has no catalog to type it from.
                column_.type()});
        }

        // resolve_table rebuilds columns from pg_attribute on each call, so
        // subsequent statements see the new column. A DML in the same txn
        // would need a fresh resolve to refresh its plan-tree metadata.
        mark_executed();
    }

} // namespace components::operators
