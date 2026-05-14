#include "operator_alter_column_add.hpp"

#include <set>
#include <vector>

#include <components/catalog/ddl_metadata_builder.hpp>
#include <components/catalog/system_table_schemas.hpp>
#include <components/context/context.hpp>
#include <services/disk/manager_disk.hpp>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_alter_column_add_t::operator_alter_column_add_t(
        std::pmr::memory_resource* resource,
        log_t                       log,
        catalog::oid_t              table_oid,
        components::table::column_definition_t column)
        : read_write_operator_t(resource, std::move(log), operator_type::alter_column_add)
        , table_oid_(table_oid)
        , column_(std::move(column)) {}

    void operator_alter_column_add_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        async_wait();
    }

    actor_zeta::unique_future<void>
    operator_alter_column_add_t::await_async_and_resume(pipeline::context_t* ctx) {
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Step 1: scan pg_attribute for max(attnum) for this table.
        constexpr catalog::oid_t pg_attr_oid = catalog::well_known_oid::pg_attribute_table;
        components::types::logical_value_t toid_lv(resource_, table_oid_);
        auto [_pa, paf] = actor_zeta::send(
            ctx->disk_address,
            &services::disk::manager_disk_t::read_rows_by_key,
            exec_ctx, pg_attr_oid,
            std::vector<std::string>{"attrelid"},
            std::vector<components::types::logical_value_t>{toid_lv});
        auto attr_rows = co_await std::move(paf);
        std::int32_t next_attnum = 1;
        for (const auto& row : attr_rows) {
            if (row.size() < 5 || row[4].is_null()) continue;
            auto n = row[4].value<std::int32_t>();
            if (n >= next_attnum) next_attnum = n + 1;
        }

        // Step 2: allocate attoid.
        auto [_oa, oaf] = actor_zeta::send(
            ctx->disk_address,
            &services::disk::manager_disk_t::allocate_oids_batch,
            std::size_t{1});
        catalog::oid_batch_t att_batch;
        att_batch.oids = co_await std::move(oaf);
        const catalog::oid_t attoid = att_batch.allocate();

        // Step 3: build + write pg_attribute row.
        const std::string typspec = catalog::encode_type_spec(column_.type());
        const std::string defspec = column_.has_default_value()
                                        ? catalog::encode_default_spec(column_.default_value())
                                        : std::string{};
        const catalog::oid_t atttypid = (column_.atttypid() != catalog::INVALID_OID)
                                            ? column_.atttypid()
                                            : catalog::builtin_type_to_oid(column_.type().type());
        auto att_row = catalog::build_pg_attribute_row(
            resource_, attoid, table_oid_, std::string(column_.name()),
            atttypid, next_attnum,
            column_.is_not_null(), column_.has_default_value(),
            /*is_dropped=*/false, typspec, defspec);
        auto [_w, wf] = actor_zeta::send(
            ctx->disk_address,
            &services::disk::manager_disk_t::append_pg_catalog_row,
            exec_ctx, pg_attr_oid, std::move(att_row));
        auto rng = co_await std::move(wf);
        if (rng.count > 0) ctx->pg_catalog_appends.push_back(std::move(rng));

        // resolve_table rebuilds columns from pg_attribute on each call, so
        // subsequent statements see the new column. A DML in the same txn
        // would need a fresh resolve to refresh its plan-tree metadata.
        mark_executed();
    }

} // namespace components::operators
