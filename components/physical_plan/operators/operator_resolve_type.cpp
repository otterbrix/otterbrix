#include "operator_resolve_type.hpp"

#include <components/catalog/catalog_codes.hpp>
#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;
    namespace types   = components::types;

    operator_resolve_type_t::operator_resolve_type_t(std::pmr::memory_resource* resource,
                                                       log_t                      log,
                                                       catalog::oid_t             namespace_oid,
                                                       std::string                name)
        // operator_type::resolve_type tags the leaf for downstream
        // consumers; like operator_resolve_namespace_t, the executor's
        // generic pipeline drives this operator via await_async_and_resume.
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_type)
        , namespace_oid_(namespace_oid)
        , name_(std::move(name)) {}

    void operator_resolve_type_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // All work is a single async pg_type read; defer to await_async_and_resume.
        async_wait();
    }

    actor_zeta::unique_future<void>
    operator_resolve_type_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgType = catalog::well_known_oid::pg_type_table;

        // Build output schema: oid, typname, typnamespace, typdefspec.
        // Order/types mirror pg_type_columns() in system_table_schemas.cpp so
        // callers can index by column position.
        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.reserve(4);
        out_types.emplace_back(types::logical_type::UINTEGER);       // oid
        out_types.emplace_back(types::logical_type::STRING_LITERAL); // typname
        out_types.emplace_back(types::logical_type::UINTEGER);       // typnamespace
        out_types.emplace_back(types::logical_type::STRING_LITERAL); // typdefspec

        // Issue the pg_type lookup keyed by (typname, typnamespace). Returns
        // zero rows when no match — surfaced as an empty output chunk so the
        // caller can distinguish "not found" from a structural error.
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        types::logical_value_t name_lv(resource_, std::string_view{name_});
        types::logical_value_t ns_lv(resource_, namespace_oid_);

        auto [_t, tf] = actor_zeta::send(
            ctx->disk_address,
            &services::disk::manager_disk_t::read_rows_by_key,
            exec_ctx, kPgType,
            std::vector<std::string>{"typname", "typnamespace"},
            std::vector<types::logical_value_t>{name_lv, ns_lv});
        auto rows = co_await std::move(tf);

        // Single-row output (or empty). Allocate with capacity for one row so
        // callers reading column_count() see all four typed slots even when
        // the lookup miss yields zero cardinality.
        vector::data_chunk_t chunk(resource_, out_types, /*capacity=*/1);

        if (!rows.empty() && rows.front().size() >= 4) {
            const auto& row = rows.front();

            // Column 0: oid — required; treat null defensively (skip emit).
            // Column 1: typname — required.
            // Column 2: typnamespace — required.
            // Column 3: typdefspec — optional (empty string for builtin
            //                         scalars; populated for STRUCT/ENUM/UDT).
            if (!row[0].is_null() && !row[1].is_null() && !row[2].is_null()) {
                chunk.set_value(0, 0, row[0]);
                chunk.set_value(1, 0, row[1]);
                chunk.set_value(2, 0, row[2]);
                if (!row[3].is_null()) {
                    chunk.set_value(3, 0, row[3]);
                } else {
                    chunk.set_value(3, 0, types::logical_value_t(resource_, std::string{}));
                }
                chunk.set_cardinality(1);
            }
        }

        set_output(make_operator_data(resource_, std::move(chunk)));
        mark_executed();
    }

} // namespace components::operators
