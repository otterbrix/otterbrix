#include "operator_resolve_database.hpp"

#include "catalog_write_helpers.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_buffer.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    operator_resolve_database_t::operator_resolve_database_t(std::pmr::memory_resource* resource,
                                                             log_t log,
                                                             std::string name)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_database)
        , name_(std::move(name))
        , output_schema_(resource) {
        // Single-column UINTEGER (database_oid) schema, built once (TASK C10).
        // Always emitted, even on miss, so downstream sees a consistent schema.
        output_schema_.emplace_back(types::logical_type::UINTEGER);
        output_schema_.back().set_alias("database_oid");
    }

    operator_resolve_database_t::operator_resolve_database_t(
        std::pmr::memory_resource* resource,
        log_t log,
        std::string name,
        components::logical_plan::node_catalog_resolve_t* target_node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_database)
        , name_(std::move(name))
        , target_node_(target_node)
        , output_schema_(resource) {
        output_schema_.emplace_back(types::logical_type::UINTEGER);
        output_schema_.back().set_alias("database_oid");
    }

    actor_zeta::unique_future<void> operator_resolve_database_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgDatabase = catalog::well_known_oid::pg_database_table;

        // Output chunk built from the constructor-cached schema (TASK C10).
        // Always emitted, even on miss, so downstream sees a consistent schema.
        vector::data_chunk_t out_chunk(resource_, output_schema_);

        if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
            out_chunk.set_cardinality(0);
            set_output(make_operator_data(resource_, std::move(out_chunk)));
            mark_executed();
            co_return;
        }

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Look up pg_database by datname.
        std::pmr::vector<std::string> db_keys(resource_);
        db_keys.emplace_back("datname");
        auto [_db, dbf] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::read_chunks_by_key,
                                           exec_ctx,
                                           kPgDatabase,
                                           std::move(db_keys),
                                           components::operators::make_key_chunk(resource_, std::string_view{name_}));
        auto db_batches = co_await std::move(dbf);

        bool resolved = false;
        if (!db_batches.empty() && db_batches[0].size() != 0 && db_batches[0].column_count() >= 1) {
            if (!db_batches[0].is_null(0, 0)) {
                const auto oid_val = static_cast<catalog::oid_t>(db_batches[0].get_value<std::uint32_t>(0, 0));
                out_chunk.set_cardinality(1);
                set_uint32(out_chunk, 0, 0, static_cast<std::uint32_t>(oid_val));
                if (target_node_) {
                    target_node_->set_database_oid(oid_val);
                }
                resolved = true;
            }
        }
        if (!resolved) {
            out_chunk.set_cardinality(0);
        }

        set_output(make_operator_data(resource_, std::move(out_chunk)));
        mark_executed();
    }

} // namespace components::operators
