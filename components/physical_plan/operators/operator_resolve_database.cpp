#include "operator_resolve_database.hpp"

#include "catalog_write_helpers.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
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

namespace components::operators {

    namespace catalog = components::catalog;

    operator_resolve_database_t::operator_resolve_database_t(std::pmr::memory_resource* resource,
                                                             log_t log,
                                                             components::logical_plan::node_catalog_resolve_t* node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_database)
        , node_(node)
        , output_schema_(resource) {
        output_schema_.emplace_back(types::logical_type::UINTEGER);
        output_schema_.back().set_alias("database_oid");
    }

    actor_zeta::unique_future<void> operator_resolve_database_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgDatabase = catalog::well_known_oid::pg_database_table;

        // No disk wired (rare — some test harnesses). Leave every entry
        // unresolved and mark executed so the pipeline doesn't stall.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

            for (auto& entry : node_->entries()) {
                // Look up pg_database by datname.
                std::pmr::vector<std::uint64_t> db_keys(resource_);
                db_keys.emplace_back(catalog::pg_database_col::datname);
                auto [_db, dbf] = actor_zeta::otterbrix::send(
                    ctx->disk_address,
                    &services::disk::manager_disk_t::read_chunks_by_key,
                    exec_ctx,
                    kPgDatabase,
                    std::move(db_keys),
                    components::operators::make_key_chunk(resource_, std::string_view{entry.dbname}),
                    std::pmr::vector<std::uint64_t>{resource_});
                auto db_batches_r = co_await std::move(dbf);
                if (db_batches_r.has_error()) {
                    // A failed pg_database read is not a miss; saying "not found" here hides it.
                    set_error(db_batches_r.error());
                    co_return;
                }
                auto& db_batches = db_batches_r.value();

                // A miss leaves the entry at INVALID_OID — that is how "database
                // does not exist" is reported.
                if (!db_batches.empty() && db_batches[0].size() != 0 && db_batches[0].column_count() >= 1 &&
                    !db_batches[0].is_null(0, 0)) {
                    entry.database_oid = static_cast<catalog::oid_t>(db_batches[0].get_value<std::uint32_t>(0, 0));
                }
            }
        }

        // 0-row sink output: the resolved data lives in the node's entries.
        output_ = make_operator_data(resource_, output_schema_, 0);
        mark_executed();
    }

} // namespace components::operators
