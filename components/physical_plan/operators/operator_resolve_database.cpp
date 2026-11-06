#include "operator_resolve_database.hpp"

#include "catalog_name_lookup.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/catalog/helpers.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve.hpp>
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
        // No disk wired (rare — some test harnesses). Leave every entry
        // unresolved and mark executed so the pipeline doesn't stall.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            for (auto& entry : node_->entries()) {
                if (entry.database_oid != catalog::INVALID_OID) {
                    continue;
                }
                // A miss leaves the entry at INVALID_OID — that is how "database does not exist" is reported.
                auto found = co_await lookup_database_oid(resource_, ctx, entry.connection, entry.database);
                if (found.has_error()) {
                    // A failed pg_database read is not a miss; saying "not found" here hides it.
                    set_error(found.error());
                    co_return;
                }
                entry.database_oid = found.value();
            }
        }

        // 0-row sink output: the resolved data lives in the node's entries.
        output_ = make_operator_data(resource_, output_schema_, 0);
        mark_executed();
    }

} // namespace components::operators
