#include "operator_resolve_namespace.hpp"

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

    operator_resolve_namespace_t::operator_resolve_namespace_t(std::pmr::memory_resource* resource,
                                                               log_t log,
                                                               components::logical_plan::node_catalog_resolve_t* node)
        // operator_type::resolve_namespace tags the leaf for downstream
        // consumers; like operator_get_schema, the executor's generic
        // pipeline drives this operator via await_async_and_resume.
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_namespace)
        , node_(node)
        , output_schema_(resource) {
        output_schema_.emplace_back(types::logical_type::UINTEGER);
        output_schema_.back().set_alias("namespace_oid");
    }

    actor_zeta::unique_future<void> operator_resolve_namespace_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgNamespace = catalog::well_known_oid::pg_namespace_table;

        // No disk wired (rare — some test harnesses). Leave every entry
        // unresolved and mark executed so the pipeline doesn't stall.
        if (ctx->disk_address != actor_zeta::address_t::empty_address()) {
            components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

            for (auto& entry : node_->entries()) {
                // pg_namespace schema: [oid (uint32), nspname (string)]. Filter on
                // nspname via the generic read_chunks_by_key actor message — a pure
                // storage primitive.
                std::pmr::vector<std::uint64_t> ns_keys(resource_);
                ns_keys.emplace_back(catalog::pg_namespace_col::nspname);
                auto [_ns, nsf] = actor_zeta::otterbrix::send(
                    ctx->disk_address,
                    &services::disk::manager_disk_t::read_chunks_by_key,
                    exec_ctx,
                    kPgNamespace,
                    std::move(ns_keys),
                    components::operators::make_key_chunk(resource_, std::string_view{entry.dbname}),
                    std::pmr::vector<std::uint64_t>{resource_});
                auto ns_batches_r = co_await std::move(nsf);
                if (ns_batches_r.has_error()) {
                    // A failed pg_namespace read is not a miss; saying "not found" here hides it.
                    set_error(ns_batches_r.error());
                    co_return;
                }
                auto& ns_batches = ns_batches_r.value();

                // First row's col 0 = namespace_oid. Mirrors
                // manager_disk_t::resolve_namespace (manager_disk_resolve.cpp),
                // which returns the first match. A miss leaves the entry at
                // INVALID_OID — that is how "namespace does not exist" is reported.
                if (!ns_batches.empty() && ns_batches[0].size() != 0 && ns_batches[0].column_count() >= 1 &&
                    !ns_batches[0].is_null(0, 0)) {
                    entry.namespace_oid = static_cast<catalog::oid_t>(ns_batches[0].get_value<std::uint32_t>(0, 0));
                }
            }
        }

        // 0-row sink output: the resolved data lives in the node's entries.
        output_ = make_operator_data(resource_, output_schema_, 0);
        mark_executed();
    }

} // namespace components::operators
