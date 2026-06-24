#include "operator_resolve_namespace.hpp"

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

    operator_resolve_namespace_t::operator_resolve_namespace_t(std::pmr::memory_resource* resource,
                                                               log_t log,
                                                               std::string name)
        // operator_type::resolve_namespace tags the leaf for downstream
        // consumers; like operator_get_schema, the executor's generic
        // pipeline drives this operator via await_async_and_resume.
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_namespace)
        , name_(std::move(name))
        , output_schema_(resource) {
        // Static single-column output schema, built once here (TASK C10).
        // We always emit a chunk (zero rows on miss, one row on hit) so
        // downstream operators rely on a consistent schema.
        output_schema_.emplace_back(types::logical_type::UINTEGER);
        output_schema_.back().set_alias("namespace_oid");
    }

    operator_resolve_namespace_t::operator_resolve_namespace_t(
        std::pmr::memory_resource* resource,
        log_t log,
        std::string name,
        components::logical_plan::node_catalog_resolve_t* target_node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_namespace)
        , name_(std::move(name))
        , target_node_(target_node)
        , output_schema_(resource) {
        output_schema_.emplace_back(types::logical_type::UINTEGER);
        output_schema_.back().set_alias("namespace_oid");
    }

    void operator_resolve_namespace_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // All work is async (single pg_namespace read). Defer to
        // await_async_and_resume — matches operator_get_schema's pattern.
        async_wait();
    }

    actor_zeta::unique_future<void> operator_resolve_namespace_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgNamespace = catalog::well_known_oid::pg_namespace_table;

        // Output chunk built from the constructor-cached schema (TASK C10). We
        // always emit a chunk (zero rows on miss, one row on hit) so downstream
        // operators can rely on a consistent schema regardless of outcome.
        vector::data_chunk_t out_chunk(resource_, output_schema_);

        if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
            // No disk wired (rare — some test harnesses). Emit empty chunk and
            // mark executed so the pipeline doesn't stall.
            out_chunk.set_cardinality(0);
            set_output(make_operator_data(resource_, std::move(out_chunk)));
            mark_executed();
            co_return;
        }

        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // pg_namespace schema: [oid (uint32), nspname (string)].
        // Filter on nspname == name_ via the generic read_chunks_by_key actor
        // message — pure storage primitive.
        std::pmr::vector<std::string> ns_keys(resource_);
        ns_keys.emplace_back("nspname");
        auto [_ns, nsf] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::read_chunks_by_key,
                                           exec_ctx,
                                           kPgNamespace,
                                           std::move(ns_keys),
                                           components::operators::make_key_chunk(resource_, std::string_view{name_}));
        auto ns_batches = co_await std::move(nsf);

        bool resolved = false;
        if (!ns_batches.empty() && ns_batches[0].size() != 0 && ns_batches[0].column_count() >= 1) {
            // First row's col 0 = namespace_oid. Mirrors
            // manager_disk_t::resolve_namespace (manager_disk_resolve.cpp:9-32)
            // which returns the first match.
            if (auto ns_oid_v = ns_batches[0].get_value<std::uint32_t>(0, 0)) {
                const auto oid_val = static_cast<catalog::oid_t>(*ns_oid_v);
                out_chunk.set_cardinality(1);
                set_uint32(out_chunk, 0, 0, static_cast<std::uint32_t>(oid_val));
                // Stamp the resolved oid onto the logical-plan node so the
                // dispatcher's Pass 2 (validate / enrich / planner) can read
                // it via plan_resolve_index_t.
                if (target_node_) {
                    target_node_->set_namespace_oid(oid_val);
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
