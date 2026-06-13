#include "operator_resolve_namespace.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/logical_plan/node_catalog_resolve_namespace.hpp>
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

    namespace {
        // File-local typed setter — mirrors the ddl_metadata_builder.cpp helpers.
        // Writes directly into the column's typed buffer at (col, row), avoiding
        // a logical_value_t variant round-trip when both sides are known to be
        // uint32_t at compile time.
        inline void set_uint32(vector::data_chunk_t& c, size_t col, size_t row, std::uint32_t v) {
            auto& vec = c.data[col];
            vec.template data<std::uint32_t>()[row] = v;
            vec.validity().set(row, true);
        }
    } // namespace

    operator_resolve_namespace_t::operator_resolve_namespace_t(std::pmr::memory_resource* resource,
                                                               log_t log,
                                                               std::string name)
        // operator_type::resolve_namespace tags the leaf for downstream
        // consumers; like operator_get_schema, the executor's generic
        // pipeline drives this operator via await_async_and_resume.
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_namespace)
        , name_(std::move(name)) {}

    operator_resolve_namespace_t::operator_resolve_namespace_t(
        std::pmr::memory_resource* resource,
        log_t log,
        std::string name,
        components::logical_plan::node_catalog_resolve_namespace_t* target_node)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_namespace)
        , name_(std::move(name))
        , target_node_(target_node) {}

    void operator_resolve_namespace_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // All work is async (single pg_namespace read). Defer to
        // await_async_and_resume — matches operator_get_schema's pattern.
        async_wait();
    }

    actor_zeta::unique_future<void> operator_resolve_namespace_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgNamespace = catalog::well_known_oid::pg_namespace_table;

        // Build the single-column output chunk schema up-front. We always emit
        // a chunk (zero rows on miss, one row on hit) so downstream operators
        // can rely on a consistent schema regardless of resolution outcome.
        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.emplace_back(types::logical_type::UINTEGER);
        out_types.back().set_alias("namespace_oid");

        vector::data_chunk_t out_chunk(resource_, out_types);

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
        types::logical_value_t name_lv(resource_, std::string_view{name_});
        std::pmr::vector<std::string> ns_keys(resource_);
        ns_keys.emplace_back("nspname");
        std::pmr::vector<types::logical_value_t> ns_vals(resource_);
        ns_vals.emplace_back(name_lv);
        auto [_ns, nsf] = actor_zeta::send(ctx->disk_address,
                                           &services::disk::manager_disk_t::read_chunks_by_key,
                                           exec_ctx,
                                           kPgNamespace,
                                           std::move(ns_keys),
                                           components::operators::make_key_chunk(resource_, std::move(ns_vals)));
        auto ns_batches = co_await std::move(nsf);

        bool resolved = false;
        if (!ns_batches.empty() && ns_batches[0].size() != 0 && ns_batches[0].column_count() >= 1) {
            // First row's col 0 = namespace_oid. Mirrors
            // manager_disk_t::resolve_namespace (manager_disk_resolve.cpp:9-32)
            // which returns the first match.
            auto ns_oid_v = ns_batches[0].value(0, 0);
            if (!ns_oid_v.is_null()) {
                const auto oid_val = static_cast<catalog::oid_t>(ns_oid_v.value<std::uint32_t>());
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
