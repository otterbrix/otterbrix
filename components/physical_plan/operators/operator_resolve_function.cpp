#include "operator_resolve_function.hpp"

#include "catalog_write_helpers.hpp"

#include <components/catalog/catalog_oids.hpp>
#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/types.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_buffer.hpp>
#include <services/disk/manager_disk.hpp>

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace components::operators {

    namespace catalog = components::catalog;

    namespace {
        // pg_proc output schema (column order):
        //   [0] oid uint32, [1] proname string, [2] pronamespace uint32,
        //   [3] pronargs int32, [4] prouid int64, [5] proargmatchers string,
        //   [6] prorettype string. Built once into the cached member (TASK C10).
        void build_output_schema(std::pmr::vector<types::complex_logical_type>& out_types) {
            out_types.reserve(7);
            out_types.emplace_back(types::logical_type::UINTEGER);       // oid
            out_types.emplace_back(types::logical_type::STRING_LITERAL); // proname
            out_types.emplace_back(types::logical_type::UINTEGER);       // pronamespace
            out_types.emplace_back(types::logical_type::INTEGER);        // pronargs
            out_types.emplace_back(types::logical_type::BIGINT);         // prouid
            out_types.emplace_back(types::logical_type::STRING_LITERAL); // proargmatchers
            out_types.emplace_back(types::logical_type::STRING_LITERAL); // prorettype
        }
    } // namespace

    operator_resolve_function_t::operator_resolve_function_t(std::pmr::memory_resource* resource,
                                                             log_t log,
                                                             components::catalog::oid_t namespace_oid,
                                                             std::string name)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_function)
        , namespace_oid_(namespace_oid)
        , name_(std::move(name))
        , output_schema_(resource) {
        build_output_schema(output_schema_);
    }

    void operator_resolve_function_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // Pg_proc read is async; defer to await_async_and_resume.
        async_wait();
    }

    actor_zeta::unique_future<void> operator_resolve_function_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgProc = catalog::well_known_oid::pg_proc_table;

        // Output schema mirrors pg_proc column order; cached on the operator
        // (output_schema_), built once in the constructor (TASK C10).

        // Empty-input guard: no actor wired or empty name → emit an empty
        // chunk that still carries the schema so consumers can inspect columns.
        if (ctx->disk_address == actor_zeta::address_t::empty_address() || name_.empty()) {
            output_ = make_operator_data(resource_, output_schema_, 0);
            output_->chunks().front().set_cardinality(0);
            mark_executed();
            co_return;
        }

        // Issue read_chunks_by_key against pg_proc keyed on (proname, pronamespace).
        // Key order matches the column ordering used at write-time, but
        // read_chunks_by_key resolves indices by name so order is informational.
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        types::logical_value_t name_lv(resource_, std::string_view{name_});
        types::logical_value_t ns_lv(resource_, namespace_oid_);

        std::pmr::vector<std::string> pr_keys(resource_);
        pr_keys.emplace_back("proname");
        pr_keys.emplace_back("pronamespace");
        std::pmr::vector<types::logical_value_t> pr_vals(resource_);
        pr_vals.emplace_back(name_lv);
        pr_vals.emplace_back(ns_lv);
        auto [_h, fut] = actor_zeta::send(ctx->disk_address,
                                          &services::disk::manager_disk_t::read_chunks_by_key,
                                          exec_ctx,
                                          kPgProc,
                                          std::move(pr_keys),
                                          components::operators::make_key_chunk(resource_, std::move(pr_vals)));
        auto batches = co_await std::move(fut);

        // Materialise the matched rows into a fresh chunk. Capacity is sized to
        // the total matched-row count across all batches (≥ 1 to avoid
        // zero-capacity vector_t allocation surprises elsewhere). Each pg_proc
        // row already follows the schema layout above, so we copy positionally —
        // missing trailing optional columns leave the destination cell null
        // (set_value is only called for non-null source values).
        std::size_t total_rows = 0;
        for (const auto& batch : batches) {
            total_rows += batch.size();
        }
        const auto cap = std::max<std::size_t>(total_rows, 1);
        output_ = make_operator_data(resource_, output_schema_, cap);
        auto& chunk = output_->chunks().front();
        chunk.set_cardinality(total_rows);

        std::size_t r = 0;
        for (const auto& batch : batches) {
            const auto ncols = batch.column_count();
            for (uint64_t i = 0; i < batch.size(); ++i, ++r) {
                // Direct typed writes against pg_proc schema. Preserves the
                // original guard: skip cells whose source is missing or null so
                // the destination validity stays false.
                if (ncols > 0) {
                    auto v = batch.value(0, i);
                    if (!v.is_null())
                        set_uint32(chunk, 0, r, v.value<std::uint32_t>());
                }
                if (ncols > 1) {
                    auto v = batch.value(1, i);
                    if (!v.is_null())
                        set_str(chunk, 1, r, v.value<std::string_view>(), resource_);
                }
                if (ncols > 2) {
                    auto v = batch.value(2, i);
                    if (!v.is_null())
                        set_uint32(chunk, 2, r, v.value<std::uint32_t>());
                }
                if (ncols > 3) {
                    auto v = batch.value(3, i);
                    if (!v.is_null())
                        set_int32(chunk, 3, r, v.value<std::int32_t>());
                }
                if (ncols > 4) {
                    auto v = batch.value(4, i);
                    if (!v.is_null())
                        set_int64(chunk, 4, r, v.value<std::int64_t>());
                }
                if (ncols > 5) {
                    auto v = batch.value(5, i);
                    if (!v.is_null())
                        set_str(chunk, 5, r, v.value<std::string_view>(), resource_);
                }
                if (ncols > 6) {
                    auto v = batch.value(6, i);
                    if (!v.is_null())
                        set_str(chunk, 6, r, v.value<std::string_view>(), resource_);
                }
            }
        }

        mark_executed();
    }

} // namespace components::operators
