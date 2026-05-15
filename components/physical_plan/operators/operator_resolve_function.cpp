#include "operator_resolve_function.hpp"

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
        // File-local typed setters — direct typed-buffer writes that skip the
        // logical_value_t variant construct/dispatch/destroy cycle. Layout of
        // the destination chunk is known statically here (pg_proc schema).
        inline void set_uint32(vector::data_chunk_t& c, size_t col, size_t row, std::uint32_t v) {
            auto& vec = c.data[col];
            vec.template data<std::uint32_t>()[row] = v;
            vec.validity().set(row, true);
        }
        inline void set_int32(vector::data_chunk_t& c, size_t col, size_t row, std::int32_t v) {
            auto& vec = c.data[col];
            vec.template data<std::int32_t>()[row] = v;
            vec.validity().set(row, true);
        }
        inline void set_int64(vector::data_chunk_t& c, size_t col, size_t row, std::int64_t v) {
            auto& vec = c.data[col];
            vec.template data<std::int64_t>()[row] = v;
            vec.validity().set(row, true);
        }
        inline void set_str(vector::data_chunk_t& c, size_t col, size_t row,
                            std::string_view v, std::pmr::memory_resource* r) {
            auto& vec = c.data[col];
            if (!vec.auxiliary()) {
                vec.set_auxiliary(std::make_shared<vector::string_vector_buffer_t>(r));
            }
            auto* sb = static_cast<vector::string_vector_buffer_t*>(vec.auxiliary().get());
            auto* ptr = sb->insert(v);
            reinterpret_cast<std::string_view*>(vec.template data<std::byte>())[row] =
                std::string_view(static_cast<const char*>(ptr), v.size());
            vec.validity().set(row, true);
        }
    } // namespace

    operator_resolve_function_t::operator_resolve_function_t(std::pmr::memory_resource*   resource,
                                                              log_t                        log,
                                                              components::catalog::oid_t   namespace_oid,
                                                              std::string                  name)
        : read_write_operator_t(resource, std::move(log), operator_type::resolve_function)
        , namespace_oid_(namespace_oid)
        , name_(std::move(name)) {}

    void operator_resolve_function_t::on_execute_impl(pipeline::context_t* /*ctx*/) {
        // Pg_proc read is async; defer to await_async_and_resume.
        async_wait();
    }

    actor_zeta::unique_future<void>
    operator_resolve_function_t::await_async_and_resume(pipeline::context_t* ctx) {
        constexpr catalog::oid_t kPgProc = catalog::well_known_oid::pg_proc_table;

        // Output schema mirrors pg_proc column order:
        //   [0] oid             UINTEGER
        //   [1] proname         STRING
        //   [2] pronamespace    UINTEGER
        //   [3] pronargs        INTEGER
        //   [4] prouid          BIGINT
        //   [5] proargmatchers  STRING
        //   [6] prorettype      STRING
        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.reserve(7);
        out_types.emplace_back(types::logical_type::UINTEGER);        // oid
        out_types.emplace_back(types::logical_type::STRING_LITERAL);  // proname
        out_types.emplace_back(types::logical_type::UINTEGER);        // pronamespace
        out_types.emplace_back(types::logical_type::INTEGER);         // pronargs
        out_types.emplace_back(types::logical_type::BIGINT);          // prouid
        out_types.emplace_back(types::logical_type::STRING_LITERAL);  // proargmatchers
        out_types.emplace_back(types::logical_type::STRING_LITERAL);  // prorettype

        // Empty-input guard: no actor wired or empty name → emit an empty
        // chunk that still carries the schema so consumers can inspect columns.
        if (ctx->disk_address == actor_zeta::address_t::empty_address() || name_.empty()) {
            output_ = make_operator_data(resource_, out_types, 0);
            output_->data_chunk().set_cardinality(0);
            mark_executed();
            co_return;
        }

        // Issue read_rows_by_key against pg_proc keyed on (proname, pronamespace).
        // Key order matches the column ordering used at write-time, but
        // read_rows_by_key resolves indices by name so order is informational.
        components::execution_context_t exec_ctx{ctx->session, ctx->txn, {}};
        types::logical_value_t name_lv(resource_, std::string_view{name_});
        types::logical_value_t ns_lv(resource_, namespace_oid_);

        std::pmr::vector<std::string> pr_keys(resource_);
        pr_keys.emplace_back("proname");
        pr_keys.emplace_back("pronamespace");
        std::pmr::vector<types::logical_value_t> pr_vals(resource_);
        pr_vals.emplace_back(name_lv);
        pr_vals.emplace_back(ns_lv);
        auto [_h, fut] = actor_zeta::send(
            ctx->disk_address,
            &services::disk::manager_disk_t::read_rows_by_key,
            exec_ctx, kPgProc,
            std::move(pr_keys),
            std::move(pr_vals));
        auto rows = co_await std::move(fut);

        // Materialise rows into a fresh chunk. Capacity is sized to the
        // matched-row count (≥ 1 to avoid zero-capacity vector_t allocation
        // surprises elsewhere). Each pg_proc row already follows the schema
        // layout above, so we copy positionally — missing trailing optional
        // columns leave the destination cell null (set_value is only called
        // for non-null source values).
        const auto cap = std::max<std::size_t>(rows.size(), 1);
        output_ = make_operator_data(resource_, out_types, cap);
        auto& chunk = output_->data_chunk();
        chunk.set_cardinality(rows.size());

        for (std::size_t r = 0; r < rows.size(); ++r) {
            const auto& row = rows[r];
            // Direct typed writes against pg_proc schema. Preserves the
            // original guard: skip cells whose source is missing or null so
            // the destination validity stays false.
            if (row.size() > 0 && !row[0].is_null())
                set_uint32(chunk, 0, r, row[0].value<std::uint32_t>());
            if (row.size() > 1 && !row[1].is_null())
                set_str(chunk, 1, r, row[1].value<std::string_view>(), resource_);
            if (row.size() > 2 && !row[2].is_null())
                set_uint32(chunk, 2, r, row[2].value<std::uint32_t>());
            if (row.size() > 3 && !row[3].is_null())
                set_int32(chunk, 3, r, row[3].value<std::int32_t>());
            if (row.size() > 4 && !row[4].is_null())
                set_int64(chunk, 4, r, row[4].value<std::int64_t>());
            if (row.size() > 5 && !row[5].is_null())
                set_str(chunk, 5, r, row[5].value<std::string_view>(), resource_);
            if (row.size() > 6 && !row[6].is_null())
                set_str(chunk, 6, r, row[6].value<std::string_view>(), resource_);
        }

        mark_executed();
    }

} // namespace components::operators
