#include "pushed_reduce_scan.hpp"

#include "full_scan.hpp" // transform_predicate — ONE WHERE lowering shared with the raw scan

#include <services/disk/manager_disk.hpp>

namespace components::operators {

    pushed_reduce_scan::pushed_reduce_scan(std::pmr::memory_resource* resource,
                                           log_t log,
                                           components::catalog::oid_t table_oid,
                                           const expressions::compare_expression_ptr& expression,
                                           std::vector<size_t> projected_cols,
                                           pushed_aggregate_spec_t spec)
        : read_only_operator_t(resource, std::move(log), operator_type::pushed_reduce_scan)
        , table_oid_(table_oid)
        , expression_(expression)
        , projected_cols_(std::move(projected_cols))
        , spec_(std::move(spec)) {}

    pushed_aggregate_spec_t pushed_reduce_scan::open_spec() {
        pushed_aggregate_spec_t ship{resource_};
        ship.group_keys.reserve(spec_.group_keys.size());
        for (const auto& k : spec_.group_keys) {
            pushed_group_key_t c{resource_};
            c.name = k.name;
            c.path = k.path;
            ship.group_keys.push_back(std::move(c));
        }
        ship.aggregates.reserve(spec_.aggregates.size());
        for (const auto& a : spec_.aggregates) {
            pushed_aggregate_t c{resource_};
            c.function_name = a.function_name;
            c.arg_col_path = a.arg_col_path;
            c.func_uid = a.func_uid;
            c.distinct = a.distinct;
            c.alias = a.alias;
            ship.aggregates.push_back(std::move(c));
        }
        ship.output_types = spec_.output_types;
        return ship;
    }

    actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
    pushed_reduce_scan::source_next(pipeline::context_t* ctx) {
        if (!opened_) {
            opened_ = true;
            // Lower the WHERE once. Only a real predicate needs the column types; a
            // null / all_true expression ships a null filter with no storage_types
            // round-trip. No all_false / null-param short-circuit: create_plan_
            // aggregate's gate only lowers here when the WHERE is absent or a pure,
            // non-all_false compare, so transform_predicate never trips its assert.
            std::unique_ptr<table::table_filter_t> filter;
            if (expression_ && expression_->type() != expressions::compare_type::all_true) {
                // Fetch the relation's schema once and cache it: the schema is invariant
                // across re-drives, so a correlated-LATERAL re-open reuses the cache and
                // SKIPS the storage_types round-trip. The filter itself is still rebuilt
                // every drive — its correlated parameter (ctx->parameters) changes per
                // outer row — so only the schema is cached, never the filter.
                if (!schema_cached_) {
                    auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                                     &services::disk::manager_disk_t::storage_types,
                                                     ctx->session,
                                                     table_oid_);
                    cached_schema_ = co_await std::move(tf);
                    schema_cached_ = true;
                }
                auto filter_result =
                    transform_predicate(resource_, expression_, cached_schema_, &ctx->parameters, ctx->session_tz);
                if (filter_result.has_error()) {
                    set_error(filter_result.error());
                    mark_failed();
                    co_return core::result_wrapper_t<vector::data_chunk_t>(filter_result.error());
                }
                filter = std::move(filter_result.value());
            }

            // ONE reduce round-trip: the owning agent runs the whole GROUP BY over its
            // slice and replies ALL final rows (bounded by #groups).
            auto [_r, rf] = actor_zeta::send(ctx->disk_address,
                                             &services::disk::manager_disk_t::storage_reduce,
                                             ctx->session,
                                             table_oid_,
                                             std::move(filter),
                                             projected_cols_,
                                             ctx->txn,
                                             open_spec());
            auto reduce_result = co_await std::move(rf);
            if (reduce_result.has_error()) {
                set_error(reduce_result.error());
                mark_failed();
                co_return reduce_result.convert_error<vector::data_chunk_t>();
            }
            reduced_ = std::move(reduce_result.value());
        }

        if (emit_idx_ < reduced_.size()) {
            auto chunk = std::move(reduced_[emit_idx_]);
            ++emit_idx_;
            co_return core::result_wrapper_t<vector::data_chunk_t>(std::move(chunk));
        }

        // Drained: the 0-column sentinel stops execute_pipeline. NO empty-guard here —
        // the operator_group_merge_t above owns the empty-input scalar row.
        co_return vector::data_chunk_t{resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 0};
    }

} // namespace components::operators
