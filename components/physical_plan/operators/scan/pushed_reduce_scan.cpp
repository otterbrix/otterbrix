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

    pushed_aggregate_spec_t pushed_reduce_scan::open_spec(std::pmr::memory_resource* target) const {
        pushed_aggregate_spec_t ship{target};
        ship.group_keys.reserve(spec_.group_keys.size());
        for (const auto& k : spec_.group_keys) {
            pushed_group_key_t c{target};
            c.name = k.name;
            c.path = k.path;
            ship.group_keys.push_back(std::move(c));
        }
        ship.aggregates.reserve(spec_.aggregates.size());
        for (const auto& a : spec_.aggregates) {
            pushed_aggregate_t c{target};
            c.function_name = a.function_name;
            c.arg_col_path = a.arg_col_path;
            c.func_uid = a.func_uid;
            c.distinct = a.distinct;
            c.alias = a.alias;
            c.result_type = a.result_type;
            ship.aggregates.push_back(std::move(c));
        }
        ship.outputs.reserve(spec_.outputs.size());
        for (const auto& output : spec_.outputs) {
            ship.outputs.push_back(output.copy(target));
        }
        ship.output_types.assign(spec_.output_types.begin(), spec_.output_types.end());
        ship.input_types.assign(spec_.input_types.begin(), spec_.input_types.end());
        return ship;
    }

    actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
    pushed_reduce_scan::source_next(pipeline::context_t* ctx) {
        if (!opened_) {
            opened_ = true;
            // The WHERE travels as a description; the owning agent types and compiles it. It is rebuilt
            // every drive, since its correlated parameter (ctx->parameters) changes per outer row.
            auto filter_result = transform_predicate(resource_,
                                                     ctx->disk_address.resource(),
                                                     expression_,
                                                     &ctx->parameters,
                                                     ctx->execution_context);
            if (filter_result.has_error()) {
                set_error(filter_result.error());
                mark_failed();
                co_return filter_result.error();
            }
            auto filter = std::move(filter_result.value());

            // ONE reduce round-trip: the owning agent runs the whole GROUP BY over its
            // slice and replies ALL final rows (bounded by #groups).
            auto [_r, rf] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                        &services::disk::manager_disk_t::storage_reduce,
                                                        ctx->session,
                                                        table_oid_,
                                                        std::move(filter),
                                                        projected_cols_,
                                                        ctx->txn,
                                                        open_spec(ctx->disk_address.resource()));
            auto reduce_result = co_await std::move(rf);
            if (reduce_result.has_error()) {
                set_error(reduce_result.error());
                mark_failed();
                co_return reduce_result.convert_error<std::optional<vector::data_chunk_t>>();
            }
            reduced_ = std::move(reduce_result.value());
        }

        if (emit_idx_ < reduced_.size()) {
            auto chunk = std::move(reduced_[emit_idx_]);
            ++emit_idx_;
            co_return std::move(chunk);
        }

        // NO empty-guard here — the operator_group_merge_t above owns the empty-input scalar row.
        co_return std::nullopt;
    }

} // namespace components::operators
