#include "operator_vector_search.hpp"

#include <cmath>

#include <components/physical_plan/operators/scan/full_scan.hpp>
#include <components/index/index_engine.hpp>
#include <components/table/column_state.hpp>
#include <components/vector/data_chunk.hpp>
#include <services/disk/manager_disk.hpp>
#include <services/index/manager_index.hpp>
#include <vector_search/top_k_heap.hpp>

namespace components::operators {

    operator_vector_search_t::operator_vector_search_t(std::pmr::memory_resource* resource,
                                                       log_t log,
                                                       components::catalog::oid_t table_oid,
                                                       std::string column_name,
                                                       std::vector<double> query_vector,
                                                       std::size_t k,
                                                       vector_search::metric_type metric,
                                                       const expressions::compare_expression_ptr& filter,
                                                       vector_search::filter_strategy strategy,
                                                       bool descending)
        : read_only_operator_t(resource, log, operator_type::vector_search)
        , table_oid_(table_oid)
        , column_name_(std::move(column_name))
        , query_vector_(std::move(query_vector))
        , k_(k)
        , metric_(metric)
        , filter_(filter)
        , strategy_(strategy)
        , descending_(descending) {}

    namespace {
        /// Evaluate a table_filter_t against a single row of a data chunk.
        /// Supports constant comparisons, IS [NOT] NULL, AND/OR conjunctions.
        bool evaluate_filter_on_row(const table::table_filter_t& filter,
                                    const vector::data_chunk_t& data,
                                    uint64_t row) {
            using namespace expressions;
            switch (filter.filter_type) {
                case compare_type::is_null: {
                    const auto& f = filter.cast<table::is_null_filter_t>();
                    if (f.table_indices.empty()) return false;
                    return data.value(f.table_indices[0], row).is_null();
                }
                case compare_type::is_not_null: {
                    const auto& f = filter.cast<table::is_null_filter_t>();
                    if (f.table_indices.empty()) return false;
                    return !data.value(f.table_indices[0], row).is_null();
                }
                case compare_type::union_or: {
                    const auto& f = filter.cast<table::conjunction_or_filter_t>();
                    for (const auto& child : f.child_filters) {
                        if (evaluate_filter_on_row(*child, data, row)) return true;
                    }
                    return false;
                }
                case compare_type::union_and: {
                    const auto& f = filter.cast<table::conjunction_and_filter_t>();
                    for (const auto& child : f.child_filters) {
                        if (!evaluate_filter_on_row(*child, data, row)) return false;
                    }
                    return true;
                }
                default: {
                    const auto& f = filter.cast<table::constant_filter_t>();
                    if (f.table_indices.empty()) return false;
                    return f.compare(data.value(f.table_indices[0], row));
                }
            }
        }
    } // namespace

    void operator_vector_search_t::on_execute_impl(pipeline::context_t* /*pipeline_context*/) {
        if (table_oid_ == components::catalog::INVALID_OID || query_vector_.empty() || k_ == 0) {
            output_ = make_operator_data(resource_, std::pmr::vector<types::complex_logical_type>{resource_});
            mark_executed();
            return;
        }
        async_wait();
    }

    actor_zeta::unique_future<void> operator_vector_search_t::await_async_and_resume(pipeline::context_t* ctx) {
        if (log_.is_valid()) {
            trace(log(),
                  "operator_vector_search_t::await_async_and_resume on oid={}",
                  static_cast<unsigned>(table_oid_));
        }

        // Step 1: Get column types from storage
        auto [_t, tf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_types,
                                         ctx->session,
                                         table_oid_);
        auto types = co_await std::move(tf);

        // Build full output schema: storage columns + vector_distance
        auto build_schema = [&](uint64_t col_count, auto col_type_at) {
            std::pmr::vector<types::complex_logical_type> out_types(resource_);
            out_types.reserve(col_count + 1);
            for (uint64_t col = 0; col < col_count; ++col) {
                out_types.push_back(col_type_at(col));
            }
            out_types.push_back(types::complex_logical_type(types::logical_type::DOUBLE, "vector_distance"));
            return out_types;
        };

        // Fast path: if an HNSW (vector) index covers this column and no filter is
        // requested, run approximate kNN through the index instead of a full scan.
        // Filtered queries keep the exact brute-force path (correct pre/post-filter).
        // Descending (K farthest) queries are always exact: the graph only
        // navigates towards neighbours, so the index cannot serve them.
        if (!filter_ && !descending_ && ctx->index_address != actor_zeta::address_t::empty_address()) {
            components::index::keys_base_storage_t keys{resource_};
            keys.emplace_back(resource_, column_name_);
            auto [_k, kf] = actor_zeta::send(ctx->index_address,
                                             &services::index::manager_index_t::knn_search,
                                             ctx->session,
                                             table_oid_,
                                             keys,
                                             query_vector_,
                                             static_cast<uint64_t>(k_),
                                             metric_);
            auto knn = co_await std::move(kf);
            if (knn.dim_mismatch) {
                set_error(core::error_t(
                    core::error_code_t::other_error,
                    std::pmr::string{"vector_search: different vector dimensions " +
                                         std::to_string(query_vector_.size()) + " and " +
                                         std::to_string(knn.expected_dim),
                                     resource_}));
                auto out_types = build_schema(static_cast<uint64_t>(types.size()),
                                              [&](uint64_t col) { return types[col]; });
                output_ = make_operator_data(resource_, std::move(out_types));
                mark_executed();
                co_return;
            }
            if (knn.index_used) {
                if (log_.is_valid()) {
                    trace(log(), "operator_vector_search_t: using HNSW index, {} hit(s)", knn.hits.size());
                }
                if (knn.hits.empty()) {
                    auto out_types = build_schema(static_cast<uint64_t>(types.size()),
                                                  [&](uint64_t col) { return types[col]; });
                    output_ = make_operator_data(resource_, std::move(out_types));
                    mark_executed();
                    co_return;
                }
                // Fetch the hit rows (storage_fetch preserves row_ids order).
                std::size_t count = knn.hits.size();
                std::vector<int64_t> ids(count);
                for (std::size_t i = 0; i < count; ++i) {
                    ids[i] = knn.hits[i].row_index;
                }
                vector::vector_t row_ids(resource_, types::logical_type::BIGINT, count);
                std::memcpy(row_ids.data(), ids.data(), count * sizeof(int64_t));

                auto [_f, ff] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::storage_fetch,
                                                 ctx->session,
                                                 table_oid_,
                                                 std::move(row_ids),
                                                 static_cast<uint64_t>(count));
                auto data = co_await std::move(ff);

                if (data && data->size() > 0) {
                    auto out_types = build_schema(data->column_count(),
                                                  [&](uint64_t col) { return data->data[col].type(); });
                    auto result_chunk = std::make_unique<vector::data_chunk_t>(
                        resource_, out_types, static_cast<uint64_t>(data->size()));
                    for (std::size_t i = 0; i < data->size(); ++i) {
                        for (uint64_t col = 0; col < data->column_count(); ++col) {
                            result_chunk->data[col].set_value(i, data->value(col, i));
                        }
                        result_chunk->data[data->column_count()].set_value(
                            i, types::logical_value_t{resource_, knn.hits[i].distance});
                    }
                    result_chunk->set_cardinality(static_cast<uint64_t>(data->size()));
                    output_ = make_operator_data(resource_, std::move(*result_chunk));
                } else {
                    auto out_types = build_schema(static_cast<uint64_t>(types.size()),
                                                  [&](uint64_t col) { return types[col]; });
                    output_ = make_operator_data(resource_, std::move(out_types));
                }
                mark_executed();
                co_return;
            }
            // No vector index on this column — fall through to the exact scan.
        }

        // Build filter from expression, if present.
        // For pre_filter strategy it's pushed to storage_scan; for post_filter we keep it
        // and evaluate row-by-row after kNN.
        std::unique_ptr<components::table::table_filter_t> scan_filter;
        std::unique_ptr<components::table::table_filter_t> post_filter;
        if (filter_) {
            auto built_res = transform_predicate(resource_, filter_, types, &ctx->parameters, ctx->session_tz);
            if (built_res.has_error()) {
                set_error(built_res.error());
                auto out_types = build_schema(static_cast<uint64_t>(types.size()),
                                              [&](uint64_t col) { return types[col]; });
                output_ = make_operator_data(resource_, std::move(out_types));
                mark_executed();
                co_return;
            }
            auto built = std::move(built_res.value());
            if (strategy_ == vector_search::filter_strategy::pre_filter) {
                scan_filter = std::move(built);
            } else {
                post_filter = std::move(built);
            }
        }

        // Step 2: Scan to get target data for kNN
        auto [_s, sf] = actor_zeta::send(ctx->disk_address,
                                         &services::disk::manager_disk_t::storage_scan,
                                         ctx->session,
                                         table_oid_,
                                         std::move(scan_filter),
                                         -1, // no limit
                                         ctx->txn);
        auto data = co_await std::move(sf);

        if (!data || data->size() == 0) {
            auto out_types = build_schema(static_cast<uint64_t>(types.size()),
                                          [&](uint64_t col) { return types[col]; });
            output_ = make_operator_data(resource_, std::move(out_types));
            mark_executed();
            co_return;
        }

        // Step 3: Find the target vector column index by name
        auto target_col = data->column_index(column_name_);

        if (target_col == static_cast<size_t>(-1)) {
            set_error(core::error_t(core::error_code_t::other_error,
                                    std::pmr::string{"vector_search: column '" + column_name_ + "' not found",
                                                     resource_}));
            auto out_types = build_schema(data->column_count(),
                                          [&](uint64_t col) { return data->data[col].type(); });
            output_ = make_operator_data(resource_, std::move(out_types));
            mark_executed();
            co_return;
        }

        // Step 4: Extract vectors from the target column and compute kNN
        uint64_t num_rows = data->size();
        std::size_t dim = query_vector_.size();

        // Dimension guard (pgvector parity): take the column's vector dimension from
        // the first stored vector; a query of a different dimension is an error, not a
        // silent empty result.
        for (uint64_t row = 0; row < num_rows; ++row) {
            auto v = data->value(target_col, row);
            auto vt = v.type().type();
            if (vt == types::logical_type::ARRAY || vt == types::logical_type::LIST) {
                std::size_t data_dim = v.children().size();
                if (data_dim != 0 && data_dim != dim) {
                    set_error(core::error_t(core::error_code_t::other_error,
                                            std::pmr::string{"vector_search: different vector dimensions " +
                                                                 std::to_string(dim) + " and " +
                                                                 std::to_string(data_dim),
                                                             resource_}));
                    auto out_types = build_schema(data->column_count(),
                                                  [&](uint64_t col) { return data->data[col].type(); });
                    output_ = make_operator_data(resource_, std::move(out_types));
                    mark_executed();
                    co_return;
                }
                break;
            }
        }

        vector_search::top_k_heap_t heap(k_, descending_);

        // Reused per-row buffer to avoid one allocation per row.
        std::vector<double> row_vec(dim);

        for (uint64_t row = 0; row < num_rows; ++row) {
            auto val = data->value(target_col, row);

            // Check that the value is an ARRAY or LIST type
            auto val_type = val.type().type();
            if (val_type != types::logical_type::ARRAY && val_type != types::logical_type::LIST) {
                continue; // skip non-array values
            }

            const auto& children = val.children();
            if (children.size() != dim) {
                continue; // skip dimension mismatch
            }

            bool valid = true;
            for (std::size_t d = 0; d < dim; ++d) {
                auto child_type = children[d].type().type();
                if (child_type == types::logical_type::DOUBLE) {
                    row_vec[d] = children[d].value<double>();
                } else if (child_type == types::logical_type::FLOAT) {
                    row_vec[d] = static_cast<double>(children[d].value<float>());
                } else if (child_type == types::logical_type::INTEGER) {
                    row_vec[d] = static_cast<double>(children[d].value<int32_t>());
                } else if (child_type == types::logical_type::BIGINT) {
                    row_vec[d] = static_cast<double>(children[d].value<int64_t>());
                } else if (child_type == types::logical_type::SMALLINT) {
                    row_vec[d] = static_cast<double>(children[d].value<int16_t>());
                } else if (child_type == types::logical_type::TINYINT) {
                    row_vec[d] = static_cast<double>(children[d].value<int8_t>());
                } else {
                    valid = false;
                    break;
                }
            }
            if (!valid) {
                continue;
            }

            double dist = vector_search::compute_distance(row_vec.data(), query_vector_.data(), dim, metric_);
            // NaN (e.g. NaN values inside a stored vector) breaks heap ordering —
            // skip the row. +inf is fine: it just ranks last.
            if (std::isnan(dist)) {
                continue;
            }
            heap.push(row, dist);
        }

        // Step 5: Build output with Top-K rows + distance score column
        auto results = heap.drain_sorted();

        // post_filter strategy: apply WHERE predicate to Top-K AFTER kNN
        if (post_filter) {
            std::vector<vector_search::scored_entry_t> kept;
            kept.reserve(results.size());
            for (const auto& entry : results) {
                if (evaluate_filter_on_row(*post_filter, *data, static_cast<uint64_t>(entry.row_id))) {
                    kept.push_back(entry);
                }
            }
            results = std::move(kept);
        }

        auto out_types =
            build_schema(data->column_count(), [&](uint64_t col) { return data->data[col].type(); });

        if (results.empty()) {
            output_ = make_operator_data(resource_, std::move(out_types));
            mark_executed();
            co_return;
        }

        auto result_chunk =
            std::make_unique<vector::data_chunk_t>(resource_, out_types, static_cast<uint64_t>(results.size()));

        for (std::size_t i = 0; i < results.size(); ++i) {
            auto source_row = static_cast<uint64_t>(results[i].row_id);

            // Copy all original columns
            for (uint64_t col = 0; col < data->column_count(); ++col) {
                result_chunk->data[col].set_value(i, data->value(col, source_row));
            }
            // Set distance score
            result_chunk->data[data->column_count()].set_value(i,
                                                               types::logical_value_t{resource_, results[i].distance});
        }
        result_chunk->set_cardinality(static_cast<uint64_t>(results.size()));

        output_ = make_operator_data(resource_, std::move(*result_chunk));
        mark_executed();
        co_return;
    }

} // namespace components::operators
