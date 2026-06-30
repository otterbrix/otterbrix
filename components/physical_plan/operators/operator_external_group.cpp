#include "operator_external_group.hpp"

#include "arithmetic_eval.hpp"
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/operator_batch.hpp>
#include <components/table/storage/file_buffer.hpp>
#include <components/table/storage/spill_file.hpp>
#include <components/table/storage/unified_format.hpp>
#include <components/vector/vector_operations.hpp>
#include <core/file/local_file_system.hpp>
#include <core/operations_helper.hpp>

#include <cassert>
#include <cstring>
#include <type_traits>

namespace components::operators {

    namespace {
        // Derive the real MVCC snapshot horizon for a spill header. Prefer the
        // executor-populated lowest_active_start_time (GC threshold); fall back to
        // the statement's own start_time.
        uint64_t spill_snapshot_horizon(const pipeline::context_t& ctx) noexcept {
            if (ctx.lowest_active_start_time != 0) {
                return ctx.lowest_active_start_time;
            }
            return ctx.txn.start_time;
        }

        // Placeholder columns (produced by projected scans) have no buffer and no auxiliary.
        // They must be skipped when reading values — vector_t::value() / data() would crash otherwise.
        bool is_placeholder(const vector::vector_t& v) noexcept {
            return v.data() == nullptr && v.auxiliary() == nullptr;
        }

        template<typename T>
        bool equals_typed(const vector::vector_t& vec, size_t row, const types::logical_value_t& val) {
            if constexpr (std::is_floating_point_v<T>) {
                T a = vec.data<T>()[row];
                T b = val.value<T>();
                return core::is_equals(a, b);
            } else {
                return vec.data<T>()[row] == val.value<T>();
            }
        }

        bool value_equals_raw(const vector::vector_t& vec, size_t row, const types::logical_value_t& val) {
            if (vec.is_null(row))
                return val.is_null();
            if (val.is_null())
                return false;
            switch (vec.type().to_physical_type()) {
                case types::physical_type::BOOL:
                case types::physical_type::INT8:
                    return equals_typed<int8_t>(vec, row, val);
                case types::physical_type::INT16:
                    return equals_typed<int16_t>(vec, row, val);
                case types::physical_type::INT32:
                    return equals_typed<int32_t>(vec, row, val);
                case types::physical_type::INT64:
                    return equals_typed<int64_t>(vec, row, val);
                case types::physical_type::UINT8:
                    return equals_typed<uint8_t>(vec, row, val);
                case types::physical_type::UINT16:
                    return equals_typed<uint16_t>(vec, row, val);
                case types::physical_type::UINT32:
                    return equals_typed<uint32_t>(vec, row, val);
                case types::physical_type::UINT64:
                    return equals_typed<uint64_t>(vec, row, val);
                case types::physical_type::INT128:
                    return equals_typed<types::int128_t>(vec, row, val);
                case types::physical_type::UINT128:
                    return equals_typed<types::uint128_t>(vec, row, val);
                case types::physical_type::FLOAT:
                    return equals_typed<float>(vec, row, val);
                case types::physical_type::DOUBLE:
                    return equals_typed<double>(vec, row, val);
                case types::physical_type::STRING:
                    return vec.data<std::string_view>()[row] == val.value<std::string_view>();
                case types::physical_type::STRUCT: {
                    auto& vec_children = vec.entries();
                    auto& val_children = val.children();
                    assert(vec_children.size() == val_children.size());
                    for (size_t field = 0; field < vec_children.size(); field++) {
                        if (!value_equals_raw(*vec_children[field], row, val_children[field])) {
                            return false;
                        }
                    }
                    return true;
                }
                case types::physical_type::ARRAY: {
                    assert(static_cast<const types::array_logical_type_extension*>(vec.type().extension())->size() ==
                           static_cast<const types::array_logical_type_extension*>(val.type().extension())->size());
                    auto& flat_array = vec.entry();
                    auto& val_children = val.children();
                    for (size_t i = 0; i < val_children.size(); i++) {
                        if (!value_equals_raw(flat_array, row * val_children.size() + i, val_children[i])) {
                            return false;
                        }
                    }
                    return true;
                }
                case types::physical_type::LIST: {
                    const auto& val_children = val.children();
                    const auto& list_entry = *(vec.data<types::list_entry_t>() + row);
                    const auto& flat_list = vec.entry();
                    assert(flat_list.type().size() != 0);
                    auto entry_size = flat_list.type().size();
                    if (list_entry.length / entry_size != val_children.size()) {
                        return false;
                    }
                    for (size_t i = 0; i < val_children.size(); i++) {
                        if (!value_equals_raw(flat_list, list_entry.offset / entry_size + i, val_children[i])) {
                            return false;
                        }
                    }
                    return true;
                }
                default:
                    assert(false && "unhandled type in value_equals_raw");
                    return false;
            }
        }

        bool keys_match(const vector::data_chunk_t& chunk,
                        const std::pmr::vector<size_t>& col_indices,
                        size_t row_idx,
                        const std::pmr::vector<types::logical_value_t>& group_key) {
            for (size_t k = 0; k < col_indices.size(); k++) {
                if (!value_equals_raw(chunk.data[col_indices[k]], row_idx, group_key[k]))
                    return false;
            }
            return true;
        }

        // Extract a key value from chunk for a given group_key_t definition
        types::logical_value_t extract_key_value(std::pmr::memory_resource* resource,
                                                 const group_key_t& key,
                                                 const vector::data_chunk_t& chunk,
                                                 size_t row_idx) {
            switch (key.type) {
                case group_key_t::kind::column: {
                    assert(!key.full_path.empty() && "group key path must be resolved before execution");
                    types::logical_value_t val = chunk.value(key.full_path, row_idx);
                    val.set_alias(std::string{key.name});
                    return val;
                }
                case group_key_t::kind::coalesce: {
                    for (const auto& entry : key.coalesce_entries) {
                        if (entry.type == group_key_t::coalesce_entry::source::constant) {
                            if (!entry.constant.is_null()) {
                                auto val = entry.constant;
                                val.set_alias(std::string{key.name});
                                return val;
                            }
                        } else {
                            // column source
                            if (!chunk.data[entry.col_index].is_null(row_idx)) {
                                auto val = chunk.value(entry.col_index, row_idx);
                                val.set_alias(std::string{key.name});
                                return val;
                            }
                        }
                    }
                    // all NULL
                    auto null_val =
                        types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
                    null_val.set_alias(std::string{key.name});
                    return null_val;
                }
                case group_key_t::kind::case_when: {
                    for (const auto& clause : key.case_clauses) {
                        auto cond_val = chunk.value(clause.condition_col, row_idx);
                        auto cmp_result = cond_val.compare(clause.condition_value);
                        bool matches = false;
                        switch (clause.cmp) {
                            case expressions::compare_type::eq:
                                matches = cmp_result == types::compare_t::equals;
                                break;
                            case expressions::compare_type::ne:
                                matches = cmp_result != types::compare_t::equals;
                                break;
                            case expressions::compare_type::gt:
                                matches = cmp_result == types::compare_t::more;
                                break;
                            case expressions::compare_type::gte:
                                matches = cmp_result >= types::compare_t::equals;
                                break;
                            case expressions::compare_type::lt:
                                matches = cmp_result == types::compare_t::less;
                                break;
                            case expressions::compare_type::lte:
                                matches = cmp_result <= types::compare_t::equals;
                                break;
                            default:
                                matches = true;
                                break;
                        }
                        if (matches) {
                            types::logical_value_t result_val =
                                (clause.res_type == group_key_t::case_clause::result_source::constant)
                                    ? clause.res_constant
                                    : chunk.value(clause.res_col, row_idx);
                            result_val.set_alias(std::string{key.name});
                            return result_val;
                        }
                    }
                    // else branch
                    types::logical_value_t else_val = [&]() -> types::logical_value_t {
                        switch (key.else_type) {
                            case group_key_t::else_source::column:
                                return chunk.value(key.else_col, row_idx);
                            case group_key_t::else_source::constant:
                                return key.else_constant;
                            case group_key_t::else_source::null_value:
                            default:
                                return types::logical_value_t(resource,
                                                              types::complex_logical_type{types::logical_type::NA});
                        }
                    }();
                    else_val.set_alias(std::string{key.name});
                    return else_val;
                }
            }
            return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
        }

        // Commutative combine of two partial aggregate results that were each
        // computed over a disjoint subset of a group's rows (grace-partition spill).
        // `kind` is the lowercase aggregate function name ("sum"/"count"/"min"/
        // "max"/"avg"). `existing_count`/`new_count` are the row counts each partial
        // was folded over (needed to weight AVG correctly). Returns the merged
        // partial in `existing`. NA inputs are treated as identity for SUM/COUNT/
        // MIN/MAX and as empty for AVG.
        void merge_aggregate_partial(std::pmr::memory_resource* resource,
                                     std::string_view kind,
                                     types::logical_value_t& existing,
                                     const types::logical_value_t& new_val,
                                     uint64_t existing_count,
                                     uint64_t new_count) {
            const bool ex_null = existing.is_null();
            const bool nw_null = new_val.is_null();

            if (kind == "count") {
                // COUNT is additive over row counts. The stored value is the count
                // itself (uint64). If either side is NA, it contributes 0.
                uint64_t a = ex_null ? 0 : existing.value<uint64_t>();
                uint64_t b = nw_null ? 0 : new_val.value<uint64_t>();
                existing = types::logical_value_t(resource, a + b);
                return;
            }
            if (kind == "sum") {
                if (ex_null) { existing = new_val; return; }
                if (nw_null) { return; }
                // SUM keeps the operand's numeric type; promote to double for a
                // uniform combine, then store back as the existing type.
                double a = existing.value<double>();
                double b = new_val.value<double>();
                existing = types::logical_value_t(resource, a + b);
                return;
            }
            if (kind == "min" || kind == "max") {
                if (ex_null) { existing = new_val; return; }
                if (nw_null) { return; }
                double a = existing.value<double>();
                double b = new_val.value<double>();
                bool take_new = (kind == "min") ? (b < a) : (b > a);
                if (take_new) { existing = new_val; }
                return;
            }
            if (kind == "avg") {
                // AVG folds mean + count. Recombine the two means weighted by their
                // row counts. If a side saw no rows it contributes nothing.
                uint64_t ec = existing_count;
                uint64_t nc = new_count;
                if (ec == 0 || ex_null) {
                    existing = (nc == 0 || nw_null) ? existing : new_val;
                    return;
                }
                if (nc == 0 || nw_null) {
                    return;
                }
                double a = existing.value<double>();
                double b = new_val.value<double>();
                double merged = ((a * static_cast<double>(ec)) + (b * static_cast<double>(nc)))
                                / static_cast<double>(ec + nc);
                existing = types::logical_value_t(resource, merged);
                return;
            }
            // Non-commutative / unknown aggregate kind: keep the first non-null
            // partial seen. This preserves prior behaviour without silently
            // dropping rows.
            if (ex_null) { existing = new_val; }
        }
    } // anonymous namespace

    operator_external_group_t::operator_external_group_t(std::pmr::memory_resource* resource,
                                                         log_t log,
                                                         expressions::expression_ptr having,
                                                         size_t internal_aggregate_count)
        : read_write_operator_t(resource, log, operator_type::grace_aggregate)
        , keys_(resource_)
        , values_(resource_)
        , computed_columns_(resource_)
        , post_aggregates_(resource_)
        , having_(std::move(having))
        , internal_aggregate_count_(internal_aggregate_count)
        , row_refs_per_group_(resource_)
        , group_keys_(resource_)
        , group_index_(resource_)
        , grace_state_(resource_) {}

    void operator_external_group_t::add_key(group_key_t&& key) { keys_.push_back(std::move(key)); }

    void operator_external_group_t::add_key(const std::pmr::string& name) {
        group_key_t key(resource_);
        key.name = name;
        key.type = group_key_t::kind::column;
        keys_.push_back(std::move(key));
    }

    void operator_external_group_t::add_value(const std::pmr::string& name, aggregate::operator_aggregate_ptr&& aggregator) {
        values_.push_back({name, std::move(aggregator)});
    }

    void operator_external_group_t::add_computed_column(computed_column_t&& col) {
        computed_columns_.emplace_back(std::move(col));
    }

    void operator_external_group_t::add_post_aggregate(post_aggregate_column_t&& col) {
        post_aggregates_.emplace_back(std::move(col));
    }

    bool operator_external_group_t::evaluate_computed_columns(pipeline::context_t* pipeline_context,
                                                              chunks_vector_t& in_chunks,
                                                              size_t& first_computed_col) {
        // Phase 1: Pre-compute arithmetic columns on EACH input chunk (no concat).
        // All chunks share the same schema, so the column index of each computed key
        // is identical across chunks.
        first_computed_col = 0;
        if (!in_chunks.empty()) {
            first_computed_col = in_chunks.front().data.size();
        }
        for (auto& chunk : in_chunks) {
            for (auto& comp : computed_columns_) {
                auto result_vec = evaluate_arithmetic(resource_,
                                                      comp.op,
                                                      comp.operands,
                                                      chunk,
                                                      pipeline_context->parameters,
                                                      pipeline_context->session_tz);
                if (result_vec.has_error()) {
                    set_error(result_vec.error());
                    return false;
                } else if (result_vec.value().type().type() == types::logical_type::NA) {
                    set_error(
                        core::error_t(core::error_code_t::physical_plan_error,
                                      std::pmr::string{"unknown error during evaluate_arithmetic", resource_}));
                    return false;
                }
                result_vec.value().set_type_alias(std::string(comp.alias));
                chunk.data.emplace_back(std::move(result_vec.value()));
            }
        }

        // Resolve col_index for computed-column keys (they were appended at known positions).
        if (!computed_columns_.empty()) {
            for (size_t ci = 0; ci < computed_columns_.size(); ci++) {
                if (computed_columns_[ci].resolved_key_index != SIZE_MAX) {
                    keys_[computed_columns_[ci].resolved_key_index].full_path.emplace_back(first_computed_col + ci);
                }
            }
        }
        return true;
    }

    void operator_external_group_t::strip_computed_columns(chunks_vector_t& in_chunks, size_t first_computed_col) const {
        if (computed_columns_.empty()) {
            return;
        }
        for (auto& chunk : in_chunks) {
            if (chunk.data.size() > first_computed_col) {
                chunk.data.erase(chunk.data.begin() + static_cast<std::ptrdiff_t>(first_computed_col),
                                 chunk.data.end());
            }
        }
    }

    void operator_external_group_t::clear_grouping_state() {
        row_refs_per_group_.clear();
        group_keys_.clear();
        group_index_.clear();
    }

    void operator_external_group_t::finalize_output(pipeline::context_t* pipeline_context,
                                                    const operator_data_ptr& in,
                                                    chunks_vector_t& in_chunks,
                                                    chunks_vector_t& batches,
                                                    size_t first_computed_col) {
        // Phase 3 (post): Post-aggregate arithmetic, internal-column removal and
        // HAVING are columnar and chunk-local, so they run per batch.
        for (auto& batch : batches) {
            // Post-aggregate arithmetic (columnar)
            size_t size_before_post = batch.data.size();
            calc_post_aggregates(pipeline_context, batch);
            if (has_error()) {
                return;
            }

            // Remove internal aggregate columns by position
            if (internal_aggregate_count_ > 0) {
                auto it_end = batch.data.begin() + static_cast<std::ptrdiff_t>(size_before_post);
                auto it_begin = it_end - static_cast<std::ptrdiff_t>(internal_aggregate_count_);
                batch.data.erase(it_begin, it_end);
            }

            // HAVING filter (columnar)
            if (having_) {
                filter_having(pipeline_context, batch);
                if (has_error()) {
                    return;
                }
            }
        }

        // Output. SELECT-order column reordering is now handled by the downstream
        // operator_select_t. Group emits columns in its internal order; the
        // explicit SELECT operator picks/reorders them by name.
        output_ = operators::make_operator_data(in->resource(), std::move(batches));

        // Cleanup: strip the temporary computed-key columns from every input chunk.
        strip_computed_columns(in_chunks, first_computed_col);

        // Clear temporary grouping state
        clear_grouping_state();
    }

    void operator_external_group_t::create_list_rows(const chunks_vector_t& in_chunks) {
        if (in_chunks.empty()) {
            return;
        }

        // Column keys must arrive with a resolved full_path: extract_key_value
        // reads chunk.value(key.full_path, ...) and an empty path is UB there
        // (its assert is compiled out in Release). Surface a clean operator
        // error instead of relying on the assert.
        for (const auto& key : keys_) {
            if (key.type == group_key_t::kind::column && key.full_path.empty()) {
                std::pmr::string msg{"group key '", resource_};
                msg += key.name;
                msg += "' has no resolved column path";
                set_error(core::error_t(core::error_code_t::schema_error, std::move(msg)));
                return;
            }
        }

        // Try fast path: all keys are simple column type with resolved col_index
        bool use_fast_path = true;
        std::pmr::vector<size_t> key_col_indices(resource_);
        for (const auto& key : keys_) {
            if (key.type != group_key_t::kind::column || key.full_path.size() != 1) {
                use_fast_path = false;
                break;
            }
            key_col_indices.push_back(key.full_path.front());
        }

        if (use_fast_path && !key_col_indices.empty()) {
            std::vector<uint64_t> col_ids(key_col_indices.begin(), key_col_indices.end());

            for (uint32_t chunk_idx = 0; chunk_idx < in_chunks.size(); ++chunk_idx) {
                const auto& chunk = in_chunks[chunk_idx];
                auto num_rows = chunk.size();
                if (num_rows == 0) {
                    continue;
                }

                // Batch hash rows of this chunk.
                vector::vector_t hash_vec(resource_, types::logical_type::UBIGINT, num_rows);
                const_cast<vector::data_chunk_t&>(chunk).hash(col_ids, hash_vec);
                auto* hashes = hash_vec.data<uint64_t>();

                for (size_t row_idx = 0; row_idx < num_rows; row_idx++) {
                    auto hash_val = static_cast<size_t>(hashes[row_idx]);
                    auto it = group_index_.find(hash_val);
                    bool is_new = true;
                    if (it != group_index_.end()) {
                        for (size_t idx : it->second) {
                            if (keys_match(chunk, key_col_indices, row_idx, group_keys_[idx])) {
                                row_refs_per_group_[idx].emplace_back(chunk_idx, static_cast<uint32_t>(row_idx));
                                is_new = false;
                                break;
                            }
                        }
                    }
                    if (is_new) {
                        // Only extract key values when creating a new group
                        std::pmr::vector<types::logical_value_t> key_vals(resource_);
                        for (size_t ki = 0; ki < key_col_indices.size(); ki++) {
                            auto val = chunk.value(key_col_indices[ki], row_idx);
                            val.set_alias(std::string{keys_[ki].name});
                            key_vals.push_back(std::move(val));
                        }
                        size_t idx = group_keys_.size();
                        group_index_[hash_val].push_back(idx);
                        group_keys_.push_back(std::move(key_vals));
                        std::pmr::vector<row_ref_t> refs(resource_);
                        refs.emplace_back(chunk_idx, static_cast<uint32_t>(row_idx));
                        row_refs_per_group_.push_back(std::move(refs));
                    }
                }
            }
        } else {
            // Slow path: handles coalesce, case_when, wildcards, nested paths
            for (uint32_t chunk_idx = 0; chunk_idx < in_chunks.size(); ++chunk_idx) {
                const auto& chunk = in_chunks[chunk_idx];
                auto num_rows = chunk.size();
                for (size_t row_idx = 0; row_idx < num_rows; row_idx++) {
                    std::pmr::vector<types::logical_value_t> key_vals(resource_);

                    for (const auto& key : keys_) {
                        auto val = extract_key_value(resource_, key, chunk, row_idx);
                        key_vals.push_back(std::move(val));
                    }

                    size_t hash_val = types::hash_row(key_vals);
                    auto it = group_index_.find(hash_val);
                    bool is_new = true;
                    if (it != group_index_.end()) {
                        for (size_t idx : it->second) {
                            if (key_vals == group_keys_[idx]) {
                                row_refs_per_group_[idx].emplace_back(chunk_idx, static_cast<uint32_t>(row_idx));
                                is_new = false;
                                break;
                            }
                        }
                    }
                    if (is_new) {
                        size_t idx = group_keys_.size();
                        group_index_[hash_val].push_back(idx);
                        group_keys_.push_back(std::move(key_vals));
                        std::pmr::vector<row_ref_t> refs(resource_);
                        refs.emplace_back(chunk_idx, static_cast<uint32_t>(row_idx));
                        row_refs_per_group_.push_back(std::move(refs));
                    }
                }
            }
        }
    }

    chunks_vector_t operator_external_group_t::calc_aggregate_values(pipeline::context_t* pipeline_context,
                                                                     chunks_vector_t& in_chunks) {
        size_t num_groups = group_keys_.size();
        size_t key_count = num_groups > 0 ? group_keys_[0].size() : 0;

        // All chunks share the same schema — take types from the first one.
        std::pmr::vector<types::complex_logical_type> result_types{resource_};
        if (!in_chunks.empty()) {
            result_types = in_chunks.front().types();
        }
        size_t col_count = result_types.size();

        // Build a group's rows by gathering (chunk_idx, row_idx) pairs from the multi-chunk
        // source into a batch of ≤DEFAULT_VECTOR_CAPACITY chunks (a single group may exceed
        // one vector's worth of rows). Consecutive rows from the same source chunk are copied
        // in a single vector_ops::copy() to keep the cost close to a flat memcpy: group refs
        // by source chunk, then issue one indexing-based copy per (chunk, column). This
        // collapses N small per-row copies into N_chunks bulk copies and is much cheaper when
        // refs are scattered (e.g. typical GROUP BY where each group's rows are spread across
        // many source chunks).
        const uint64_t cap = vector::DEFAULT_VECTOR_CAPACITY;
        auto gather_group_chunks = [&](const std::pmr::vector<row_ref_t>& refs) {
            chunks_vector_t out(resource_);
            uint64_t cnt = static_cast<uint64_t>(refs.size());
            if (cnt == 0) {
                vector::data_chunk_t empty(resource_, result_types, 1);
                empty.set_cardinality(0);
                out.emplace_back(std::move(empty));
                return out;
            }
            out.reserve(static_cast<size_t>((cnt + cap - 1) / cap));
            // refs are inserted in (chunk_idx, row_idx) order during create_list_rows, so all
            // refs for a given source chunk form one contiguous span. Each output chunk covers
            // refs[base, base+window); a span never crosses an output-chunk boundary.
            for (uint64_t base = 0; base < cnt; base += cap) {
                const uint64_t window = std::min(cap, cnt - base);
                vector::data_chunk_t grp(resource_, result_types, window);
                grp.set_cardinality(window);
                const uint64_t window_end = base + window;
                uint64_t pos = base;
                while (pos < window_end) {
                    uint32_t src_chunk = refs[pos].first;
                    uint64_t span_start = pos;
                    while (pos < window_end && refs[pos].first == src_chunk) {
                        ++pos;
                    }
                    uint64_t span_len = pos - span_start;
                    const auto& src = in_chunks[src_chunk];

                    // Build indexing into source chunk's row_idx values for this span.
                    vector::indexing_vector_t indexing(resource_, span_len);
                    auto* idx_data = indexing.data();
                    for (uint64_t i = 0; i < span_len; ++i) {
                        idx_data[i] = refs[span_start + i].second;
                    }

                    const uint64_t dst_offset = span_start - base;
                    for (size_t c = 0; c < col_count; ++c) {
                        if (is_placeholder(src.data[c]))
                            continue;
                        vector::vector_ops::copy(src.data[c], grp.data[c], indexing, span_len, 0, dst_offset);
                    }
                    vector::vector_ops::copy(src.row_ids, grp.row_ids, indexing, span_len, 0, dst_offset);
                }
                out.emplace_back(std::move(grp));
            }
            return out;
        };

        // Compute aggregate results: agg_results[agg_idx][group_idx]. Each aggregate function
        // consumes a whole group's batch and returns one value (it folds the per-chunk states
        // internally — combining finalized per-chunk partials would be wrong for AVG etc.).
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> agg_results(resource_);
        agg_results.reserve(values_.size());
        for (size_t a = 0; a < values_.size(); ++a) {
            std::pmr::vector<types::logical_value_t> col(resource_);
            col.reserve(num_groups);
            agg_results.push_back(std::move(col));
        }

        // Gather each group's batch ONCE, then run every aggregator over it (set_children +
        // on_execute; operator_func_t restores the batch after appending/removing its computed
        // argument columns, so the same batch is safely reused across aggregators). This keeps
        // the gather at O(groups × rows) rather than O(aggregates × groups × rows).
        for (size_t g = 0; g < num_groups; ++g) {
            auto group_batch = make_operator_batch(resource_, gather_group_chunks(row_refs_per_group_[g]));

            for (size_t a = 0; a < values_.size(); ++a) {
                auto& value = values_[a];
                auto& aggregator = value.aggregator;

                aggregator->clear();
                aggregator->set_children(group_batch);
                aggregator->on_execute(pipeline_context);
                if (aggregator->has_error()) {
                    set_error(aggregator->get_error());
                    return chunks_vector_t(resource_);
                }

                auto datum = aggregator->take_batch_values();
                types::logical_value_t val(resource_, types::logical_type::NA);
                if (std::holds_alternative<std::pmr::vector<types::logical_value_t>>(datum)) {
                    auto& vals = std::get<std::pmr::vector<types::logical_value_t>>(datum);
                    if (!vals.empty()) {
                        val = std::move(vals.front());
                    }
                } else {
                    auto& result_chunk = std::get<vector::data_chunk_t>(datum);
                    if (result_chunk.size() > 0 && !result_chunk.data.empty()) {
                        val = result_chunk.value(0, 0);
                    }
                }
                val.set_alias(std::string(value.name));
                agg_results[a].push_back(std::move(val));
            }
        }

        return build_result_chunk(num_groups, key_count, agg_results, in_chunks);
    }

    chunks_vector_t
    operator_external_group_t::build_result_chunk(size_t num_groups,
                                                  size_t key_count,
                                                  std::pmr::vector<std::pmr::vector<types::logical_value_t>>& agg_results,
                                                  const chunks_vector_t& in_chunks) {
        // Build result types: key types + aggregate types.
        // Source key types from the incoming chunk's column schema (stable across
        // NULL handling), not from group_keys_[0][k] — a NULL value in the first
        // group would otherwise set out_types[k] = logical_type::NA, and any later
        // group with a typed key would trip vector_t::set_value's cast_as path
        // (NA has no cast handler → assert in logical_value.cpp).
        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        if (num_groups > 0) {
            for (size_t k = 0; k < key_count; k++) {
                const auto& key = keys_[k];
                bool got = false;
                if (key.type == group_key_t::kind::column && !key.full_path.empty() && !in_chunks.empty()) {
                    const auto col_idx = key.full_path.front();
                    if (col_idx < in_chunks.front().column_count()) {
                        // Walk the remaining path components through the nested
                        // type (STRUCT fields by index, ARRAY/LIST element type)
                        // so multi-part paths get their source type too; fall
                        // back to the value type when a component cannot be
                        // resolved.
                        const auto* walked = &in_chunks.front().data[col_idx].type();
                        bool ok = true;
                        for (auto it = std::next(key.full_path.begin()); ok && it != key.full_path.end(); ++it) {
                            switch (walked->type()) {
                                case types::logical_type::ARRAY:
                                case types::logical_type::LIST:
                                    walked = &walked->child_type();
                                    break;
                                case types::logical_type::STRUCT:
                                    if (*it < walked->child_types().size()) {
                                        walked = &walked->child_types()[*it];
                                    } else {
                                        ok = false;
                                    }
                                    break;
                                default:
                                    ok = false;
                                    break;
                            }
                        }
                        if (ok) {
                            out_types.push_back(*walked);
                            got = true;
                        }
                    }
                }
                if (!got) {
                    out_types.push_back(group_keys_[0][k].type());
                }
            }
        }
        // One column per aggregate, unconditionally: the fill loop below writes
        // every aggregate at the fixed position key_count + agg_idx, so a
        // skipped type here would shift all later columns and write past the
        // chunk's column array. An aggregate with no results gets an NA-typed
        // column filled with NULLs.
        for (size_t a = 0; a < values_.size(); a++) {
            out_types.push_back(agg_results[a].empty() ? types::complex_logical_type(types::logical_type::NA)
                                                       : agg_results[a][0].type());
        }

        // Emit the group rows directly as ≤DEFAULT_VECTOR_CAPACITY batches: the result
        // never exists as one oversized chunk. A zero-group result still emits one empty,
        // correctly-typed chunk so downstream operators can read the schema.
        chunks_vector_t batches(resource_);
        if (num_groups == 0) {
            vector::data_chunk_t empty(resource_, out_types, 1);
            empty.set_cardinality(0);
            batches.emplace_back(std::move(empty));
            return batches;
        }

        const size_t cap = vector::DEFAULT_VECTOR_CAPACITY;
        batches.reserve((num_groups + cap - 1) / cap);
        for (size_t base = 0; base < num_groups; base += cap) {
            const size_t window = std::min(cap, num_groups - base);
            vector::data_chunk_t result(resource_, out_types, window);
            result.set_cardinality(static_cast<uint64_t>(window));
            for (size_t row = 0; row < window; row++) {
                const size_t group_idx = base + row;
                // Fill key columns
                for (size_t key_idx = 0; key_idx < key_count; key_idx++) {
                    result.set_value(key_idx, row, std::move(group_keys_[group_idx][key_idx]));
                }
                // Fill aggregate columns at their fixed position key_count + agg_idx
                for (size_t agg_idx = 0; agg_idx < values_.size(); agg_idx++) {
                    if (group_idx < agg_results[agg_idx].size()) {
                        result.set_value(key_count + agg_idx, row, std::move(agg_results[agg_idx][group_idx]));
                    } else {
                        result.set_value(key_count + agg_idx,
                                         row,
                                         types::logical_value_t(resource_, types::logical_type::NA));
                    }
                }
            }
            batches.emplace_back(std::move(result));
        }

        return batches;
    }

    void operator_external_group_t::calc_post_aggregates(pipeline::context_t* pipeline_context, vector::data_chunk_t& result) {
        auto num_groups = result.size();
        result.data.reserve(result.data.size() + post_aggregates_.size());

        for (auto& post : post_aggregates_) {
            // Determine result type from first row computation
            types::complex_logical_type col_type{types::logical_type::NA};

            auto resolve =
                [&](const expressions::param_storage& param, size_t row_idx, auto& self) -> types::logical_value_t {
                if (std::holds_alternative<expressions::key_t>(param)) {
                    auto& key = std::get<expressions::key_t>(param);
                    assert(!key.path().empty());
                    return result.value(key.path()[0], row_idx);
                } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                    auto id = std::get<core::parameter_id_t>(param);
                    return pipeline_context->parameters.parameters.at(id);
                } else {
                    auto& sub_expr = std::get<expressions::expression_ptr>(param);
                    if (sub_expr->group() == expressions::expression_group::scalar) {
                        auto* sub_scalar = static_cast<const expressions::scalar_expression_t*>(sub_expr.get());
                        if (sub_scalar->type() == expressions::scalar_type::unary_minus &&
                            !sub_scalar->params().empty()) {
                            auto inner = self(sub_scalar->params()[0], row_idx, self);
                            return types::logical_value_t::subtract(types::logical_value_t(resource_, int64_t(0)),
                                                                    inner);
                        }
                        if (sub_scalar->params().size() >= 2) {
                            auto left_val = self(sub_scalar->params()[0], row_idx, self);
                            auto right_val = self(sub_scalar->params()[1], row_idx, self);
                            switch (sub_scalar->type()) {
                                case expressions::scalar_type::add:
                                    return types::logical_value_t::sum(left_val, right_val);
                                case expressions::scalar_type::subtract:
                                    return types::logical_value_t::subtract(left_val, right_val);
                                case expressions::scalar_type::multiply:
                                    return types::logical_value_t::mult(left_val, right_val);
                                case expressions::scalar_type::divide:
                                    return types::logical_value_t::divide(left_val, right_val);
                                case expressions::scalar_type::mod:
                                    return types::logical_value_t::modulus(left_val, right_val);
                                default:
                                    break;
                            }
                        }
                    }
                    assert(false && "Post-aggregate: unsupported sub-expression");
                    return types::logical_value_t(resource_, types::complex_logical_type{types::logical_type::NA});
                }
            };

            // Compute result for each group and collect into a new vector
            if (post.op == expressions::scalar_type::unary_minus) {
                if (post.operands.empty())
                    continue;
                std::pmr::vector<types::logical_value_t> col_values(resource_);
                for (size_t group_idx = 0; group_idx < num_groups; group_idx++) {
                    auto inner = resolve(post.operands[0], group_idx, resolve);
                    auto result_val =
                        types::logical_value_t::subtract(types::logical_value_t(resource_, int64_t(0)), inner);
                    result_val.set_alias(std::string(post.alias));
                    if (group_idx == 0) {
                        col_type = result_val.type();
                    }
                    col_values.push_back(std::move(result_val));
                }
                vector::vector_t new_col(resource_, col_type, result.capacity());
                for (size_t group_idx = 0; group_idx < num_groups; group_idx++) {
                    new_col.set_value(group_idx, std::move(col_values[group_idx]));
                }
                new_col.set_type_alias(std::string(post.alias));
                result.data.emplace_back(std::move(new_col));
                continue;
            }
            if (post.operands.size() < 2)
                continue;
            std::pmr::vector<types::logical_value_t> col_values(resource_);
            for (size_t group_idx = 0; group_idx < num_groups; group_idx++) {
                auto left_val = resolve(post.operands[0], group_idx, resolve);
                auto right_val = resolve(post.operands[1], group_idx, resolve);
                types::logical_value_t result_val(resource_, types::complex_logical_type{types::logical_type::NA});
                switch (post.op) {
                    case expressions::scalar_type::add:
                        result_val = types::logical_value_t::sum(left_val, right_val);
                        break;
                    case expressions::scalar_type::subtract:
                        result_val = types::logical_value_t::subtract(left_val, right_val);
                        break;
                    case expressions::scalar_type::multiply:
                        result_val = types::logical_value_t::mult(left_val, right_val);
                        break;
                    case expressions::scalar_type::divide:
                        result_val = types::logical_value_t::divide(left_val, right_val);
                        break;
                    case expressions::scalar_type::mod:
                        result_val = types::logical_value_t::modulus(left_val, right_val);
                        break;
                    default:
                        break;
                }
                result_val.set_alias(std::string(post.alias));
                if (group_idx == 0) {
                    col_type = result_val.type();
                }
                col_values.push_back(std::move(result_val));
            }

            // Add new column to result chunk
            vector::vector_t new_col(resource_, col_type, result.capacity());
            for (size_t group_idx = 0; group_idx < num_groups; group_idx++) {
                new_col.set_value(group_idx, std::move(col_values[group_idx]));
            }
            new_col.set_type_alias(std::string(post.alias));
            result.data.emplace_back(std::move(new_col));
        }
    }

    void operator_external_group_t::filter_having(pipeline::context_t* pipeline_context, vector::data_chunk_t& result) {
        if (!having_ || having_->group() != expressions::expression_group::compare) {
            return;
        }
        auto* cmp = static_cast<const expressions::compare_expression_t*>(having_.get());

        auto resolve =
            [&](const expressions::param_storage& param, size_t row_idx, auto& self) -> types::logical_value_t {
            if (std::holds_alternative<expressions::key_t>(param)) {
                auto& key = std::get<expressions::key_t>(param);
                assert(!key.path().empty());
                return result.value(key.path()[0], row_idx);
            } else if (std::holds_alternative<core::parameter_id_t>(param)) {
                auto id = std::get<core::parameter_id_t>(param);
                return pipeline_context->parameters.parameters.at(id);
            } else {
                auto& sub_expr = std::get<expressions::expression_ptr>(param);
                if (sub_expr->group() == expressions::expression_group::scalar) {
                    auto* scalar = static_cast<const expressions::scalar_expression_t*>(sub_expr.get());
                    if (scalar->type() == expressions::scalar_type::unary_minus && !scalar->params().empty()) {
                        auto inner = self(scalar->params()[0], row_idx, self);
                        return types::logical_value_t::subtract(types::logical_value_t(resource_, int64_t(0)), inner);
                    }
                    if (scalar->params().size() >= 2) {
                        auto left_val = self(scalar->params()[0], row_idx, self);
                        auto right_val = self(scalar->params()[1], row_idx, self);
                        switch (scalar->type()) {
                            case expressions::scalar_type::add:
                                return types::logical_value_t::sum(left_val, right_val);
                            case expressions::scalar_type::subtract:
                                return types::logical_value_t::subtract(left_val, right_val);
                            case expressions::scalar_type::multiply:
                                return types::logical_value_t::mult(left_val, right_val);
                            case expressions::scalar_type::divide:
                                return types::logical_value_t::divide(left_val, right_val);
                            case expressions::scalar_type::mod:
                                return types::logical_value_t::modulus(left_val, right_val);
                            default:
                                break;
                        }
                    }
                }
                return types::logical_value_t(resource_, types::complex_logical_type{types::logical_type::NA});
            }
        };

        std::pmr::vector<size_t> keep_indices(resource_);
        for (size_t group_idx = 0; group_idx < result.size(); group_idx++) {
            auto left_val = resolve(cmp->left(), group_idx, resolve);
            auto right_val = resolve(cmp->right(), group_idx, resolve);
            auto promoted_type = types::promote_type(left_val.type().type(), right_val.type().type());
            left_val = left_val.cast_as(promoted_type, pipeline_context->session_tz);
            right_val = right_val.cast_as(promoted_type, pipeline_context->session_tz);
            auto cmp_result = left_val.compare(right_val);
            bool passes = false;
            switch (cmp->type()) {
                case expressions::compare_type::gt:
                    passes = cmp_result == types::compare_t::more;
                    break;
                case expressions::compare_type::gte:
                    passes = cmp_result >= types::compare_t::equals;
                    break;
                case expressions::compare_type::lt:
                    passes = cmp_result == types::compare_t::less;
                    break;
                case expressions::compare_type::lte:
                    passes = cmp_result <= types::compare_t::equals;
                    break;
                case expressions::compare_type::eq:
                    passes = cmp_result == types::compare_t::equals;
                    break;
                case expressions::compare_type::ne:
                    passes = cmp_result != types::compare_t::equals;
                    break;
                default:
                    passes = true;
                    break;
            }
            if (passes) {
                keep_indices.push_back(group_idx);
            }
        }

        if (keep_indices.size() < result.size()) {
            static_assert(sizeof(size_t) == sizeof(uint64_t), "size_t must be 64-bit");
            auto keep_count = static_cast<uint64_t>(keep_indices.size());
            vector::indexing_vector_t idx(resource_, reinterpret_cast<uint64_t*>(keep_indices.data()));
            result.slice(idx, keep_count);
            result.flatten();
        }
    }

    void operator_external_group_t::stamp_spill_context(pipeline::context_t* pipeline_context) {
        // Stamp the pipeline's real MVCC snapshot into the spill state so the
        // partition headers carry a meaningful horizon.
        grace_state_.snapshot_horizon = pipeline_context ? spill_snapshot_horizon(*pipeline_context) : 0;
        // Per-query unique id so concurrent spill files never collide.
        grace_state_.query_id = pipeline_context ? pipeline_context->session.data() : 0;
        // R10: resolve the spill dir from ctx->disk_config. When the pipeline
        // context is absent (unit-test paths) fall back to the OS temp dir so
        // spill_file_t still has a concrete create_directory target.
        grace_state_.spill_dir = components::table::storage::resolve_spill_dir(
            pipeline_context ? pipeline_context->disk_config : nullptr);
        // R10: also read the per-query partition_count from ctx->disk_config.
        // The struct default stands when ctx is null.
        if (pipeline_context && pipeline_context->disk_config) {
            grace_state_.partition_count = pipeline_context->disk_config->partition_count;
        }
    }

    bool operator_external_group_t::partition_and_spill_groups(std::pmr::string& error_msg) {
        // R6: no memory-threshold gate, no fallback. This operator exists precisely
        // because the optimizer decided spill is required — so it always spills.

        // Create partition buffers: each holds (key_values, row_refs) for that partition
        struct partition_group_data_t {
            std::pmr::vector<std::pmr::vector<types::logical_value_t>> group_keys;
            std::pmr::vector<std::pmr::vector<row_ref_t>> row_refs_per_group;
            std::pmr::memory_resource* resource;

            partition_group_data_t(std::pmr::memory_resource* res)
                : group_keys(res)
                , row_refs_per_group(res)
                , resource(res) {}

            void add_group(const std::pmr::vector<types::logical_value_t>& keys,
                          const std::pmr::vector<row_ref_t>& refs) {
                group_keys.emplace_back(keys);
                row_refs_per_group.emplace_back(refs);
            }
        };

        std::pmr::vector<partition_group_data_t> partitions(resource_);
        partitions.reserve(grace_state_.partition_count);
        for (uint32_t i = 0; i < grace_state_.partition_count; ++i) {
            partitions.emplace_back(resource_);
        }

        // Partition groups by hash(composite_key) % partition_count
        for (size_t g = 0; g < group_keys_.size(); ++g) {
            const auto& key_vals = group_keys_[g];
            size_t hash_val = types::hash_row(key_vals);
            uint32_t partition_id = static_cast<uint32_t>(hash_val % grace_state_.partition_count);

            partitions[partition_id].add_group(key_vals, row_refs_per_group_[g]);
        }

        // Spill each partition through spill_file_t (RAII). The spill dir is
        // created lazily, names are unique per query, and the file is removed on
        // destruction (every exit path). Handles live in grace_state_.partition_handles
        // across the whole spill -> merge cycle.
        auto dir = grace_state_.spill_dir;

        for (uint32_t pid = 0; pid < grace_state_.partition_count; ++pid) {
            auto& partition = partitions[pid];
            if (partition.group_keys.empty()) {
                continue; // Skip empty partitions
            }

            auto name = components::table::storage::make_spill_name(
                grace_state_.query_id,
                std::string("agg_partition_") + std::to_string(pid));
            auto sp = std::make_unique<components::table::storage::spill_file_t>(
                grace_state_.fs, dir, name);
            if (!sp->valid()) {
                error_msg = "Failed to create partition file: " + sp->full_path();
                return false;
            }
            auto& fs = sp->fs();
            auto& file_handle = sp->handle();

            // For aggregate spill, we store (group_keys + row_refs) per partition
            // We serialize this as a single chunk with:
            // - Column 0: partition_id (constant)
            // - Column 1: group_index within partition
            // - Columns 2+: group key values
            // - Then append row_refs metadata

            // Build a chunk containing all groups from this partition
            // For simplicity, we serialize groups one by one
            for (size_t g = 0; g < partition.group_keys.size(); ++g) {
                const auto& keys = partition.group_keys[g];
                const auto& refs = partition.row_refs_per_group[g];

                if (keys.empty()) {
                    continue; // Skip groups with no keys (global aggregate)
                }

                // Create a chunk for this group's keys
                std::pmr::vector<types::complex_logical_type> key_types(resource_);
                for (const auto& key : keys) {
                    key_types.push_back(key.type());
                }

                if (key_types.empty()) {
                    continue;
                }

                vector::data_chunk_t group_chunk(resource_, key_types, 1);
                group_chunk.set_cardinality(1);

                // Fill key values
                for (size_t k = 0; k < keys.size(); ++k) {
                    group_chunk.set_value(k, 0, types::logical_value_t(keys[k]));
                }

                // Serialize using unified format
                components::table::storage::unified_format_header header;
                std::memset(&header, 0, sizeof(header));
                std::memcpy(header.magic, "OTSC1.0", 8);
                header.version = 1;
                // Stamp the real MVCC snapshot.
                header.snapshot_horizon = grace_state_.snapshot_horizon;
                header.table_oid = pid;
                header.column_count = static_cast<uint32_t>(key_types.size());
                header.row_count = 1;
                header.row_group_count = 1;

                components::table::storage::file_buffer_t file_buffer(resource_,
                    components::table::storage::file_buffer_type::MANAGED_BUFFER, 0);

                auto serialize_err =
                    components::table::storage::serialize_unified(group_chunk, file_buffer, header);
                if (serialize_err.contains_error()) {
                    error_msg = serialize_err.what; // carry the serializer's real reason
                    return false;
                }

                // Write to file. Layout per group is length-prefixed so the reader
                // can walk frames by their exact serialized size:
                //   [uint64 frame_size][frame: frame_size bytes]
                //   [uint32 ref_count ][ref_count * row_ref_t]
                // Offsets are tracked explicitly because file_buffer_t::write writes
                // at an absolute location and does not advance a seek cursor reliably
                // across all platforms.
                const uint64_t frame_size = file_buffer.size();
                components::table::storage::file_buffer_t size_buffer(resource_,
                    components::table::storage::file_buffer_type::MANAGED_BUFFER, 0);
                size_buffer.resize(sizeof(uint64_t));
                std::memcpy(size_buffer.internal_buffer(), &frame_size, sizeof(uint64_t));

                uint64_t offset = core::filesystem::seek_position(fs, file_handle);
                // Write EXACTLY sizeof(uint64_t) bytes — file_buffer_t::write emits
                // the full sector-rounded internal_size_ allocation (4096 B for an
                // 8-byte buffer), which overwrites the following frame and refs and
                // corrupts the offset of the next group's length prefix.
                file_handle.write(size_buffer.internal_buffer(), sizeof(uint64_t), offset);
                // Write exactly frame_size bytes for the same reason.
                file_handle.write(file_buffer.internal_buffer(), frame_size,
                                  offset + sizeof(uint64_t));
                // Advance past size + frame so the refs land contiguously.
                core::filesystem::seek(fs, file_handle,
                                       offset + sizeof(uint64_t) + frame_size);
                uint64_t refs_offset = offset + sizeof(uint64_t) + frame_size;

                // Write row_refs count and data as metadata using a separate buffer
                uint32_t ref_count = static_cast<uint32_t>(refs.size());
                uint64_t refs_size = sizeof(uint32_t) + (ref_count * sizeof(row_ref_t));

                components::table::storage::file_buffer_t refs_buffer(resource_,
                    components::table::storage::file_buffer_type::MANAGED_BUFFER, 0);
                refs_buffer.resize(refs_size);

                // Write ref count
                std::memcpy(refs_buffer.internal_buffer(), &ref_count, sizeof(uint32_t));

                // Write refs data
                if (ref_count > 0) {
                    std::memcpy(static_cast<std::byte*>(refs_buffer.internal_buffer()) + sizeof(uint32_t),
                               refs.data(), ref_count * sizeof(row_ref_t));
                }

                // Write EXACTLY refs_size bytes (see size_buffer note above).
                file_handle.write(refs_buffer.internal_buffer(), refs_size, refs_offset);
                // Advance past the refs so the next group starts at the right spot.
                core::filesystem::seek(fs, file_handle, refs_offset + refs_size);
            }

            sp->sync();
            // Keep the RAII owner across the spill -> merge cycle.
            grace_state_.partition_handles.push_back(std::move(sp));
        }

        grace_state_.spilled = true;
        return true;
    }

    bool operator_external_group_t::load_aggregate_partition(uint32_t partition_id,
                                                             std::pmr::vector<std::pmr::vector<types::logical_value_t>>& loaded_keys,
                                                             std::pmr::vector<std::pmr::vector<row_ref_t>>& loaded_refs,
                                                             std::pmr::string& error_msg) {
        std::string partition_path = grace_state_.spill_dir + "/" +
            components::table::storage::make_spill_name(
                grace_state_.query_id,
                std::string("agg_partition_") + std::to_string(partition_id));

        core::filesystem::local_file_system_t fs;

        // Check if file exists
        if (!core::filesystem::file_exists(fs, partition_path)) {
            // Empty partition - not an error
            return true;
        }

        // Open file for reading
        auto file_handle = core::filesystem::open_file(fs, partition_path,
            core::filesystem::file_flags::READ);

        if (!file_handle) {
            error_msg = "Failed to open partition file: " + partition_path;
            return false;
        }

        // Get file size
        int64_t file_sz = core::filesystem::file_size(fs, *file_handle);
        if (file_sz <= 0) {
            // Empty partition
            return true;
        }

        // Read exactly file_sz bytes into the buffer. file_buffer_t::resize rounds
        // the allocation up to a sector boundary and reserves a header
        // (size_ = internal_size_ - header), so file_buffer.size() OVER-reports
        // the usable byte count for a small partition frame file — a 200-byte
        // file reports size_ ≈ 4088. Walking frames against that inflated bound
        // reads past the real data into uninitialized allocation and trips
        // "Invalid/truncated frame". Bind the loop to the real file length.
        const uint64_t data_size = static_cast<uint64_t>(file_sz);
        components::table::storage::file_buffer_t file_buffer(resource_,
            components::table::storage::file_buffer_type::MANAGED_BUFFER, 0);
        file_buffer.resize(data_size);
        file_buffer.read(*file_handle, 0);

        // Deserialize groups from unified format. Per-group layout written by
        // partition_and_spill_groups is length-prefixed:
        //   [uint64 frame_size][frame: frame_size bytes][uint32 ref_count][refs...]
        // so the reader walks frames by their exact serialized size.
        uint64_t offset = 0;

        while (offset < data_size) {
            // Read the length prefix.
            if (offset + sizeof(uint64_t) > data_size) {
                break; // trailing padding / partial frame
            }
            uint64_t frame_size;
            std::memcpy(&frame_size, file_buffer.internal_buffer() + offset, sizeof(uint64_t));
            offset += sizeof(uint64_t);

            if (frame_size == 0 || offset + frame_size > data_size) {
                error_msg = "Invalid/truncated frame in partition file: " + partition_path;
                return false;
            }

            // Copy the frame into its own buffer and deserialize.
            components::table::storage::file_buffer_t chunk_buffer(resource_,
                components::table::storage::file_buffer_type::MANAGED_BUFFER, 0);
            chunk_buffer.resize(frame_size);
            std::memcpy(chunk_buffer.internal_buffer(),
                        file_buffer.internal_buffer() + offset, frame_size);
            offset += frame_size;

            components::table::storage::unified_format_header header;
            auto de = components::table::storage::deserialize_unified(chunk_buffer, resource_, header);
            if (de.has_error()) {
                error_msg = de.error().what; // carry the deserializer's real reason
                return false;
            }
            auto group_chunk = std::move(de.value());
            if (group_chunk.size() != 1) {
                error_msg = "Deserialized group chunk has unexpected row count from partition: " + partition_path;
                return false;
            }

            // Read row_refs metadata
            if (offset + sizeof(uint32_t) > data_size) {
                error_msg = "Unexpected end of file while reading row_refs: " + partition_path;
                return false;
            }

            uint32_t ref_count;
            std::memcpy(&ref_count, file_buffer.internal_buffer() + offset, sizeof(uint32_t));
            offset += sizeof(uint32_t);

            std::pmr::vector<row_ref_t> refs(resource_);
            if (ref_count > 0) {
                if (offset + ref_count * sizeof(row_ref_t) > data_size) {
                    error_msg = "Insufficient data for row_refs in partition: " + partition_path;
                    return false;
                }

                refs.resize(ref_count);
                std::memcpy(refs.data(), file_buffer.internal_buffer() + offset, ref_count * sizeof(row_ref_t));
                offset += ref_count * sizeof(row_ref_t);
            }

            // Extract key values.
            std::pmr::vector<types::logical_value_t> key_vals(resource_);
            for (size_t k = 0; k < group_chunk.column_count(); ++k) {
                key_vals.push_back(group_chunk.value(k, 0));
            }
            loaded_keys.emplace_back(std::move(key_vals));
            loaded_refs.emplace_back(std::move(refs));
        }

        return true;
    }

    chunks_vector_t operator_external_group_t::merge_partition_aggregates(pipeline::context_t* pipeline_context,
                                                                          const chunks_vector_t& in_chunks,
                                                                          std::pmr::string& error_msg) {
        // Accumulate final results across all partitions
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> final_agg_results(resource_);
        final_agg_results.reserve(values_.size());
        for (size_t a = 0; a < values_.size(); ++a) {
            final_agg_results.emplace_back(std::pmr::vector<types::logical_value_t>(resource_));
        }
        // Per-group accumulated row count across partitions, used to weight
        // AVG during commutative merge (mean + count combine).
        std::pmr::vector<uint64_t> group_row_counts(resource_);

        // Accumulate group keys (need to deduplicate by key hash)
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> all_keys(resource_);
        std::pmr::unordered_map<size_t, size_t> key_to_result_idx(resource_);

        // Process each partition
        for (uint32_t pid = 0; pid < grace_state_.partition_count; ++pid) {
            // Load partition data
            std::pmr::vector<std::pmr::vector<types::logical_value_t>> partition_keys(resource_);
            std::pmr::vector<std::pmr::vector<row_ref_t>> partition_refs(resource_);

            if (!load_aggregate_partition(pid, partition_keys, partition_refs, error_msg)) {
                // Return empty result on failure
                chunks_vector_t empty(resource_);
                return empty;
            }

            if (partition_keys.empty()) {
                continue; // Skip empty partitions
            }

            // Compute aggregates for this partition
            for (size_t g = 0; g < partition_keys.size(); ++g) {
                const auto& keys = partition_keys[g];
                const auto& refs = partition_refs[g];

                if (refs.empty()) {
                    continue; // Skip groups with no rows
                }

                // Build chunks from refs.
                std::pmr::vector<vector::data_chunk_t> gathered_chunks(resource_);
                for (const auto& [chunk_idx, row_idx] : refs) {
                    if (chunk_idx >= in_chunks.size()) {
                        continue;
                    }
                    const auto& src_chunk = in_chunks[chunk_idx];
                    if (row_idx >= src_chunk.size()) {
                        continue;
                    }

                    // Create single-row chunk
                    vector::data_chunk_t single_row(resource_, src_chunk.types(), 1);
                    single_row.set_cardinality(1);
                    for (size_t c = 0; c < src_chunk.column_count(); ++c) {
                        single_row.set_value(c, 0, src_chunk.value(c, row_idx));
                    }
                    gathered_chunks.emplace_back(std::move(single_row));
                }

                if (gathered_chunks.empty()) {
                    continue;
                }

                auto group_batch = make_operator_batch(resource_, std::move(gathered_chunks));

                // Check if this key combination already exists in final results
                size_t key_hash = types::hash_row(keys);
                auto it = key_to_result_idx.find(key_hash);
                bool is_new = (it == key_to_result_idx.end());
                size_t result_idx = is_new ? all_keys.size() : it->second;

                if (is_new) {
                    // New key combination: add to all_keys and compute fresh aggregates
                    all_keys.emplace_back(keys);
                    key_to_result_idx[key_hash] = result_idx;

                    // Initialize aggregate results for this new group
                    for (size_t a = 0; a < values_.size(); ++a) {
                        final_agg_results[a].push_back(types::logical_value_t(resource_, types::logical_type::NA));
                    }
                    // Seed this group's running row count with the rows in this
                    // partition (used to weight AVG during the merge below).
                    group_row_counts.push_back(refs.size());

                    // Compute aggregates for this group
                    for (size_t a = 0; a < values_.size(); ++a) {
                        auto& aggregator = values_[a].aggregator;
                        aggregator->clear();
                        aggregator->set_children(group_batch);
                        aggregator->on_execute(pipeline_context);

                        if (aggregator->has_error()) {
                            set_error(aggregator->get_error());
                            chunks_vector_t empty(resource_);
                            return empty;
                        }

                        auto datum = aggregator->take_batch_values();
                        types::logical_value_t val(resource_, types::logical_type::NA);
                        if (std::holds_alternative<std::pmr::vector<types::logical_value_t>>(datum)) {
                            auto& vals = std::get<std::pmr::vector<types::logical_value_t>>(datum);
                            if (!vals.empty()) {
                                val = std::move(vals.front());
                            }
                        }
                        val.set_alias(std::string(values_[a].name));
                        final_agg_results[a][result_idx] = std::move(val);
                    }
                } else {
                    // Existing key: merge aggregates
                    // For commutative aggregates (SUM, COUNT, MIN, MAX), we combine results
                    for (size_t a = 0; a < values_.size(); ++a) {
                        auto& aggregator = values_[a].aggregator;
                        aggregator->clear();
                        aggregator->set_children(group_batch);
                        aggregator->on_execute(pipeline_context);

                        if (aggregator->has_error()) {
                            set_error(aggregator->get_error());
                            chunks_vector_t empty(resource_);
                            return empty;
                        }

                        auto datum = aggregator->take_batch_values();
                        types::logical_value_t new_val(resource_, types::logical_type::NA);
                        if (std::holds_alternative<std::pmr::vector<types::logical_value_t>>(datum)) {
                            auto& vals = std::get<std::pmr::vector<types::logical_value_t>>(datum);
                            if (!vals.empty()) {
                                new_val = std::move(vals.front());
                            }
                        }

                        // Commutative combine of the new partial (folded over this
                        // partition's rows for the group) into the accumulated result:
                        // SUM +=, COUNT +=, MIN/MAX compare, AVG = weighted mean.
                        auto& existing = final_agg_results[a][result_idx];
                        const std::string kind = values_[a].aggregator->aggregate_key();
                        uint64_t existing_count = group_row_counts[result_idx];
                        uint64_t new_count = refs.size();
                        merge_aggregate_partial(resource_, kind, existing, new_val,
                                                existing_count, new_count);
                        // Advance the running count so a later partition's AVG combine
                        // sees the full accumulated weight.
                        group_row_counts[result_idx] = existing_count + new_count;
                    }
                }
            }
        }

        // Build final result chunk
        size_t num_groups = all_keys.size();
        size_t key_count = num_groups > 0 ? all_keys[0].size() : 0;
        auto result = build_result_chunk(num_groups, key_count, final_agg_results, in_chunks);

        // Drop the RAII spill handles — their destructors remove the temp
        // files (success path). On an error early-return above, grace_state_ still
        // owns them and they are cleaned when the operator is destroyed.
        grace_state_.partition_handles.clear();

        return result;
    }

    void operator_external_group_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            auto in = left_->output();
            auto& in_chunks = in->chunks();

            // Phase 1: Pre-compute arithmetic columns on EACH input chunk (no concat)
            // and resolve the col_index for computed-column keys.
            size_t first_computed_col = 0;
            if (!evaluate_computed_columns(pipeline_context, in_chunks, first_computed_col)) {
                return; // arithmetic-eval error already stamped via set_error()
            }

            // Phase 2: Group by keys, or treat entire input as one group when there are no keys.
            if (keys_.empty()) {
                // Global aggregate (no GROUP BY): logically a single group, so spilling
                // is pointless. Aggregate in memory — the optimizer only selects the
                // external strategy when the GROUP BY cardinality risks exceeding the
                // in-memory budget, which never applies to a single group.
                size_t total = 0;
                for (const auto& c : in_chunks) total += c.size();
                std::pmr::vector<row_ref_t> all_refs(resource_);
                all_refs.reserve(total);
                for (uint32_t ci = 0; ci < in_chunks.size(); ++ci) {
                    auto sz = static_cast<uint32_t>(in_chunks[ci].size());
                    for (uint32_t r = 0; r < sz; ++r) {
                        all_refs.emplace_back(ci, r);
                    }
                }
                row_refs_per_group_.push_back(std::move(all_refs));
                group_keys_.push_back({});

                // In-memory aggregate for the single global group.
                auto batches = calc_aggregate_values(pipeline_context, in_chunks);
                if (has_error()) {
                    return;
                }
                finalize_output(pipeline_context, in, in_chunks, batches, first_computed_col);
                return;
            }

            // Keyed aggregate: build the in-memory grouping index first (needed to
            // partition groups by key hash for the spill).
            create_list_rows(in_chunks);
            if (has_error()) {
                return;
            }

            // R10: stamp the real MVCC snapshot + per-query id + resolve the
            // spill dir from ctx->disk_config before touching the spill state.
            stamp_spill_context(pipeline_context);

            // Guard against partition_count == 0 — partition_and_spill_groups
            // divides by partition_count (hash % partition_count), so a zero would
            // raise SIGFPE. Treat as a hard error (R2: surface failure, no throw).
            if (grace_state_.partition_count == 0) {
                set_error(core::error_t(core::error_code_t::invalid_parameter,
                                        std::pmr::string("grace aggregate: partition_count is zero", resource_)));
                return;
            }

            // Proactive spill (R6: no threshold gate, no in-memory fallback). This
            // operator exists because the optimizer decided spill is required, so it
            // always partitions and spills the groups, then merges the per-partition
            // partials via commutative combine.
            std::pmr::string error_msg(resource_);
            bool spilled = partition_and_spill_groups(error_msg);
            if (!spilled) {
                if (log_.is_valid()) {
                    warn(log(), "operator_external_group: spill failed: {}", static_cast<std::string>(error_msg));
                }
                set_error(core::error_t(core::error_code_t::io_error,
                                        std::pmr::string{"grace aggregate spill failed: ", resource_} + error_msg));
                return;
            }
            grace_state_.spilled = true;
            if (log_.is_valid()) {
                trace(log(), "operator_external_group::groups_spilled: {}", true);
            }

            // Execute grace merge from spilled partitions.
            auto batches = merge_partition_aggregates(pipeline_context, in_chunks, error_msg);
            if (has_error()) {
                return;
            }
            if (batches.empty()) {
                if (log_.is_valid()) {
                    trace(log(), "operator_external_group::grace_merge_failed: {}", static_cast<std::string>(error_msg));
                }
                set_error(core::error_t(core::error_code_t::io_error,
                                        std::pmr::string{"grace aggregate merge failed: ", resource_} + error_msg));
                return;
            }

            finalize_output(pipeline_context, in, in_chunks, batches, first_computed_col);
        } else if (keys_.empty() && !values_.empty()) {
            // Global aggregate over empty input (e.g. SELECT COUNT(*) FROM empty_table).
            // Same path as the in-memory operator: spill is meaningless for zero rows.
            std::pmr::vector<std::pmr::vector<types::logical_value_t>> agg_results(resource_);
            agg_results.reserve(values_.size());
            chunks_vector_t empty_chunks(resource_);
            auto shared_batch = make_operator_batch(resource_, std::move(empty_chunks));
            for (const auto& value : values_) {
                value.aggregator->clear();
                value.aggregator->set_children(shared_batch);
                value.aggregator->on_execute(pipeline_context);
                if (value.aggregator->has_error()) {
                    set_error(value.aggregator->get_error());
                    return;
                }
                auto datum = value.aggregator->take_batch_values();
                std::pmr::vector<types::logical_value_t> results(resource_);
                if (std::holds_alternative<std::pmr::vector<types::logical_value_t>>(datum)) {
                    auto& vals = std::get<std::pmr::vector<types::logical_value_t>>(datum);
                    types::logical_value_t val =
                        vals.empty() ? types::logical_value_t(resource_, types::logical_type::NA) : std::move(vals[0]);
                    val.set_alias(std::string(value.name));
                    results.push_back(std::move(val));
                }
                agg_results.push_back(std::move(results));
            }
            group_keys_.push_back({});
            chunks_vector_t empty_in_chunks(resource_);
            auto batches = build_result_chunk(1, 0, agg_results, empty_in_chunks);
            output_ = operators::make_operator_data(resource_, std::move(batches));
        } else if (!computed_columns_.empty()) {
            // Constants-only query (no FROM clause): evaluate arithmetic on a virtual single row.
            std::pmr::vector<types::complex_logical_type> empty_types(resource_);
            vector::data_chunk_t chunk(resource_, empty_types, 1);
            chunk.set_cardinality(1);

            for (auto& comp : computed_columns_) {
                auto result_vec = evaluate_arithmetic(resource_,
                                                      comp.op,
                                                      comp.operands,
                                                      chunk,
                                                      pipeline_context->parameters,
                                                      pipeline_context->session_tz);
                if (result_vec.has_error()) {
                    set_error(result_vec.error());
                    return;
                }
                result_vec.value().set_type_alias(std::string(comp.alias));
                chunk.data.emplace_back(std::move(result_vec.value()));
            }

            output_ = operators::make_operator_data(resource_, std::move(chunk));
        }
    }

} // namespace components::operators
