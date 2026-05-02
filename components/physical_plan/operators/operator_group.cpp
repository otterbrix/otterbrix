#include "operator_group.hpp"

#include "arithmetic_eval.hpp"
#include <cassert>
#include <components/compute/function.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/aggregate/grouped_aggregate.hpp>
#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/operator_batch.hpp>
#include <components/vector/vector_operations.hpp>
#include <core/operations_helper.hpp>
#include <type_traits>

namespace components::operators {

    namespace {
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
    } // anonymous namespace

    operator_group_t::operator_group_t(std::pmr::memory_resource* resource,
                                       log_t log,
                                       expressions::expression_ptr having,
                                       size_t internal_aggregate_count)
        : read_write_operator_t(resource, log, operator_type::aggregate)
        , keys_(resource_)
        , values_(resource_)
        , computed_columns_(resource_)
        , post_aggregates_(resource_)
        , having_(std::move(having))
        , internal_aggregate_count_(internal_aggregate_count)
        , row_refs_per_group_(resource_)
        , group_keys_(resource_)
        , group_index_(resource_) {}

    void operator_group_t::add_key(group_key_t&& key) { keys_.push_back(std::move(key)); }

    void operator_group_t::add_key(const std::pmr::string& name) {
        group_key_t key(resource_);
        key.name = name;
        key.type = group_key_t::kind::column;
        keys_.push_back(std::move(key));
    }

    void operator_group_t::add_value(const std::pmr::string& name, aggregate::operator_aggregate_ptr&& aggregator) {
        values_.push_back({name, std::move(aggregator)});
    }

    void operator_group_t::add_computed_column(computed_column_t&& col) {
        computed_columns_.emplace_back(std::move(col));
    }

    void operator_group_t::add_post_aggregate(post_aggregate_column_t&& col) {
        post_aggregates_.emplace_back(std::move(col));
    }

    void operator_group_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            auto in = left_->output();
            auto& in_chunks = in->chunks();

            // Phase 1: Pre-compute arithmetic columns on EACH input chunk (no concat).
            // All chunks share the same schema, so the column index of each computed key
            // is identical across chunks.
            size_t first_computed_col = 0;
            if (!in_chunks.empty()) {
                first_computed_col = in_chunks.front().data.size();
            }
            for (auto& chunk : in_chunks) {
                for (auto& comp : computed_columns_) {
                    auto result_vec = evaluate_arithmetic(resource_,
                                                          comp.op,
                                                          comp.operands,
                                                          chunk,
                                                          pipeline_context->parameters);
                    if (result_vec.has_error()) {
                        set_error(result_vec.error());
                        return;
                    } else if (result_vec.value().type().type() == types::logical_type::NA) {
                        set_error(core::error_t(
                            core::error_code_t::physical_plan_error,
                            std::pmr::string{"unknown error during evaluate_arithmetic", resource_}));
                        return;
                    }
                    result_vec.value().set_type_alias(std::string(comp.alias));
                    chunk.data.emplace_back(std::move(result_vec.value()));
                }
            }

            // Resolve col_index for computed-column keys (they were appended at known positions).
            if (!computed_columns_.empty()) {
                for (size_t ci = 0; ci < computed_columns_.size(); ci++) {
                    if (computed_columns_[ci].resolved_key_index != SIZE_MAX) {
                        keys_[computed_columns_[ci].resolved_key_index].full_path.emplace_back(
                            first_computed_col + ci);
                    }
                }
            }

            // Phase 2: Group by keys, or treat entire input as one group when there are no keys.
            if (keys_.empty()) {
                size_t total = 0;
                for (const auto& c : in_chunks)
                    total += c.size();
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
            } else {
                create_list_rows(in_chunks);
            }

            // Phase 3: Aggregate per group + build result chunk
            auto result = calc_aggregate_values(pipeline_context, in_chunks);

            // Phase 4: Post-aggregate arithmetic (columnar)
            size_t size_before_post = result.data.size();
            calc_post_aggregates(pipeline_context, result);

            // Phase 5: Remove internal aggregate columns by position
            if (internal_aggregate_count_ > 0) {
                auto it_end = result.data.begin() + static_cast<std::ptrdiff_t>(size_before_post);
                auto it_begin = it_end - static_cast<std::ptrdiff_t>(internal_aggregate_count_);
                result.data.erase(it_begin, it_end);
            }

            // Phase 6: HAVING filter (columnar)
            if (having_) {
                filter_having(pipeline_context, result);
            }

            // Phase 7: Output
            output_ = operators::make_operator_data(in->resource(), std::move(result));
            output_ = operators::split_large_output(in->resource(), std::move(output_));

            // Cleanup: strip the temporary computed-key columns from every input chunk.
            if (!computed_columns_.empty()) {
                for (auto& chunk : in_chunks) {
                    if (chunk.data.size() > first_computed_col) {
                        chunk.data.erase(chunk.data.begin() + static_cast<std::ptrdiff_t>(first_computed_col),
                                         chunk.data.end());
                    }
                }
            }

            // Clear temporary grouping state
            row_refs_per_group_.clear();
            group_keys_.clear();
            group_index_.clear();
        } else if (keys_.empty() && !values_.empty()) {
            // Global aggregate over empty input (e.g. SELECT COUNT(*) FROM empty_table).
            // Run each aggregator over zero rows and emit one result row.
            std::pmr::vector<std::pmr::vector<types::logical_value_t>> agg_results(resource_);
            agg_results.reserve(values_.size());
            for (const auto& value : values_) {
                std::vector<vector::data_chunk_t> empty_chunks;
                value.aggregator->clear();
                value.aggregator->set_children(make_operator_batch(resource_, std::move(empty_chunks)));
                value.aggregator->on_execute(pipeline_context);
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
            auto result = build_result_chunk(1, 0, agg_results);
            output_ = operators::make_operator_data(resource_, std::move(result));
        } else if (!computed_columns_.empty()) {
            // Constants-only query (no FROM clause): evaluate arithmetic on a virtual single row
            std::pmr::vector<types::complex_logical_type> empty_types(resource_);
            vector::data_chunk_t chunk(resource_, empty_types, 1);
            chunk.set_cardinality(1);

            for (auto& comp : computed_columns_) {
                auto result_vec =
                    evaluate_arithmetic(resource_, comp.op, comp.operands, chunk, pipeline_context->parameters);
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

    void operator_group_t::create_list_rows(const std::vector<vector::data_chunk_t>& in_chunks) {
        if (in_chunks.empty()) {
            return;
        }

        // Try fast path: all keys are simple column type with resolved col_index
        bool use_fast_path = true;
        std::pmr::vector<size_t> key_col_indices(resource_);
        for (const auto& key : keys_) {
            if (key.type != group_key_t::kind::column || key.full_path.size() > 1) {
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
                                row_refs_per_group_[idx].emplace_back(chunk_idx,
                                                                      static_cast<uint32_t>(row_idx));
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
                                row_refs_per_group_[idx].emplace_back(chunk_idx,
                                                                      static_cast<uint32_t>(row_idx));
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

    vector::data_chunk_t operator_group_t::calc_aggregate_values(pipeline::context_t* pipeline_context,
                                                                 std::vector<vector::data_chunk_t>& in_chunks) {
        size_t num_groups = group_keys_.size();
        size_t key_count = num_groups > 0 ? group_keys_[0].size() : 0;

        size_t total_rows = 0;
        for (const auto& c : in_chunks)
            total_rows += c.size();

        // Try vectorized path: check if all aggregators are builtin with simple column args
        bool can_vectorize = num_groups > 0 && total_rows > 0;
        struct agg_info_t {
            aggregate::builtin_agg kind;
            std::pmr::vector<size_t> full_path;
            types::logical_type col_type;
            bool is_count_star = false;
        };
        std::pmr::vector<agg_info_t> agg_infos(resource_);

        if (can_vectorize) {
            agg_infos.reserve(values_.size());
            // Use the first non-empty chunk to resolve column types. All chunks share schema.
            const vector::data_chunk_t* schema_chunk = nullptr;
            for (const auto& c : in_chunks) {
                if (c.size() > 0) {
                    schema_chunk = &c;
                    break;
                }
            }
            if (!schema_chunk) {
                schema_chunk = in_chunks.empty() ? nullptr : &in_chunks.front();
            }

            for (const auto& value : values_) {
                auto* func_op = dynamic_cast<aggregate::operator_func_t*>(value.aggregator.get());
                if (!func_op || !func_op->func()) {
                    can_vectorize = false;
                    break;
                }
                auto kind = aggregate::classify(func_op->func()->name());
                if (kind == aggregate::builtin_agg::UNKNOWN) {
                    can_vectorize = false;
                    break;
                }
                if (func_op->distinct()) {
                    can_vectorize = false;
                    break;
                }

                std::pmr::vector<size_t> col_path{{SIZE_MAX}, resource_};
                types::logical_type col_type = types::logical_type::NA;

                bool count_star = (kind == aggregate::builtin_agg::COUNT && func_op->args().empty());
                if (count_star) {
                    col_type = types::logical_type::UBIGINT;
                } else if (func_op->args().size() == 1 &&
                           std::holds_alternative<expressions::key_t>(func_op->args()[0])) {
                    auto& key = std::get<expressions::key_t>(func_op->args()[0]);
                    assert(!key.path().empty());
                    col_path = key.path();
                    if (col_path.empty() || col_path.front() == SIZE_MAX || !schema_chunk) {
                        can_vectorize = false;
                        break;
                    }
                    col_type = schema_chunk->at(col_path)->type().type();
                    if (!types::is_numeric(col_type)) {
                        can_vectorize = false;
                        break;
                    }
                } else {
                    can_vectorize = false;
                    break;
                }
                agg_infos.push_back({kind, col_path, col_type, count_star});
            }
        }

        if (can_vectorize) {
            // Build group_ids per chunk: group_ids_per_chunk[ci][row_in_chunk] = group_id.
            std::vector<std::pmr::vector<uint32_t>> group_ids_per_chunk;
            group_ids_per_chunk.reserve(in_chunks.size());
            for (const auto& chunk : in_chunks) {
                std::pmr::vector<uint32_t> ids(resource_);
                ids.assign(chunk.size(), UINT32_MAX);
                group_ids_per_chunk.push_back(std::move(ids));
            }
            for (size_t g = 0; g < num_groups; g++) {
                for (const auto& ref : row_refs_per_group_[g]) {
                    group_ids_per_chunk[ref.first][ref.second] = static_cast<uint32_t>(g);
                }
            }

            // For each aggregate, run vectorized update across all input chunks,
            // accumulating `states` incrementally.
            std::pmr::vector<std::pmr::vector<types::logical_value_t>> agg_results(resource_);
            agg_results.reserve(values_.size());

            for (size_t a = 0; a < values_.size(); a++) {
                std::pmr::vector<aggregate::raw_agg_state_t> states(resource_);
                states.resize(num_groups);

                auto& info = agg_infos[a];
                for (size_t ci = 0; ci < in_chunks.size(); ++ci) {
                    auto& chunk = in_chunks[ci];
                    auto n = chunk.size();
                    if (n == 0)
                        continue;
                    const auto* gids = group_ids_per_chunk[ci].data();
                    if (info.is_count_star) {
                        for (uint64_t i = 0; i < n; i++) {
                            if (gids[i] != UINT32_MAX) {
                                states[gids[i]].update_count();
                            }
                        }
                    } else {
                        aggregate::update_all(info.kind, *chunk.at(info.full_path), gids, n, states);
                    }
                }

                // Finalize states to logical_value_t
                std::pmr::vector<types::logical_value_t> results(resource_);
                results.reserve(num_groups);
                for (size_t g = 0; g < num_groups; g++) {
                    auto val = aggregate::finalize_state(resource_, info.kind, states[g], info.col_type);
                    val.set_alias(std::string(values_[a].name));
                    results.push_back(std::move(val));
                }
                agg_results.push_back(std::move(results));
            }

            return build_result_chunk(num_groups, key_count, agg_results);
        }

        // Fallback: gather per-group subchunk from multi-chunk source.
        return calc_aggregate_values_fallback(pipeline_context, in_chunks);
    }

    vector::data_chunk_t
    operator_group_t::calc_aggregate_values_fallback(pipeline::context_t* pipeline_context,
                                                     std::vector<vector::data_chunk_t>& in_chunks) {
        size_t num_groups = group_keys_.size();
        size_t key_count = num_groups > 0 ? group_keys_[0].size() : 0;

        // All chunks share the same schema — take types from the first one.
        std::pmr::vector<types::complex_logical_type> result_types{resource_};
        if (!in_chunks.empty()) {
            result_types = in_chunks.front().types();
        }
        size_t col_count = result_types.size();

        // Build per-group subchunk by gathering (chunk_idx, row_idx) pairs from the
        // multi-chunk source. Consecutive rows from the same source chunk are copied
        // in a single vector_ops::copy() to keep the cost close to a flat memcpy.
        // Group refs by source chunk, then issue one indexing-based copy per (chunk, column).
        // This collapses N small per-row copies into N_chunks bulk copies and is much cheaper
        // when refs are scattered (e.g. typical GROUP BY where each group's rows are spread
        // across many source chunks).
        auto gather_group = [&](const std::pmr::vector<row_ref_t>& refs) {
            uint64_t cnt = static_cast<uint64_t>(refs.size());
            vector::data_chunk_t grp(resource_, result_types, cnt > 0 ? cnt : 1);
            grp.set_cardinality(cnt);
            if (cnt == 0) {
                return grp;
            }
            // refs are inserted in (chunk_idx, row_idx) order during create_list_rows,
            // so all refs for a given chunk form one contiguous span.
            uint64_t pos = 0;
            while (pos < cnt) {
                uint32_t src_chunk = refs[pos].first;
                uint64_t span_start = pos;
                while (pos < cnt && refs[pos].first == src_chunk) {
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

                for (size_t c = 0; c < col_count; ++c) {
                    if (is_placeholder(src.data[c])) continue;
                    vector::vector_ops::copy(src.data[c],
                                             grp.data[c],
                                             indexing,
                                             span_len,
                                             0,
                                             span_start);
                }
                vector::vector_ops::copy(src.row_ids, grp.row_ids, indexing, span_len, 0, span_start);
            }
            return grp;
        };

        // Compute aggregate results: agg_results[agg_idx][group_idx]
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> agg_results(resource_);
        agg_results.reserve(values_.size());

        for (const auto& value : values_) {
            auto& aggregator = value.aggregator;
            std::pmr::vector<types::logical_value_t> results(resource_);
            results.reserve(num_groups);

            std::vector<vector::data_chunk_t> group_chunks;
            group_chunks.reserve(num_groups);
            for (size_t i = 0; i < num_groups; i++) {
                group_chunks.emplace_back(gather_group(row_refs_per_group_[i]));
            }

            aggregator->clear();
            aggregator->set_children(make_operator_batch(resource_, std::move(group_chunks)));
            aggregator->on_execute(pipeline_context);

            auto datum = aggregator->take_batch_values();

            if (std::holds_alternative<std::pmr::vector<types::logical_value_t>>(datum)) {
                auto& vals = std::get<std::pmr::vector<types::logical_value_t>>(datum);
                for (auto& v : vals) {
                    v.set_alias(std::string(value.name));
                    results.push_back(std::move(v));
                }
            } else {
                // data_chunk_t — each row corresponds to a group
                auto& result_chunk = std::get<vector::data_chunk_t>(datum);
                for (size_t i = 0; i < num_groups && i < result_chunk.size(); i++) {
                    auto val = result_chunk.data.empty() ? types::logical_value_t(resource_, types::logical_type::NA)
                                                         : result_chunk.value(0, i);
                    val.set_alias(std::string(value.name));
                    results.push_back(std::move(val));
                }
                while (results.size() < num_groups) {
                    results.emplace_back(resource_, types::logical_type::NA);
                }
            }

            agg_results.push_back(std::move(results));
        }

        return build_result_chunk(num_groups, key_count, agg_results);
    }

    vector::data_chunk_t
    operator_group_t::build_result_chunk(size_t num_groups,
                                         size_t key_count,
                                         std::pmr::vector<std::pmr::vector<types::logical_value_t>>& agg_results) {
        // Build result types: key types + aggregate types
        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        if (num_groups > 0) {
            for (size_t k = 0; k < key_count; k++) {
                out_types.push_back(group_keys_[0][k].type());
            }
        }
        for (size_t a = 0; a < values_.size(); a++) {
            if (!agg_results[a].empty()) {
                out_types.push_back(agg_results[a][0].type());
            }
        }

        // Create result chunk
        uint64_t cap = num_groups > 0 ? static_cast<uint64_t>(num_groups) : 1;
        vector::data_chunk_t result(resource_, out_types, cap);
        result.set_cardinality(static_cast<uint64_t>(num_groups));

        // Fill key columns
        for (size_t group_idx = 0; group_idx < num_groups; group_idx++) {
            for (size_t key_idx = 0; key_idx < key_count; key_idx++) {
                result.set_value(key_idx, group_idx, std::move(group_keys_[group_idx][key_idx]));
            }
        }

        // Fill aggregate columns
        for (size_t agg_idx = 0; agg_idx < values_.size(); agg_idx++) {
            for (size_t group_idx = 0; group_idx < num_groups; group_idx++) {
                result.set_value(key_count + agg_idx, group_idx, std::move(agg_results[agg_idx][group_idx]));
            }
        }

        return result;
    }

    void operator_group_t::calc_post_aggregates(pipeline::context_t* pipeline_context, vector::data_chunk_t& result) {
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

    void operator_group_t::filter_having(pipeline::context_t* pipeline_context, vector::data_chunk_t& result) {
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

} // namespace components::operators
