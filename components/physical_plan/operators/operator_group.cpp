#include "operator_group.hpp"

#include "arithmetic_eval.hpp"
#include <cassert>
#include <components/compute/function.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/aggregate/grouped_aggregate.hpp>
#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <core/operations_helper.hpp>
#include <type_traits>

namespace components::operators {

    namespace {
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
        , select_order_(resource_)
        , row_ids_per_group_(resource_)
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

    void operator_group_t::set_select_order(std::pmr::vector<size_t>&& order) { select_order_ = std::move(order); }

    void operator_group_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (left_ && left_->output()) {
            auto& chunk = left_->output()->data_chunk();

            // Phase 1: Pre-compute arithmetic columns (before grouping)
            for (auto& comp : computed_columns_) {
                auto [result_vec, arith_error] =
                    evaluate_arithmetic(resource_, comp.op, comp.operands, chunk, pipeline_context->parameters);
                if (!arith_error.empty()) {
                    set_error(std::move(arith_error));
                    return;
                }
                result_vec.set_type_alias(std::string(comp.alias));
                chunk.data.emplace_back(std::move(result_vec));
            }

            // Resolve col_index for computed-column keys (they were appended at known positions)
            if (!computed_columns_.empty()) {
                size_t base = chunk.column_count() - computed_columns_.size();
                for (size_t ci = 0; ci < computed_columns_.size(); ci++) {
                    if (computed_columns_[ci].resolved_key_index != SIZE_MAX) {
                        keys_[computed_columns_[ci].resolved_key_index].full_path.emplace_back(base + ci);
                    }
                }
            }

            // Phase 2: Group by keys (columnar, no transpose)
            create_list_rows();

            // Phase 3: Aggregate per group + build result chunk
            auto result = calc_aggregate_values(pipeline_context);

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

            // Phase 7: Reorder columns to match SELECT clause order
            if (!select_order_.empty()) {
                std::vector<vector::vector_t> reordered;
                reordered.reserve(select_order_.size());
                for (size_t idx : select_order_) {
                    reordered.emplace_back(std::move(result.data[idx]));
                }
                result.data.assign(std::make_move_iterator(reordered.begin()),
                                   std::make_move_iterator(reordered.end()));
            }

            // Phase 8: Output
            output_ = operators::make_operator_data(left_->output()->resource(), std::move(result));

            // Clear temporary grouping state
            row_ids_per_group_.clear();
            group_keys_.clear();
            group_index_.clear();
        } else if (!computed_columns_.empty()) {
            // Constants-only query (no FROM clause): evaluate arithmetic on a virtual single row
            std::pmr::vector<types::complex_logical_type> empty_types(resource_);
            vector::data_chunk_t chunk(resource_, empty_types, 1);
            chunk.set_cardinality(1);

            for (auto& comp : computed_columns_) {
                auto [result_vec, arith_error] =
                    evaluate_arithmetic(resource_, comp.op, comp.operands, chunk, pipeline_context->parameters);
                if (!arith_error.empty()) {
                    set_error(std::move(arith_error));
                    return;
                }
                result_vec.set_type_alias(std::string(comp.alias));
                chunk.data.emplace_back(std::move(result_vec));
            }

            output_ = operators::make_operator_data(resource_, std::move(chunk));
        }
    }

    void operator_group_t::create_list_rows() {
        auto& chunk = left_->output()->data_chunk();
        auto num_rows = chunk.size();

        if (num_rows == 0) {
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
            // Batch hash all rows at once using type-dispatched vector_ops::hash
            vector::vector_t hash_vec(resource_, types::logical_type::UBIGINT, num_rows);
            std::vector<uint64_t> col_ids(key_col_indices.begin(), key_col_indices.end());
            chunk.hash(col_ids, hash_vec);
            auto* hashes = hash_vec.data<uint64_t>();

            for (size_t row_idx = 0; row_idx < num_rows; row_idx++) {
                // Check for NULL keys via validity mask (no logical_value_t creation)
                bool is_valid = true;
                for (size_t col_idx : key_col_indices) {
                    if (chunk.data[col_idx].is_null(row_idx)) {
                        is_valid = false;
                        break;
                    }
                }
                if (!is_valid) {
                    continue;
                }

                auto hash_val = static_cast<size_t>(hashes[row_idx]);
                auto it = group_index_.find(hash_val);
                bool is_new = true;
                if (it != group_index_.end()) {
                    for (size_t idx : it->second) {
                        if (keys_match(chunk, key_col_indices, row_idx, group_keys_[idx])) {
                            row_ids_per_group_[idx].push_back(row_idx);
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
                    std::pmr::vector<size_t> row_ids(resource_);
                    row_ids.push_back(row_idx);
                    row_ids_per_group_.push_back(std::move(row_ids));
                }
            }
        } else {
            // Slow path: handles coalesce, case_when, wildcards, nested paths
            for (size_t row_idx = 0; row_idx < num_rows; row_idx++) {
                std::pmr::vector<types::logical_value_t> key_vals(resource_);
                bool is_valid = true;

                for (const auto& key : keys_) {
                    auto val = extract_key_value(resource_, key, chunk, row_idx);
                    if (val.is_null() && key.type == group_key_t::kind::column) {
                        is_valid = false;
                        break;
                    }
                    key_vals.push_back(std::move(val));
                }

                if (!is_valid) {
                    continue;
                }

                size_t hash_val = types::hash_row(key_vals);
                auto it = group_index_.find(hash_val);
                bool is_new = true;
                if (it != group_index_.end()) {
                    for (size_t idx : it->second) {
                        if (key_vals == group_keys_[idx]) {
                            row_ids_per_group_[idx].push_back(row_idx);
                            is_new = false;
                            break;
                        }
                    }
                }
                if (is_new) {
                    size_t idx = group_keys_.size();
                    group_index_[hash_val].push_back(idx);
                    group_keys_.push_back(std::move(key_vals));
                    std::pmr::vector<size_t> row_ids(resource_);
                    row_ids.push_back(row_idx);
                    row_ids_per_group_.push_back(std::move(row_ids));
                }
            }
        }
    }

    vector::data_chunk_t operator_group_t::calc_aggregate_values(pipeline::context_t* pipeline_context) {
        auto& chunk = left_->output()->data_chunk();
        size_t num_groups = group_keys_.size();
        size_t key_count = num_groups > 0 ? group_keys_[0].size() : 0;
        auto num_rows = chunk.size();

        // Try vectorized path: check if all aggregators are builtin with simple column args
        bool can_vectorize = num_groups > 0 && num_rows > 0;
        struct agg_info_t {
            aggregate::builtin_agg kind;
            std::pmr::vector<size_t> full_path;
            types::logical_type col_type;
            bool is_count_star = false;
        };
        std::pmr::vector<agg_info_t> agg_infos(resource_);

        if (can_vectorize) {
            agg_infos.reserve(values_.size());
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

                std::pmr::vector<size_t> col_path{{SIZE_MAX}, resource_};
                types::logical_type col_type = types::logical_type::NA;

                bool count_star = (kind == aggregate::builtin_agg::COUNT && func_op->args().empty());
                if (count_star) {
                    // COUNT(*) — no column needed
                    col_type = types::logical_type::UBIGINT;
                } else if (func_op->args().size() == 1 &&
                           std::holds_alternative<expressions::key_t>(func_op->args()[0])) {
                    auto& key = std::get<expressions::key_t>(func_op->args()[0]);
                    assert(!key.path().empty());
                    col_path = key.path();
                    col_type = chunk.at(col_path)->type().type();
                    if (col_path.empty() || col_path.front() == SIZE_MAX) {
                        can_vectorize = false;
                        break;
                    }
                    // Only vectorize numeric types
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
            // Build group_ids: for each row, which group does it belong to?
            std::pmr::vector<uint32_t> group_ids(resource_);
            group_ids.resize(num_rows, UINT32_MAX);
            for (size_t g = 0; g < num_groups; g++) {
                for (size_t row_id : row_ids_per_group_[g]) {
                    group_ids[row_id] = static_cast<uint32_t>(g);
                }
            }

            // For each aggregate, run vectorized update
            std::pmr::vector<std::pmr::vector<types::logical_value_t>> agg_results(resource_);
            agg_results.reserve(values_.size());

            for (size_t a = 0; a < values_.size(); a++) {
                std::pmr::vector<aggregate::raw_agg_state_t> states(resource_);
                states.resize(num_groups);

                auto& info = agg_infos[a];
                if (info.is_count_star) {
                    // COUNT(*): count all rows per group (including nulls)
                    for (uint64_t i = 0; i < num_rows; i++) {
                        if (group_ids[i] != UINT32_MAX) {
                            states[group_ids[i]].update_count();
                        }
                    }
                } else {
                    aggregate::update_all(info.kind, *chunk.at(info.full_path), group_ids.data(), num_rows, states);
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

            // Build result chunk
            return build_result_chunk(num_groups, key_count, agg_results);
        }

        // Fallback: gather + slice_contiguous per group
        return calc_aggregate_values_fallback(pipeline_context);
    }

    vector::data_chunk_t operator_group_t::calc_aggregate_values_fallback(pipeline::context_t* pipeline_context) {
        auto& chunk = left_->output()->data_chunk();
        size_t num_groups = group_keys_.size();
        size_t key_count = num_groups > 0 ? group_keys_[0].size() : 0;

        // Build gather order: all row_ids concatenated by group
        std::pmr::vector<uint64_t> gather_order(resource_);
        std::pmr::vector<uint64_t> group_offsets(resource_);
        gather_order.reserve(chunk.size());
        group_offsets.reserve(num_groups + 1);

        for (size_t i = 0; i < num_groups; i++) {
            group_offsets.push_back(gather_order.size());
            for (size_t row_id : row_ids_per_group_[i]) {
                gather_order.push_back(row_id);
            }
        }
        group_offsets.push_back(gather_order.size());

        // ONE copy: gather all rows in group order
        auto total = static_cast<uint64_t>(gather_order.size());
        vector::indexing_vector_t gather_indexing(resource_, gather_order.data());

        auto result_types = chunk.types();
        uint64_t gathered_cap = total > 0 ? total : 1;
        vector::data_chunk_t gathered(resource_, result_types, gathered_cap);
        chunk.copy(gathered, gather_indexing, total, 0);

        // Compute aggregate results: agg_results[agg_idx][group_idx]
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> agg_results(resource_);
        agg_results.reserve(values_.size());

        for (const auto& value : values_) {
            auto& aggregator = value.aggregator;
            std::pmr::vector<types::logical_value_t> results(resource_);
            results.reserve(num_groups);

            for (size_t i = 0; i < num_groups; i++) {
                auto off = group_offsets[i];
                auto cnt = group_offsets[i + 1] - group_offsets[i];
                auto sub_chunk = gathered.slice_contiguous(resource_, off, cnt);
                aggregator->execute_on(operators::make_operator_data(left_->output()->resource(), std::move(sub_chunk)),
                                       pipeline_context);
                auto agg_val = aggregator->value();
                agg_val.set_alias(std::string(value.name));
                results.push_back(std::move(agg_val));
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
        }
    }

} // namespace components::operators
