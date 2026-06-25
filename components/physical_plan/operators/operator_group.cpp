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

        // Plan-time resolved type for final output position `pos`, or nullptr when
        // unavailable / NA (caller then keeps today's data-derived type). Free helper —
        // it only indexes the carried type list, needs no operator state.
        const types::complex_logical_type*
        resolved_type_at(const std::pmr::vector<types::complex_logical_type>& cols, size_t pos) noexcept {
            if (pos < cols.size() && cols[pos].type() != types::logical_type::NA) {
                return &cols[pos];
            }
            return nullptr;
        }

        template<typename T>
        bool cells_equal_typed(const vector::vector_t& a, size_t ra, const vector::vector_t& b, size_t rb) {
            if constexpr (std::is_floating_point_v<T>) {
                return core::is_equals(a.data<T>()[ra], b.data<T>()[rb]);
            } else {
                return a.data<T>()[ra] == b.data<T>()[rb];
            }
        }

        // Typed cell-by-cell equality between two vectors (R1-b VERIFY). No
        // logical_value_t round-trip: dispatches on physical_type like
        // value_equals_raw but compares two raw cells directly.
        bool cells_equal_raw(const vector::vector_t& a, size_t ra, const vector::vector_t& b, size_t rb) {
            bool a_null = a.is_null(ra);
            bool b_null = b.is_null(rb);
            if (a_null || b_null)
                return a_null == b_null;
            switch (a.type().to_physical_type()) {
                case types::physical_type::BOOL:
                case types::physical_type::INT8:
                    return cells_equal_typed<int8_t>(a, ra, b, rb);
                case types::physical_type::INT16:
                    return cells_equal_typed<int16_t>(a, ra, b, rb);
                case types::physical_type::INT32:
                    return cells_equal_typed<int32_t>(a, ra, b, rb);
                case types::physical_type::INT64:
                    return cells_equal_typed<int64_t>(a, ra, b, rb);
                case types::physical_type::UINT8:
                    return cells_equal_typed<uint8_t>(a, ra, b, rb);
                case types::physical_type::UINT16:
                    return cells_equal_typed<uint16_t>(a, ra, b, rb);
                case types::physical_type::UINT32:
                    return cells_equal_typed<uint32_t>(a, ra, b, rb);
                case types::physical_type::UINT64:
                    return cells_equal_typed<uint64_t>(a, ra, b, rb);
                case types::physical_type::INT128:
                    return cells_equal_typed<types::int128_t>(a, ra, b, rb);
                case types::physical_type::UINT128:
                    return cells_equal_typed<types::uint128_t>(a, ra, b, rb);
                case types::physical_type::FLOAT:
                    return cells_equal_typed<float>(a, ra, b, rb);
                case types::physical_type::DOUBLE:
                    return cells_equal_typed<double>(a, ra, b, rb);
                case types::physical_type::STRING:
                    return a.data<std::string_view>()[ra] == b.data<std::string_view>()[rb];
                default:
                    // Nested / unsupported physical types fall back to the typed
                    // logical_value comparison (preserves struct/list/array
                    // semantics; only reached for non-trivial group keys).
                    return a.value(ra) == b.value(rb);
            }
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

        // Row-wise concatenation of the per-batch gathers of ONE group into a single
        // chunk (same schema, cardinality = sum of parts). The non-vectorizable
        // aggregate path needs every row of a group in ONE batch entry: the
        // operator_func_t batch path emits one aggregate value per chunk, so a group
        // must map to exactly one chunk. fuse() is column-wise (appends columns,
        // asserts equal row counts) and is wrong here; this appends rows with the same
        // vector_ops::copy primitive the gather step itself uses (line copying input
        // rows into grp), including row_ids, so results match the legacy single-batch
        // materialize path exactly.
        vector::data_chunk_t concat_parts_rowwise(std::pmr::memory_resource* resource, chunks_vector_t& parts) {
            assert(!parts.empty());
            uint64_t total = 0;
            for (auto& part : parts) {
                total += part.size();
            }
            auto types = parts.front().types();
            uint64_t cap = total > 0 ? total : 1;
            vector::data_chunk_t merged(resource, types, cap);
            merged.set_cardinality(total);
            size_t col_count = types.size();
            uint64_t row_offset = 0;
            for (auto& part : parts) {
                uint64_t part_rows = part.size();
                if (part_rows == 0) {
                    continue;
                }
                for (size_t c = 0; c < col_count; c++) {
                    vector::vector_ops::copy(part.data[c], merged.data[c], part_rows, 0, row_offset);
                }
                vector::vector_ops::copy(part.row_ids, merged.row_ids, part_rows, 0, row_offset);
                row_offset += part_rows;
            }
            return merged;
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
        , output_types_(resource_)
        , having_(std::move(having))
        , internal_aggregate_count_(internal_aggregate_count)
        , agg_plan_(resource_)
        , group_key_chunk_storage_(resource_)
        , group_hash_index_(resource_)
        , agg_states_(resource_)
        , gathered_rows_per_group_(resource_) {}

    void operator_group_t::set_output_types(std::pmr::vector<types::complex_logical_type> types) {
        output_types_ = std::move(types);
    }

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

    core::error_t operator_group_t::build_plan(const vector::data_chunk_t& probe) {
        // Resolve col_index for computed-column keys. In the streaming model the
        // computed columns are appended at the end of every input chunk at a stable
        // index; the probe chunk passed here already carries them, so the first
        // computed column sits at (column_count - #computed_columns).
        if (!computed_columns_.empty()) {
            size_t first_computed_col = probe.column_count() - computed_columns_.size();
            for (size_t ci = 0; ci < computed_columns_.size(); ci++) {
                if (computed_columns_[ci].resolved_key_index != SIZE_MAX) {
                    keys_[computed_columns_[ci].resolved_key_index].full_path.clear();
                    keys_[computed_columns_[ci].resolved_key_index].full_path.emplace_back(first_computed_col + ci);
                }
            }
        }

        // Column keys must arrive with a resolved full_path.
        for (const auto& key : keys_) {
            if (key.type == group_key_t::kind::column && key.full_path.empty()) {
                std::pmr::string msg{"group key '", resource_};
                msg += key.name;
                msg += "' has no resolved column path";
                return core::error_t(core::error_code_t::schema_error, std::move(msg));
            }
        }

        key_count_ = keys_.size();

        // Per-aggregate plan: a builtin SUM/COUNT/MIN/MAX/AVG with a single numeric
        // column arg (or COUNT(*)) folds incrementally into a typed accumulator;
        // anything else (DISTINCT / custom funcs / non-numeric / multi-arg) keeps the
        // gather-rows path. need_row_gather_ stays false in the common analytical
        // case → state bounded by #groups.
        agg_plan_.clear();
        agg_plan_.reserve(values_.size());
        need_row_gather_ = false;
        for (const auto& value : values_) {
            agg_plan_t plan(resource_);
            auto* func_op = dynamic_cast<aggregate::operator_func_t*>(value.aggregator.get());
            bool vectorizable = false;
            if (func_op && func_op->func() && !func_op->distinct()) {
                auto kind = aggregate::classify(func_op->func()->name());
                if (kind != aggregate::builtin_agg::UNKNOWN) {
                    bool count_star = (kind == aggregate::builtin_agg::COUNT && func_op->args().empty());
                    if (count_star) {
                        plan.kind = kind;
                        plan.is_count_star = true;
                        plan.col_type = types::logical_type::UBIGINT;
                        vectorizable = true;
                    } else if (func_op->args().size() == 1 &&
                               std::holds_alternative<expressions::key_t>(func_op->args()[0])) {
                        auto& key = std::get<expressions::key_t>(func_op->args()[0]);
                        const auto& path = key.path();
                        if (!path.empty() && path.front() != SIZE_MAX) {
                            const auto* arg_vec = probe.at(path);
                            if (arg_vec) {
                                auto col_type = arg_vec->type().type();
                                if (types::is_numeric(col_type)) {
                                    plan.kind = kind;
                                    plan.col_type = col_type;
                                    plan.arg_path.assign(path.begin(), path.end());
                                    vectorizable = true;
                                }
                            }
                        }
                    }
                }
            }
            plan.vectorizable = vectorizable;
            if (!vectorizable) {
                need_row_gather_ = true;
            }
            agg_plan_.push_back(std::move(plan));
        }

        plan_built_ = true;
        return core::error_t::no_error();
    }

    vector::data_chunk_t operator_group_t::make_key_probe(const vector::data_chunk_t& input) {
        // One uniform probe chunk holding exactly the group-key columns, in key order.
        // Pure single-column keys reference the source column (zero copy); derived
        // keys (coalesce / case_when / multi-part path) materialize a column. The
        // result feeds typed hash + typed verify identically for single/multi keys.
        uint64_t n = input.size();
        std::pmr::vector<types::complex_logical_type> key_types(resource_);
        key_types.reserve(keys_.size());
        for (const auto& key : keys_) {
            if (key.type == group_key_t::kind::column && key.full_path.size() == 1) {
                key_types.push_back(input.data[key.full_path.front()].type());
            } else if (key.type == group_key_t::kind::column && !key.full_path.empty()) {
                const auto* v = input.at(key.full_path);
                key_types.push_back(v ? v->type() : types::complex_logical_type{types::logical_type::NA});
            } else {
                // Derived key: resolve a single sample value to obtain the type.
                key_types.push_back(types::complex_logical_type{types::logical_type::NA});
            }
        }

        vector::data_chunk_t probe(resource_, key_types, n > 0 ? n : 1);
        probe.set_cardinality(n);
        for (size_t k = 0; k < keys_.size(); k++) {
            const auto& key = keys_[k];
            if (key.type == group_key_t::kind::column && key.full_path.size() == 1) {
                probe.data[k].reference(input.data[key.full_path.front()]);
            } else if (key.type == group_key_t::kind::column && !key.full_path.empty()) {
                for (uint64_t r = 0; r < n; r++) {
                    probe.set_value(k, r, input.value(key.full_path, r));
                }
            } else {
                // Derived key: compute per-row via extract_key_value (rare path; not
                // the hot column-key path). Re-type the column from the first value.
                for (uint64_t r = 0; r < n; r++) {
                    auto val = extract_key_value(resource_, key, input, r);
                    if (r == 0) {
                        probe.data[k].type() = val.type();
                    }
                    probe.set_value(k, r, val);
                }
            }
        }
        return probe;
    }

    core::error_t operator_group_t::accumulate(pipeline::context_t* pipeline_context, vector::data_chunk_t& input) {
        // Pre-compute arithmetic key columns on this chunk (appended at the tail).
        for (auto& comp : computed_columns_) {
            auto result_vec = evaluate_arithmetic(resource_,
                                                  comp.op,
                                                  comp.operands,
                                                  input,
                                                  pipeline_context->parameters,
                                                  pipeline_context->session_tz);
            if (result_vec.has_error()) {
                return result_vec.error();
            }
            if (result_vec.value().type().type() == types::logical_type::NA) {
                return core::error_t(core::error_code_t::physical_plan_error,
                                     std::pmr::string{"unknown error during evaluate_arithmetic", resource_});
            }
            result_vec.value().set_type_alias(std::string(comp.alias));
            input.data.emplace_back(std::move(result_vec.value()));
        }

        if (!plan_built_) {
            auto err = build_plan(input);
            if (err.contains_error()) {
                return err;
            }
        }

        uint64_t n = input.size();

        // Assign each row to a group id (find-or-create). With GROUP BY keys this
        // is a typed HASH+VERIFY into the running group table; with no keys every
        // row maps to the single implicit group 0.
        std::pmr::vector<uint32_t> row_group(resource_);
        row_group.assign(n, 0);

        if (keys_.empty()) {
            if (n > 0 && group_count_ == 0) {
                group_count_ = 1;
            }
        } else {
            auto probe = make_key_probe(input);
            // Lazily create the per-group key chunk from the probe schema.
            if (group_key_chunk_storage_.empty()) {
                auto key_types = probe.types();
                group_key_chunk_storage_.emplace_back(resource_, key_types, vector::DEFAULT_VECTOR_CAPACITY);
            }
            auto& key_chunk = group_key_chunk_storage_.front();

            // Batch-hash all key columns of the probe.
            vector::vector_t hash_vec(resource_, types::logical_type::UBIGINT, n > 0 ? n : 1);
            std::vector<uint64_t> col_ids(keys_.size());
            for (size_t k = 0; k < keys_.size(); k++) {
                col_ids[k] = k;
            }
            if (n > 0) {
                probe.hash(col_ids, hash_vec);
            }
            const auto* hashes = hash_vec.data<uint64_t>();

            for (uint64_t row = 0; row < n; row++) {
                uint64_t h = hashes[row];
                uint32_t gid = UINT32_MAX;
                auto it = group_hash_index_.find(h);
                if (it != group_hash_index_.end()) {
                    for (uint32_t cand : it->second) {
                        bool match = true;
                        for (size_t k = 0; k < keys_.size(); k++) {
                            if (!cells_equal_raw(probe.data[k], row, key_chunk.data[k], cand)) {
                                match = false;
                                break;
                            }
                        }
                        if (match) {
                            gid = cand;
                            break;
                        }
                    }
                }
                if (gid == UINT32_MAX) {
                    gid = static_cast<uint32_t>(group_count_);
                    if (group_count_ >= key_chunk.capacity()) {
                        key_chunk.resize(key_chunk.capacity() * 2);
                    }
                    for (size_t k = 0; k < keys_.size(); k++) {
                        vector::indexing_vector_t idx(resource_, 1);
                        idx.data()[0] = static_cast<uint64_t>(row);
                        vector::vector_ops::copy(probe.data[k], key_chunk.data[k], idx, 1, 0, group_count_);
                    }
                    group_hash_index_[h].push_back(gid);
                    group_count_++;
                    key_chunk.set_cardinality(group_count_);
                }
                row_group[row] = gid;
            }
        }

        // Grow per-group accumulator storage to cover newly created groups.
        if (agg_states_.empty() && !need_row_gather_) {
            agg_states_.resize(values_.size(), std::pmr::vector<aggregate::raw_agg_state_t>(resource_));
        }
        for (size_t a = 0; a < values_.size(); a++) {
            if (agg_plan_[a].vectorizable) {
                if (agg_states_.size() <= a) {
                    agg_states_.resize(values_.size(), std::pmr::vector<aggregate::raw_agg_state_t>(resource_));
                }
                agg_states_[a].resize(group_count_);
            }
        }
        if (need_row_gather_) {
            while (gathered_rows_per_group_.size() < group_count_) {
                gathered_rows_per_group_.emplace_back();
            }
        }

        // Fold this chunk's rows into the running aggregates.
        const uint32_t* gids = row_group.data();
        for (size_t a = 0; a < values_.size(); a++) {
            if (!agg_plan_[a].vectorizable) {
                continue;
            }
            auto& plan = agg_plan_[a];
            auto& states = agg_states_[a];
            if (plan.is_count_star) {
                for (uint64_t i = 0; i < n; i++) {
                    states[gids[i]].update_count();
                }
            } else {
                const auto* arg_vec = input.at(plan.arg_path);
                if (arg_vec) {
                    aggregate::update_all(plan.kind, *arg_vec, gids, n, states);
                }
            }
        }

        // Non-vectorizable aggregates: gather the contributing rows per group so the
        // general operator_func_t batch path can run once at finalize. Consecutive
        // rows that share a group are copied in one indexed gather.
        if (need_row_gather_ && n > 0) {
            auto in_types = input.types();
            size_t col_count = in_types.size();
            // Stable per-group row lists for this chunk.
            std::pmr::vector<std::pmr::vector<uint64_t>> rows_by_group(resource_);
            rows_by_group.resize(group_count_, std::pmr::vector<uint64_t>(resource_));
            for (uint64_t r = 0; r < n; r++) {
                rows_by_group[gids[r]].push_back(r);
            }
            for (size_t g = 0; g < group_count_; g++) {
                auto& rows = rows_by_group[g];
                if (rows.empty()) {
                    continue;
                }
                uint64_t cnt = static_cast<uint64_t>(rows.size());
                vector::data_chunk_t grp(resource_, in_types, cnt);
                grp.set_cardinality(cnt);
                vector::indexing_vector_t indexing(resource_, cnt);
                for (uint64_t i = 0; i < cnt; i++) {
                    indexing.data()[i] = rows[i];
                }
                for (size_t c = 0; c < col_count; c++) {
                    if (is_placeholder(input.data[c])) {
                        continue;
                    }
                    vector::vector_ops::copy(input.data[c], grp.data[c], indexing, cnt, 0, 0);
                }
                vector::vector_ops::copy(input.row_ids, grp.row_ids, indexing, cnt, 0, 0);
                gathered_rows_per_group_[g].emplace_back(std::move(grp));
            }
        }

        // Strip the temporary computed-key columns appended above.
        if (!computed_columns_.empty()) {
            size_t first_computed_col = input.data.size() - computed_columns_.size();
            input.data.erase(input.data.begin() + static_cast<std::ptrdiff_t>(first_computed_col), input.data.end());
        }
        return core::error_t::no_error();
    }

    vector::data_chunk_t operator_group_t::materialize_groups(pipeline::context_t* pipeline_context) {
        size_t num_groups = group_count_;

        // Output types: key column types (straight off the key chunk — always typed,
        // never NA) + one column per aggregate.
        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.reserve(key_count_ + values_.size());
        for (size_t k = 0; k < key_count_; k++) {
            out_types.push_back(group_key_chunk_storage_.empty()
                                    ? types::complex_logical_type{types::logical_type::NA}
                                    : group_key_chunk_storage_.front().data[k].type());
        }

        // Finalize aggregates into per-group value columns.
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> agg_results(resource_);
        agg_results.reserve(values_.size());

        // Gather-once shared batch for the non-vectorizable aggregators: one fused
        // chunk per group (each group = one batch entry), mirroring the old
        // dual-path fallback exactly so multi-chunk/node_data semantics match.
        boost::intrusive_ptr<operator_batch_t> shared_batch;
        if (need_row_gather_) {
            chunks_vector_t group_chunks(resource_);
            group_chunks.reserve(num_groups);
            for (size_t g = 0; g < num_groups; g++) {
                auto& parts = gathered_rows_per_group_[g];
                if (parts.empty()) {
                    std::pmr::vector<types::complex_logical_type> empty_types(resource_);
                    vector::data_chunk_t empty(resource_, empty_types, 1);
                    empty.set_cardinality(0);
                    group_chunks.emplace_back(std::move(empty));
                    continue;
                }
                // ROW-WISE concat: one chunk per group holding ALL the group's rows
                // (same schema, summed cardinality). The single-batch case (parts.size()
                // == 1) just moves the lone part through. Column-wise fuse() was wrong:
                // it appended the second batch's columns and asserted equal row counts.
                vector::data_chunk_t merged =
                    parts.size() == 1 ? std::move(parts.front()) : concat_parts_rowwise(resource_, parts);
                group_chunks.emplace_back(std::move(merged));
            }
            shared_batch = make_operator_batch(resource_, std::move(group_chunks));
        }

        for (size_t a = 0; a < values_.size(); a++) {
            std::pmr::vector<types::logical_value_t> results(resource_);
            results.reserve(num_groups);
            if (agg_plan_[a].vectorizable) {
                auto& plan = agg_plan_[a];
                for (size_t g = 0; g < num_groups; g++) {
                    auto val =
                        aggregate::finalize_state(resource_,
                                                  plan.kind,
                                                  g < agg_states_[a].size() ? agg_states_[a][g]
                                                                            : aggregate::raw_agg_state_t{},
                                                  plan.col_type);
                    val.set_alias(std::string(values_[a].name));
                    results.push_back(std::move(val));
                }
            } else {
                auto& aggregator = values_[a].aggregator;
                aggregator->compute(pipeline_context, shared_batch);
                if (aggregator->has_error()) {
                    set_error(aggregator->get_error());
                    return vector::data_chunk_t(resource_,
                                                std::pmr::vector<types::complex_logical_type>{resource_},
                                                1);
                }
                auto datum = aggregator->take_batch_values();
                if (std::holds_alternative<std::pmr::vector<types::logical_value_t>>(datum)) {
                    auto& vals = std::get<std::pmr::vector<types::logical_value_t>>(datum);
                    for (auto& v : vals) {
                        v.set_alias(std::string(values_[a].name));
                        results.push_back(std::move(v));
                    }
                } else {
                    auto& result_chunk = std::get<vector::data_chunk_t>(datum);
                    for (size_t i = 0; i < num_groups && i < result_chunk.size(); i++) {
                        auto val = result_chunk.data.empty()
                                       ? types::logical_value_t(resource_, types::logical_type::NA)
                                       : result_chunk.value(0, i);
                        val.set_alias(std::string(values_[a].name));
                        results.push_back(std::move(val));
                    }
                }
                while (results.size() < num_groups) {
                    results.emplace_back(resource_, types::logical_type::NA);
                }
            }
            agg_results.push_back(std::move(results));
        }

        // One column per aggregate, unconditionally (fixed position key_count + a).
        // For an all-NULL/empty-group aggregate column the data-derived type is NA; use
        // the plan-resolved type instead so the column is storable. The data path (a
        // column with a real value) is untouched.
        for (size_t a = 0; a < values_.size(); a++) {
            const bool col_is_na =
                agg_results[a].empty() || agg_results[a][0].type().type() == types::logical_type::NA;
            const auto* carried = col_is_na ? resolved_type_at(output_types_, key_count_ + a) : nullptr;
            if (carried) {
                out_types.push_back(*carried);
            } else {
                out_types.push_back(agg_results[a].empty() ? types::complex_logical_type(types::logical_type::NA)
                                                           : agg_results[a][0].type());
            }
        }

        uint64_t cap = num_groups > 0 ? static_cast<uint64_t>(num_groups) : 1;
        vector::data_chunk_t result(resource_, out_types, cap);
        result.set_cardinality(static_cast<uint64_t>(num_groups));

        // Key columns: copy straight from the typed per-group key chunk.
        if (num_groups > 0 && key_count_ > 0 && !group_key_chunk_storage_.empty()) {
            auto& key_chunk = group_key_chunk_storage_.front();
            for (size_t k = 0; k < key_count_; k++) {
                vector::vector_ops::copy(key_chunk.data[k], result.data[k], num_groups, 0, 0);
            }
        }

        // Aggregate columns. NULL cells keep their NA-typed logical_value; the COLUMN
        // type (out_types above) carries the plan-resolved type, so a null cell becomes a
        // typed NULL of a storable column instead of a 0-byte NA column.
        for (size_t a = 0; a < values_.size(); a++) {
            for (size_t g = 0; g < num_groups; g++) {
                if (g < agg_results[a].size()) {
                    result.set_value(key_count_ + a, g, std::move(agg_results[a][g]));
                } else {
                    result.set_value(key_count_ + a, g, types::logical_value_t(resource_, types::logical_type::NA));
                }
            }
        }

        // Post-aggregate arithmetic (columnar).
        size_t size_before_post = result.data.size();
        calc_post_aggregates(pipeline_context, result);

        // Remove internal aggregate columns by position.
        if (internal_aggregate_count_ > 0) {
            auto it_end = result.data.begin() + static_cast<std::ptrdiff_t>(size_before_post);
            auto it_begin = it_end - static_cast<std::ptrdiff_t>(internal_aggregate_count_);
            result.data.erase(it_begin, it_end);
        }

        // HAVING filter (columnar).
        if (having_) {
            filter_having(pipeline_context, result);
        }

        return result;
    }

    vector::data_chunk_t operator_group_t::empty_aggregate_result(pipeline::context_t* pipeline_context) {
        // Global aggregate over empty input (e.g. SELECT COUNT(*) FROM empty_table).
        // Run each aggregator over zero rows and emit one result row.
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> agg_results(resource_);
        agg_results.reserve(values_.size());
        chunks_vector_t empty_chunks(resource_);
        auto shared_batch = make_operator_batch(resource_, std::move(empty_chunks));
        for (const auto& value : values_) {
            value.aggregator->compute(pipeline_context, shared_batch);
            if (value.aggregator->has_error()) {
                set_error(value.aggregator->get_error());
                return vector::data_chunk_t(resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 1);
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

        // Plan-time types let an empty global aggregate emit a correctly-typed NULL
        // (e.g. SUM(int) over empty -> INTEGER NULL) instead of the 0-byte NA sentinel
        // that crashes downstream under gcc -O3. We override ONLY the NA/empty column;
        // the data path is otherwise untouched. Output position == aggregate index here
        // (this path has no group keys).
        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        for (size_t a = 0; a < values_.size(); a++) {
            const bool col_is_na =
                agg_results[a].empty() || agg_results[a][0].type().type() == types::logical_type::NA;
            const auto* carried = col_is_na ? resolved_type_at(output_types_, a) : nullptr;
            if (carried) {
                out_types.push_back(*carried);
            } else {
                out_types.push_back(agg_results[a].empty() ? types::complex_logical_type(types::logical_type::NA)
                                                           : agg_results[a][0].type());
            }
        }
        vector::data_chunk_t result(resource_, out_types, 1);
        result.set_cardinality(1);
        // Keep the original NULL cell (NULL is an NA-typed logical_value; set_value into
        // the now-typed column just marks validity false). The COLUMN type (out_types) is
        // what was NA and is now the plan-resolved type -> no 0-byte NA column, no crash.
        for (size_t a = 0; a < values_.size(); a++) {
            result.set_value(a,
                             0,
                             agg_results[a].empty() ? types::logical_value_t(resource_, types::logical_type::NA)
                                                    : std::move(agg_results[a][0]));
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

    core::error_t
    operator_group_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& /*out*/) {
        // SINK: fold this batch INCREMENTALLY into the running group table. State is
        // bounded by #groups (typed per-group accumulators), not by input size. The
        // batch is discarded once folded; append nothing to `out`.
        if (input.size() > 0) {
            any_input_ = true;
        }
        return accumulate(ctx, input);
    }

    core::error_t operator_group_t::finalize(pipeline::context_t* ctx, chunks_vector_t& out) {
        if (any_input_) {
            // Materialize the accumulated group table into the result chunk(s).
            auto result = materialize_groups(ctx);
            if (has_error()) {
                return get_error();
            }
            // Match split_large_output: keep each emitted chunk within DEFAULT_VECTOR_CAPACITY.
            auto batches = operators::split_chunk_into_batches(resource_, std::move(result));
            for (auto& c : batches) {
                out.emplace_back(std::move(c));
            }
            return core::error_t::no_error();
        }

        // No input rows were pushed (source drained immediately): the
        // no-left-output branches.
        if (keys_.empty() && !values_.empty()) {
            // Global aggregate over empty input — e.g. SELECT COUNT(*) FROM empty.
            auto result = empty_aggregate_result(ctx);
            if (has_error()) {
                return get_error();
            }
            out.emplace_back(std::move(result));
        } else if (!computed_columns_.empty()) {
            // Constants-only query (no FROM): evaluate arithmetic on a virtual row.
            std::pmr::vector<types::complex_logical_type> empty_types(resource_);
            vector::data_chunk_t chunk(resource_, empty_types, 1);
            chunk.set_cardinality(1);
            for (auto& comp : computed_columns_) {
                auto result_vec = evaluate_arithmetic(resource_,
                                                      comp.op,
                                                      comp.operands,
                                                      chunk,
                                                      ctx->parameters,
                                                      ctx->session_tz);
                if (result_vec.has_error()) {
                    return result_vec.error();
                }
                result_vec.value().set_type_alias(std::string(comp.alias));
                chunk.data.emplace_back(std::move(result_vec.value()));
            }
            out.emplace_back(std::move(chunk));
        }
        return core::error_t::no_error();
    }

} // namespace components::operators
