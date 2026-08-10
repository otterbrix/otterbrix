#include "operator_group.hpp"

#include <cassert>
#include <components/compute/function.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/vector/cell_equal.hpp>
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

        // Row-wise concatenation of the per-batch gathers of ONE group into a sequence
        // of <=DEFAULT_VECTOR_CAPACITY chunks (same schema; total cardinality = sum of
        // parts). CORE INVARIANT: a data_chunk_t may never be constructed with capacity
        // > DEFAULT_VECTOR_CAPACITY (data_chunk.cpp:16 asserts and aborts), so a group
        // whose rows exceed 1024 must NOT be fused into one chunk. The aggregate batch
        // path treats every chunk of one compute() batch as the SAME logical group:
        // aggregate_executor consumes+merges each chunk into one running state and
        // finalizes once (kernel_executor.cpp:197), so the parts fold to a single value
        // regardless of how the group's rows are split. Rows (and row_ids) are appended
        // with the same vector_ops::copy primitive the gather step uses, in order, so
        // the folded value matches the legacy single-fused-chunk path exactly.
        chunks_vector_t concat_parts_into_batches(std::pmr::memory_resource* resource, chunks_vector_t& parts) {
            assert(!parts.empty());
            uint64_t total = 0;
            for (auto& part : parts) {
                total += part.size();
            }
            auto types = parts.front().types();
            size_t col_count = types.size();
            chunks_vector_t out(resource);
            if (total == 0) {
                vector::data_chunk_t empty(resource, types, 1);
                empty.set_cardinality(0);
                out.emplace_back(std::move(empty));
                return out;
            }
            out.reserve((total + vector::DEFAULT_VECTOR_CAPACITY - 1) / vector::DEFAULT_VECTOR_CAPACITY);
            // Flatten the group's rows into back-to-back <=1024-row chunks. A source part
            // may straddle a chunk boundary, so copy it in (possibly two) windows.
            uint64_t part_idx = 0;
            uint64_t part_pos = 0; // rows already copied out of parts[part_idx]
            uint64_t remaining = total;
            while (remaining > 0) {
                uint64_t cap = std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY, remaining);
                vector::data_chunk_t chunk(resource, types, cap);
                chunk.set_cardinality(cap);
                uint64_t filled = 0;
                while (filled < cap) {
                    while (part_idx < parts.size() && parts[part_idx].size() == part_pos) {
                        part_idx++;
                        part_pos = 0;
                    }
                    auto& part = parts[part_idx];
                    uint64_t avail = part.size() - part_pos;
                    uint64_t take = std::min<uint64_t>(avail, cap - filled);
                    for (size_t c = 0; c < col_count; c++) {
                        vector::vector_ops::copy(part.data[c], chunk.data[c], take, part_pos, filled);
                    }
                    vector::vector_ops::copy(part.row_ids, chunk.row_ids, take, part_pos, filled);
                    part_pos += take;
                    filled += take;
                }
                remaining -= cap;
                out.emplace_back(std::move(chunk));
            }
            return out;
        }
    } // anonymous namespace

    operator_group_t::operator_group_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, log, operator_type::aggregate)
        , keys_(resource_)
        , values_(resource_)
        , outputs_(resource_)
        , output_types_(resource_)
        , group_key_chunk_storage_(resource_)
        , group_hash_index_(resource_)
        , gathered_rows_per_group_(resource_) {}

    void operator_group_t::set_output_types(const std::pmr::vector<types::complex_logical_type>& types) {
        output_types_.assign(types.begin(), types.end());
    }

    void operator_group_t::set_input_types(const std::pmr::vector<types::complex_logical_type>& types) {
        input_types_.assign(types.begin(), types.end());
    }

    void operator_group_t::add_key(group_key_t&& key) { keys_.push_back(std::move(key)); }

    void operator_group_t::add_key(const std::pmr::string& name) {
        group_key_t key(resource_);
        key.name = name;
        key.type = group_key_t::kind::column;
        keys_.push_back(std::move(key));
    }

    void operator_group_t::add_value(const std::pmr::string& name, const types::complex_logical_type& result_type) {
        values_.push_back({name, result_type});
    }

    void operator_group_t::add_output(const expressions::expression_ptr& output) { outputs_.push_back(output); }

    core::error_t operator_group_t::build_output_graph(pipeline::context_t* pipeline_context,
                                                       const vector::data_chunk_t& probe) {
        if (output_graph_) {
            return core::error_t::no_error();
        }
        std::pmr::vector<const expressions::expression_i*> output_expressions(resource_);
        output_expressions.reserve(outputs_.size());
        for (const auto& output : outputs_) {
            output_expressions.push_back(output.get());
        }

        auto built = expressions::build_graph(resource_,
                                              pipeline_context->parameters.parameters,
                                              output_expressions,
                                              probe.types());
        if (built.has_error()) {
            return built.error();
        }
        output_graph_ = std::move(built.value());
        return core::error_t::no_error();
    }

    core::error_t operator_group_t::build_plan(const vector::data_chunk_t&) {
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

    core::error_t operator_group_t::accumulate(pipeline::context_t*, vector::data_chunk_t& input) {
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
                            if (!vector::cells_equal(probe.data[k], row, key_chunk.data[k], cand)) {
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

        // Every group keeps its rows: the graph evaluates the group ON them.
        while (gathered_rows_per_group_.size() < group_count_) {
            gathered_rows_per_group_.emplace_back();
        }

        const uint32_t* gids = row_group.data();

        // Gather the contributing rows per group so the graph can reduce each group at finalize.
        // Consecutive rows that share a group are copied in one indexed gather.
        if (n > 0) {
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

        return core::error_t::no_error();
    }

    void operator_group_t::materialize_groups(pipeline::context_t* pipeline_context, chunks_vector_t& out) {
        const size_t num_groups = group_count_;
        if (num_groups == 0) {
            return;
        }

        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        chunks_vector_t emitted(resource_);
        for (size_t group = 0; group < num_groups; group++) {
            auto& parts = gathered_rows_per_group_[group];
            if (parts.empty()) {
                continue;
            }
            // TODO: each group should have the same column types
            if (auto error = build_output_graph(pipeline_context, parts.front()); error.contains_error()) {
                set_error(error);
                return;
            }
            output_graph_->set_parameters(&pipeline_context->parameters.parameters);
            uint64_t group_rows = 0;
            for (const auto& chunk : parts) {
                if (auto error = output_graph_->process(chunk, pipeline_context->execution_context);
                    error.contains_error()) {
                    set_error(error);
                    return;
                }
                group_rows += chunk.size();
            }
            auto computed = output_graph_->finalize(pipeline_context->execution_context, group_rows);
            if (computed.has_error()) {
                set_error(computed.error());
                return;
            }
            if (out_types.empty()) {
                out_types =
                    output_types_.size() == computed.value().data.size() ? output_types_ : computed.value().types();
            }
            // .finalize() does not copy data by itself
            vector::data_chunk_t single(resource_, out_types, 1);
            single.set_cardinality(1);
            for (size_t column = 0; column < computed.value().data.size(); column++) {
                vector::vector_ops::copy(computed.value().data[column], single.data[column], 1, 0, 0);
            }
            emitted.emplace_back(std::move(single));
        }
        if (emitted.empty()) {
            return;
        }
        for (auto& chunk : concat_parts_into_batches(resource_, emitted)) {
            out.emplace_back(std::move(chunk));
        }
    }

    vector::data_chunk_t operator_group_t::empty_aggregate_result(pipeline::context_t* pipeline_context) {
        vector::data_chunk_t empty(resource_, input_types_, 1);
        empty.set_cardinality(0);
        if (auto error = build_output_graph(pipeline_context, empty); error.contains_error()) {
            set_error(error);
            return vector::data_chunk_t(resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 1);
        }
        output_graph_->set_parameters(&pipeline_context->parameters.parameters);
        if (auto error = output_graph_->process(empty, pipeline_context->execution_context); error.contains_error()) {
            set_error(error);
            return vector::data_chunk_t(resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 1);
        }
        auto computed = output_graph_->finalize(pipeline_context->execution_context, 0);
        if (computed.has_error()) {
            set_error(computed.error());
            return vector::data_chunk_t(resource_, std::pmr::vector<types::complex_logical_type>{resource_}, 1);
        }
        vector::data_chunk_t result(resource_,
                                    output_types_.size() == computed.value().data.size() ? output_types_
                                                                                         : computed.value().types(),
                                    1);
        result.set_cardinality(1);
        for (size_t column = 0; column < computed.value().data.size(); column++) {
            vector::vector_ops::copy(computed.value().data[column], result.data[column], 1, 0, 0);
        }
        return result;
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
            // Materialize the accumulated group table directly into <=1024-group result
            // chunks. materialize_groups now slices internally, so no post-hoc
            // split_chunk_into_batches is needed (it was dead code for the >1024-group
            // crash anyway: the oversized chunk aborted in its ctor before finalize ran).
            materialize_groups(ctx, out);
            if (has_error()) {
                return get_error();
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
        } else if (keys_.empty() && values_.empty()) {
            // Value-less scalar group (0 keys, 0 aggregates) forced by a
            // HAVING clause — an implicit GROUP BY () makes the whole table ONE group, so it must
            // emit exactly one 0-column row even over EMPTY input (SELECT 1 FROM empty HAVING true
            // returns one row). The operator_having above then keeps or drops that row and
            // operator_select projects the constant. Mirrors materialize_groups' 1-row emission for
            // the non-empty value-less-scalar-group case (num_groups == 1). A plain non-grouped
            // SELECT 1 FROM t builds NO group operator, so this never affects it.
            std::pmr::vector<types::complex_logical_type> empty_types(resource_);
            vector::data_chunk_t chunk(resource_, empty_types, 1);
            chunk.set_cardinality(1);
            out.emplace_back(std::move(chunk));
        }
        return core::error_t::no_error();
    }

} // namespace components::operators
