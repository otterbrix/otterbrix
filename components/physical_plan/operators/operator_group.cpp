#include "operator_group.hpp"

#include "projection_executor.hpp"
#include "compare_3vl.hpp"
#include <cassert>
#include <optional>
#include <components/compute/function.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/expressions/scalar_expression.hpp>
#include <components/physical_plan/operators/aggregate/grouped_aggregate.hpp>
#include <components/physical_plan/operators/aggregate/operator_func.hpp>
#include <components/physical_plan/operators/operator_batch.hpp>
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

        using expressions::to_arithmetic_op;

        // Extract a key value from chunk for a given group_key_t definition
        types::logical_value_t extract_key_value(std::pmr::memory_resource* resource,
                                                 const group_key_t& key,
                                                 const vector::data_chunk_t& chunk,
                                                 size_t row_idx) {
            switch (key.type) {
                case group_key_t::kind::column: {
                    assert(!key.full_path.empty() && "group key path must be resolved before execution");
                    return chunk.value(key.full_path, row_idx);
                }
                case group_key_t::kind::coalesce: {
                    for (const auto& entry : key.coalesce_entries) {
                        if (entry.type == group_key_t::coalesce_entry::source::constant) {
                            if (!entry.constant.is_null()) {
                                return entry.constant;
                            }
                        } else {
                            // column source
                            if (!chunk.data[entry.col_index].is_null(row_idx)) {
                                return chunk.value(entry.col_index, row_idx);
                            }
                        }
                    }
                    // all NULL
                    return types::logical_value_t(resource, types::complex_logical_type{types::logical_type::NA});
                }
                case group_key_t::kind::case_when: {
                    for (const auto& clause : key.case_clauses) {
                        auto cond_val = chunk.value(clause.condition_col, row_idx);
                        bool matches;
                        switch (clause.cmp) {
                            case expressions::compare_type::eq:
                            case expressions::compare_type::ne:
                            case expressions::compare_type::gt:
                            case expressions::compare_type::gte:
                            case expressions::compare_type::lt:
                            case expressions::compare_type::lte:
                                // A NULL operand makes the WHEN condition UNKNOWN -> fall through.
                                matches =
                                    types::selects(eval_compare_3vl(clause.cmp, cond_val, clause.condition_value));
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

    operator_group_t::operator_group_t(std::pmr::memory_resource* resource, log_t log, size_t internal_aggregate_count)
        : read_write_operator_t(resource, log, operator_type::aggregate)
        , keys_(resource_)
        , values_(resource_)
        , computed_columns_(resource_)
        , post_aggregates_(resource_)
        , output_schema_(resource_)
        , internal_aggregate_count_(internal_aggregate_count)
        , agg_plan_(resource_)
        , group_key_chunk_storage_(resource_)
        , group_hash_index_(resource_)
        , agg_states_(resource_)
        , gathered_rows_per_group_(resource_) {}

    void operator_group_t::set_output_schema(const vector::schema_t& schema) {
        output_schema_ = vector::clone_schema(resource_, schema);
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
            // Tag-free, RTTI-free: the aggregator answers for itself (rule 14).
            const auto* func_op = value.aggregator->as_function_call();
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
                               expressions::is_key(func_op->args()[0])) {
                        auto& key = expressions::as_key(func_op->args()[0]);
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
            // The probe column's NAME is set on the column (M3-B5). A derived key used to get
            // it by stamping every extracted VALUE and letting the r == 0 retype below carry
            // the stamp into the column's type — a per-row heap allocation for a name that is
            // the same on every row, and one that the retype could only deliver by accident.
            if (key.type == group_key_t::kind::column && key.full_path.size() == 1) {
                probe.data[k].reference(input.data[key.full_path.front()]);
                probe.data[k].set_name(input.data[key.full_path.front()].name());
            } else if (key.type == group_key_t::kind::column && !key.full_path.empty()) {
                for (uint64_t r = 0; r < n; r++) {
                    probe.set_value(k, r, input.value(key.full_path, r));
                }
                // A nested key names itself after the group key. The leaf VECTOR cannot
                // supply the name: struct-child and list-element vectors are name-less by
                // contract (vector.hpp — "a nested child vector genuinely has none"), so
                // asking the leaf always answered the empty string. `key.name` is the
                // planner's leaf field name and survives LIST/ARRAY paths.
                probe.data[k].set_name(key.name);
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
                probe.data[k].set_name(key.name);
            }
        }
        return probe;
    }

    core::error_t operator_group_t::accumulate(pipeline::context_t* pipeline_context, vector::data_chunk_t& input) {
        // Pre-compute arithmetic key columns on this chunk (appended at the tail).
        for (auto& comp : computed_columns_) {
            auto result_vec = evaluate_scalar(resource_,
                                              comp.op,
                                              comp.operands,
                                              input,
                                              pipeline_context->function_registry,
                                              pipeline_context->parameters,
                                              pipeline_context->session_tz);
            if (result_vec.has_error()) {
                return result_vec.error();
            }
            result_vec.value().set_name(comp.alias);
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
            // Lazily create the per-group key chunk from the probe schema. types() carries
            // TYPES ONLY — the column names live on the probe's vectors (M3-B5), and
            // materialize_groups reads the output key names back from THIS chunk, so they
            // are copied across explicitly or the whole result comes out name-less.
            if (group_key_chunk_storage_.empty()) {
                auto key_types = probe.types();
                group_key_chunk_storage_.emplace_back(resource_, key_types, vector::DEFAULT_VECTOR_CAPACITY);
                for (uint64_t k = 0; k < probe.column_count(); k++) {
                    group_key_chunk_storage_.front().set_column_name(k, probe.data[k].name());
                }
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

    void operator_group_t::materialize_groups(pipeline::context_t* pipeline_context, chunks_vector_t& out) {
        size_t num_groups = group_count_;

        // Output types: key column types (straight off the key chunk — always typed,
        // never NA) + one column per aggregate.
        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.reserve(key_count_ + values_.size());
        // Output NAMES, carried beside the types rather than inside them (M3-B5). A chunk
        // built from a list of types can only be named afterwards, so the list of names has
        // to be built in the same order and in the same two passes.
        std::pmr::vector<std::pmr::string> out_names(resource_);
        out_names.reserve(key_count_ + values_.size());
        for (size_t k = 0; k < key_count_; k++) {
            out_types.push_back(group_key_chunk_storage_.empty() ? types::complex_logical_type{types::logical_type::NA}
                                                                 : group_key_chunk_storage_.front().data[k].type());
            const std::string_view key_name =
                group_key_chunk_storage_.empty() ? std::string_view{} : group_key_chunk_storage_.front().data[k].name();
            // Uses-allocator construction: the string lands on the vector's resource.
            out_names.emplace_back(key_name);
        }

        // Finalize aggregates into per-group value columns.
        std::pmr::vector<std::pmr::vector<types::logical_value_t>> agg_results(resource_);
        agg_results.reserve(values_.size());

        // Build the per-group operator batches ONCE, BEFORE the per-aggregate loop, and reuse them
        // across every non-vectorizable aggregator. The gathered parts are MOVED into a batch, so
        // rebuilding inside the aggregate loop would feed a moved-from (0-column) chunk to the second
        // aggregator (the SUM(CASE...) + internal-aggregate case). Each group is one logical unit: a
        // group fitting in <=DEFAULT_VECTOR_CAPACITY rows is one fused chunk (legacy semantics, incl.
        // cross-part DISTINCT); a larger group is fed as multiple <=1024 chunks the aggregate batch
        // path folds into ONE value (kernel_executor consume-all + finalize-once). Per-group compute
        // also fixes the old single-shared-batch hazard where one func_->execute over num_groups
        // chunks folded ALL groups into one value (group0=value, group1..n=NULL). compute() restores
        // the batch chunk to its base columns after each call, so reuse across aggregators is safe.
        std::pmr::vector<boost::intrusive_ptr<operator_batch_t>> group_batches(resource_);
        if (need_row_gather_) {
            group_batches.reserve(num_groups);
            for (size_t g = 0; g < num_groups; g++) {
                auto& parts = gathered_rows_per_group_[g];
                chunks_vector_t group_chunks(resource_);
                if (parts.empty()) {
                    std::pmr::vector<types::complex_logical_type> empty_types(resource_);
                    vector::data_chunk_t empty(resource_, empty_types, 1);
                    empty.set_cardinality(0);
                    group_chunks.emplace_back(std::move(empty));
                } else if (parts.size() == 1) {
                    group_chunks.emplace_back(std::move(parts.front()));
                } else {
                    // >1 part: flatten into <=1024-row chunks (one chunk if the group fits,
                    // several the aggregator folds as one logical group when it exceeds 1024).
                    group_chunks = concat_parts_into_batches(resource_, parts);
                }
                group_batches.emplace_back(make_operator_batch(resource_, std::move(group_chunks)));
            }
        }

        for (size_t a = 0; a < values_.size(); a++) {
            std::pmr::vector<types::logical_value_t> results(resource_);
            results.reserve(num_groups);
            if (agg_plan_[a].vectorizable) {
                auto& plan = agg_plan_[a];
                for (size_t g = 0; g < num_groups; g++) {
                    auto val = aggregate::finalize_state(resource_,
                                                         plan.kind,
                                                         g < agg_states_[a].size() ? agg_states_[a][g]
                                                                                   : aggregate::raw_agg_state_t{},
                                                         plan.col_type);
                    results.push_back(std::move(val));
                }
            } else {
                auto& aggregator = values_[a].aggregator;
                for (size_t g = 0; g < num_groups; g++) {
                    aggregator->compute(pipeline_context, group_batches[g]);
                    if (aggregator->has_error()) {
                        set_error(aggregator->get_error());
                        return;
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
                        if (!result_chunk.data.empty() && result_chunk.size() > 0) {
                            val = result_chunk.value(0, 0);
                        }
                    }
                    results.push_back(std::move(val));
                }
            }
            agg_results.push_back(std::move(results));
        }

        // One column per aggregate at fixed position key_count + a. The output column type
        // is the plan-resolved type (forwarded into output_schema_), authoritative even for
        // an all-NULL/empty group.
        append_aggregate_output_columns(key_count_, agg_results, out_types, out_names);

        // CORE INVARIANT: never construct a data_chunk_t with capacity > 1024. Emit the
        // group table in <=DEFAULT_VECTOR_CAPACITY-group slices straight into `out`
        // (loop num_groups in steps of 1024). Each slice carries its own key window +
        // per-group aggregate values, and runs post-aggregate arithmetic / internal-
        // aggregate erase / HAVING independently (all are per-row/per-group operations,
        // so a per-slice pass is identical to the whole-chunk pass). This makes the
        // post-hoc split_chunk_into_batches in finalize() redundant.
        const uint64_t total_groups = static_cast<uint64_t>(num_groups);
        uint64_t emitted = 0;
        do {
            const uint64_t slice = std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY, total_groups - emitted);
            const uint64_t cap = slice > 0 ? slice : 1;
            vector::data_chunk_t result(resource_, out_types, cap);
            result.set_cardinality(slice);
            for (size_t col = 0; col < out_names.size(); col++) {
                result.set_column_name(col, out_names[col]);
            }

            // Key columns: copy the [emitted, emitted+slice) window of the typed per-group
            // key chunk (the key chunk may legally exceed 1024 — it grows via
            // data_chunk_t::resize, which has no capacity assert).
            if (slice > 0 && key_count_ > 0 && !group_key_chunk_storage_.empty()) {
                auto& key_chunk = group_key_chunk_storage_.front();
                for (size_t k = 0; k < key_count_; k++) {
                    // 5-arg copy is (source, target, source_count, source_offset, target_offset) and
                    // copies source_count - source_offset elements; to copy the [emitted, emitted+slice)
                    // window the source_count must be the END index, not the slice length.
                    vector::vector_ops::copy(key_chunk.data[k], result.data[k], emitted + slice, emitted, 0);
                }
            }

            // Aggregate columns. NULL cells keep their NA-typed logical_value; the COLUMN
            // type (out_types above) carries the plan-resolved type, so a null cell
            // becomes a typed NULL of a storable column instead of a 0-byte NA column.
            for (size_t a = 0; a < values_.size(); a++) {
                for (uint64_t r = 0; r < slice; r++) {
                    const size_t g = static_cast<size_t>(emitted + r);
                    if (g < agg_results[a].size()) {
                        result.set_value(key_count_ + a, r, std::move(agg_results[a][g]));
                    } else {
                        result.set_value(key_count_ + a, r, types::logical_value_t(resource_, types::logical_type::NA));
                    }
                }
            }

            // Post-aggregate arithmetic (columnar, per-slice).
            size_t size_before_post = result.data.size();
            if (auto err = calc_post_aggregates(pipeline_context, result); err.contains_error()) {
                set_error(std::move(err));
                return;
            }

            // Remove internal aggregate columns by position.
            if (internal_aggregate_count_ > 0) {
                auto it_end = result.data.begin() + static_cast<std::ptrdiff_t>(size_before_post);
                auto it_begin = it_end - static_cast<std::ptrdiff_t>(internal_aggregate_count_);
                result.data.erase(it_begin, it_end);
            }

            // HAVING is applied by a dedicated operator_having filter spliced ABOVE this group
            // (create_plan_aggregate), which filters the emitted aggregated chunk. This group
            // operator only aggregates.

            out.emplace_back(std::move(result));
            emitted += slice;
        } while (emitted < total_groups);
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
                results.push_back(std::move(val));
            }
            agg_results.push_back(std::move(results));
        }

        // The output column type is the plan-resolved type (the validator resolved it
        // data-independently and forwarded it into output_schema_), so a global aggregate
        // over zero rows emits a correctly-typed NULL (e.g. SUM(int) -> INTEGER NULL)
        // rather than the 0-byte NA sentinel. Output position == aggregate index here
        // (no group keys). Trailing internal aggregates (HAVING/post-agg helpers, erased
        // before output) have no plan output column -> their intermediate type is the
        // computed value's.
        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        std::pmr::vector<std::pmr::string> out_names(resource_);
        append_aggregate_output_columns(/*base=*/0, agg_results, out_types, out_names);
        vector::data_chunk_t result(resource_, out_types, 1);
        result.set_cardinality(1);
        for (size_t col = 0; col < out_names.size(); col++) {
            result.set_column_name(col, out_names[col]);
        }
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

    void operator_group_t::append_aggregate_output_columns(
        size_t base,
        const std::pmr::vector<std::pmr::vector<types::logical_value_t>>& agg_results,
        std::pmr::vector<types::complex_logical_type>& out_types,
        std::pmr::vector<std::pmr::string>& out_names) const {
        for (size_t a = 0; a < values_.size(); a++) {
            const size_t out_pos = base + a;
            if (out_pos < output_schema_.size()) {
                out_types.push_back(output_schema_[out_pos].type);
                // The plan-stamped record spells the name out beside the type, so this read
                // is total: an unnamed output column answers with an empty name.
                out_names.emplace_back(output_schema_[out_pos].name);
            } else {
                // Trailing internal aggregate: no plan output column, so the aggregate's own
                // name is the answer. It used to arrive by stamping every finalized VALUE.
                out_types.push_back(agg_results[a].empty() ? types::complex_logical_type(types::logical_type::NA)
                                                           : agg_results[a][0].type());
                out_names.emplace_back(values_[a].name);
            }
        }
    }

    core::error_t operator_group_t::calc_post_aggregates(pipeline::context_t* pipeline_context,
                                                         vector::data_chunk_t& result) {
        auto num_groups = result.size();
        result.data.reserve(result.data.size() + post_aggregates_.size());

        for (auto& post : post_aggregates_) {
            // Determine result type from first row computation
            types::complex_logical_type col_type{types::logical_type::NA};

            // -x is 0 - x. Both operands are numeric here, so the entry point promotes them to
            // one type before subtracting; it errors on anything it has no arm for.
            auto negate = [&](const types::logical_value_t& value) {
                return types::logical_value_t::arithmetic(resource_,
                                                          vector::arithmetic_op::subtract,
                                                          types::logical_value_t(resource_, int64_t(0)),
                                                          value);
            };

            auto resolve = [&](const expressions::param_storage& param, size_t row_idx, auto& self)
                -> core::result_wrapper_t<types::logical_value_t> {
                if (expressions::is_key(param)) {
                    auto& key = expressions::as_key(param);
                    assert(!key.path().empty());
                    return result.value(key.path()[0], row_idx);
                }
                if (expressions::is_parameter(param)) {
                    auto id = expressions::as_parameter(param);
                    // Total lookup: .at() threw std::out_of_range out of an actor coroutine
                    // for an unbound id — rule 2, errors leave via the return value.
                    const auto* value = logical_plan::get_parameter(&pipeline_context->parameters, id);
                    if (!value) {
                        return core::error_t{
                            core::error_code_t::invalid_parameter,
                            std::pmr::string{"post-aggregate: operand references an unbound parameter", resource_}};
                    }
                    return *value;
                }
                auto& sub_expr = expressions::as_expr(param);
                if (sub_expr->group() == expressions::expression_group::scalar) {
                    auto* sub_scalar = static_cast<const expressions::scalar_expression_t*>(sub_expr.get());
                    if (sub_scalar->type() == expressions::scalar_type::unary_minus && !sub_scalar->params().empty()) {
                        auto inner = self(sub_scalar->params()[0], row_idx, self);
                        if (inner.has_error()) {
                            return inner;
                        }
                        return negate(inner.value());
                    }
                    if (sub_scalar->params().size() >= 2) {
                        if (auto op = to_arithmetic_op(sub_scalar->type())) {
                            auto left_val = self(sub_scalar->params()[0], row_idx, self);
                            if (left_val.has_error()) {
                                return left_val;
                            }
                            auto right_val = self(sub_scalar->params()[1], row_idx, self);
                            if (right_val.has_error()) {
                                return right_val;
                            }
                            return types::logical_value_t::arithmetic(resource_,
                                                                      *op,
                                                                      left_val.value(),
                                                                      right_val.value());
                        }
                    }
                }
                return core::error_t{core::error_code_t::physical_plan_error,
                                     std::pmr::string{"post-aggregate: unsupported sub-expression", resource_}};
            };

            // One value per group, then one column built from them. The column's type comes from
            // group 0 -- with no groups there is nothing to type it from and nothing to emit.
            std::pmr::vector<types::logical_value_t> col_values(resource_);
            const bool unary = post.op == expressions::scalar_type::unary_minus;
            if (unary ? post.operands.empty() : post.operands.size() < 2) {
                continue;
            }
            auto op = to_arithmetic_op(post.op);
            if (!unary && !op) {
                return core::error_t{core::error_code_t::physical_plan_error,
                                     std::pmr::string{"post-aggregate: unsupported operator", resource_}};
            }

            for (size_t group_idx = 0; group_idx < num_groups; group_idx++) {
                auto left_val = resolve(post.operands[0], group_idx, resolve);
                if (left_val.has_error()) {
                    return left_val.error();
                }
                auto computed = [&]() -> core::result_wrapper_t<types::logical_value_t> {
                    if (unary) {
                        return negate(left_val.value());
                    }
                    auto right_val = resolve(post.operands[1], group_idx, resolve);
                    if (right_val.has_error()) {
                        return right_val;
                    }
                    return types::logical_value_t::arithmetic(resource_, *op, left_val.value(), right_val.value());
                }();
                if (computed.has_error()) {
                    return computed.error();
                }
                auto result_val = std::move(computed.value());
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
            new_col.set_name(post.alias);
            result.data.emplace_back(std::move(new_col));
        }
        return core::error_t::no_error();
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
        } else if (!computed_columns_.empty()) {
            // Constants-only query (no FROM): evaluate arithmetic on a virtual row.
            std::pmr::vector<types::complex_logical_type> empty_types(resource_);
            vector::data_chunk_t chunk(resource_, empty_types, 1);
            chunk.set_cardinality(1);
            for (auto& comp : computed_columns_) {
                auto result_vec = evaluate_scalar(resource_,
                                                  comp.op,
                                                  comp.operands,
                                                  chunk,
                                                  ctx->function_registry,
                                                  ctx->parameters,
                                                  ctx->session_tz);
                if (result_vec.has_error()) {
                    return result_vec.error();
                }
                result_vec.value().set_name(comp.alias);
                chunk.data.emplace_back(std::move(result_vec.value()));
            }
            out.emplace_back(std::move(chunk));
        } else if (keys_.empty() && values_.empty()) {
            // Value-less scalar group (0 keys, 0 aggregates, 0 computed columns) forced by a
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
