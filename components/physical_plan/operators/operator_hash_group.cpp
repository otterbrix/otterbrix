#include "operator_hash_group.hpp"

#include <cassert>
#include <components/expressions/scalar_expression.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/vector_operations.hpp>
#include <core/operations_helper.hpp>

namespace components::operators {

    namespace {
        constexpr uint64_t initial_index_capacity = 1024;
        // Open addressing degrades fast past half full.
        bool index_is_full(uint64_t group_count, uint64_t capacity) noexcept { return group_count * 2 >= capacity; }
    } // namespace

    operator_hash_group_t::operator_hash_group_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, log, operator_type::aggregate)
        , keys_(resource_)
        , values_(resource_)
        , outputs_(resource_)
        , output_types_(resource_)
        , key_blocks_(resource_)
        , index_(resource_) {}

    void operator_hash_group_t::set_output_types(const std::pmr::vector<types::complex_logical_type>& types) {
        output_types_.assign(types.begin(), types.end());
    }

    void operator_hash_group_t::set_input_types(const std::pmr::vector<types::complex_logical_type>& types) {
        input_types_.assign(types.begin(), types.end());
    }

    void operator_hash_group_t::add_key(group_key_t&& key) { keys_.push_back(std::move(key)); }

    void operator_hash_group_t::add_key(const std::pmr::string& name) {
        group_key_t key(resource_);
        key.name = name;
        key.type = group_key_t::kind::column;
        keys_.push_back(std::move(key));
    }

    void operator_hash_group_t::add_value(const std::pmr::string& name,
                                          const types::complex_logical_type& result_type) {
        values_.push_back({name, result_type});
    }

    void operator_hash_group_t::add_output(const expressions::expression_ptr& output) { outputs_.push_back(output); }

    core::error_t operator_hash_group_t::build_plan(pipeline::context_t* pipeline_context,
                                                    const vector::data_chunk_t& probe) {
        auto types = probe.types();
        graph_ = std::make_unique<execution_dag::execution_dag_t>(resource_, vector::DEFAULT_VECTOR_CAPACITY);

        // A grouping key is an expression like any other: the graph evaluates it per chunk, and
        // its slot is what the operator hashes and later writes back per group.
        for (const auto& key : keys_) {
            if (key.type != group_key_t::kind::column || key.full_path.empty()) {
                std::pmr::string msg{"group key '", resource_};
                msg += key.name;
                msg += "' has no resolved column path";
                return core::error_t(core::error_code_t::schema_error, std::move(msg));
            }
            expressions::key_t field{resource_, std::string{key.name}};
            field.set_path(std::pmr::vector<size_t>{key.full_path.begin(), key.full_path.end(), resource_});
            field.set_side(key.side);
            auto expression =
                expressions::make_scalar_expression(resource_, expressions::scalar_type::get_field, field);
            auto slot = expressions::build_expression(graph_.get(),
                                                      pipeline_context->parameters.parameters,
                                                      expression.get(),
                                                      types);
            if (slot.has_error()) {
                return slot.error();
            }
            key_slots_.push_back(slot.value());
            graph_->add_key_slot(slot.value());
        }

        execution_dag::slot_list_t outputs(resource_);
        outputs.reserve(outputs_.size());
        for (const auto& output : outputs_) {
            auto slot = expressions::build_expression(graph_.get(),
                                                      pipeline_context->parameters.parameters,
                                                      output.get(),
                                                      types);
            if (slot.has_error()) {
                return slot.error();
            }
            outputs.push_back(slot.value());
        }
        graph_->set_output(outputs);
        graph_->set_parameters(&pipeline_context->parameters.parameters);
        if (auto error = graph_->prepare(); error.contains_error()) {
            return error;
        }

        // A key must be hashable: a type vector_ops::hash cannot reduce would throw mid-chunk.
        for (size_t index = 0; index < key_slots_.size(); index++) {
            if (!vector::vector_ops::is_hashable(graph_->slot_type(key_slots_[index]))) {
                std::pmr::string msg{"cannot group by '", resource_};
                msg += keys_[index].name;
                msg += "': its type has no hash";
                return core::error_t(core::error_code_t::schema_error, std::move(msg));
            }
        }

        // The per-chunk scratch is built once here: the probe chunk only REFERENCES the key slots
        // the graph writes, so a chunk costs no allocation at all.
        std::pmr::vector<types::complex_logical_type> key_types(resource_);
        key_types.reserve(key_slots_.size());
        for (auto slot : key_slots_) {
            key_types.push_back(graph_->slot_type(slot));
        }
        key_probe_.emplace_back(resource_, key_types, std::vector<size_t>{}, vector::DEFAULT_VECTOR_CAPACITY);
        hashes_.emplace_back(resource_, types::logical_type::UBIGINT, vector::DEFAULT_VECTOR_CAPACITY);
        key_columns_.resize(key_slots_.size());
        for (size_t key = 0; key < key_slots_.size(); key++) {
            key_columns_[key] = key;
        }
        row_groups_.reserve(vector::DEFAULT_VECTOR_CAPACITY);

        index_.assign(initial_index_capacity, hash_entry_t{});
        index_mask_ = initial_index_capacity - 1;
        plan_built_ = true;
        return core::error_t::no_error();
    }

    bool operator_hash_group_t::keys_equal(const vector::data_chunk_t& keys, uint64_t row, uint32_t group) const {
        const auto& block = key_blocks_[group / vector::DEFAULT_VECTOR_CAPACITY];
        const uint64_t block_row = group % vector::DEFAULT_VECTOR_CAPACITY;
        for (size_t key = 0; key < keys.column_count(); key++) {
            if (!vector::cells_equal(keys.data[key], row, block.data[key], block_row)) {
                return false;
            }
        }
        return true;
    }

    void operator_hash_group_t::grow_index() {
        std::pmr::vector<hash_entry_t> grown(index_.size() * 2, hash_entry_t{}, resource_);
        const uint64_t mask = grown.size() - 1;
        for (const auto& entry : index_) {
            if (entry.group == empty_group) {
                continue;
            }
            uint64_t slot = entry.hash & mask;
            while (grown[slot].group != empty_group) {
                slot = (slot + 1) & mask;
            }
            grown[slot] = entry;
        }
        index_ = std::move(grown);
        index_mask_ = mask;
    }

    void operator_hash_group_t::append_group(const vector::data_chunk_t& keys, uint64_t row, uint64_t hash) {
        if (group_count_ % vector::DEFAULT_VECTOR_CAPACITY == 0) {
            key_blocks_.emplace_back(resource_, keys.types(), vector::DEFAULT_VECTOR_CAPACITY);
            key_blocks_.back().set_cardinality(0);
        }
        auto& block = key_blocks_.back();
        const uint64_t block_row = group_count_ % vector::DEFAULT_VECTOR_CAPACITY;
        for (size_t key = 0; key < keys.column_count(); key++) {
            vector::vector_ops::copy(keys.data[key], block.data[key], row + 1, row, block_row);
        }
        block.set_cardinality(block_row + 1);

        if (index_is_full(group_count_ + 1, index_.size())) {
            grow_index();
        }
        uint64_t slot = hash & index_mask_;
        while (index_[slot].group != empty_group) {
            slot = (slot + 1) & index_mask_;
        }
        index_[slot] = hash_entry_t{hash, static_cast<uint32_t>(group_count_)};
        group_count_++;
    }

    core::error_t operator_hash_group_t::resolve_groups(vector::data_chunk_t& keys) {
        const uint64_t count = keys.size();
        if (keys.column_count() == 0) {
            // No GROUP BY: the whole input is one group, which exists even over zero rows. Every
            // row folds into group 0 for the life of the operator, so the ids are grown to the
            // widest chunk seen and never written again.
            if (row_groups_.size() < count) {
                row_groups_.resize(count, 0);
            }
            group_count_ = 1;
            return core::error_t::no_error();
        }

        // The probe below assigns every element, so last chunk's ids need no clearing first.
        row_groups_.resize(count);
        if (count == 0) {
            return core::error_t::no_error();
        }
        auto& hashes = hashes_.front();
        keys.hash(key_columns_, hashes);
        const auto* row_hashes = hashes.data<uint64_t>();

        for (uint64_t row = 0; row < count; row++) {
            const uint64_t hash = row_hashes[row];
            uint64_t slot = hash & index_mask_;
            // The stored hash only filters: equal hashes still have to prove the keys equal,
            // cell by cell, which is what makes struct and array keys correct.
            while (index_[slot].group != empty_group) {
                if (index_[slot].hash == hash && keys_equal(keys, row, index_[slot].group)) {
                    break;
                }
                slot = (slot + 1) & index_mask_;
            }
            if (index_[slot].group == empty_group) {
                row_groups_[row] = static_cast<uint32_t>(group_count_);
                append_group(keys, row, hash);
                continue;
            }
            row_groups_[row] = index_[slot].group;
        }
        return core::error_t::no_error();
    }

    core::error_t
    operator_hash_group_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& /*out*/) {
        // SINK: fold this batch into the group table and append nothing. What is retained is the
        // group keys and one accumulator per group per aggregate — never the rows.
        if (input.size() > 0) {
            any_input_ = true;
        }
        if (!plan_built_) {
            if (auto error = build_plan(ctx, input); error.contains_error()) {
                return error;
            }
        }

        if (auto error = graph_->process_keys(input, ctx->execution_context); error.contains_error()) {
            return error;
        }

        // The key values the graph just wrote. The probe only references them — a key slot may be
        // an input column bound straight to the chunk, or a computed slot, and either way this
        // copies nothing.
        auto& keys = key_probe_.front();
        for (size_t key = 0; key < key_slots_.size(); key++) {
            keys.data[key].reference(graph_->key_values(key));
        }
        keys.set_cardinality(input.size());

        if (auto error = resolve_groups(keys); error.contains_error()) {
            return error;
        }
        if (auto error = graph_->reserve_groups(group_count_); error.contains_error()) {
            return error;
        }
        return graph_->process_groups({row_groups_.data(), input.size()}, ctx->execution_context);
    }

    core::error_t operator_hash_group_t::finalize(pipeline::context_t* ctx, chunks_vector_t& out) {
        // GROUP BY over an empty input has no groups, so it emits no ROWS — but it still emits its columns
        const bool no_groups = !keys_.empty() && group_count_ == 0;
        if (!plan_built_) {
            // The source drained before a single chunk arrived, so the graph was never built. It
            // still has to exist to emit that one row, over the schema the plan resolved.
            vector::data_chunk_t empty(resource_, input_types_, 1);
            empty.set_cardinality(0);
            if (auto error = build_plan(ctx, empty); error.contains_error()) {
                return error;
            }
            if (auto error = graph_->process_keys(empty, ctx->execution_context); error.contains_error()) {
                return error;
            }
        }
        if (!no_groups) {
            group_count_ = std::max<uint64_t>(group_count_, 1);
            if (auto error = graph_->reserve_groups(group_count_); error.contains_error()) {
                return error;
            }
        }

        std::pmr::vector<types::complex_logical_type> out_types(resource_);
        out_types.reserve(graph_->output_slots().size());
        for (auto slot : graph_->output_slots()) {
            out_types.push_back(graph_->slot_type(slot));
        }
        // size and types will match, but alias may change, so we just copy it just in case
        if (!output_types_.empty()) {
            assert(output_types_.size() == out_types.size() && "group output width disagrees with the plan");
            out_types = output_types_;
        }

        // One output chunk per block of groups: the blocks are already
        // DEFAULT_VECTOR_CAPACITY-sized, so no chunk can exceed it however many groups there are.
        std::pmr::vector<const vector::vector_t*> keys(resource_);
        for (uint64_t first = 0; first < group_count_; first += vector::DEFAULT_VECTOR_CAPACITY) {
            const uint64_t count = std::min<uint64_t>(vector::DEFAULT_VECTOR_CAPACITY, group_count_ - first);
            keys.clear();
            if (!key_blocks_.empty()) {
                const auto& block = key_blocks_[first / vector::DEFAULT_VECTOR_CAPACITY];
                for (size_t key = 0; key < key_slots_.size(); key++) {
                    keys.push_back(&block.data[key]);
                }
            }
            vector::data_chunk_t batch(resource_, out_types, count);
            if (auto error =
                    graph_
                        ->finalize_groups(ctx->execution_context, {keys.data(), keys.size()}, first, count, &batch, 0);
                error.contains_error()) {
                return error;
            }
            batch.set_cardinality(count);
            out.emplace_back(std::move(batch));
            note_emitted();
        }

        if (!emitted()) {
            vector::data_chunk_t batch(resource_, out_types, 0);
            batch.set_cardinality(0);
            out.emplace_back(std::move(batch));
        }
        return core::error_t::no_error();
    }

} // namespace components::operators
