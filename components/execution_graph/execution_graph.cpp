#include "execution_graph.hpp"

#include <components/vector/operations/apply_operator.hpp>
#include <components/vector/vector_operations.hpp>

#include <algorithm>
#include <cassert>

namespace components::execution_graph {

    execution_node_t::execution_node_t(std::pmr::memory_resource* resource, slot_list_t inputs, slot_list_t outputs)
        : input_indices_(std::move(inputs), resource)
        , output_indices_(std::move(outputs), resource) {}

    const vector::vector_t& execution_node_t::input(size_t n) const {
        assert(storage_ && "execution node is not attached to a graph storage");
        assert(input_indices_[n] < storage_->size());
        return (*storage_)[input_indices_[n]];
    }

    vector::vector_t& execution_node_t::output(size_t n) const {
        assert(storage_ && "execution node is not attached to a graph storage");
        assert(output_indices_[n] < storage_->size());
        return (*storage_)[output_indices_[n]];
    }

    const bool* execution_node_t::active_rows() const {
        if (constraint_slot_ == invalid_slot) {
            return nullptr;
        }
        assert(storage_ && "execution node is not attached to a graph storage");
        assert(constraint_slot_ < storage_->size());
        return (*storage_)[constraint_slot_].data<bool>();
    }

    uint64_t execution_node_t::input_size(size_t n) const {
        assert(sizes_ && "execution node is not attached to a graph storage");
        return sizes_[input_indices_[n]];
    }

    void execution_node_t::set_output_size(size_t n, uint64_t rows) const {
        assert(sizes_ && "execution node is not attached to a graph storage");
        sizes_[output_indices_[n]] = rows;
    }

    std::pmr::memory_resource* execution_node_t::resource() const noexcept {
        return input_indices_.get_allocator().resource();
    }

    void execution_node_t::set_input(size_t position, slot_id_t slot) {
        assert(position < input_indices_.size());
        input_indices_[position] = slot;
    }

    core::error_t execution_node_t::validate() const {
        for (auto slot : input_indices_) {
            if (slot == invalid_slot) {
                return core::error_t(core::error_code_t::schema_error,
                                     std::pmr::string{"execution graph: node input is not connected", resource()});
            }
        }
        for (auto slot : output_indices_) {
            if (slot == invalid_slot) {
                return core::error_t(core::error_code_t::schema_error,
                                     std::pmr::string{"execution graph: node output is not connected", resource()});
            }
        }
        return core::error_t::no_error();
    }

    cast_node_t::cast_node_t(std::pmr::memory_resource* resource,
                             casts::cast_kind kind,
                             slot_id_t input,
                             slot_id_t output)
        : execution_node_t(resource, slot_list_t({input}, resource), slot_list_t({output}, resource))
        , kind_(kind) {}

    core::error_t cast_node_t::process(const graph_execution_context& context, uint64_t count) {
        return cast_(kind_, input(0), &output(0), context, count);
    }

    core::error_t cast_node_t::validate() const {
        auto error = execution_node_t::validate();
        if (error.contains_error()) {
            return error;
        }
        if (!cast_) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"execution graph: cast node has no resolved cast", resource()});
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<uint64_t> function_node_t::select_distinct(const vector::data_chunk_t& arguments,
                                                                      uint64_t count) {
        if (!distinct_states_) {
            distinct_states_.emplace(resource(), compute::aggregate_state_of<value_set_t>());
        }
        distinct_states_->reserve(states_ ? states_->size() : 0);
        auto sets = distinct_states_->states();
        distinct_groups_.clear();
        distinct_groups_.reserve(count);
        uint64_t kept = 0;
        for (uint64_t row = 0; row < count; row++) {
            const uint32_t group = group_ids_[row];
            if (!sets.at<value_set_t>(group).insert(arguments.data.front().value(row)).second) {
                continue;
            }
            distinct_rows_.set_index(kept, row);
            distinct_groups_.push_back(group);
            kept++;
        }
        return kept;
    }

    vector::data_chunk_t function_node_t::argument_chunk(uint64_t count) const {
        std::pmr::vector<types::complex_logical_type> argument_types(resource());
        argument_types.reserve(input_indices_.size());
        for (size_t position = 0; position < input_indices_.size(); position++) {
            argument_types.push_back(input(position).type());
        }
        vector::data_chunk_t arguments(resource(), argument_types, std::vector<size_t>{}, count > 0 ? count : 1);
        for (size_t position = 0; position < input_indices_.size(); position++) {
            arguments.data[position].reference(input(position));
        }
        arguments.set_cardinality(count);
        // TODO: function kernel accept only flat vectors for now
        arguments.flatten();
        return arguments;
    }

    core::error_t function_node_t::ensure_executor() {
        if (executor_) {
            return core::error_t::no_error();
        }
        auto arguments = argument_chunk(0);
        auto executor = function_->make_executor(resource(), arguments.types(), nullptr, context_);
        if (executor.has_error()) {
            return executor.error();
        }
        executor_ = std::move(executor.value());
        return core::error_t::no_error();
    }

    core::error_t function_node_t::reserve_groups(uint64_t group_count) {
        assert(reduces_ && "only a reducing node accumulates per group");
        if (auto error = ensure_executor(); error.contains_error()) {
            return error;
        }
        if (!states_) {
            auto layout = executor_->state_layout();
            if (layout.size == 0) {
                return core::error_t(
                    core::error_code_t::kernel_error,
                    std::pmr::string{"execution graph: aggregate cannot accumulate its argument type", resource()});
            }
            states_.emplace(resource(), layout);
        }
        states_->reserve(group_count);
        return core::error_t::no_error();
    }

    core::error_t function_node_t::emit(uint64_t first, uint64_t count) {
        assert(reduces_ && "only a reducing node emits accumulators");
        if (auto error = reserve_groups(first + count); error.contains_error()) {
            return error;
        }
        if (auto error = executor_->finalize(states_->states(), first, count, output(0)); error.contains_error()) {
            return error;
        }
        set_output_size(0, count);
        return core::error_t::no_error();
    }

    field_node_t::field_node_t(std::pmr::memory_resource* resource,
                               std::pmr::vector<size_t> path,
                               slot_id_t input,
                               slot_id_t output)
        : execution_node_t(resource, slot_list_t({input}, resource), slot_list_t({output}, resource))
        , path_(std::move(path), resource) {}

    core::error_t field_node_t::process(const graph_execution_context&, uint64_t count) {
        const vector::vector_t* nested = &input(0);
        bool subscripted = false;
        for (size_t step : path_) {
            const auto kind = nested->type().type();
            if (kind == types::logical_type::ARRAY || kind == types::logical_type::LIST ||
                kind == types::logical_type::MAP) {
                subscripted = true;
                break;
            }
            const auto& entries = nested->entries();
            if (step >= entries.size()) {
                return core::error_t(
                    core::error_code_t::schema_error,
                    std::pmr::string{"execution graph: field node step is outside the nested value", resource()});
            }
            nested = entries[step].get();
        }
        if (!subscripted) {
            // Best case, no copy needed
            output(0).reference(*nested);
            return core::error_t::no_error();
        }

        // required elements are not stored contiguously, so they are copied to single vector
        std::pmr::vector<uint64_t> element_path(path_.size() + 1, 0, resource());
        std::copy(path_.begin(), path_.end(), element_path.begin() + 1);
        for (uint64_t row = 0; row < count; ++row) {
            element_path[0] = row;
            uint64_t leaf_index = 0;
            bool absent = false;
            const vector::vector_t* leaf = input(0).resolve_nested_location(element_path, &leaf_index, &absent);
            if (absent) {
                output(0).set_null(row, true);
                continue;
            }
            vector::vector_ops::copy(*leaf, output(0), leaf_index + 1, leaf_index, row);
        }
        return core::error_t::no_error();
    }

    core::error_t field_node_t::validate() const {
        auto error = execution_node_t::validate();
        if (error.contains_error()) {
            return error;
        }
        if (path_.empty()) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"execution graph: field node has no path", resource()});
        }
        return core::error_t::no_error();
    }

    operator_node_t::operator_node_t(std::pmr::memory_resource* resource,
                                     operators::operator_code op,
                                     slot_id_t left,
                                     slot_id_t right,
                                     slot_id_t output)
        : execution_node_t(resource, slot_list_t({left, right}, resource), slot_list_t({output}, resource))
        , op_(op) {}

    operator_node_t::operator_node_t(std::pmr::memory_resource* resource,
                                     operators::operator_code op,
                                     slot_id_t operand,
                                     slot_id_t output)
        : execution_node_t(resource, slot_list_t({operand}, resource), slot_list_t({output}, resource))
        , op_(op) {}

    core::error_t operator_node_t::process(const graph_execution_context& context, uint64_t count) {
        if (input_indices_.size() == 2) {
            return vector::operations::apply_binary(op_, input(0), input(1), &output(0), context, count, active_rows());
        }
        return vector::operations::apply_unary(op_, input(0), &output(0), context, count);
    }

    core::error_t operator_node_t::validate() const {
        auto error = execution_node_t::validate();
        if (error.contains_error()) {
            return error;
        }
        const size_t expected = operators::arity_of(op_) == operators::operator_arity::binary ? 2 : 1;
        if (input_indices_.size() != expected) {
            return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{"execution graph: operator node operand count does not match its arity", resource()});
        }
        return core::error_t::no_error();
    }

    blend_node_t::blend_node_t(std::pmr::memory_resource* resource,
                               blend_kind kind,
                               slot_list_t inputs,
                               slot_id_t output)
        : execution_node_t(resource, std::move(inputs), slot_list_t({output}, resource))
        , kind_(kind) {}

    core::error_t blend_node_t::process(const graph_execution_context&, uint64_t count) {
        const size_t arity = input_indices_.size();
        vector::vector_t& result = output(0);
        if (kind_ == blend_kind::logical_and || kind_ == blend_kind::logical_or) {
            const bool decisive = kind_ == blend_kind::logical_or;
            for (uint64_t row = 0; row < count; row++) {
                bool settled = false;
                bool unknown = false;
                for (size_t position = 0; position < arity; position++) {
                    if (input(position).is_null(row)) {
                        unknown = true;
                        continue;
                    }
                    if (input(position).get_value<bool>(row) == decisive) {
                        settled = true;
                        break;
                    }
                }
                // Nothing settled it and an operand was UNKNOWN: the fold is UNKNOWN.
                if (!settled && unknown) {
                    result.set_null(row, true);
                    continue;
                }
                result.set_null(row, false);
                result.set_value(row, settled ? decisive : !decisive);
            }
            return core::error_t::no_error();
        }
        for (uint64_t row = 0; row < count; row++) {
            // arity means "nothing selected": every condition was false or UNKNOWN and there is no
            // ELSE, or every COALESCE operand was null. SQL calls that NULL.
            size_t chosen = arity;
            if (kind_ == blend_kind::coalesce) {
                for (size_t position = 0; position < arity; position++) {
                    if (!input(position).is_null(row)) {
                        chosen = position;
                        break;
                    }
                }
            } else {
                for (size_t position = 0; position + 1 < arity; position += 2) {
                    const vector::vector_t& condition = input(position);
                    if (!condition.is_null(row) && condition.get_value<bool>(row)) {
                        chosen = position + 1;
                        break;
                    }
                }
                // A trailing odd input is the ELSE value.
                if (chosen == arity && arity % 2 == 1) {
                    chosen = arity - 1;
                }
            }

            if (chosen == arity || input(chosen).is_null(row)) {
                result.set_null(row, true);
                continue;
            }
            result.set_null(row, false);
            vector::vector_ops::copy(input(chosen), result, row + 1, row, row);
        }
        return core::error_t::no_error();
    }

    case_when_node_t::case_when_node_t(std::pmr::memory_resource* resource, slot_list_t conditions, slot_list_t masks)
        : execution_node_t(resource, std::move(conditions), std::move(masks)) {}

    core::error_t case_when_node_t::process(const graph_execution_context&, uint64_t count) {
        const size_t arms = input_indices_.size();
        for (uint64_t row = 0; row < count; row++) {
            // Walk the conditions in order and let the first definitely-TRUE one claim the row.
            // An UNKNOWN condition claims nothing but does not stop the search, exactly as a
            // false one does not: SQL only takes the arm whose WHEN is TRUE.
            size_t chosen = arms;
            for (size_t arm = 0; arm < arms; arm++) {
                const vector::vector_t& condition = input(arm);
                if (!condition.is_null(row) && condition.get_value<bool>(row)) {
                    chosen = arm;
                    break;
                }
            }
            for (size_t arm = 0; arm <= arms; arm++) {
                vector::vector_t& mask = output(arm);
                mask.set_null(row, false);
                mask.set_value(row, arm == chosen);
            }
        }
        for (size_t arm = 0; arm <= arms; arm++) {
            set_output_size(arm, count);
        }
        return core::error_t::no_error();
    }

    core::error_t case_when_node_t::validate() const {
        auto error = execution_node_t::validate();
        if (error.contains_error()) {
            return error;
        }
        if (input_indices_.empty()) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"execution graph: case_when node has no condition", resource()});
        }
        // One mask per condition, plus the DEFAULT arm's.
        if (output_indices_.size() != input_indices_.size() + 1) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"execution graph: case_when node needs one mask per condition "
                                                  "plus a default",
                                                  resource()});
        }
        return core::error_t::no_error();
    }

    case_then_node_t::case_then_node_t(std::pmr::memory_resource* resource,
                                       slot_list_t values,
                                       slot_list_t masks,
                                       slot_id_t output)
        : execution_node_t(resource, std::move(values), slot_list_t({output}, resource))
        , arm_count_(input_indices_.size()) {
        input_indices_.insert(input_indices_.end(), masks.begin(), masks.end());
    }

    core::error_t case_then_node_t::process(const graph_execution_context&, uint64_t count) {
        vector::vector_t& result = output(0);
        for (uint64_t row = 0; row < count; row++) {
            size_t chosen = arm_count_;
            for (size_t arm = 0; arm < arm_count_; arm++) {
                const vector::vector_t& mask = input(arm_count_ + arm);
                if (!mask.is_null(row) && mask.get_value<bool>(row)) {
                    chosen = arm;
                    break;
                }
            }
            // No arm claimed the row: no WHEN matched and there was no ELSE.
            if (chosen == arm_count_) {
                result.set_null(row, true);
                continue;
            }
            const vector::vector_t& value = input(chosen);
            if (value.is_null(row)) {
                result.set_null(row, true);
                continue;
            }
            vector::vector_ops::copy(value, result, row + 1, row, row);
        }
        set_output_size(0, count);
        return core::error_t::no_error();
    }

    core::error_t case_then_node_t::validate() const {
        auto error = execution_node_t::validate();
        if (error.contains_error()) {
            return error;
        }
        if (arm_count_ == 0 || input_indices_.size() != arm_count_ * 2) {
            return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{"execution graph: case_then node needs one mask per value", resource()});
        }
        return core::error_t::no_error();
    }

    core::error_t blend_node_t::validate() const {
        auto error = execution_node_t::validate();
        if (error.contains_error()) {
            return error;
        }
        if (kind_ == blend_kind::case_when) {
            if (input_indices_.size() < 2) {
                return core::error_t(
                    core::error_code_t::schema_error,
                    std::pmr::string{"execution graph: case node has no condition and result pair", resource()});
            }
        } else if (input_indices_.empty()) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"execution graph: blend node has no operands", resource()});
        }
        return core::error_t::no_error();
    }

    function_node_t::function_node_t(std::pmr::memory_resource* resource,
                                     const compute::function* function,
                                     slot_list_t inputs,
                                     slot_list_t outputs,
                                     bool reduces,
                                     bool distinct)
        : execution_node_t(resource, std::move(inputs), std::move(outputs))
        , function_(function)
        , context_(resource)
        , distinct_(distinct)
        , reduces_(reduces)
        , distinct_rows_(resource, vector::DEFAULT_VECTOR_CAPACITY)
        , distinct_groups_(resource) {}

    core::error_t function_node_t::process(const graph_execution_context&, uint64_t count) {
        auto arguments = argument_chunk(count);
        // The executor is built once and kept, so the accumulators survive across chunks
        if (auto error = ensure_executor(); error.contains_error()) {
            return error;
        }
        // A reduction folds the chunk into one accumulator per group here and produces its values
        // group by group in emit().
        if (reduces_) {
            if (count == 0) {
                return core::error_t::no_error();
            }
            if (!states_) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"execution graph: the accumulators must be reserved before a chunk", resource()});
            }
            if (!distinct_ || input_indices_.empty()) {
                return executor_->update(arguments, {group_ids_.data(), count}, states_->states());
            }
            auto kept = select_distinct(arguments, count);
            if (kept.has_error()) {
                return kept.error();
            }
            vector::data_chunk_t unique(resource(), arguments.types(), kept.value() > 0 ? kept.value() : 1);
            arguments.copy(unique, distinct_rows_, kept.value(), 0);
            unique.set_cardinality(kept.value());
            return executor_->update(unique, {distinct_groups_.data(), kept.value()}, states_->states());
        }
        auto result = executor_->execute(arguments);
        if (result.has_error()) {
            return result.error();
        }
        auto& produced = result.value();
        if (std::holds_alternative<vector::data_chunk_t>(produced)) {
            auto& chunk = std::get<vector::data_chunk_t>(produced);
            assert(chunk.size() == count);
            if (chunk.column_count() != output_indices_.size()) {
                return core::error_t(
                    core::error_code_t::incorrect_function_return_type,
                    std::pmr::string{"execution graph: function returned an unexpected column count", resource()});
            }
            for (size_t position = 0; position < output_indices_.size(); position++) {
                output(position).reference(chunk.data[position]);
            }
            return core::error_t::no_error();
        }

        auto& values = std::get<std::pmr::vector<types::logical_value_t>>(produced);
        assert(values.size() == count);
        if (output_indices_.size() != 1) {
            return core::error_t(
                core::error_code_t::incorrect_function_return_type,
                std::pmr::string{"execution graph: function returned one column but the node declares several",
                                 resource()});
        }
        for (uint64_t row = 0; row < values.size(); row++) {
            output(0).set_null(row, values[row].is_null());
            if (!values[row].is_null()) {
                output(0).set_value(row, values[row]);
            }
        }
        return core::error_t::no_error();
    }

    core::error_t function_node_t::validate() const {
        auto error = execution_node_t::validate();
        if (error.contains_error()) {
            return error;
        }
        if (function_ == nullptr) {
            return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{"execution graph: function node has no resolved function", resource()});
        }
        if (reduces_ && output_indices_.size() != 1) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"execution graph: reduction produces exactly one value", resource()});
        }
        return core::error_t::no_error();
    }

    parameter_node_t::parameter_node_t(std::pmr::memory_resource* resource, core::parameter_id_t id, slot_id_t output)
        : execution_node_t(resource, slot_list_t(resource), slot_list_t({output}, resource))
        , id_(id) {}

    core::error_t parameter_node_t::process(const graph_execution_context&, uint64_t) {
        if (placed_) {
            return core::error_t::no_error();
        }
        if (parameters_ == nullptr) {
            return core::error_t(core::error_code_t::invalid_parameter,
                                 std::pmr::string{"execution graph: parameters are not bound", resource()});
        }
        auto value = parameters_->find(id_);
        if (value == parameters_->end()) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{"execution graph: parameter is not in the bound parameter map", resource()});
        }
        // parameters stored as logical_value_t, and it can not hold NULL with type associated with it
        // but output type is already resolved, so we just place null there
        if (value->second.is_null()) {
            output(0).reference(types::logical_value_t(resource(), output(0).type()));
            output(0).set_null(true);
        } else {
            output(0).reference(value->second);
        }
        placed_ = true;
        return core::error_t::no_error();
    }

    void parameter_node_t::set_parameters(const parameter_map_t* parameters) noexcept {
        parameters_ = parameters;
        placed_ = false;
    }

    execution_graph_t::execution_graph_t(std::pmr::memory_resource* resource, uint64_t capacity)
        : resource_(resource)
        , capacity_(capacity)
        , slots_(resource)
        , data_storage_(resource)
        , output_slots_(resource)
        , reduction_nodes_(resource)
        , nodes_(resource)
        , order_(resource) {}

    node_id_t execution_graph_t::append(execution_node_ptr node) {
        prepared_ = false;
        nodes_.push_back(std::move(node));
        return node_id_t{nodes_.size() - 1};
    }

    slot_id_t execution_graph_t::declare_slot() {
        prepared_ = false;
        slots_.emplace_back();
        return slot_id_t{slots_.size() - 1};
    }

    node_id_t
    execution_graph_t::add_aggregate(const compute::function* function, const slot_list_t& inputs, bool distinct) {
        auto output = declare_slot();
        return append(new function_node_t(resource_,
                                          function,
                                          slot_list_t(inputs, resource_),
                                          slot_list_t({output}, resource_),
                                          /*reduces=*/true,
                                          distinct));
    }

    node_id_t execution_graph_t::add_field(slot_id_t input, const std::pmr::vector<size_t>& path) {
        for (node_id_t id{0}; id < nodes_.size(); id++) {
            const auto* field = dynamic_cast<const field_node_t*>(nodes_[id].get());
            if (field != nullptr && field->input_indices().front() == input && field->path() == path) {
                return id;
            }
        }
        auto output = declare_slot();
        return append(new field_node_t(resource_, std::pmr::vector<size_t>(path, resource_), input, output));
    }

    node_id_t execution_graph_t::add_cast(slot_id_t input, casts::cast_kind kind) {
        auto output = declare_slot();
        return append(new cast_node_t(resource_, kind, input, output));
    }

    node_id_t execution_graph_t::add_operator(operators::operator_code op, slot_id_t left, slot_id_t right) {
        auto output = declare_slot();
        return append(new operator_node_t(resource_, op, left, right, output));
    }

    node_id_t execution_graph_t::add_operator(operators::operator_code op, slot_id_t operand) {
        auto output = declare_slot();
        return append(new operator_node_t(resource_, op, operand, output));
    }

    node_id_t
    execution_graph_t::add_function(const compute::function* function, const slot_list_t& inputs, size_t output_count) {
        slot_list_t outputs(resource_);
        outputs.reserve(output_count);
        for (size_t position = 0; position < output_count; position++) {
            outputs.push_back(declare_slot());
        }
        return append(new function_node_t(resource_, function, slot_list_t(inputs, resource_), std::move(outputs)));
    }

    node_id_t execution_graph_t::add_parameter(core::parameter_id_t id) {
        auto output = declare_slot();
        return append(new parameter_node_t(resource_, id, output));
    }

    node_id_t execution_graph_t::add_blend(blend_node_t::blend_kind kind, const slot_list_t& inputs) {
        auto output = declare_slot();
        return append(new blend_node_t(resource_, kind, slot_list_t(inputs, resource_), output));
    }

    node_id_t execution_graph_t::add_case_when(const slot_list_t& conditions, slot_list_t& masks) {
        masks.clear();
        masks.reserve(conditions.size() + 1);
        // One mask per condition plus the DEFAULT arm's. They are BOOLEAN and always written, so
        // they are typed here rather than left for validation to stamp.
        for (size_t arm = 0; arm <= conditions.size(); arm++) {
            auto mask = declare_slot();
            set_slot_type(mask, types::complex_logical_type{types::logical_type::BOOLEAN});
            masks.push_back(mask);
        }
        return append(
            new case_when_node_t(resource_, slot_list_t(conditions, resource_), slot_list_t(masks, resource_)));
    }

    node_id_t execution_graph_t::add_case_then(const slot_list_t& values, const slot_list_t& masks) {
        auto output = declare_slot();
        return append(
            new case_then_node_t(resource_, slot_list_t(values, resource_), slot_list_t(masks, resource_), output));
    }

    void execution_graph_t::set_constraint(node_id_t node, slot_id_t mask) {
        prepared_ = false;
        nodes_[node]->set_constraint(mask);
    }

    void execution_graph_t::connect(node_id_t consumer, size_t position, slot_id_t producer) {
        prepared_ = false;
        nodes_[consumer]->set_input(position, producer);
    }

    slot_id_t execution_graph_t::output_slot(node_id_t node, size_t position) const {
        return nodes_[node]->output_indices()[position];
    }

    void execution_graph_t::bind_input(slot_id_t slot, size_t column_index, const types::complex_logical_type& type) {
        prepared_ = false;
        slots_[slot].input_column = column_index;
        slots_[slot].bound = true;
        set_slot_type(slot, type);
    }

    void execution_graph_t::set_slot_type(slot_id_t slot, const types::complex_logical_type& type) {
        prepared_ = false;
        slots_[slot].type = type;
        slots_[slot].typed = true;
    }

    void execution_graph_t::set_cast(node_id_t node, casts::cast_t cast) {
        auto* target = dynamic_cast<cast_node_t*>(nodes_[node].get());
        assert(target && "execution graph: set_cast on a node that is not a cast");
        target->set_cast(std::move(cast));
    }

    node_id_t execution_graph_t::insert_cast_before(node_id_t consumer,
                                                    size_t position,
                                                    casts::cast_t cast,
                                                    casts::cast_kind kind,
                                                    const types::complex_logical_type& target) {
        auto source = nodes_[consumer]->input_indices()[position];
        auto node = add_cast(source, kind);
        auto produced = output_slot(node);
        set_slot_type(produced, target);
        set_cast(node, std::move(cast));
        nodes_[consumer]->set_input(position, produced);
        auto at = std::find(order_.begin(), order_.end(), consumer);
        if (at != order_.end()) {
            order_.insert(at, node);
        }
        return node;
    }

    core::error_t execution_graph_t::validate() {
        std::pmr::vector<node_id_t> producer(slots_.size(), invalid_node, resource_);
        for (size_t index = 0; index < nodes_.size(); index++) {
            auto error = nodes_[index]->validate();
            if (error.contains_error()) {
                return error;
            }
            for (auto slot : nodes_[index]->input_indices()) {
                if (slot >= slots_.size()) {
                    return core::error_t(core::error_code_t::schema_error,
                                         std::pmr::string{"execution graph: input slot misconfigured", resource()});
                }
            }
            for (auto slot : nodes_[index]->output_indices()) {
                if (slot >= slots_.size() || slots_[slot].bound || producer[slot] != invalid_node) {
                    return core::error_t(core::error_code_t::schema_error,
                                         std::pmr::string{"execution graph: output slot misconfigured", resource()});
                }
                producer[slot] = node_id_t{index};
            }
        }
        for (size_t index = 0; index < slots_.size(); index++) {
            if (!slots_[index].typed || (!slots_[index].bound && producer[index] == invalid_node)) {
                return core::error_t(core::error_code_t::schema_error,
                                     std::pmr::string{"execution graph: intermediate slot misconfigured", resource()});
            }
        }
        for (auto slot : output_slots_) {
            if (slot >= slots_.size()) {
                return core::error_t(core::error_code_t::schema_error,
                                     std::pmr::string{"execution graph: output slot misconfigured", resource()});
            }
        }
        return order_nodes();
    }

    core::error_t execution_graph_t::order_nodes() {
        std::pmr::vector<node_id_t> producer(slots_.size(), invalid_node, resource_);
        for (size_t index = 0; index < nodes_.size(); index++) {
            for (auto slot : nodes_[index]->output_indices()) {
                producer[slot] = node_id_t{index};
            }
        }

        // A node is live when an output slot depends on it; the rest lead nowhere and never run.
        // Before an output is selected there is nothing to reach from, so every node counts and
        // validating a half-built graph still covers all of it.
        std::pmr::vector<uint8_t> live(nodes_.size(), output_slots_.empty() ? uint8_t{1} : uint8_t{0}, resource_);
        size_t live_count = output_slots_.empty() ? nodes_.size() : 0;
        std::pmr::vector<node_id_t> reached(resource_);
        for (auto slot : output_slots_) {
            if (producer[slot] != invalid_node && live[producer[slot]] == 0) {
                live[producer[slot]] = 1;
                live_count++;
                reached.push_back(producer[slot]);
            }
        }
        auto reach = [&](slot_id_t slot) {
            auto from = producer[slot];
            if (from != invalid_node && live[from] == 0) {
                live[from] = 1;
                live_count++;
                reached.push_back(from);
            }
        };
        while (!reached.empty()) {
            auto node = reached.back();
            reached.pop_back();
            for (auto slot : nodes_[node]->input_indices()) {
                reach(slot);
            }
            // A constrained node needs its mask, so the mask's producer is live too.
            if (nodes_[node]->constraint_slot() != invalid_slot) {
                reach(nodes_[node]->constraint_slot());
            }
        }

        using node_list_t = std::pmr::vector<node_id_t>;
        std::pmr::vector<size_t> pending(nodes_.size(), 0, resource_);
        std::pmr::vector<node_list_t> consumers(nodes_.size(), node_list_t(resource_), resource_);
        for (size_t index = 0; index < nodes_.size(); index++) {
            if (live[index] == 0) {
                continue;
            }
            auto depend_on = [&](slot_id_t slot) {
                auto from = producer[slot];
                if (from == invalid_node) {
                    return;
                }
                pending[index]++;
                consumers[from].push_back(node_id_t{index});
            };
            for (auto slot : nodes_[index]->input_indices()) {
                depend_on(slot);
            }
            // The mask is an edge like any other: a CASE arm cannot run before case_when wrote it.
            if (nodes_[index]->constraint_slot() != invalid_slot) {
                depend_on(nodes_[index]->constraint_slot());
            }
        }

        order_.clear();
        order_.reserve(live_count);
        std::pmr::vector<node_id_t> ready(resource_);
        for (size_t index = 0; index < nodes_.size(); index++) {
            if (live[index] != 0 && pending[index] == 0) {
                ready.push_back(node_id_t{index});
            }
        }
        while (!ready.empty()) {
            auto node = ready.back();
            ready.pop_back();
            order_.push_back(node);
            for (auto consumer : consumers[node]) {
                if (--pending[consumer] == 0) {
                    ready.push_back(consumer);
                }
            }
        }
        if (order_.size() != live_count) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"execution graph: cycle detected", resource()});
        }
        return core::error_t::no_error();
    }

    std::pmr::vector<execution_graph_t::input_binding_t> execution_graph_t::input_bindings() const {
        std::pmr::vector<input_binding_t> bindings(resource_);
        for (size_t index = 0; index < slots_.size(); index++) {
            if (slots_[index].bound) {
                bindings.push_back(input_binding_t{slot_id_t{index}, slots_[index].input_column});
            }
        }
        return bindings;
    }

    void execution_graph_t::set_output(const slot_list_t& selected) {
        prepared_ = false;
        output_slots_.assign(selected.begin(), selected.end());
    }

    void execution_graph_t::set_parameters(const parameter_map_t* parameters) {
        parameters_ = parameters;
        for (auto& node : nodes_) {
            node->set_parameters(parameters);
        }
    }

    core::error_t execution_graph_t::prepare() {
        if (output_slots_.empty()) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"execution graph: no outputs were set", resource()});
        }
        auto error = validate();
        if (error.contains_error()) {
            return error;
        }
        data_storage_.clear();
        data_storage_.reserve(slots_.size());
        slot_sizes_.assign(slots_.size(), 0);
        for (const auto& slot : slots_) {
            // a bound slot is referenced from the input chunk, so it needs no buffer of its own
            data_storage_.emplace_back(resource_, slot.type, !slot.bound, false, capacity_);
        }
        reduction_nodes_.clear();
        for (node_id_t id{0}; id < nodes_.size(); id++) {
            nodes_[id]->set_storage(&data_storage_, slot_sizes_.data());
            nodes_[id]->set_parameters(parameters_);
            if (nodes_[id]->reduces()) {
                reduction_nodes_.push_back(id);
            }
        }
        split_at_group();
        single_group_.assign(capacity_, 0);
        prepared_ = true;
        return core::error_t::no_error();
    }

    void execution_graph_t::split_at_group() {
        key_nodes_.clear();
        chunk_nodes_.clear();
        group_nodes_.clear();

        // A slot carries a value per GROUP once it is a grouping key or a reduction's output, and
        // the property spreads to whatever reads it.
        std::pmr::vector<bool> grouped(slots_.size(), false, resource_);
        for (auto slot : key_slots_) {
            grouped[slot] = true;
        }
        std::pmr::vector<bool> reads_grouped(nodes_.size(), false, resource_);
        for (auto id : order_) {
            const auto& node = *nodes_[id];
            for (auto slot : node.input_indices()) {
                reads_grouped[id] = reads_grouped[id] || grouped[slot];
            }
            for (auto slot : node.output_indices()) {
                grouped[slot] = grouped[slot] || node.reduces() || reads_grouped[id];
            }
        }

        // Reading a key does not by itself make a node run per group: a reduction reading one
        // still folds the group's ROWS (sum(k) over three rows is 3k, not k), and so does
        // anything feeding a reduction. Both directions are settled by walking back from the key
        // slots and from the reductions' arguments.
        std::pmr::vector<bool> produces_key(nodes_.size(), false, resource_);
        std::pmr::vector<bool> feeds_reduction(nodes_.size(), false, resource_);
        std::pmr::vector<bool> wanted_by_key(slots_.size(), false, resource_);
        std::pmr::vector<bool> wanted_by_reduction(slots_.size(), false, resource_);
        for (auto slot : key_slots_) {
            wanted_by_key[slot] = true;
        }
        for (auto id : reduction_nodes_) {
            for (auto slot : nodes_[id]->input_indices()) {
                wanted_by_reduction[slot] = true;
            }
        }
        for (auto id = order_.rbegin(); id != order_.rend(); ++id) {
            const auto& node = *nodes_[*id];
            for (auto slot : node.output_indices()) {
                produces_key[*id] = produces_key[*id] || wanted_by_key[slot];
                feeds_reduction[*id] = feeds_reduction[*id] || wanted_by_reduction[slot];
            }
            for (auto slot : node.input_indices()) {
                wanted_by_key[slot] = wanted_by_key[slot] || produces_key[*id];
                wanted_by_reduction[slot] = wanted_by_reduction[slot] || feeds_reduction[*id];
            }
        }

        for (auto id : order_) {
            // A key is computed before the caller can group anything, and at finalize its slot is
            // supplied by the caller instead, so a key node belongs to that pass alone.
            if (produces_key[id]) {
                key_nodes_.push_back(id);
                continue;
            }
            const bool reduces = nodes_[id]->reduces();
            if (!reads_grouped[id] || feeds_reduction[id] || reduces) {
                chunk_nodes_.push_back(id);
            }
            // An expression over a key or a reduction result yields one value per group. The same
            // node can also sit under a reduction (SELECT k + 1, sum(k + 1)), in which case it
            // belongs to both passes: per row for the fold, per group for the output.
            if (reads_grouped[id] && !reduces) {
                group_nodes_.push_back(id);
            }
        }
    }

    void execution_graph_t::add_key_slot(slot_id_t slot) {
        prepared_ = false;
        key_slots_.push_back(slot);
    }

    core::error_t
    execution_graph_t::run(execution_node_t* node, const graph_execution_context& context, uint64_t ambient) {
        uint64_t count = unconstrained_rows;
        for (auto slot : node->input_indices()) {
            count = std::min(count, slot_sizes_[slot]);
        }
        for (auto slot : node->output_indices()) {
            slot_sizes_[slot] = count;
        }

        // A node with NO inputs has nothing to take a size from, so it works at the row count it was
        // handed
        const bool constant = count == unconstrained_rows && !node->input_indices().empty();
        const uint64_t execute_over = node->input_indices().empty() ? ambient : (constant ? uint64_t{1} : count);
        auto error = node->process(context, execute_over);
        if (error.contains_error()) {
            return error;
        }
        if (constant) {
            for (auto slot : node->output_indices()) {
                // set type resets auxiliary and can not be called, since we need it
                if (data_storage_[slot].get_vector_type() != vector::vector_type::CONSTANT) {
                    data_storage_[slot].set_vector_type(vector::vector_type::CONSTANT);
                }
            }
        }
        return core::error_t::no_error();
    }

    core::error_t execution_graph_t::bind_inputs(const vector::data_chunk_t& input) {
        if (!prepared_) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"execution graph: process() before prepare()", resource()});
        }
        const uint64_t count = input.size();
        if (count > capacity_) {
            return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{"execution graph: input chunk exceeds the graph capacity", resource()});
        }
        for (size_t index = 0; index < slots_.size(); index++) {
            if (!slots_[index].bound) {
                continue;
            }
            if (slots_[index].input_column >= input.column_count()) {
                return core::error_t(
                    core::error_code_t::schema_error,
                    std::pmr::string{"execution graph: input column is missing from the chunk", resource()});
            }
            const auto& column = input.data[slots_[index].input_column];
            // catches a chunk laid out differently than the schema the slots were bound against
            if (column.type() != slots_[index].type) {
                return core::error_t(
                    core::error_code_t::schema_error,
                    std::pmr::string{"execution graph: input column type does not match its slot", resource()});
            }
            data_storage_[index].reference(column);
            slot_sizes_[index] = count;
        }
        return core::error_t::no_error();
    }

    core::error_t execution_graph_t::process(const vector::data_chunk_t& input,
                                             const graph_execution_context& context) {
        // An ungrouped run is a grouped one over the single group 0.
        if (auto error = reserve_groups(1); error.contains_error()) {
            return error;
        }
        if (auto error = process_keys(input, context); error.contains_error()) {
            return error;
        }
        return process_groups({single_group_.data(), input.size()}, context);
    }

    core::error_t execution_graph_t::process_keys(const vector::data_chunk_t& input,
                                                  const graph_execution_context& context) {
        if (auto error = bind_inputs(input); error.contains_error()) {
            return error;
        }
        bound_rows_ = input.size();
        for (auto node : key_nodes_) {
            auto error = run(nodes_[node].get(), context, bound_rows_);
            if (error.contains_error()) {
                return error;
            }
        }
        return core::error_t::no_error();
    }

    const vector::vector_t& execution_graph_t::key_values(size_t index) const {
        assert(index < key_slots_.size());
        return data_storage_[key_slots_[index]];
    }

    core::error_t execution_graph_t::process_groups(core::span<const uint32_t> group_ids,
                                                    const graph_execution_context& context) {
        if (group_ids.size() < bound_rows_) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{"execution graph: one group id per input row is required", resource()});
        }
        for (auto id : reduction_nodes_) {
            static_cast<function_node_t*>(nodes_[id].get())->set_group_ids(group_ids);
        }

        for (auto node : chunk_nodes_) {
            auto error = run(nodes_[node].get(), context, bound_rows_);
            if (error.contains_error()) {
                return error;
            }
        }

        return core::error_t::no_error();
    }

    core::error_t execution_graph_t::reserve_groups(uint64_t group_count) {
        if (!prepared_) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"execution graph: reserve_groups() before prepare()", resource()});
        }
        for (auto id : reduction_nodes_) {
            if (auto error = static_cast<function_node_t*>(nodes_[id].get())->reserve_groups(group_count);
                error.contains_error()) {
                return error;
            }
        }
        return core::error_t::no_error();
    }

    core::error_t
    execution_graph_t::emit_groups(const graph_execution_context& context, uint64_t first, uint64_t count) {
        if (!prepared_) {
            return core::error_t(core::error_code_t::schema_error,
                                 std::pmr::string{"execution graph: finalize() before prepare()", resource()});
        }
        for (auto id : reduction_nodes_) {
            if (auto error = static_cast<function_node_t*>(nodes_[id].get())->emit(first, count);
                error.contains_error()) {
                return error;
            }
        }
        for (auto node : group_nodes_) {
            auto error = run(nodes_[node].get(), context, count);
            if (error.contains_error()) {
                return error;
            }
        }
        return core::error_t::no_error();
    }

    core::error_t execution_graph_t::finalize_groups(const graph_execution_context& context,
                                                     core::span<const vector::vector_t* const> keys,
                                                     uint64_t first,
                                                     uint64_t count,
                                                     vector::data_chunk_t* target,
                                                     uint64_t target_row) {
        if (keys.size() != key_slots_.size()) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{"execution graph: one key vector per declared key slot is required", resource()});
        }
        // The key values of THESE groups replace what the last chunk left in the key slots, so the
        // expressions above them evaluate once per group instead of once per row.
        for (size_t index = 0; index < keys.size(); index++) {
            data_storage_[key_slots_[index]].reference(*keys[index]);
            slot_sizes_[key_slots_[index]] = count;
        }
        if (auto error = emit_groups(context, first, count); error.contains_error()) {
            return error;
        }
        return copy_outputs(emitted_rows(count), target, target_row);
    }

    core::error_t execution_graph_t::finalize_inplace(const graph_execution_context& context,
                                                      uint64_t count,
                                                      vector::data_chunk_t* target,
                                                      uint64_t target_row) {
        // No grouping: every reduction holds exactly one accumulator, and a graph without one
        // simply completes the rows it was fed.
        const uint64_t ambient = reduction_nodes_.empty() ? count : uint64_t{1};
        if (auto error = emit_groups(context, 0, ambient); error.contains_error()) {
            return error;
        }
        return copy_outputs(emitted_rows(ambient), target, target_row);
    }

    // What the outputs actually hold: every node stamped its output size, so the emitted row count
    // is the narrowest of them. All-unconstrained means every output is constant, which broadcasts
    // to whatever the caller asked for.
    uint64_t execution_graph_t::emitted_rows(uint64_t ambient) const {
        uint64_t emitted = unconstrained_rows;
        for (auto slot : output_slots_) {
            emitted = std::min(emitted, slot_sizes_[slot]);
        }
        return emitted == unconstrained_rows ? ambient : emitted;
    }

    core::error_t
    execution_graph_t::copy_outputs(uint64_t rows, vector::data_chunk_t* target, uint64_t target_row) const {
        if (target->column_count() < output_slots_.size()) {
            return core::error_t(
                core::error_code_t::schema_error,
                std::pmr::string{"execution graph: finalize target has fewer columns than the graph outputs",
                                 resource()});
        }
        if (target_row + rows > target->capacity()) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{"execution graph: finalize target cannot hold the outputs at that row", resource()});
        }
        for (size_t index = 0; index < output_slots_.size(); index++) {
            vector::vector_ops::copy(data_storage_[output_slots_[index]], target->data[index], rows, 0, target_row);
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<vector::data_chunk_t> execution_graph_t::finalize(const graph_execution_context& context,
                                                                             uint64_t count) {
        const uint64_t ambient = reduction_nodes_.empty() ? count : uint64_t{1};
        if (auto error = emit_groups(context, 0, ambient); error.contains_error()) {
            return error;
        }
        const uint64_t rows = emitted_rows(ambient);
        std::pmr::vector<types::complex_logical_type> output_types(resource_);
        output_types.reserve(output_slots_.size());
        for (auto slot : output_slots_) {
            output_types.push_back(slots_[slot].type);
        }
        vector::data_chunk_t result(resource_, output_types, std::vector<size_t>{}, capacity_);
        for (size_t index = 0; index < output_slots_.size(); index++) {
            result.data[index].reference(data_storage_[output_slots_[index]]);
        }
        result.set_cardinality(rows);
        return result;
    }

} // namespace components::execution_graph