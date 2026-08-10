#include "execution_graph_builder.hpp"

namespace components::expressions {

    namespace {

        using execution_graph::execution_graph_t;
        using execution_graph::node_id_t;
        using execution_graph::slot_id_t;
        using types::complex_logical_type;
        using types::logical_type;

        bool is_stamped(const complex_logical_type& type) noexcept { return type.type() != logical_type::INVALID; }

        class builder_t {
        public:
            builder_t(execution_graph_t* graph,
                      const types::parameter_map_t& parameters,
                      const std::pmr::vector<complex_logical_type>& input_types,
                      size_t right_offset)
                : graph_(graph)
                , parameters_(parameters)
                , input_types_(input_types)
                , right_offset_(right_offset) {}

            core::result_wrapper_t<slot_id_t> slot_of(const scalar_expression_t* expression);
            core::result_wrapper_t<slot_id_t> slot_of_expression(const expression_i* expression);

        private:
            core::result_wrapper_t<slot_id_t> slot_of(const param_storage& param);
            core::result_wrapper_t<slot_id_t> column_slot(const key_t& key);
            core::result_wrapper_t<slot_id_t> operator_slot(const scalar_expression_t* expression);
            core::result_wrapper_t<slot_id_t> cast_slot(const cast_expression_t* expression);
            core::result_wrapper_t<slot_id_t> function_slot(const function_expression_t* expression);
            core::result_wrapper_t<slot_id_t> aggregate_slot(const aggregate_expression_t* expression);
            core::result_wrapper_t<slot_id_t> blend_slot(const scalar_expression_t* expression);
            core::result_wrapper_t<slot_id_t> compare_slot(const compare_expression_t* expression);
            core::result_wrapper_t<slot_id_t> quantified_slot(const compare_expression_t* expression);
            std::pmr::memory_resource* resource() const noexcept { return graph_->resource(); }

            execution_graph_t* graph_;
            const types::parameter_map_t& parameters_;
            const std::pmr::vector<complex_logical_type>& input_types_;
            size_t right_offset_;
        };

        core::result_wrapper_t<slot_id_t> builder_t::slot_of_expression(const expression_i* expression) {
            switch (expression->group()) {
                case expression_group::scalar:
                    return slot_of(static_cast<const scalar_expression_t*>(expression));
                case expression_group::function:
                    return function_slot(static_cast<const function_expression_t*>(expression));
                case expression_group::cast:
                    return cast_slot(static_cast<const cast_expression_t*>(expression));
                case expression_group::compare:
                    return compare_slot(static_cast<const compare_expression_t*>(expression));
                case expression_group::aggregate:
                    return aggregate_slot(static_cast<const aggregate_expression_t*>(expression));
                default:
                    return core::error_t(
                        core::error_code_t::unimplemented_yet,
                        std::pmr::string{"execution graph builder: unsupported projection expression", resource()});
            }
        }

        std::optional<types::complex_logical_type>
        nested_type(const types::complex_logical_type& column, const std::pmr::vector<size_t>& path, size_t from) {
            const types::complex_logical_type* current = &column;
            for (size_t position = from; position < path.size(); position++) {
                if (!current->is_nested()) {
                    return std::nullopt;
                }
                if (current->type() == types::logical_type::STRUCT) {
                    if (path[position] >= current->child_types().size()) {
                        return std::nullopt;
                    }
                    current = &current->child_types()[path[position]];
                } else {
                    current = &current->child_type();
                }
            }
            return *current;
        }

        core::result_wrapper_t<slot_id_t> builder_t::column_slot(const key_t& key) {
            const auto& path = key.path();
            if (path.empty()) {
                return core::error_t(
                    core::error_code_t::unimplemented_yet,
                    std::pmr::string{"execution graph builder: column is not a resolved ordinal", resource()});
            }
            // A right-side key is resolved against the right schema, so its ordinal is relative to
            // that side; right_offset_ places it in the merged chunk the caller feeds.
            const size_t column = key.side() == side_t::right ? right_offset_ + path.front() : path.front();
            if (column >= input_types_.size()) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"execution graph builder: column ordinal " +
                                                          std::to_string(column) + " is outside the input of " +
                                                          std::to_string(input_types_.size()) + " columns",
                                                      resource()});
            }
            if (key.has_cast_type()) {
                return core::error_t(
                    core::error_code_t::unimplemented_yet,
                    std::pmr::string{"execution graph builder: cast spelled on a column reference", resource()});
            }
            slot_id_t slot = execution_graph::invalid_slot;
            for (const auto& binding : graph_->input_bindings()) {
                if (binding.column == column) {
                    slot = binding.slot;
                    break;
                }
            }
            if (slot == execution_graph::invalid_slot) {
                slot = graph_->declare_slot();
                graph_->bind_input(slot, column, input_types_[column]);
            }
            if (path.size() == 1) {
                return slot;
            }

            auto field_type = nested_type(input_types_[column], path, 1);
            if (!field_type.has_value()) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"execution graph builder: nested path does not match the column type",
                                     resource()});
            }
            std::pmr::vector<size_t> steps(path.begin() + 1, path.end(), resource());
            auto node = graph_->add_field(slot, steps);
            auto field_slot = graph_->output_slot(node);
            graph_->set_slot_type(field_slot, field_type.value());
            return field_slot;
        }

        core::result_wrapper_t<slot_id_t> builder_t::slot_of(const param_storage& param) {
            if (std::holds_alternative<key_t>(param)) {
                return column_slot(std::get<key_t>(param));
            }
            if (std::holds_alternative<core::parameter_id_t>(param)) {
                auto id = std::get<core::parameter_id_t>(param);
                auto value = parameters_.find(id);
                if (value == parameters_.end()) {
                    return core::error_t(
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"execution graph builder: parameter is not bound", resource()});
                }
                auto node = graph_->add_parameter(id);
                graph_->set_slot_type(graph_->output_slot(node), value->second.type());
                return graph_->output_slot(node);
            }
            const auto& nested = std::get<expression_ptr>(param);
            if (!nested) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"execution graph builder: sub-expression is empty", resource()});
            }

            return slot_of_expression(nested.get());
        }

        // COALESCE and CASE
        core::result_wrapper_t<slot_id_t> builder_t::blend_slot(const scalar_expression_t* expression) {
            const bool is_coalesce = expression->type() == scalar_type::coalesce;
            if (!is_stamped(expression->result_type())) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"execution graph builder: blend was not stamped by validation", resource()});
            }
            execution_graph::slot_list_t inputs(resource());
            inputs.reserve(expression->params().size());
            for (const auto& param : expression->params()) {
                auto slot = slot_of(param);
                if (slot.has_error()) {
                    return slot;
                }
                inputs.push_back(slot.value());
            }
            auto node = graph_->add_blend(is_coalesce ? execution_graph::blend_node_t::blend_kind::coalesce
                                                      : execution_graph::blend_node_t::blend_kind::case_when,
                                          inputs);
            graph_->set_slot_type(graph_->output_slot(node), expression->result_type());
            return graph_->output_slot(node);
        }

        // ANY / ALL
        core::result_wrapper_t<slot_id_t> builder_t::quantified_slot(const compare_expression_t* expression) {
            const bool is_any = expression->type() == compare_type::any;
            const auto inner = to_operator_code(expression->inner_op());
            if (inner == operators::operator_code::invalid) {
                return core::error_t(core::error_code_t::unimplemented_yet,
                                     std::pmr::string{"execution graph builder: " + to_string(expression->type()) +
                                                          " over " + to_string(expression->inner_op()),
                                                      resource()});
            }
            if (!std::holds_alternative<core::parameter_id_t>(expression->right())) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"execution graph builder: " + to_string(expression->type()) +
                                                          " expects its set as a bound parameter",
                                                      resource()});
            }
            const auto id = std::get<core::parameter_id_t>(expression->right());
            const auto bound = parameters_.find(id);
            if (bound == parameters_.end()) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"execution graph builder: set parameter is not bound", resource()});
            }
            auto left = slot_of(expression->left());
            if (left.has_error()) {
                return left;
            }
            auto set_node = graph_->add_parameter(id);
            const auto set_slot = graph_->output_slot(set_node);
            graph_->set_slot_type(set_slot, bound->second.type());

            if (bound->second.type().type() == logical_type::NA) {
                auto node = graph_->add_operator(is_any ? operators::operator_code::is_not_null
                                                        : operators::operator_code::is_null,
                                                 set_slot);
                graph_->set_slot_type(graph_->output_slot(node), complex_logical_type{logical_type::BOOLEAN});
                return graph_->output_slot(node);
            }

            const auto& elements = bound->second.children();
            if (elements.empty()) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"execution graph builder: set parameter holds no elements and "
                                                      "is not the empty-set sentinel",
                                                      resource()});
            }
            const auto element_type = bound->second.type().child_type();
            const auto left_type = graph_->slot_type(left.value());
            if (left_type.type() == logical_type::NA) {
                graph_->set_slot_type(left.value(), element_type);
            } else if (left_type != element_type) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"execution graph builder: " + to_string(expression->type()) +
                                                          " operands were not unified",
                                                      resource()});
            }

            const auto fold = is_any ? operators::operator_code::logical_or : operators::operator_code::logical_and;
            auto folded = execution_graph::invalid_slot;
            for (size_t index = 0; index < elements.size(); index++) {
                std::pmr::vector<size_t> step({index}, resource());
                auto element_node = graph_->add_field(set_slot, step);
                const auto element_slot = graph_->output_slot(element_node);
                graph_->set_slot_type(element_slot, element_type);
                auto compared = graph_->add_operator(inner, left.value(), element_slot);
                auto compared_slot = graph_->output_slot(compared);
                graph_->set_slot_type(compared_slot, complex_logical_type{logical_type::BOOLEAN});
                if (folded == execution_graph::invalid_slot) {
                    folded = compared_slot;
                    continue;
                }
                auto combined = graph_->add_operator(fold, folded, compared_slot);
                folded = graph_->output_slot(combined);
                graph_->set_slot_type(folded, complex_logical_type{logical_type::BOOLEAN});
            }
            return folded;
        }

        core::result_wrapper_t<slot_id_t> builder_t::compare_slot(const compare_expression_t* expression) {
            if (expression->type() == compare_type::any || expression->type() == compare_type::all) {
                return quantified_slot(expression);
            }
            const auto code = to_operator_code(expression->type());
            if (code == operators::operator_code::invalid) {
                return core::error_t(
                    core::error_code_t::unimplemented_yet,
                    std::pmr::string{"execution graph builder: " + to_string(expression->type()), resource()});
            }
            if (expression->is_union()) {
                if (expression->children().empty()) {
                    return core::error_t(core::error_code_t::invalid_parameter,
                                         std::pmr::string{"execution graph builder: " + to_string(expression->type()) +
                                                              " has no operands",
                                                          resource()});
                }
                auto folded = slot_of_expression(expression->children().front().get());
                if (folded.has_error()) {
                    return folded;
                }
                if (operators::arity_of(code) == operators::operator_arity::unary) {
                    auto node = graph_->add_operator(code, folded.value());
                    graph_->set_slot_type(graph_->output_slot(node), complex_logical_type{logical_type::BOOLEAN});
                    return graph_->output_slot(node);
                }
                for (size_t index = 1; index < expression->children().size(); index++) {
                    auto next = slot_of_expression(expression->children()[index].get());
                    if (next.has_error()) {
                        return next;
                    }
                    auto node = graph_->add_operator(code, folded.value(), next.value());
                    graph_->set_slot_type(graph_->output_slot(node), complex_logical_type{logical_type::BOOLEAN});
                    folded = graph_->output_slot(node);
                }
                return folded;
            }
            auto left = slot_of(expression->left());
            if (left.has_error()) {
                return left;
            }
            node_id_t node{0};
            if (operators::arity_of(code) == operators::operator_arity::binary) {
                auto right = slot_of(expression->right());
                if (right.has_error()) {
                    return right;
                }
                const auto left_type = graph_->slot_type(left.value());
                const auto right_type = graph_->slot_type(right.value());
                const bool left_is_na = left_type.type() == logical_type::NA;
                const bool right_is_na = right_type.type() == logical_type::NA;
                if (left_is_na != right_is_na) {
                    graph_->set_slot_type(left_is_na ? left.value() : right.value(),
                                          left_is_na ? right_type : left_type);
                } else if (left_type != right_type) {
                    return core::error_t(
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"execution graph builder: comparison operands were not unified", resource()});
                }
                node = graph_->add_operator(code, left.value(), right.value());
            } else {
                node = graph_->add_operator(code, left.value());
            }
            graph_->set_slot_type(graph_->output_slot(node), complex_logical_type{logical_type::BOOLEAN});
            return graph_->output_slot(node);
        }

        core::result_wrapper_t<slot_id_t> builder_t::aggregate_slot(const aggregate_expression_t* expression) {
            if (expression->function_uid() == compute::invalid_function_uid) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"execution graph builder: aggregate was not resolved by validation", resource()});
            }
            if (!is_stamped(expression->result_type())) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"execution graph builder: aggregate was not stamped by validation", resource()});
            }
            const auto* function =
                compute::function_registry_t::get_default()->get_function(expression->function_uid());
            if (function == nullptr) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"execution graph builder: resolved aggregate is not registered", resource()});
            }
            execution_graph::slot_list_t inputs(resource());
            inputs.reserve(expression->params().size());
            for (const auto& argument : expression->params()) {
                auto slot = slot_of(argument);
                if (slot.has_error()) {
                    return slot;
                }
                inputs.push_back(slot.value());
            }
            auto node = graph_->add_aggregate(expression->function_name(), inputs, expression->is_distinct());
            graph_->set_aggregate_function(node, function);
            graph_->set_slot_type(graph_->output_slot(node), expression->result_type());
            return graph_->output_slot(node);
        }

        core::result_wrapper_t<slot_id_t> builder_t::function_slot(const function_expression_t* expression) {
            if (expression->function_uid() == compute::invalid_function_uid) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"execution graph builder: function was not resolved by validation", resource()});
            }
            if (!is_stamped(expression->result_type())) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"execution graph builder: function was not stamped by validation", resource()});
            }
            const auto* function =
                compute::function_registry_t::get_default()->get_function(expression->function_uid());
            if (function == nullptr) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"execution graph builder: resolved function is not registered", resource()});
            }
            execution_graph::slot_list_t inputs(resource());
            inputs.reserve(expression->args().size());
            for (const auto& argument : expression->args()) {
                auto slot = slot_of(argument);
                if (slot.has_error()) {
                    return slot;
                }
                inputs.push_back(slot.value());
            }
            auto node = graph_->add_function(expression->name(), inputs, 1);
            graph_->set_function(node, function);
            graph_->set_slot_type(graph_->output_slot(node), expression->result_type());
            return graph_->output_slot(node);
        }

        core::result_wrapper_t<slot_id_t> builder_t::cast_slot(const cast_expression_t* expression) {
            auto source = slot_of(expression->child());
            if (source.has_error()) {
                return source;
            }
            auto node = graph_->add_cast(source.value(), expression->kind());
            graph_->set_cast(node, expression->cast());
            graph_->set_slot_type(graph_->output_slot(node), expression->result_type());
            return graph_->output_slot(node);
        }

        core::result_wrapper_t<slot_id_t> builder_t::operator_slot(const scalar_expression_t* expression) {
            const auto code = to_operator_code(expression->type());
            const size_t arity = operators::arity_of(code) == operators::operator_arity::binary ? 2 : 1;
            if (expression->params().size() != arity) {
                return core::error_t(
                    core::error_code_t::invalid_parameter,
                    std::pmr::string{"execution graph builder: operand count does not match the operator", resource()});
            }
            if (!is_stamped(expression->result_type())) {
                return core::error_t(core::error_code_t::invalid_parameter,
                                     std::pmr::string{"execution graph builder: operator was not stamped by "
                                                      "validation: " +
                                                          expression->to_string(),
                                                      resource()});
            }

            auto left = slot_of(expression->params()[0]);
            if (left.has_error()) {
                return left;
            }
            node_id_t node{0};
            if (arity == 2) {
                auto right = slot_of(expression->params()[1]);
                if (right.has_error()) {
                    return right;
                }
                node = graph_->add_operator(code, left.value(), right.value());
            } else {
                node = graph_->add_operator(code, left.value());
            }
            graph_->set_slot_type(graph_->output_slot(node), expression->result_type());
            return graph_->output_slot(node);
        }

        core::result_wrapper_t<slot_id_t> builder_t::slot_of(const scalar_expression_t* expression) {
            switch (expression->type()) {
                case scalar_type::get_field: {
                    const auto& field = expression->params().empty() ? expression->key()
                                                                     : std::get<key_t>(expression->params().front());
                    auto slot = column_slot(field);
                    if (slot.has_error()) {
                        return slot;
                    }
                    if (is_stamped(expression->result_type()) &&
                        expression->result_type() != graph_->slot_type(slot.value())) {
                        return core::error_t(
                            core::error_code_t::unimplemented_yet,
                            std::pmr::string{"execution graph builder: reference carries an unresolved cast",
                                             resource()});
                    }
                    return slot;
                }
                case scalar_type::constant: {
                    if (expression->params().empty()) {
                        return core::error_t(
                            core::error_code_t::invalid_parameter,
                            std::pmr::string{"execution graph builder: constant carries no parameter", resource()});
                    }
                    auto slot = slot_of(expression->params().front());
                    if (slot.has_error()) {
                        return slot;
                    }
                    if (is_stamped(expression->result_type()) &&
                        graph_->slot_type(slot.value()).type() == logical_type::NA) {
                        graph_->set_slot_type(slot.value(), expression->result_type());
                    }
                    return slot;
                }
                case scalar_type::coalesce:
                case scalar_type::case_expr: {
                    return blend_slot(expression);
                }
                default: {
                    if (to_operator_code(expression->type()) == operators::operator_code::invalid) {
                        return core::error_t(
                            core::error_code_t::unimplemented_yet,
                            std::pmr::string{"execution graph builder: " + to_string(expression->type()), resource()});
                    }
                    return operator_slot(expression);
                }
            }
        }

    } // namespace

    core::result_wrapper_t<execution_graph::slot_id_t>
    build_expression(execution_graph::execution_graph_t* graph,
                     const types::parameter_map_t& parameters,
                     const expression_i* expression,
                     const std::pmr::vector<types::complex_logical_type>& input_types,
                     size_t right_offset) {
        if (graph == nullptr || expression == nullptr) {
            return core::error_t(
                core::error_code_t::invalid_parameter,
                std::pmr::string{"execution graph builder: nothing to build", std::pmr::get_default_resource()});
        }
        builder_t builder(graph, parameters, input_types, right_offset);
        return builder.slot_of_expression(expression);
    }

    core::result_wrapper_t<std::unique_ptr<execution_graph::execution_graph_t>>
    build_graph(std::pmr::memory_resource* resource,
                const types::parameter_map_t& parameters,
                core::span<const expression_i* const> expressions,
                const std::pmr::vector<types::complex_logical_type>& input_types,
                size_t right_offset) {
        auto graph = std::make_unique<execution_graph::execution_graph_t>(resource);
        execution_graph::slot_list_t outputs(resource);
        outputs.reserve(expressions.size());
        for (const auto* expression : expressions) {
            auto slot = build_expression(graph.get(), parameters, expression, input_types, right_offset);
            if (slot.has_error()) {
                return slot.error();
            }
            outputs.push_back(slot.value());
        }
        graph->set_output(outputs);
        if (auto error = graph->prepare(); error.contains_error()) {
            return error;
        }
        return graph;
    }

    // Outputs the N computed SET values, then one trailing is_modified column. Writing the
    // values back is the caller's job — the graph only reads.
    core::result_wrapper_t<std::unique_ptr<execution_graph::execution_graph_t>>
    build_update_graph(std::pmr::memory_resource* resource,
                       const types::parameter_map_t& parameters,
                       core::span<const expression_i* const> values,
                       const std::pmr::vector<types::complex_logical_type>& input_types,
                       size_t right_offset) {
        const types::complex_logical_type boolean{types::logical_type::BOOLEAN};
        auto graph = std::make_unique<execution_graph::execution_graph_t>(resource);
        execution_graph::slot_list_t outputs(resource);
        outputs.reserve(values.size() + 1);
        auto modified = execution_graph::invalid_slot;

        for (const auto* value : values) {
            auto slot = build_expression(graph.get(), parameters, value, input_types, right_offset);
            if (slot.has_error()) {
                return slot.error();
            }
            outputs.push_back(slot.value());

            // left-hand side is pure assignment
            // right-hand side is evaluated expr
            // is_modified is calculated by comparing those
            const auto& path = value->key().path();
            const size_t column = path.front();
            auto current = execution_graph::invalid_slot;
            for (const auto& binding : graph->input_bindings()) {
                if (binding.column == column) {
                    current = binding.slot;
                    break;
                }
            }
            if (current == execution_graph::invalid_slot) {
                current = graph->declare_slot();
                graph->bind_input(current, column, input_types[column]);
            }
            if (path.size() > 1) {
                auto field_type = nested_type(input_types[column], path, 1);
                if (!field_type.has_value()) {
                    return core::error_t(
                        core::error_code_t::invalid_parameter,
                        std::pmr::string{"execution graph builder: update target path does not match the column type",
                                         resource});
                }
                std::pmr::vector<size_t> steps(path.begin() + 1, path.end(), resource);
                current = graph->output_slot(graph->add_field(current, steps));
                graph->set_slot_type(current, field_type.value());
            }

            auto same =
                graph->output_slot(graph->add_operator(operators::operator_code::strict_equal, current, slot.value()));
            graph->set_slot_type(same, boolean);
            auto changed = graph->output_slot(graph->add_operator(operators::operator_code::logical_not, same));
            graph->set_slot_type(changed, boolean);

            if (modified == execution_graph::invalid_slot) {
                modified = changed;
            } else {
                modified =
                    graph->output_slot(graph->add_operator(operators::operator_code::logical_or, modified, changed));
                graph->set_slot_type(modified, boolean);
            }
        }

        outputs.push_back(modified);
        graph->set_output(outputs);
        if (auto error = graph->prepare(); error.contains_error()) {
            return error;
        }
        return graph;
    }

    core::result_wrapper_t<std::unique_ptr<execution_graph::execution_graph_t>>
    build_condition_graph(std::pmr::memory_resource* resource,
                          const types::parameter_map_t& parameters,
                          const expression_i* expression,
                          const std::pmr::vector<types::complex_logical_type>& input_types,
                          size_t right_offset) {
        return build_graph(resource, parameters, {&expression, 1}, input_types, right_offset);
    }

    core::result_wrapper_t<vector::data_chunk_t> run_graph(execution_graph::execution_graph_t* graph,
                                                           const types::parameter_map_t& parameters,
                                                           const vector::data_chunk_t& input,
                                                           const components::graph_execution_context& context) {
        // TODO: parameters could be set only once
        graph->set_parameters(&parameters);
        if (auto error = graph->process(input, context); error.contains_error()) {
            return error;
        }
        return graph->finalize(context, input.size());
    }

} // namespace components::expressions