#include "expression_executor.hpp"

#include <components/compute/function.hpp>
#include <components/logical_plan/param_storage.hpp>
#include <components/types/operations_helper.hpp>
#include <core/operations_helper.hpp>
#include <components/types/tri_bool.hpp>
#include <components/vector/vector_operations.hpp>

#include <components/expressions/like_to_regex.hpp>

#include <variant>

namespace components::expressions {

    namespace {

        core::error_t exec_error(std::pmr::memory_resource* resource, core::error_code_t code, const char* what) {
            return core::error_t(code, std::pmr::string{what, resource});
        }

        // The physical types simple_physical_type_switch has an arm for. It std::abort()s on
        // anything else, so every dispatch in this file is guarded by this first.
        bool is_dispatchable(types::physical_type type) noexcept {
            switch (type) {
                case types::physical_type::BOOL:
                case types::physical_type::UINT8:
                case types::physical_type::INT8:
                case types::physical_type::UINT16:
                case types::physical_type::INT16:
                case types::physical_type::UINT32:
                case types::physical_type::INT32:
                case types::physical_type::UINT64:
                case types::physical_type::INT64:
                case types::physical_type::UINT128:
                case types::physical_type::INT128:
                case types::physical_type::FLOAT:
                case types::physical_type::DOUBLE:
                case types::physical_type::STRING:
                    return true;
                default:
                    return false;
            }
        }

        // A projected-out column carries its type but no buffer: data_chunk_t's projected
        // constructor allocates real storage only for the columns the pruner selected and leaves
        // the rest as placeholders that no operator is meant to read.
        bool is_placeholder(const vector::vector_t& column) noexcept {
            return column.get_vector_type() == vector::vector_type::FLAT && column.data() == nullptr &&
                   !column.auxiliary();
        }

        // Comparison functors. Hand-written rather than <functional>'s, so the file names no
        // std:: callable at all.
        //
        // Equality goes through core::is_equals, not a raw ==: that is the engine's own equality,
        // including its Postgres float rule that NaN equals NaN. The hash-based dedup paths (GROUP
        // BY, hash join, UNIQUE, FK semi-join) bucket by that rule, so a predicate that disagreed
        // with it would answer differently depending on whether an index was used.
        struct op_eq {
            template<typename T>
            bool operator()(const T& a, const T& b) const {
                return core::is_equals(a, b);
            }
        };
        struct op_ne {
            template<typename T>
            bool operator()(const T& a, const T& b) const {
                return !core::is_equals(a, b);
            }
        };
        struct op_lt {
            template<typename T>
            bool operator()(const T& a, const T& b) const {
                return a < b;
            }
        };
        struct op_gt {
            template<typename T>
            bool operator()(const T& a, const T& b) const {
                return a > b;
            }
        };
        struct op_lte {
            template<typename T>
            bool operator()(const T& a, const T& b) const {
                return a <= b;
            }
        };
        struct op_gte {
            template<typename T>
            bool operator()(const T& a, const T& b) const {
                return a >= b;
            }
        };

        // Both operands are FLAT by the executor's invariant, so the loop indexes storage directly.
        // A NULL operand yields UNKNOWN, recorded in the output's validity -- NOT folded into FALSE,
        // which is what lets NOT stay correct over a null.
        template<typename OP>
        struct compare_wrapper {
            template<typename...>
            struct callback {
                template<typename T>
                void operator()(const vector::vector_t& left,
                                const vector::vector_t& right,
                                uint64_t count,
                                vector::vector_t& out) const {
                    const T* lhs = left.data<T>();
                    const T* rhs = right.data<T>();
                    bool* result = out.data<bool>();
                    const auto& left_validity = left.validity();
                    const auto& right_validity = right.validity();
                    OP op{};
                    for (uint64_t row = 0; row < count; ++row) {
                        const bool defined = left_validity.row_is_valid(row) && right_validity.row_is_valid(row);
                        out.validity().set(row, defined);
                        result[row] = defined && op(lhs[row], rhs[row]);
                    }
                }
            };
        };

        // Row-wise copy of one already-typed value from source to target. Typed, not boxed: no
        // logical_value_t is built per cell (rule 1).
        template<typename...>
        struct copy_row_callback {
            template<typename T>
            void operator()(const vector::vector_t& source,
                            uint64_t source_row,
                            vector::vector_t& target,
                            uint64_t target_row) const {
                if constexpr (std::is_same_v<T, std::string_view>) {
                    // Re-inserted into the target's own string buffer: a raw view copy would point
                    // into a slot the next chunk overwrites.
                    target.set_value(target_row, source.get_value<std::string_view>(source_row));
                } else {
                    target.data<T>()[target_row] = source.data<T>()[source_row];
                }
            }
        };

        // The six operators applied to two values KNOWN to share a type.
        //
        // A deliberate duplicate of table::compare_same_type_matches (column_state.hpp), and the
        // duplication is forced: components/table already includes components/expressions, so
        // calling the canonical one from here would close an expressions <-> table cycle, and moving
        // it somewhere both can see means editing components/table. The two must stay in step --
        // this file is the in-memory answer, that one is also the pushed disk filter's.
        bool compare_same_type(const types::logical_value_t& left,
                               const types::logical_value_t& right,
                               compare_type op) {
            switch (op) {
                case compare_type::eq:
                    return left == right;
                case compare_type::ne:
                    return left != right;
                case compare_type::lt:
                    return left < right;
                case compare_type::lte:
                    return left <= right;
                case compare_type::gt:
                    return left > right;
                case compare_type::gte:
                    return left >= right;
                default:
                    return false;
            }
        }

        types::tri_bool_t read_tri(const vector::vector_t& source, uint64_t row) noexcept {
            // Not the two-argument tri_of: an invalid row's slot may hold anything, so the read has
            // to stay behind the short-circuiting guard.
            return source.validity().row_is_valid(row) ? types::tri_of(source.data<bool>()[row])
                                                       : types::tri_bool_t::unknown;
        }

        void write_tri(vector::vector_t& target, uint64_t row, types::tri_bool_t value) {
            const bool defined = value != types::tri_bool_t::unknown;
            target.validity().set(row, defined);
            target.data<bool>()[row] = value == types::tri_bool_t::yes;
        }

        // Writes whichever shape compute::function::execute answered into the node's preallocated
        // slot.
        //
        // The registry's return type is a sum of two shapes -- a boxed value per row (row_function)
        // and a chunk whose first column is the result (vector_function). This visitor is the ONLY
        // place either shape is named: std::visit picks the arm, so nothing here asks which
        // alternative is active (no holds_alternative) and nothing names one to extract it (no
        // std::get). Adding a third shape to the registry breaks THIS struct at compile time rather
        // than silently taking a wrong branch at run time -- which is the property a chain of
        // holds_alternative tests does not have.
        struct function_result_writer {
            std::pmr::memory_resource* resource;
            vector::vector_t& target;
            uint64_t count;
            types::physical_type promised;

            core::error_t operator()(const std::pmr::vector<types::logical_value_t>& values) const {
                if (values.size() < count) {
                    return exec_error(resource,
                                      core::error_code_t::incorrect_function_return_type,
                                      "expression executor: function returned fewer results than rows");
                }
                for (uint64_t row = 0; row < count; ++row) {
                    if (values[row].is_null()) {
                        target.validity().set(row, false);
                        continue;
                    }
                    if (values[row].type().to_physical_type() != promised) {
                        return exec_error(resource,
                                          core::error_code_t::incorrect_function_return_type,
                                          "expression executor: function produced a type the node did not promise");
                    }
                    target.validity().set(row, true);
                    if (auto error = target.set_value(row, values[row]); error.contains_error()) {
                        return error;
                    }
                }
                return core::error_t::no_error();
            }

            core::error_t operator()(const vector::data_chunk_t& produced) const {
                // The LENGTH check is on the column, not on produced.size(). vector_executor::execute
                // builds its answer as `data_chunk_t out(resource, {})` and only pushes the result
                // vector into out.data (kernel_executor.cpp), so the chunk's CARDINALITY is
                // never set and reads as 0 however many rows the kernel actually wrote. The column
                // itself is sized by prepare_vector_output(inputs.size()), i.e. to exactly the count
                // this executor handed in.
                if (produced.data.empty()) {
                    return exec_error(resource,
                                      core::error_code_t::incorrect_function_return_type,
                                      "expression executor: function returned no result column");
                }
                const auto& column = produced.data.front();
                if (column.type().to_physical_type() != promised) {
                    return exec_error(resource,
                                      core::error_code_t::incorrect_function_return_type,
                                      "expression executor: function produced a type the node did not promise");
                }
                return vector::vector_ops::copy(column, target, count, 0, 0);
            }
        };

    } // namespace

    expression_executor_t::expression_executor_t(std::pmr::memory_resource* resource,
                                                 bound_expression_ptr root,
                                                 uint64_t capacity)
        : resource_(resource)
        , root_(std::move(root))
        , capacity_(capacity)
        , nodes_(resource)
        , child_begin_(resource)
        , children_(resource)
        , slot_of_(resource)
        , folded_(resource)
        , slots_(resource)
        , results_(resource)
        , function_args_(resource)
        , arg_chunk_of_(resource)
        , regex_caches_(resource)
        , cache_of_(resource) {}

    core::result_wrapper_t<expression_executor_t>
    expression_executor_t::create(std::pmr::memory_resource* resource, bound_expression_ptr root, uint64_t capacity) {
        if (!root) {
            return exec_error(resource, core::error_code_t::invalid_parameter, "expression executor: no root");
        }
        if (capacity == 0) {
            return exec_error(resource, core::error_code_t::invalid_parameter, "expression executor: zero capacity");
        }
        expression_executor_t executor(resource, std::move(root), capacity);
        executor.flatten(executor.root_);
        if (auto error = executor.allocate_slots(); error.contains_error()) {
            return error;
        }
        context_t empty;
        if (auto error = executor.fold(empty); error.contains_error()) {
            return error;
        }
        return core::result_wrapper_t<expression_executor_t>{std::move(executor)};
    }

    void expression_executor_t::flatten(const bound_expression_ptr& node) {
        std::pmr::vector<uint32_t> child_indices{resource_};
        child_indices.reserve(node->children().size());
        for (const auto& child : node->children()) {
            flatten(child);
            child_indices.push_back(static_cast<uint32_t>(nodes_.size() - 1));
        }
        child_begin_.push_back(static_cast<uint32_t>(children_.size()));
        for (auto index : child_indices) {
            children_.push_back(index);
        }
        nodes_.push_back(node.get());
    }

    core::error_t expression_executor_t::allocate_slots() {
        const size_t count = nodes_.size();
        slot_of_.assign(count, -1);
        arg_chunk_of_.assign(count, -1);
        cache_of_.assign(count, -1);
        folded_.assign(count, 0);
        results_.assign(count, nullptr);
        // Reserved exactly, so no element ever moves: the pointers handed out by execute() stay
        // valid for the executor's whole life.
        slots_.reserve(count);
        size_t function_nodes = 0;
        size_t any_all_nodes = 0;
        for (const auto* node : nodes_) {
            if (node->kind() == bound_kind::function) {
                ++function_nodes;
            }
            if (node->kind() == bound_kind::any_all) {
                ++any_all_nodes;
            }
        }
        function_args_.reserve(function_nodes);
        regex_caches_.reserve(any_all_nodes);
        for (size_t i = 0; i < count; ++i) {
            const auto* node = nodes_[i];
            if (node->return_type().type() == types::logical_type::NA) {
                return exec_error(resource_,
                                  core::error_code_t::schema_error,
                                  "expression executor: node has no result type");
            }
            slot_of_[i] = static_cast<int32_t>(slots_.size());
            slots_.emplace_back(resource_, node->return_type(), capacity_);
            if (node->kind() == bound_kind::any_all) {
                cache_of_[i] = static_cast<int32_t>(regex_caches_.size());
                regex_caches_.emplace_back();
            }
            if (node->kind() != bound_kind::function) {
                continue;
            }
            if (node->children().empty()) {
                // function::execute derives the kernel from the chunk's column types, so a
                // zero-column chunk has nothing to dispatch on.
                return exec_error(resource_,
                                  core::error_code_t::invalid_parameter,
                                  "expression executor: a bound function needs at least one argument");
            }
            // The argument chunk is typed from what the arguments PROMISED, once. Nothing
            // re-derives an argument type per batch, which is what makes the kernel choice stable
            // across chunks instead of a property of the first batch's data.
            std::pmr::vector<types::complex_logical_type> argument_types{resource_};
            argument_types.reserve(node->children().size());
            for (const auto& argument : node->children()) {
                argument_types.push_back(argument->return_type());
            }
            arg_chunk_of_[i] = static_cast<int32_t>(function_args_.size());
            function_args_.emplace_back(resource_, argument_types, capacity_);
        }
        return core::error_t::no_error();
    }

    core::error_t expression_executor_t::fold(const context_t& context) {
        // A foldable subtree reads neither a row nor a parameter, so an empty chunk is all it needs.
        std::pmr::vector<types::complex_logical_type> no_types{resource_};
        vector::data_chunk_t nothing(resource_, no_types, capacity_);
        for (size_t i = 0; i < nodes_.size(); ++i) {
            if (!nodes_[i]->traits().foldable) {
                continue;
            }
            if (auto error = eval(i, nothing, capacity_, context); error.contains_error()) {
                return error;
            }
            folded_[i] = 1;
        }
        return core::error_t::no_error();
    }

    size_t expression_executor_t::folded_node_count() const noexcept {
        size_t total = 0;
        for (auto folded : folded_) {
            total += folded;
        }
        return total;
    }

    core::result_wrapper_t<const vector::vector_t*>
    expression_executor_t::execute(const vector::data_chunk_t& input, uint64_t count, const context_t& context) {
        if (count > capacity_) {
            return exec_error(resource_,
                              core::error_code_t::invalid_parameter,
                              "expression executor: chunk is larger than the capacity it was created for");
        }
        for (size_t i = 0; i < nodes_.size(); ++i) {
            if (folded_[i]) {
                continue;
            }
            if (auto error = eval(i, input, count, context); error.contains_error()) {
                return error;
            }
        }
        return results_.back();
    }

    core::result_wrapper_t<uint64_t> expression_executor_t::select(const vector::data_chunk_t& input,
                                                                   uint64_t count,
                                                                   const context_t& context,
                                                                   vector::indexing_vector_t& selection) {
        if (root_->return_type().type() != types::logical_type::BOOLEAN) {
            return exec_error(resource_,
                              core::error_code_t::invalid_parameter,
                              "expression executor: select() needs a BOOLEAN root");
        }
        auto result = execute(input, count, context);
        if (result.has_error()) {
            return result.error();
        }
        const auto* predicate = result.value();
        uint64_t selected = 0;
        for (uint64_t row = 0; row < count; ++row) {
            // Only a definite TRUE admits a row: UNKNOWN (a NULL operand) does not select.
            if (types::selects(read_tri(*predicate, row))) {
                selection.set_index(selected, row);
                ++selected;
            }
        }
        return selected;
    }

    core::error_t expression_executor_t::eval(size_t index,
                                              const vector::data_chunk_t& input,
                                              uint64_t count,
                                              const context_t& context) {
        switch (nodes_[index]->kind()) {
            case bound_kind::reference:
                return eval_reference(index, input, count, context);
            case bound_kind::parameter:
                return eval_parameter(index, count, context);
            case bound_kind::constant:
                return eval_constant(index, count);
            case bound_kind::cast:
                return eval_cast(index, count);
            case bound_kind::arithmetic:
                return eval_arithmetic(index, count);
            case bound_kind::comparison:
                return eval_comparison(index, count, context);
            case bound_kind::conjunction:
                return eval_conjunction(index, count);
            case bound_kind::case_expr:
                return eval_case(index, count);
            case bound_kind::function:
                return eval_function(index, count);
            case bound_kind::regex:
                return eval_regex(index, count);
            case bound_kind::any_all:
                return eval_any_all(index, count, context);
            case bound_kind::coalesce:
                return eval_coalesce(index, count);
            case bound_kind::negate:
                return eval_negate(index, count);
        }
        return exec_error(resource_, core::error_code_t::other_error, "expression executor: unknown node kind");
    }

    // The chunk a reference reads from. A right-side reference reads the right chunk when the
    // caller supplied one; with a single merged input (a SELECT over a join) both sides index the
    // same chunk, which is what passing none means.
    const vector::data_chunk_t& expression_executor_t::source_chunk(const bound_reference_t& node,
                                                                    const vector::data_chunk_t& input,
                                                                    const context_t& context) noexcept {
        return node.side() == side_t::right && context.right_input != nullptr ? *context.right_input : input;
    }

    core::error_t expression_executor_t::eval_reference(size_t index,
                                                        const vector::data_chunk_t& chunk,
                                                        uint64_t count,
                                                        const context_t& context) {
        const auto* node = static_cast<const bound_reference_t*>(nodes_[index]);
        if (node->is_nested()) {
            return eval_nested_reference(index, chunk, count, context);
        }
        const auto& input = source_chunk(*node, chunk, context);
        if (node->column_index() >= input.column_count()) {
            return exec_error(resource_,
                              core::error_code_t::field_not_exists,
                              "expression executor: reference addresses a column the chunk does not have");
        }
        const auto& column = input.data[node->column_index()];
        if (is_placeholder(column)) {
            return exec_error(resource_,
                              core::error_code_t::schema_error,
                              "expression executor: reference addresses an unprojected placeholder column");
        }
        // The load-bearing invariant of a typed layer: what the node PROMISED is what the chunk
        // carries. Reading an INT64 column through a node that claims FLOAT would misread the
        // bytes, so a mismatch is answered instead.
        if (column.type().to_physical_type() != node->physical_type()) {
            return exec_error(resource_,
                              core::error_code_t::schema_error,
                              "expression executor: chunk column type contradicts the bound reference type");
        }
        // A BROADCAST left operand: one row of the left chunk, repeated for the whole batch. The
        // typed copy switch writes it, so no cell goes through a logical_value_t (rule 1).
        if (context.left_row.has_value() && node->side() != side_t::right) {
            const uint64_t source_row = *context.left_row;
            if (source_row >= input.size()) {
                return exec_error(resource_,
                                  core::error_code_t::invalid_parameter,
                                  "expression executor: broadcast row is past the left chunk");
            }
            auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
            slot.validity().reset(capacity_);
            results_[index] = &slot;
            const bool defined = column.validity().row_is_valid(source_row);
            const auto physical = column.type().to_physical_type();
            if (!is_dispatchable(physical)) {
                return exec_error(resource_,
                                  core::error_code_t::unimplemented_yet,
                                  "expression executor: broadcast of this physical type is not implemented");
            }
            for (uint64_t row = 0; row < count; ++row) {
                slot.validity().set(row, defined);
                if (defined) {
                    types::simple_physical_type_switch<copy_row_callback>(physical, column, source_row, slot, row);
                }
            }
            return core::error_t::no_error();
        }
        if (column.get_vector_type() == vector::vector_type::FLAT) {
            results_[index] = &column; // zero-copy: the common case costs nothing
            return core::error_t::no_error();
        }
        // Non-flat source (constant / dictionary / sequence): materialise it into this node's own
        // slot so everything downstream sees the FLAT invariant.
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        slot.validity().reset(capacity_);
        if (auto error = vector::vector_ops::copy(column, slot, count, 0, 0); error.contains_error()) {
            return error;
        }
        results_[index] = &slot;
        return core::error_t::no_error();
    }

    // A DEEP address (a STRUCT field, an ARRAY/LIST element). data_chunk_t::at() resolves a path to
    // the flat CHILD vector and would then index it by row number, which is right for a struct field
    // and wrong for an array element -- an ARRAY child is strided and a LIST child is addressed
    // through (offset, length). The one accessor that understands all three is
    // data_chunk_t::value(path, row), and it answers a boxed value.
    //
    // So this arm boxes, per row, deliberately: the shape is cold (every key reaching
    // operator_match resolves to a single ordinal), so the round-trip lands on no hot path.
    // Making it columnar needs a strided nested gather in components/vector, which is where that
    // work belongs.
    core::error_t expression_executor_t::eval_nested_reference(size_t index,
                                                                const vector::data_chunk_t& chunk,
                                                                uint64_t count,
                                                                const context_t& context) {
        const auto* node = static_cast<const bound_reference_t*>(nodes_[index]);
        const auto& input = source_chunk(*node, chunk, context);
        if (node->column_index() >= input.column_count()) {
            return exec_error(resource_,
                              core::error_code_t::field_not_exists,
                              "expression executor: reference addresses a column the chunk does not have");
        }
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        slot.validity().reset(capacity_);
        results_[index] = &slot;
        // A BROADCAST left operand reads ONE row for the whole batch, deep address included. Without
        // this the loop below would walk the batch's row indices -- which belong to the RIGHT chunk
        // when a broadcast is in force -- through the LEFT chunk: a join ON a nested field
        // (`(t.u).f1 = (c.u).f1`) would read the wrong rows, or past the end of the probe.
        const bool broadcast = context.left_row.has_value() && node->side() != side_t::right;
        if (broadcast && *context.left_row >= input.size()) {
            return exec_error(resource_,
                              core::error_code_t::invalid_parameter,
                              "expression executor: broadcast row is past the left chunk");
        }
        for (uint64_t row = 0; row < count; ++row) {
            auto value = input.value(node->path(), broadcast ? *context.left_row : row);
            if (value.is_null()) {
                slot.validity().set(row, false);
                continue;
            }
            if (value.type().to_physical_type() != node->physical_type()) {
                return exec_error(resource_,
                                  core::error_code_t::schema_error,
                                  "expression executor: nested field type contradicts the bound reference type");
            }
            slot.validity().set(row, true);
            if (auto error = slot.set_value(row, value); error.contains_error()) {
                return error;
            }
        }
        return core::error_t::no_error();
    }

    core::error_t expression_executor_t::eval_constant(size_t index, uint64_t count) {
        const auto* node = static_cast<const bound_constant_t*>(nodes_[index]);
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        results_[index] = &slot;
        // A constant is foldable by construction, so this runs ONCE, in create(), for the full
        // capacity -- never again, whatever the chunk count turns out to be.
        slot.validity().reset(capacity_);
        if (node->value().is_null()) {
            slot.validity().set_all_invalid(count);
            return core::error_t::no_error();
        }
        for (uint64_t row = 0; row < count; ++row) {
            if (auto error = slot.set_value(row, node->value()); error.contains_error()) {
                return error;
            }
        }
        return core::error_t::no_error();
    }

    core::error_t expression_executor_t::eval_parameter(size_t index, uint64_t count, const context_t& context) {
        const auto* node = static_cast<const bound_parameter_t*>(nodes_[index]);
        if (!context.parameters) {
            return exec_error(resource_,
                              core::error_code_t::invalid_parameter,
                              "expression executor: parameter slot read without a parameter map");
        }
        // Read LIVE, every execution: this is the LATERAL contract.
        const auto* value = logical_plan::get_parameter(context.parameters, node->id());
        if (!value) {
            return exec_error(resource_,
                              core::error_code_t::invalid_parameter,
                              "expression executor: parameter slot is not bound");
        }
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        slot.validity().reset(capacity_);
        results_[index] = &slot;
        if (value->is_null()) {
            slot.validity().set_all_invalid(count);
            return core::error_t::no_error();
        }
        if (value->type().to_physical_type() == node->physical_type()) {
            for (uint64_t row = 0; row < count; ++row) {
                if (auto error = slot.set_value(row, *value); error.contains_error()) {
                    return error;
                }
            }
            return core::error_t::no_error();
        }
        // A slot bound to a differently-typed value is converted once, through the cast kernel's
        // error channel -- never silently reinterpreted.
        auto converted = value->cast_as(node->return_type(), context.session_tz);
        if (converted.has_error()) {
            return converted.error();
        }
        // cast_as answers an NA VALUE, not an error, for a conversion it has no implementation for
        // (logical_value.cpp -- the `assert(false && "cast to value is not implemented")` there is
        // commented out). Writing that NA would silently null the parameter for every row, so the
        // mismatch is named here instead. Rule 6: no silent degradation.
        if (converted.value().type().to_physical_type() != node->physical_type()) {
            return exec_error(resource_,
                              core::error_code_t::conversion_failure,
                              "expression executor: parameter value cannot be converted to the bound slot type");
        }
        for (uint64_t row = 0; row < count; ++row) {
            if (auto error = slot.set_value(row, converted.value()); error.contains_error()) {
                return error;
            }
        }
        return core::error_t::no_error();
    }

    core::error_t expression_executor_t::eval_cast(size_t index, uint64_t count) {
        const auto& source = *results_[children_[child_begin_[index]]];
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        // cast_vector answers a fresh vector; it is moved into the preallocated slot rather than
        // kept, so the slot identity (and every pointer into it) survives.
        slot = vector::vector_ops::cast_vector(resource_, source, nodes_[index]->return_type(), count);
        results_[index] = &slot;
        return core::error_t::no_error();
    }

    core::error_t expression_executor_t::eval_arithmetic(size_t index, uint64_t count) {
        const auto* node = static_cast<const bound_arithmetic_t*>(nodes_[index]);
        const auto begin = child_begin_[index];
        const auto& left = *results_[children_[begin]];
        const auto& right = *results_[children_[begin + 1]];
        // A SCALAR zero divisor is a query ERROR; a COLUMN divisor holding zero NULLs that row. The
        // node carries the difference (divisor_is_scalar).
        //
        // The divisor is compared against a DEFAULT-CONSTRUCTED value of its own type, which is not
        // the same as "== 0": a NULL divisor also compares equal to it, so a scalar `x / NULL`
        // errors here too. Every row of a scalar operand holds the same value, so row 0 decides for
        // the batch.
        if (node->divisor_is_scalar() && count > 0) {
            const types::logical_value_t divisor = right.validity().row_is_valid(0)
                                                       ? right.value(0)
                                                       : types::logical_value_t(resource_, right.type());
            const types::logical_value_t zero(resource_, right.type());
            if (divisor == zero) {
                return exec_error(resource_, core::error_code_t::arithmetics_failure, "division by zero");
            }
        }
        auto computed = vector::compute_binary_arithmetic(resource_, node->op(), left, right, count);
        if (computed.has_error()) {
            return computed.error();
        }
        // The kernel types its output with the same rule the node was bound with. If those two ever
        // disagree the node is promising a width the kernel does not write -- say so, do not ship it.
        if (computed.value().type().type() != node->return_type().type()) {
            return exec_error(resource_,
                              core::error_code_t::schema_error,
                              "expression executor: arithmetic kernel produced a type the node did not promise");
        }
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        slot = std::move(computed.value());
        results_[index] = &slot;
        return core::error_t::no_error();
    }

    core::error_t
    expression_executor_t::eval_comparison(size_t index, uint64_t count, const context_t& context) {
        const auto* node = static_cast<const bound_comparison_t*>(nodes_[index]);
        const auto begin = child_begin_[index];
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        slot.validity().reset(capacity_);
        results_[index] = &slot;

        if (node->op() == compare_type::is_null || node->op() == compare_type::is_not_null) {
            const auto& operand = *results_[children_[begin]];
            const bool answer_when_null = node->op() == compare_type::is_null;
            for (uint64_t row = 0; row < count; ++row) {
                slot.data<bool>()[row] = operand.validity().row_is_valid(row) ? !answer_when_null : answer_when_null;
            }
            return core::error_t::no_error();
        }

        const auto& left = *results_[children_[begin]];
        const auto& right = *results_[children_[begin + 1]];
        if (node->promoting()) {
            return eval_promoting_comparison(index, left, right, count, context);
        }
        const auto physical = left.type().to_physical_type();
        if (physical != right.type().to_physical_type()) {
            return exec_error(resource_,
                              core::error_code_t::comparison_failure,
                              "expression executor: comparison operands have different physical types");
        }
        if (!is_dispatchable(physical)) {
            return exec_error(resource_,
                              core::error_code_t::comparison_failure,
                              "expression executor: comparison is not implemented for this physical type");
        }
        switch (node->op()) {
            case compare_type::eq:
                types::simple_physical_type_switch<compare_wrapper<op_eq>::template callback>(physical,
                                                                                              left,
                                                                                              right,
                                                                                              count,
                                                                                              slot);
                return core::error_t::no_error();
            case compare_type::ne:
                types::simple_physical_type_switch<compare_wrapper<op_ne>::template callback>(physical,
                                                                                              left,
                                                                                              right,
                                                                                              count,
                                                                                              slot);
                return core::error_t::no_error();
            case compare_type::lt:
                types::simple_physical_type_switch<compare_wrapper<op_lt>::template callback>(physical,
                                                                                              left,
                                                                                              right,
                                                                                              count,
                                                                                              slot);
                return core::error_t::no_error();
            case compare_type::gt:
                types::simple_physical_type_switch<compare_wrapper<op_gt>::template callback>(physical,
                                                                                              left,
                                                                                              right,
                                                                                              count,
                                                                                              slot);
                return core::error_t::no_error();
            case compare_type::lte:
                types::simple_physical_type_switch<compare_wrapper<op_lte>::template callback>(physical,
                                                                                               left,
                                                                                               right,
                                                                                               count,
                                                                                               slot);
                return core::error_t::no_error();
            case compare_type::gte:
                types::simple_physical_type_switch<compare_wrapper<op_gte>::template callback>(physical,
                                                                                               left,
                                                                                               right,
                                                                                               count,
                                                                                               slot);
                return core::error_t::no_error();
            default:
                return exec_error(resource_,
                                  core::error_code_t::comparison_failure,
                                  "expression executor: unsupported comparison operator");
        }
    }

    // Operand types that differ and are not both numeric. This is a faithful port of
    // table::compare_values_promoting (column_state.hpp) INCLUDING its retry: cast right to
    // left's type; when that answers NULL (a value the narrower side cannot hold, or a conversion
    // that direction has no implementation for) cast left to right's type instead; and when neither
    // direction lands, answer a definite FALSE -- not UNKNOWN, which would let a NOT above resurrect
    // the row.
    //
    // Boxed per row, and it has to be: cast_vector is a physical-width reinterpretation
    // (vector_operations.cpp, it asserts on strings and never answers NULL), while this
    // conversion is SEMANTIC and needs the session timezone. logical_value_t::cast_as is the only
    // thing that performs it. The round-trip stays confined to a rare shape: the typed dispatch
    // above still takes every same-type and every numeric pair.
    core::error_t expression_executor_t::eval_promoting_comparison(size_t index,
                                                                   const vector::vector_t& left,
                                                                   const vector::vector_t& right,
                                                                   uint64_t count,
                                                                   const context_t& context) {
        const auto* node = static_cast<const bound_comparison_t*>(nodes_[index]);
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        for (uint64_t row = 0; row < count; ++row) {
            // SQL 3VL: a NULL operand is UNKNOWN, which stays distinct from FALSE under NOT. Gated
            // BEFORE the read, because an invalid row's slot may hold anything.
            if (!left.validity().row_is_valid(row) || !right.validity().row_is_valid(row)) {
                write_tri(slot, row, types::tri_bool_t::unknown);
                continue;
            }
            const auto left_value = left.value(row);
            const auto right_value = right.value(row);
            if (left_value.type() == right_value.type()) {
                write_tri(slot, row, types::tri_of(compare_same_type(left_value, right_value, node->op())));
                continue;
            }
            auto right_as_left = right_value.cast_as(left_value.type(), context.session_tz);
            if (right_as_left.has_error()) {
                return right_as_left.error();
            }
            if (!right_as_left.value().is_null()) {
                write_tri(slot, row, types::tri_of(compare_same_type(left_value, right_as_left.value(), node->op())));
                continue;
            }
            auto left_as_right = left_value.cast_as(right_value.type(), context.session_tz);
            if (left_as_right.has_error()) {
                return left_as_right.error();
            }
            if (!left_as_right.value().is_null()) {
                write_tri(slot, row, types::tri_of(compare_same_type(left_as_right.value(), right_value, node->op())));
                continue;
            }
            write_tri(slot, row, types::tri_bool_t::no);
        }
        return core::error_t::no_error();
    }

    core::error_t expression_executor_t::eval_conjunction(size_t index, uint64_t count) {
        const auto* node = static_cast<const bound_conjunction_t*>(nodes_[index]);
        const auto begin = child_begin_[index];
        const auto arity = nodes_[index]->children().size();
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        slot.validity().reset(capacity_);
        results_[index] = &slot;

        if (node->op() == compare_type::union_not) {
            const auto& operand = *results_[children_[begin]];
            for (uint64_t row = 0; row < count; ++row) {
                write_tri(slot, row, types::tri_not(read_tri(operand, row)));
            }
            return core::error_t::no_error();
        }
        const bool conjunctive = node->op() == compare_type::union_and;
        for (uint64_t row = 0; row < count; ++row) {
            auto accumulated = conjunctive ? types::tri_bool_t::yes : types::tri_bool_t::no;
            for (size_t operand = 0; operand < arity; ++operand) {
                const auto value = read_tri(*results_[children_[begin + operand]], row);
                accumulated = conjunctive ? types::tri_and(accumulated, value) : types::tri_or(accumulated, value);
            }
            write_tri(slot, row, accumulated);
        }
        return core::error_t::no_error();
    }

    core::error_t expression_executor_t::eval_case(size_t index, uint64_t count) {
        const auto* node = static_cast<const bound_case_t*>(nodes_[index]);
        const auto begin = child_begin_[index];
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        const auto physical = slot.type().to_physical_type();
        if (!is_dispatchable(physical)) {
            return exec_error(resource_,
                              core::error_code_t::unimplemented_yet,
                              "expression executor: CASE over this physical type is not implemented");
        }
        slot.validity().reset(capacity_);
        results_[index] = &slot;

        const size_t when_count = node->when_count();
        for (uint64_t row = 0; row < count; ++row) {
            const vector::vector_t* chosen = nullptr;
            for (size_t branch = 0; branch < when_count; ++branch) {
                const auto& condition = *results_[children_[begin + branch * 2]];
                // A WHEN fires only on a definite TRUE -- UNKNOWN falls through to the next branch,
                // exactly as it does in a WHERE.
                if (types::selects(read_tri(condition, row))) {
                    chosen = results_[children_[begin + branch * 2 + 1]];
                    break;
                }
            }
            if (!chosen && node->has_else()) {
                chosen = results_[children_[begin + when_count * 2]];
            }
            if (!chosen || !chosen->validity().row_is_valid(row)) {
                // No branch fired and no ELSE: SQL says NULL.
                slot.validity().set(row, false);
                continue;
            }
            if (chosen->type().to_physical_type() != physical) {
                return exec_error(resource_,
                                  core::error_code_t::schema_error,
                                  "expression executor: CASE branch type contradicts the bound result type");
            }
            slot.validity().set(row, true);
            types::simple_physical_type_switch<copy_row_callback>(physical, *chosen, row, slot, row);
        }
        return core::error_t::no_error();
    }

    // Unary minus. compute_unary_neg answers a fresh vector IN THE OPERAND'S TYPE; it is moved into
    // the preallocated slot rather than kept, so the slot identity survives -- the same shape
    // eval_arithmetic and eval_cast use.
    core::error_t expression_executor_t::eval_negate(size_t index, uint64_t count) {
        const auto& operand = *results_[children_[child_begin_[index]]];
        auto computed = vector::compute_unary_neg(resource_, operand, count);
        if (computed.has_error()) {
            return computed.error();
        }
        if (computed.value().type().type() != nodes_[index]->return_type().type()) {
            return exec_error(resource_,
                              core::error_code_t::schema_error,
                              "expression executor: unary minus produced a type the node did not promise");
        }
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        slot = std::move(computed.value());
        results_[index] = &slot;
        return core::error_t::no_error();
    }

    // First operand that is VALID, per row. Typed: the chosen cell is copied through the physical
    // switch, never boxed into a logical_value_t.
    core::error_t expression_executor_t::eval_coalesce(size_t index, uint64_t count) {
        const auto begin = child_begin_[index];
        const auto arity = nodes_[index]->children().size();
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        const auto physical = slot.type().to_physical_type();
        if (!is_dispatchable(physical)) {
            return exec_error(resource_,
                              core::error_code_t::unimplemented_yet,
                              "expression executor: COALESCE over this physical type is not implemented");
        }
        slot.validity().reset(capacity_);
        results_[index] = &slot;
        for (uint64_t row = 0; row < count; ++row) {
            const vector::vector_t* chosen = nullptr;
            for (size_t operand = 0; operand < arity; ++operand) {
                const auto* candidate = results_[children_[begin + operand]];
                if (candidate->validity().row_is_valid(row)) {
                    chosen = candidate;
                    break;
                }
            }
            if (!chosen) {
                slot.validity().set(row, false); // every operand NULL -> NULL
                continue;
            }
            slot.validity().set(row, true);
            types::simple_physical_type_switch<copy_row_callback>(physical, *chosen, row, slot, row);
        }
        return core::error_t::no_error();
    }

    core::error_t expression_executor_t::eval_regex(size_t index, uint64_t count) {
        const auto* node = static_cast<const bound_regex_t*>(nodes_[index]);
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        slot.validity().reset(capacity_);
        results_[index] = &slot;
        if (node->failure()) {
            // A pattern RE2 refused, or a non-string pattern: the bind-time verdict, delivered now.
            return core::error_t(node->failure()->type, std::pmr::string{node->failure()->what, resource_});
        }
        const auto begin = child_begin_[index];
        const auto& subject = *results_[children_[begin]];
        if (node->regex_mode() == bound_regex_t::mode::null_pattern) {
            for (uint64_t row = 0; row < count; ++row) {
                write_tri(slot, row, types::tri_bool_t::unknown);
            }
            return core::error_t::no_error();
        }
        // types::is_string on the LOGICAL type -- NOT physical_type == STRING. The two disagree: a
        // type can be physically STRING without being one of the string logical types, and the
        // narrower physical test would reject such subjects.
        if (!types::is_string(subject.type().type())) {
            // `int_col LIKE 'p'`: the validator does not type-check a regex, so this is where it is
            // caught -- and it is an ERROR, never a read of a non-string payload as a std::string*.
            return exec_error(resource_,
                              core::error_code_t::comparison_failure,
                              "incorrect argument type for regex");
        }
        if (node->regex_mode() == bound_regex_t::mode::compiled) {
            const auto* compiled = node->compiled();
            for (uint64_t row = 0; row < count; ++row) {
                // A NULL subject is UNKNOWN (SQL 3VL), not FALSE: the two diverge under the NOT that
                // a NOT LIKE arrives wrapped in.
                if (!subject.validity().row_is_valid(row)) {
                    write_tri(slot, row, types::tri_bool_t::unknown);
                    continue;
                }
                write_tri(slot, row, types::tri_of(compiled->match(subject.get_value<std::string_view>(row))));
            }
            return core::error_t::no_error();
        }
        // dynamic: the pattern is an expression of its own, so it genuinely varies per row and
        // cannot be compiled once. Everything else about it is identical.
        const auto& pattern = *results_[children_[begin + 1]];
        if (!types::is_string(pattern.type().type())) {
            return exec_error(resource_,
                              core::error_code_t::comparison_failure,
                              "incorrect argument type for regex");
        }
        for (uint64_t row = 0; row < count; ++row) {
            if (!subject.validity().row_is_valid(row) || !pattern.validity().row_is_valid(row)) {
                write_tri(slot, row, types::tri_bool_t::unknown);
                continue;
            }
            auto compiled = core::regex_t::compile(resource_, pattern.get_value<std::string_view>(row), node->icase());
            if (compiled.has_error()) {
                return compiled.error();
            }
            write_tri(slot, row, types::tri_of(compiled.value().match(subject.get_value<std::string_view>(row))));
        }
        return core::error_t::no_error();
    }

    core::error_t
    expression_executor_t::eval_any_all(size_t index, uint64_t count, const context_t& context) {
        const auto* node = static_cast<const bound_any_all_t*>(nodes_[index]);
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        slot.validity().reset(capacity_);
        results_[index] = &slot;
        if (!context.parameters) {
            return exec_error(resource_,
                              core::error_code_t::invalid_parameter,
                              "expression executor: ANY/ALL read without a parameter map");
        }
        // Read LIVE: a correlated sub-query refills this slot per outer row.
        const auto* array = logical_plan::get_parameter(context.parameters, node->array_id());
        if (!array) {
            return exec_error(resource_,
                              core::error_code_t::invalid_parameter,
                              "expression executor: the ANY/ALL array slot is not bound");
        }
        const auto begin = child_begin_[index];
        const auto& subject = *results_[children_[begin]];
        auto& cache = regex_caches_[static_cast<size_t>(cache_of_[index])];

        for (uint64_t row = 0; row < count; ++row) {
            // Empty sub-query list: compact_to_array_value answers the NA-null sentinel for a
            // zero-row `x [NOT] IN (SELECT ...)`. PostgreSQL says `x = ANY(empty)` is FALSE and
            // `x <> ALL(empty)` is TRUE. That answer is TOTAL -- it holds even for a NULL x -- so it
            // is decided before the subject is looked at.
            if (array->is_null()) {
                write_tri(slot, row, node->is_any() ? types::tri_bool_t::no : types::tri_bool_t::yes);
                continue;
            }
            if (!subject.validity().row_is_valid(row)) {
                // A NULL subject makes every element comparison UNKNOWN, so the whole fold is.
                write_tri(slot, row, types::tri_bool_t::unknown);
                continue;
            }
            const auto subject_value = subject.value(row);
            auto folded = fold_any_all(*node, subject_value, *array, cache, context);
            if (folded.has_error()) {
                return folded.error();
            }
            write_tri(slot, row, folded.value());
        }
        return core::error_t::no_error();
    }

    core::result_wrapper_t<types::tri_bool_t>
    expression_executor_t::fold_any_all(const bound_any_all_t& node,
                                        const types::logical_value_t& subject,
                                        const types::logical_value_t& array,
                                        regex_cache_t& cache,
                                        const context_t& context) {
        const bool regex_elements = node.inner_op() == compare_type::regex;
        if (regex_elements && !types::is_string(subject.type().type())) {
            return core::error_t{core::error_code_t::comparison_failure,
                                 std::pmr::string{"incorrect argument type for regex", resource_}};
        }
        // Three-valued membership: a NULL element (or one whose cast to the subject's type yields
        // NULL) contributes an UNKNOWN comparison, never a match. Remembering that one was seen is
        // what lets the exhausted loop answer UNKNOWN instead of a definite FALSE/TRUE -- so
        // `x = ANY(S with a NULL)` on a miss is UNKNOWN, and `x <> ALL(S with a NULL)` (NOT IN) is
        // too. Either way the row drops, and NOT cannot resurrect it.
        bool saw_null_element = false;
        for (const auto& element : array.children()) {
            if (element.is_null()) {
                saw_null_element = true;
                continue;
            }
            bool matched = false;
            if (regex_elements) {
                std::string_view pattern;
                types::logical_value_t coerced{resource_, types::complex_logical_type{types::logical_type::NA}};
                if (types::is_string(element.type().type())) {
                    pattern = element.value<std::string_view>();
                } else {
                    // An exotic non-text element (`col LIKE ANY(SELECT int_col ...)`) is coerced to
                    // the subject's string type so it stringifies; a coercion that yields NULL is a
                    // NULL element.
                    auto casted = element.cast_as(subject.type(), context.session_tz);
                    if (casted.has_error()) {
                        return casted.error();
                    }
                    if (casted.value().is_null()) {
                        saw_null_element = true;
                        continue;
                    }
                    coerced = std::move(casted.value());
                    pattern = coerced.value<std::string_view>();
                }
                const auto* compiled = compiled_for(cache, pattern, node.like(), node.icase());
                if (!compiled) {
                    return core::error_t{
                        core::error_code_t::comparison_failure,
                        std::pmr::string{"invalid regular expression in ANY/ALL pattern", resource_}};
                }
                matched = compiled->match(subject.value<std::string_view>());
                if (node.negate()) {
                    matched = !matched; // NOT LIKE inverts EACH element, before the fold
                }
            } else {
                auto casted = element.cast_as(subject.type(), context.session_tz);
                if (casted.has_error()) {
                    return casted.error();
                }
                if (casted.value().is_null()) {
                    saw_null_element = true;
                    continue;
                }
                matched = compare_same_type(subject, casted.value(), node.inner_op());
            }
            if (node.is_any() && matched) {
                return types::tri_bool_t::yes;
            }
            if (!node.is_any() && !matched) {
                return types::tri_bool_t::no;
            }
        }
        if (saw_null_element) {
            return types::tri_bool_t::unknown;
        }
        return node.is_any() ? types::tri_bool_t::no : types::tri_bool_t::yes;
    }

    // Compile-once per DISTINCT element pattern, into a cache the EXECUTOR owns. The node cannot own
    // it: a bound node is immutable, and this fills up as rows arrive. A hit allocates nothing (the
    // map is probed heterogeneously with the raw string_view); only a miss materialises the key.
    // A pattern that does not compile is cached as an empty slot, so it is not retried per row.
    const core::regex_t*
    expression_executor_t::compiled_for(regex_cache_t& cache, std::string_view pattern, bool like, bool icase) {
        if (auto it = cache.find(pattern); it != cache.end()) {
            return it->second ? &*it->second : nullptr;
        }
        const std::pmr::string source = like ? std::pmr::string{like_to_regex(std::string{pattern}), resource_}
                                             : std::pmr::string{pattern.data(), pattern.size(), resource_};
        auto compiled = core::regex_t::compile(resource_, std::string_view{source}, icase);
        std::optional<core::regex_t> slot;
        if (!compiled.has_error()) {
            slot = std::optional<core::regex_t>(std::move(compiled.value()));
        }
        auto inserted = cache.emplace(std::pmr::string{pattern.data(), pattern.size(), resource_}, std::move(slot));
        return inserted.first->second ? &*inserted.first->second : nullptr;
    }

    core::error_t expression_executor_t::eval_function(size_t index, uint64_t count) {
        const auto* node = static_cast<const bound_function_t*>(nodes_[index]);
        auto& slot = slots_[static_cast<size_t>(slot_of_[index])];
        results_[index] = &slot;
        if (count == 0) {
            return core::error_t::no_error();
        }
        slot.validity().reset(capacity_);

        // Fill the preallocated argument chunk from this batch's argument results, COLUMN at a time.
        //
        // Not vector_t::reference(), which would be free: it assigns the source's validity mask, and
        // validity_mask_t::operator= asserts both masks live on the SAME resource
        // (validation.cpp). The executor's resource and the input chunk's are routinely different
        // -- over a sink the operator's resource outlives batches allocated on another arena -- so
        // referencing here aborts the moment those two differ.
        auto& arguments = function_args_[static_cast<size_t>(arg_chunk_of_[index])];
        const auto begin = child_begin_[index];
        const auto arity = node->children().size();
        for (size_t argument = 0; argument < arity; ++argument) {
            const auto& value = *results_[children_[begin + argument]];
            auto& target = arguments.data[argument];
            if (value.type().to_physical_type() != target.type().to_physical_type()) {
                return exec_error(resource_,
                                  core::error_code_t::schema_error,
                                  "expression executor: function argument type contradicts the bound type");
            }
            target.validity().reset(capacity_);
            if (auto error = vector::vector_ops::copy(value, target, count, 0, 0); error.contains_error()) {
                return error;
            }
        }
        arguments.set_cardinality(count);

        // The executor's OWN resource, never default_exec_context(): that one is a function-local
        // static over std::pmr::get_default_resource() (kernel_utils.cpp), which rule 8 keeps out
        // of an operator's allocation path.
        compute::exec_context_t context{resource_};
        auto produced = node->function()->execute(arguments, nullptr, context);
        if (produced.has_error()) {
            return produced.error();
        }
        // datum_t is a sum of "one boxed value per row" (a row_function) and "a chunk whose first
        // column is the result" (a vector_function). Both are written into the SAME preallocated
        // slot by a visitor, so this file names neither alternative: no std::get, no
        // holds_alternative, and no way for a third alternative to be added without the compiler
        // pointing at this visitor (rule 14, rule 6).
        return std::visit(function_result_writer{resource_, slot, count, node->physical_type()},
                          produced.value());
    }

} // namespace components::expressions
