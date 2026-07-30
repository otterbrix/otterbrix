#include "predicate_executor.hpp"

namespace components::operators {

    namespace {

        // The input schema a tree binds against, restated in the binder's own carrier. Each column's
        // name is taken from its schema record, which is where the column's name IS; it is still a
        // fallback for binding, because every key reaching an operator already carries the ordinals
        // validate_logical_plan resolved for it and binder_t::bind_key uses those first.
        //
        // This used to read the name out of the type's name slot behind a guard —
        // alias() asserts on a type carrying no extension, which was reached for real three times
        // while migrating the earlier operators. Reading a record needs no guard and cannot assert.
        expressions::bind_schema_t schema_of(std::pmr::memory_resource* resource, const vector::schema_t& columns) {
            expressions::bind_schema_t schema{resource};
            for (const auto& column : columns) {
                schema.add(std::string_view{column.name}, column.type);
            }
            return schema;
        }

    } // namespace

    predicate_executor_t::predicate_executor_t(expressions::expression_executor_t executor)
        : executor_(std::move(executor)) {}

    core::result_wrapper_t<predicate_executor_t>
    predicate_executor_t::create(std::pmr::memory_resource* resource,
                                 const expressions::expression_ptr& expression,
                                 const vector::schema_t& left_columns,
                                 const vector::schema_t& right_columns,
                                 const compute::function_registry_t* functions,
                                 const logical_plan::storage_parameters* parameters,
                                 core::date::timezone_offset_t session_tz) {
        auto left = schema_of(resource, left_columns);
        auto right = schema_of(resource, right_columns);

        expressions::binder_context_t context{};
        context.left = &left;
        context.right = &right;
        context.functions = functions;
        context.parameters = parameters;
        context.session_tz = session_tz;

        expressions::bound_expression_ptr root;
        if (expression) {
            expressions::binder_t binder{resource};
            auto bound = binder.bind(expression, context);
            if (bound.has_error()) {
                return bound.error();
            }
            root = std::move(bound.value());
        } else {
            // "Every row matches", as a constant rather than a branch around the evaluation.
            root = expressions::bound_expression_ptr{
                expressions::make_bound_constant(resource, types::logical_value_t{resource, true})};
        }

        auto executor = expressions::expression_executor_t::create(resource, std::move(root));
        if (executor.has_error()) {
            return executor.error();
        }
        return core::result_wrapper_t<predicate_executor_t>{predicate_executor_t{std::move(executor.value())}};
    }

    uint64_t predicate_executor_t::capacity() const noexcept { return executor_.capacity(); }

    core::result_wrapper_t<uint64_t>
    predicate_executor_t::select(const vector::data_chunk_t& chunk,
                                 uint64_t count,
                                 const logical_plan::storage_parameters& parameters,
                                 core::date::timezone_offset_t session_tz,
                                 vector::indexing_vector_t& selection) {
        expressions::expression_executor_t::context_t execution{};
        // Read LIVE on every call, never captured: a correlated (LATERAL) sub-query rebinds its
        // slots between two runs of the same compiled predicate.
        execution.parameters = &parameters;
        execution.session_tz = session_tz;
        return executor_.select(chunk, count, execution, selection);
    }

    core::result_wrapper_t<uint64_t>
    predicate_executor_t::select_matches(const vector::data_chunk_t& left,
                                         uint64_t left_row,
                                         const vector::data_chunk_t& right,
                                         uint64_t right_count,
                                         const logical_plan::storage_parameters& parameters,
                                         core::date::timezone_offset_t session_tz,
                                         vector::indexing_vector_t& selection) {
        expressions::expression_executor_t::context_t execution{};
        execution.parameters = &parameters;
        execution.session_tz = session_tz;
        execution.right_input = &right;
        execution.left_row = left_row;
        // `right` supplies the ROW COUNT here: the left operand is fixed for the whole batch, so the
        // batch is as long as the build side. The chunk handed to the executor stays the LEFT one,
        // because that is what a left-side reference indexes.
        return executor_.select(left, right_count, execution, selection);
    }

} // namespace components::operators
