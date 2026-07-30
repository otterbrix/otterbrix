#include "operator_select.hpp"

#include "projection_executor.hpp"

#include <components/compute/function.hpp>

namespace components::operators {

    operator_select_t::operator_select_t(std::pmr::memory_resource* resource, log_t log)
        : read_write_operator_t(resource, log, operator_type::select)
        , columns_(resource) {}

    // Out-of-line: projection_executor_t is only complete in this translation unit.
    operator_select_t::~operator_select_t() = default;

    void operator_select_t::add_column(select_column_t&& col) { columns_.push_back(std::move(col)); }

    void operator_select_t::set_output_schema(const vector::schema_t& schema) {
        for (size_t i = 0; i < columns_.size() && i < schema.size(); ++i) {
            columns_[i].result_type = schema[i].type;
        }
    }

    core::error_t
    operator_select_t::push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) {
        // Streaming projection: apply the per-chunk transform to the single batch handed in via
        // `input`. No accumulation, no read of left_->output(). A SELECT over a JOIN receives one
        // merged chunk holding both sides' columns, so the chunk doubles as the right input: a
        // column's resolved side indexes the same merged chunk either way.
        // The EMPTY sentinel: no columns AND no rows. Both halves matter. No columns alone is not
        // enough -- `SELECT 1 FROM t HAVING true` over an empty table emits a chunk with no columns
        // and ONE row (the implicit GROUP BY () group), and that row must survive the projection as
        // the constant 1. Only when there are no rows either is there nothing an expression could
        // read, and then a resolved ordinal has no schema to address. Handled before the executor is
        // built so the cached one is never poisoned by a sentinel batch.
        if (input.column_count() == 0 && input.size() == 0) {
            out.emplace_back(empty_projection(resource_, columns_));
            return core::error_t::no_error();
        }
        if (!stream_executor_) {
            // The chunk's own schema is the bind schema: {name, type} per column, from the columns
            // themselves. A SELECT over a JOIN receives ONE merged chunk holding both sides, so the
            // same schema is both inputs and a key's resolved side stops mattering.
            auto executor = projection_executor_t::create(resource_,
                                                          columns_,
                                                          input.schema(),
                                                          input.schema(),
                                                          ctx->function_registry,
                                                          &ctx->parameters,
                                                          ctx->session_tz);
            if (executor.has_error()) {
                return executor.error();
            }
            stream_executor_ = std::make_unique<projection_executor_t>(std::move(executor.value()));
        }
        // Parameters are re-read from the context on EVERY batch, never captured: a LATERAL
        // correlation rebinds its slots between two runs of this same operator.
        auto result = stream_executor_->evaluate(input, ctx->parameters, ctx->session_tz);
        if (result.has_error()) {
            return result.error();
        }
        out.emplace_back(std::move(result.value()));
        return core::error_t::no_error();
    }

    core::result_wrapper_t<vector::data_chunk_t> evaluate_projection(std::pmr::memory_resource* resource,
                                                                     const std::pmr::vector<select_column_t>& columns,
                                                                     vector::data_chunk_t* input,
                                                                     const logical_plan::storage_parameters& parameters,
                                                                     core::date::timezone_offset_t session_tz,
                                                                     vector::data_chunk_t* right_input) {
        // The RETURNING path: one projection over one already-gathered chunk. It binds per call, as
        // it did before -- a RETURNING list is evaluated once per DML statement, not per streamed
        // batch, so there is no per-chunk rebuild to remove here. What matters is that the meaning
        // of a select_column_t is defined in ONE place, projection_executor_t, so the streamed and
        // the one-shot projection cannot drift.
        if (input->column_count() == 0 && input->size() == 0) {
            return empty_projection(resource, columns);
        }
        const vector::data_chunk_t* right = right_input != nullptr ? right_input : input;
        auto executor = projection_executor_t::create(resource,
                                                      columns,
                                                      input->schema(),
                                                      right->schema(),
                                                      compute::function_registry_t::get_default(),
                                                      &parameters,
                                                      session_tz);
        if (executor.has_error()) {
            return executor.error();
        }
        return executor.value().evaluate(*input, parameters, session_tz, right_input);
    }

} // namespace components::operators
