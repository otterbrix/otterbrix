#pragma once

#include <components/expressions/bound/binder.hpp>
#include <components/expressions/bound/expression_executor.hpp>
#include <components/physical_plan/operators/operator_select.hpp>

#include <optional>

namespace components::operators {

    // Evaluates a whole SELECT list over one chunk at a time.
    //
    // WHY THIS BINDS select_column_t DIRECTLY, and does not reverse it into a parsed expression
    // first: select_column_t and group_key_t ARE a hand-written bound layer. They already carry
    // resolved column ordinals, a plan-resolved result type, and a decoded COALESCE / CASE shape --
    // everything a parsed expression would have to be re-analysed to recover. Turning one back into
    // the parsed form so the binder could re-derive what it already knows would run the pipeline
    // backwards through the very layer this replaces. So the struct is a BINDING INPUT, on the same
    // footing as an expression_ptr, and where it carries a shape the bound nodes could not express
    // (COALESCE) the node was added rather than the struct rewritten.
    //
    // Every column's tree is bound and its executor built ONCE, in create(); evaluate() after that
    // fills preallocated slots.
    class projection_executor_t {
    public:
        projection_executor_t(const projection_executor_t&) = delete;
        projection_executor_t& operator=(const projection_executor_t&) = delete;
        projection_executor_t(projection_executor_t&&) noexcept = default;
        projection_executor_t& operator=(projection_executor_t&&) = delete;
        ~projection_executor_t() = default;

        // `left` / `right` are the input chunks' SCHEMAS -- {name, type} per column, which is what
        // a bind schema is, taken from the chunk that carries it rather than reconstructed out of
        // its types. A SELECT over a JOIN receives ONE merged chunk holding both sides' columns, so
        // callers pass the same schema twice and a key's resolved side stops mattering.
        [[nodiscard]] static core::result_wrapper_t<projection_executor_t>
        create(std::pmr::memory_resource* resource,
               const std::pmr::vector<select_column_t>& columns,
               const vector::schema_t& left,
               const vector::schema_t& right,
               const compute::function_registry_t* functions,
               const logical_plan::storage_parameters* parameters,
               core::date::timezone_offset_t session_tz);

        // One output chunk per input chunk (a projection is a 1:1 row mapping). `parameters` is read
        // LIVE: a LATERAL correlation rebinds its slots between two runs of the same projection.
        // `right_input` is the chunk a RIGHT-side column reads from when that is a different
        // object -- a DML RETURNING evaluates its target rows against a separately gathered
        // USING/FROM chunk. A SELECT over a join receives one merged chunk and passes nothing.
        [[nodiscard]] core::result_wrapper_t<vector::data_chunk_t>
        evaluate(vector::data_chunk_t& input,
                 const logical_plan::storage_parameters& parameters,
                 core::date::timezone_offset_t session_tz,
                 const vector::data_chunk_t* right_input = nullptr);

    private:
        explicit projection_executor_t(std::pmr::memory_resource* resource);

        // A column is either a bound tree or the bare '*' fan-out, which is not an expression at all
        // -- it copies the input's columns through untouched, so it has no tree and no slot.
        struct column_plan_t {
            bool star_expand = false;
            std::pmr::string alias;
            std::optional<expressions::expression_executor_t> executor;
        };

        std::pmr::memory_resource* resource_;
        std::pmr::vector<column_plan_t> plans_;
    };

    // The projection of a chunk that carries NO COLUMNS.
    //
    // A 0-column chunk is the engine's empty/drain sentinel -- an aggregate over an empty build
    // side, a sourceless match. There is nothing to project FROM, so nothing may be bound against
    // it: a resolved ordinal cannot address a schema with no entries. What the output must still
    // carry is the PLAN-RESOLVED type of every column, which is authoritative precisely because it
    // was derived data-independently and therefore survives having no data.
    [[nodiscard]] vector::data_chunk_t empty_projection(std::pmr::memory_resource* resource,
                                                        const std::pmr::vector<select_column_t>& columns);

    // Evaluate ONE scalar expression (arithmetic, unary minus, CASE) over a chunk, answering the
    // result column.
    //
    // Binds per call -- not the "allocate once" the streaming operators get: a cached executor
    // would have to live on each caller, and the callers evaluate a whole chunk per call. Worth
    // caching when one of them shows up in a profile, not before.
    [[nodiscard]] core::result_wrapper_t<vector::vector_t>
    evaluate_scalar(std::pmr::memory_resource* resource,
                    expressions::scalar_type op,
                    const std::pmr::vector<expressions::param_storage>& operands,
                    const vector::data_chunk_t& chunk,
                    const compute::function_registry_t* functions,
                    const logical_plan::storage_parameters& parameters,
                    core::date::timezone_offset_t session_tz);

    // Bind ONE projection column against the input schemas. Exposed so the DML RETURNING paths and
    // operator_select share one definition of what a select_column_t MEANS.
    [[nodiscard]] core::result_wrapper_t<expressions::bound_expression_ptr>
    bind_select_column(std::pmr::memory_resource* resource,
                       expressions::binder_t& binder,
                       const select_column_t& column,
                       const expressions::binder_context_t& context);

} // namespace components::operators
