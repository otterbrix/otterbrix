#pragma once

#include <components/expressions/bound/binder.hpp>
#include <components/expressions/bound/expression_executor.hpp>
#include <components/physical_plan/operators/operator.hpp>

#include <optional>

namespace components::operators {

    // ONE compiled predicate, shared by every operator that filters or joins on an expression
    // (having, join, lateral_join, delete, update). The callers ask three different questions --
    // filter a chunk against itself, test one probe row against a whole build side, test a single
    // pair -- and all three live here, once, so the callers cannot drift apart on 3VL, on NULL
    // handling, or on what an evaluation error means.
    //
    // Built ONCE per execution from the input schemas; every intermediate the evaluation needs is
    // allocated then.
    class predicate_executor_t {
    public:
        predicate_executor_t(const predicate_executor_t&) = delete;
        predicate_executor_t& operator=(const predicate_executor_t&) = delete;
        predicate_executor_t(predicate_executor_t&&) noexcept = default;
        predicate_executor_t& operator=(predicate_executor_t&&) = delete;
        ~predicate_executor_t() = default;

        // `expression` may be NULL, which means "every row matches" -- the shape the DML operators
        // use when the scan already applied the WHERE. It becomes a constant TRUE rather than a
        // branch around the evaluation, so there is exactly one path below (rule 6) and, being
        // foldable, it is evaluated once at create() and never per chunk.
        //
        // A single-input filter passes the same schema twice: the chunk is compared against itself.
        //
        // The inputs are SCHEMAS, not type lists, because a bind schema is {name, type} per column
        // and every caller but one already holds a chunk that carries exactly that. Reading the name
        // out of the type instead is a guess dressed as a lookup: it needs a guard in front of it
        // (alias() asserts on a type with no extension), it cannot tell a column's name from a
        // STRUCT type's own, and it answers nothing at all once the name leaves the type. A chunk's
        // schema() answers all three.
        [[nodiscard]] static core::result_wrapper_t<predicate_executor_t>
        create(std::pmr::memory_resource* resource,
               const expressions::expression_ptr& expression,
               const vector::schema_t& left,
               const vector::schema_t& right,
               const compute::function_registry_t* functions,
               const logical_plan::storage_parameters* parameters,
               core::date::timezone_offset_t session_tz);

        // FILTER: row k of `chunk` against row k of the same chunk. Writes the surviving row indices
        // into `selection` (which the caller sizes to at least `count`) and answers how many.
        // Only a definite TRUE selects -- a NULL operand yields UNKNOWN, which does not.
        [[nodiscard]] core::result_wrapper_t<uint64_t> select(const vector::data_chunk_t& chunk,
                                                              uint64_t count,
                                                              const logical_plan::storage_parameters& parameters,
                                                              core::date::timezone_offset_t session_tz,
                                                              vector::indexing_vector_t& selection);

        // JOIN: ONE row of `left` against `right[0 .. right_count)`. Writes the matching RIGHT row
        // indices into `selection` and answers how many.
        [[nodiscard]] core::result_wrapper_t<uint64_t> select_matches(const vector::data_chunk_t& left,
                                                                      uint64_t left_row,
                                                                      const vector::data_chunk_t& right,
                                                                      uint64_t right_count,
                                                                      const logical_plan::storage_parameters& parameters,
                                                                      core::date::timezone_offset_t session_tz,
                                                                      vector::indexing_vector_t& selection);

        uint64_t capacity() const noexcept;

    private:
        explicit predicate_executor_t(expressions::expression_executor_t executor);

        expressions::expression_executor_t executor_;
    };

} // namespace components::operators
