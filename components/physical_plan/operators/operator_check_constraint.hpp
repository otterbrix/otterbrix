#pragma once

#include <components/expressions/execution_dag_builder.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/parameter_map.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace components::operators {

    // Checks NOT NULL constraints and CHECK predicates over the incoming chunk.
    //
    // Each predicate is rebound from table ordinals to the write-set chunk's positions before its
    // graph is built. A column the chunk doesn't carry FAILS the statement, not substituted with
    // NULL: the write-set is materialised (INSERT defaults expanded, UPDATE is the gathered row),
    // so substituting NULL for a missing column would leave the predicate UNKNOWN and permit the row.
    class operator_check_constraint_t final : public read_write_operator_t {
    public:
        // No column_defaults / write_set_named params: rows are always materialised (see class
        // comment), so there's nothing left to guess. check_params has no default argument because
        // a parameter_map_t needs a resource, and a default could only name the process-wide one.
        operator_check_constraint_t(std::pmr::memory_resource* resource,
                                    log_t log,
                                    std::vector<std::string> not_null_columns,
                                    std::vector<std::pair<std::string, expressions::expression_ptr>> check_predicates,
                                    std::vector<std::pair<std::string, uint64_t>> array_size_reqs,
                                    types::parameter_map_t check_params);

        // STREAMING CONSTRAINT SINK: check_constraint is the PARENT of a DML sink (check_constraint ->
        // insert/update -> scan). Validation is synchronous but must run AFTER the DML's await (which
        // snapshots rows into constraint_input()), so needs_async_finalize routes it into the bottom-up
        // async-finalize drive instead of the FLUSH phase (too early). push()/finalize() are no-ops.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        // No streaming input of its own: the child DML sink drains the pumped
        // stream, so push() is never reached with rows. Explicit no-ops.
        [[nodiscard]] core::error_t push(pipeline::context_t*, vector::data_chunk_t&&, chunks_vector_t&) override {
            return core::error_t::no_error();
        }
        [[nodiscard]] core::error_t finalize(pipeline::context_t*, chunks_vector_t&) override {
            return core::error_t::no_error();
        }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // Synchronous validation core. Resolves the rows to validate (the DML's
        // constraint_input() snapshot, or the fallbacks) and runs NOT NULL +
        // fixed-ARRAY length + CHECK-expression checks over them; on the first
        // violation it sets the error and returns. Called by await_async_and_resume.
        void validate_();

        struct compiled_check_t {
            expressions::condition_kind condition{expressions::condition_kind::always};
            std::unique_ptr<execution_dag::execution_dag_t> graph;
        };

        // Rebinds `predicate` from table ordinals to `chunk` positions; REFUSES (invalid_constraint) a
        // column reference the chunk doesn't carry, rather than silently passing it with NULL.
        // `constraint_name` names the constraint in that refusal.
        // TODO: constraint should recive already remapped expressions or properrly filled data_chunk_t
        [[nodiscard]] core::result_wrapper_t<expressions::expression_ptr>
        bind_to_write_set_(const expressions::expression_ptr& predicate,
                           const vector::data_chunk_t& chunk,
                           std::string_view constraint_name);

        std::vector<std::string> not_null_columns_;
        // Fixed-ARRAY columns (NOT NULL, no DEFAULT) and their declared sizes: a value
        // shorter than the size cannot be padded and is rejected with an error.
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs_;
        // (name, predicate) — resolved by validation, so column references carry the table's
        // ordinals and every cast and call is bound. NOT the SQL text: unrecognised shapes would
        // compile to the constant TRUE.
        std::vector<std::pair<std::string, expressions::expression_ptr>> check_predicates_;
        // Constants the predicates reference, read by their graphs.
        types::parameter_map_t check_params_{resource_};
    };

} // namespace components::operators