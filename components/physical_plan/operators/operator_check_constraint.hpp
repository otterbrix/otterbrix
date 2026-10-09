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

    // A column the write-set chunk doesn't carry FAILS the statement rather than substituting NULL,
    // which would leave the predicate UNKNOWN and silently permit the row.
    class operator_check_constraint_t final : public read_write_operator_t {
    public:
        // check_params has no default -- a parameter_map_t needs a resource, not a process-wide default.
        operator_check_constraint_t(std::pmr::memory_resource* resource,
                                    log_t log,
                                    std::vector<std::string> not_null_columns,
                                    std::vector<std::pair<std::string, expressions::expression_ptr>> check_predicates,
                                    std::vector<std::pair<std::string, uint64_t>> array_size_reqs,
                                    types::parameter_map_t check_params);

        // Validation must run after the DML's await, which snapshots rows into constraint_input() --
        // so needs_async_finalize routes it into the bottom-up async-finalize drive, not the flush phase.
        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        // The child DML sink drains the pumped stream, so push() never sees rows.
        [[nodiscard]] core::error_t push(pipeline::context_t*, vector::data_chunk_t&&, chunks_vector_t&) override {
            return core::error_t::no_error();
        }
        [[nodiscard]] core::error_t finalize(pipeline::context_t*, chunks_vector_t&) override {
            return core::error_t::no_error();
        }

    private:
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // Resolves rows from the DML's constraint_input() snapshot (or the fallbacks) and stops at
        // the first NOT NULL / fixed-ARRAY / CHECK violation, setting the error there.
        void validate_();

        struct compiled_check_t {
            expressions::condition_kind condition{expressions::condition_kind::always};
            std::unique_ptr<execution_dag::execution_dag_t> graph;
        };

        // Refuses with invalid_constraint, naming `constraint_name` (see class comment).
        // TODO: constraint should recive already remapped expressions or properrly filled data_chunk_t
        [[nodiscard]] core::result_wrapper_t<expressions::expression_ptr>
        bind_to_write_set_(const expressions::expression_ptr& predicate,
                           const vector::data_chunk_t& chunk,
                           std::string_view constraint_name);

        std::vector<std::string> not_null_columns_;
        // A value shorter than the declared fixed-ARRAY size cannot be padded and is rejected.
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs_;
        // Not the SQL text -- resolved expression trees with ordinals and casts already bound.
        // Unrecognised shapes compile to the constant true.
        std::vector<std::pair<std::string, expressions::expression_ptr>> check_predicates_;
        types::parameter_map_t check_params_{resource_};
    };

} // namespace components::operators