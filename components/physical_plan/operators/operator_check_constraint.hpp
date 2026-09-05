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
    // A predicate arrives already resolved against the TABLE's columns, while the chunk it judges
    // is the write-set, whose column order is the statement's. So each predicate is rebound to the
    // chunk before its graph is built: a column the write-set carries becomes that chunk position.
    // A column it does NOT carry FAILS the statement rather than being substituted: the write-set
    // is the MATERIALISED row (the INSERT's omissions are expanded to their DEFAULT, or NULL,
    // above the journal; the UPDATE write-set is the gathered storage row), so every column of the
    // table is in it, and substituting NULL for one that is missing would leave the predicate
    // UNKNOWN — which PERMITS the row, i.e. a declared constraint enforced by nothing.
    class operator_check_constraint_t final : public read_write_operator_t {
    public:
        // The rows this validates are MATERIALISED: every table column is present,
        // because the INSERT's omissions are expanded (to their DEFAULT, or NULL) above
        // the journal and the UPDATE write-set is the gathered storage row. So the
        // predicates read the column, never a plan-side copy of what the column was
        // going to become — which is exactly how a CHECK comes to admit a row it judged
        // against a value the write path did not store. That is why this constructor
        // takes no column_defaults / write_set_named: there is nothing left to guess.
        //
        // check_params carries the constants the predicates reference; it has NO default
        // argument because a parameter_map_t needs a resource, and the only resource a
        // default could name is the process-wide one (rule 14).
        operator_check_constraint_t(std::pmr::memory_resource* resource,
                                    log_t log,
                                    std::vector<std::string> not_null_columns,
                                    std::vector<std::pair<std::string, expressions::expression_ptr>> check_predicates,
                                    std::vector<std::pair<std::string, uint64_t>> array_size_reqs,
                                    types::parameter_map_t check_params);

        // STREAMING CONSTRAINT SINK. check_constraint is the PARENT of a DML sink in the plan chain
        // (check_constraint -> insert/update -> scan). Its validation is SYNCHRONOUS (no cross-actor I/O),
        // but it must run AFTER the DML's await — the DML snapshots the just-written rows into
        // constraint_input() there, and on the streaming path the scan SOURCE's output_ is empty.
        // needs_async_finalize routes the validation into the executor's bottom-up async-finalize drive (the
        // FLUSH phase runs BEFORE the DML await, which would be too early). push()/finalize() are no-ops; the
        // validation runs in await_async_and_resume via the validate_() core, completing synchronously.
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

        // Rebind `predicate` from table ordinals to the positions of `chunk`. Returns the rebound
        // copy; the original is left alone, since it belongs to the plan and outlives this
        // execution. REFUSES (invalid_constraint) a column reference the chunk does not carry:
        // see the class comment — the write-set is the materialised row, so a name that is not in
        // it is a predicate that would judge nothing, and answering it with NULL is the silent
        // pass this operator used to give every unrecognised CHECK. `constraint_name` names the
        // constraint in that refusal, so the message points at the declaration the user wrote
        // rather than at an anonymous predicate.
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
        // ordinals and every cast and call is bound. NOT the SQL text: a text form has to be read
        // back by a hand-written recogniser, and everything outside the shapes it knows compiled
        // to the constant TRUE.
        std::vector<std::pair<std::string, expressions::expression_ptr>> check_predicates_;
        // Constants the predicates reference, read by their graphs.
        types::parameter_map_t check_params_{resource_};
    };

} // namespace components::operators