#pragma once

#include <components/expressions/execution_dag_builder.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/types/logical_value.hpp>
#include <components/types/parameter_map.hpp>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace components::operators {

    // Checks NOT NULL constraints and CHECK predicates over the incoming chunk.
    //
    // A predicate arrives already resolved against the TABLE's columns, while the chunk it judges
    // is the write-set, whose column order is the statement's. So each predicate is rebound to the
    // chunk before its graph is built: a column the write-set carries becomes that chunk position,
    // and a column it omits becomes the value the stored row will hold — the column's DEFAULT, or
    // NULL when it has none, which leaves the comparison UNKNOWN and so permits the row.
    class operator_check_constraint_t final : public read_write_operator_t {
    public:
        // column_defaults: decoded DEFAULT values (name -> value) of the target
        // table. A column ABSENT from the write-set stores its DEFAULT (filled
        // agent-side at storage_append), so the compiled predicates evaluate an
        // absent column AS its default; absent with NO default means stored NULL.
        // write_set_named: absent-by-name is only meaningful when the write-set's
        // aliases are the statement's column names (SQL INSERT with a column list;
        // every UPDATE). Positional / hand-built inserts alias arbitrarily, so
        // absent columns keep the legacy pass-through there.
        operator_check_constraint_t(
            std::pmr::memory_resource* resource,
            log_t log,
            std::vector<std::string> not_null_columns,
            std::vector<std::pair<std::string, expressions::expression_ptr>> check_predicates = {},
            std::vector<std::pair<std::string, uint64_t>> array_size_reqs = {},
            std::vector<std::pair<std::string, types::logical_value_t>> column_defaults = {},
            bool write_set_named = false,
            types::parameter_map_t check_params = types::parameter_map_t{std::pmr::get_default_resource()});

        // STREAMING CONSTRAINT SINK. check_constraint is the PARENT of a DML sink in
        // the plan chain (check_constraint -> insert/update -> scan). Its validation
        // is SYNCHRONOUS (no cross-actor I/O), but it must run AFTER the DML's await —
        // the DML snapshots the just-written rows into constraint_input() there, and
        // on the streaming path the scan SOURCE's output_ is empty. needs_async_finalize
        // routes the validation into the executor's bottom-up async-finalize drive (the
        // FLUSH phase runs BEFORE the DML await, which would be too early). push()/
        // finalize() are no-ops; the validation runs in await_async_and_resume
        // via the validate_() core, completing synchronously (co_return).
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

        // Rebind `predicate` from table ordinals to the positions of `chunk`, substituting a
        // constant for every column the write-set does not carry. Returns the rebound copy; the
        // original is left alone, since it belongs to the plan and outlives this execution.
        // TODO: constraint should recive already remapped expressions or properrly filled data_chunk_t
        [[nodiscard]] expressions::expression_ptr bind_to_write_set_(const expressions::expression_ptr& predicate,
                                                                     const vector::data_chunk_t& chunk);

        std::vector<std::string> not_null_columns_;
        // Decoded DEFAULTs (name -> value); consulted for columns absent from the
        // write-set by the NOT NULL loop and the compiled CHECK predicates.
        std::vector<std::pair<std::string, types::logical_value_t>> column_defaults_;
        bool write_set_named_{false}; // see ctor note
        // Fixed-ARRAY columns (NOT NULL, no DEFAULT) and their declared sizes: a value
        // shorter than the size cannot be padded and is rejected with an error.
        std::vector<std::pair<std::string, uint64_t>> array_size_reqs_;
        // (name, predicate) — resolved by validation, so column references carry the table's
        // ordinals and every cast and call is bound.
        std::vector<std::pair<std::string, expressions::expression_ptr>> check_predicates_;
        // Constants the predicates reference, read by their graphs.
        types::parameter_map_t check_params_{resource_};
    };

} // namespace components::operators