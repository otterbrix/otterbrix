#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/expressions/compare_expression.hpp>
#include <components/logical_plan/node_create_index.hpp>

#include <components/logical_plan/node_limit.hpp>
#include <components/physical_plan/operators/operator.hpp>

namespace components::operators {

    class index_scan final : public read_only_operator_t {
    public:
        index_scan(std::pmr::memory_resource* resource,
                   log_t log,
                   components::catalog::oid_t table_oid,
                   const expressions::key_t& key,
                   const types::logical_value_t& value,
                   expressions::compare_type compare_type,
                   components::logical_plan::index_type preferred_index_type,
                   logical_plan::limit_t limit);

        components::catalog::oid_t table_oid() const noexcept { return table_oid_; }
        const expressions::key_t& key() const { return key_; }
        const types::logical_value_t& value() const { return value_; }
        expressions::compare_type compare_type() const { return compare_type_; }
        components::logical_plan::index_type preferred_index_type() const { return preferred_index_type_; }
        const logical_plan::limit_t& limit() const { return limit_; }

        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // --- Push-based streaming pipeline source (WINDOWED point-fetch, bounded) ---
        // The index search is ONE-SHOT — it returns the whole matched row-id set in a single
        // future — so this source materializes the ids ONCE (the FIRST source_next call), applies
        // OFFSET/LIMIT to a [pos_, end_) window over them, and then WINDOWS the per-row-id
        // storage_fetch: each subsequent call fetches exactly one ≤ DEFAULT_VECTOR_CAPACITY slice
        // of ids and returns it as ONE chunk (no split). Peak fetch memory is one window, not the
        // whole matched set. The fetched chunk carries the absolute row_ids (the agent stamps them),
        // so a downstream DELETE/UPDATE/index sees the same ids the materialized path produced.
        //   The N sequential cross-actor co_awaits (one index search + one fetch per window) live in
        // this NESTED operator coroutine (driven by co_await from execute_pipeline), not in a
        // behavior() handler, so the actor-zeta single-slot awaited continuation is
        // republished+cleared between sequential awaits — no lost-wakeup (same shape as
        // await_async_and_resume's sequential awaits).
        //   A no-table sentinel scan (INVALID_OID) is not a real source: role()==none keeps it on
        // the legacy path.
        [[nodiscard]] pipeline_role role() const noexcept override {
            return table_oid_ == components::catalog::INVALID_OID ? pipeline_role::none : pipeline_role::source;
        }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        source_next(pipeline::context_t* ctx) override;

        // Rewind the windowed point-fetch cursor so a re-driven sub-plan re-runs the
        // one-shot index search from scratch (recursive-CTE recursive term, per iteration).
        void reset_pipeline_state() noexcept override {
            opened_ = false;
            drained_ = false;
            emitted_any_ = false;
            pos_ = 0;
            end_ = 0;
            row_ids_vec_.clear();
            guard_types_.clear();
        }

    private:
        void on_execute_impl(pipeline::context_t* pipeline_context) override;

        // Shared windowing core (Rule 6: ONE implementation for source_next and the materialized
        // await_async_and_resume). Run the one-shot index search (txn-aware visibility), store the
        // matched ids in row_ids_vec_, and compute the OFFSET/LIMIT window [pos_=start_, end_).
        // If there is no index service or the search matched nothing, leaves an empty window.
        actor_zeta::unique_future<void> open_index_window(pipeline::context_t* ctx);

        // Fetch the absolute-row-id window [start, start+count) as ONE chunk via the existing
        // per-row-id storage_fetch handler (no new agent_disk handler). On a null fetch reply,
        // returns a schema'd 0-row chunk via storage_types (await). count must be > 0.
        actor_zeta::unique_future<core::result_wrapper_t<vector::data_chunk_t>>
        fetch_window(pipeline::context_t* ctx, size_t start, size_t count);

        // Schema'd 0-row chunk for the empty/drained case (storage_types await).
        actor_zeta::unique_future<vector::data_chunk_t> make_empty_chunk(pipeline::context_t* ctx);

        components::catalog::oid_t table_oid_;
        const expressions::key_t key_;
        const types::logical_value_t value_;
        const expressions::compare_type compare_type_;
        const components::logical_plan::index_type preferred_index_type_;
        const logical_plan::limit_t limit_;

        // Windowed point-fetch cursor state (mirrors full_scan's source cursor):
        //   opened_   : false until the first source_next runs open_index_window (the one-shot
        //               index search + OFFSET/LIMIT window computation).
        //   row_ids_vec_ : the materialized matched row-id set (the one-shot search result).
        //   pos_ / end_  : the live [pos_, end_) window cursor over row_ids_vec_ AFTER offset/limit.
        //   drained_  : pos_ reached end_ ⇒ source exhausted.
        //   emitted_any_ / guard_types_: if the scan drains having produced zero rows, emit ONE
        //               schema'd 0-row guard chunk (scalar aggregate COUNT=0 / OUTER-join NULL-pad),
        //               then the 0-column drain sentinel.
        bool opened_{false};
        bool drained_{false};
        bool emitted_any_{false};
        size_t pos_{0};
        size_t end_{0};
        std::pmr::vector<int64_t> row_ids_vec_{resource_};
        std::pmr::vector<types::complex_logical_type> guard_types_{resource_};
    };

} // namespace components::operators
