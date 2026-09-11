#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_select.hpp>

namespace components::operators {

#ifdef DEV_MODE
    // Counts index-mirror sends (a deep copy per send); must read zero on a table with no indexes.
    uint64_t insert_index_mirror_sends() noexcept;
    void reset_insert_index_mirror_sends() noexcept;
#endif

    class operator_insert final : public read_write_operator_t {
    public:
        // `returning` is the RETURNING projection (empty when absent); read back from storage when set.
        operator_insert(std::pmr::memory_resource* resource,
                        log_t log,
                        catalog::oid_t table_oid,
                        std::pmr::vector<projected_column_t> returning);

        catalog::oid_t table_oid() const noexcept { return table_oid_; }

        // Per incoming column: the name the append routes on, and the cast to its stored type.
        void set_column_bindings(logical_plan::insert_column_bindings_t bindings) {
            column_bindings_ = std::move(bindings);
        }

        // Omitted columns' fill values, resolved by enrich from pg_attribute.attdefspec (the only
        // place a default is read); push() materialises them so storage never substitutes anything.
        void set_fill_list(logical_plan::insert_fill_list_t fill) { fill_list_ = std::move(fill); }

        // Stamped by enrich; false skips the index mirror. Defaults to true for unstamped plans.
        void set_table_has_indexes(bool value) noexcept { table_has_indexes_ = value; }

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }

        [[nodiscard]] core::error_t
        push(pipeline::context_t* ctx, vector::data_chunk_t&& input, chunks_vector_t& out) override;

        // storage_append writes its WAL entry atomically inside the disk agent, unlike DELETE's
        // separate storage-then-WAL -- then index::insert_rows runs and mark_executed fires.
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

        // Compared against the mid-pump flush threshold; catalog inserts return 0 so it never mid-flushes.
        [[nodiscard]] uint64_t buffered_rows() const noexcept override {
            return (output_ && !components::catalog::is_catalog_table(table_oid_)) ? output_->size() : 0;
        }

    private:
        catalog::oid_t table_oid_;
        std::pmr::vector<projected_column_t> returning_;
        std::unique_ptr<execution_dag::execution_dag_t> returning_graph_;
        // Accumulates RETURNING rows (or tallies affected_rows_ without RETURNING) until the final drive.
        chunks_vector_t returning_accum_{resource_};
        uint64_t affected_rows_{0};
        logical_plan::insert_column_bindings_t column_bindings_{resource_};
        logical_plan::insert_fill_list_t fill_list_{resource_};
        bool table_has_indexes_{true};
    };

} // namespace components::operators
