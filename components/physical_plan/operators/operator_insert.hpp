#pragma once

#include <components/catalog/catalog_oids.hpp>
#include <components/logical_plan/node_insert.hpp>
#include <components/physical_plan/operators/operator.hpp>
#include <components/physical_plan/operators/operator_select.hpp>
#include <components/table/storage_address.hpp>

namespace components::operators {

#ifdef DEV_MODE
    // Counts index-mirror sends (a deep copy per send); must read zero on a table with no indexes.
    uint64_t insert_index_mirror_sends() noexcept;
    void reset_insert_index_mirror_sends() noexcept;
#endif

    class operator_insert final : public read_write_operator_t {
    public:
        // `returning` is the RETURNING projection (empty when absent), computed over the rows as the storage wrote
        // them. `attoids` name the table's columns in their declared order (none for a dynamic-schema table).
        operator_insert(std::pmr::memory_resource* resource,
                        log_t log,
                        catalog::oid_t table_oid,
                        table::storage_address_t address,
                        std::pmr::string name,
                        std::pmr::vector<catalog::oid_t> attoids,
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
        // The table's visible columns, from the catalog: an empty RETURNING has them.
        void set_table_types(std::pmr::vector<types::complex_logical_type> types) { table_types_ = std::move(types); }

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
        // Refuses a RETURNING of constants that fails (a division by zero) before any row reaches the storage.
        actor_zeta::unique_future<core::error_t> open_impl(pipeline::context_t* ctx) override;
        bool opens_at_start_impl() const noexcept override {
            return !components::catalog::is_catalog_table(table_oid_);
        }
        // A storage that caps its writes flushes an INSERT on its cap (PostgreSQL 18 batches a foreign INSERT by the
        // FDW's batch size).
        uint64_t flush_row_threshold_impl(uint64_t engine) const noexcept override {
            return address_.max_rows_per_write() != 0 ? address_.max_rows_per_write() : engine;
        }

        std::pmr::string explain_label_impl() const override;

        catalog::oid_t table_oid_;
        table::storage_address_t address_;
        std::pmr::string name_;
        std::pmr::vector<catalog::oid_t> attoids_;
        std::pmr::vector<projected_column_t> returning_;
        std::unique_ptr<execution_dag::execution_dag_t> returning_graph_;
        // Accumulate the RETURNING rows and the appended-row count until the final drive.
        chunks_vector_t returning_accum_{resource_};
        logical_plan::insert_column_bindings_t column_bindings_{resource_};
        logical_plan::insert_fill_list_t fill_list_{resource_};
        bool table_has_indexes_{true};
        std::pmr::vector<types::complex_logical_type> table_types_{resource_};
    };

} // namespace components::operators
