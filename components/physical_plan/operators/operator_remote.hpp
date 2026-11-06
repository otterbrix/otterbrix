#pragma once

#include "operator.hpp"
#include "operator_select.hpp"

#include <components/table/storage_address.hpp>

#include <memory>

namespace components::operators {

    // A read a table's storage took: it runs `request` (storage_run_block) when the plan opens, so the reads of a plan
    // run at the same time, and again on every re-drive with the parameters of that drive. Its pages are checked
    // against the types otterbrix expects and against the width of a chunk. EXPLAIN names a connection's read
    // "Remote Query on <connection>", and the disk's "Seq Scan" of its table.
    class operator_remote_scan final : public read_only_operator_t {
    public:
        operator_remote_scan(std::pmr::memory_resource* resource,
                             log_t log,
                             table::storage_address_t address,
                             std::pmr::string request,
                             std::pmr::vector<std::pmr::string> explain,
                             std::pmr::vector<types::complex_logical_type> output_types,
                             std::pmr::vector<catalog::oid_t> tables);

        [[nodiscard]] pipeline_role role() const noexcept override { return pipeline_role::source; }
        [[nodiscard]] actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
        source_next(pipeline::context_t* ctx) override;

        void reset_pipeline_state() noexcept override {
            drained_ = false;
            emitted_any_ = false;
            cursor_id_ = 0;
            page_.reset();
        }

        [[nodiscard]] actor_zeta::unique_future<void> release_cursor(pipeline::context_t* ctx) override;
        [[nodiscard]] bool holds_open_cursor() const noexcept override { return cursor_id_ != 0 && !drained_; }

    private:
        actor_zeta::unique_future<core::error_t> open_impl(pipeline::context_t* ctx) override;
        void explain_impl(const explain_sink& s) const override;
        std::pmr::string explain_label_impl() const override;
        std::pmr::vector<std::pmr::string> explain_details_impl() const override;

        // The current page as one batch, or the end when the backend answered an empty page.
        core::result_wrapper_t<std::optional<vector::data_chunk_t>> take_from_page();

        table::storage_address_t address_;
        std::pmr::string request_;
        std::pmr::vector<std::pmr::string> explain_;
        std::pmr::vector<types::complex_logical_type> output_types_;
        std::pmr::vector<catalog::oid_t> tables_;

        bool drained_{false};
        bool emitted_any_{false};
        uint64_t cursor_id_{0};
        std::unique_ptr<vector::data_chunk_t> page_;
    };

    // A write a backend took (storage_run_write): the rows it changed are the statement's count, and RETURNING is
    // computed over the values its receipt carries.
    class operator_remote_write final : public read_write_operator_t {
    public:
        operator_remote_write(std::pmr::memory_resource* resource,
                              log_t log,
                              table::storage_address_t address,
                              std::pmr::string request,
                              std::pmr::vector<std::pmr::string> explain,
                              std::pmr::vector<projected_column_t> returning,
                              std::pmr::vector<types::complex_logical_type> table_types,
                              std::vector<size_t> scan_columns,
                              std::pmr::vector<catalog::oid_t> tables);

        [[nodiscard]] bool needs_async_finalize() const noexcept override { return true; }
        actor_zeta::unique_future<void> await_async_and_resume(pipeline::context_t* ctx) override;

    private:
        std::pmr::string explain_label_impl() const override;
        std::pmr::vector<std::pmr::string> explain_details_impl() const override;

        table::storage_address_t address_;
        std::pmr::string request_;
        std::pmr::vector<std::pmr::string> explain_;
        std::pmr::vector<projected_column_t> returning_;
        std::unique_ptr<execution_dag::execution_dag_t> returning_graph_;
        std::pmr::vector<types::complex_logical_type> table_types_;
        std::vector<size_t> scan_columns_;
        std::pmr::vector<catalog::oid_t> tables_;
    };

} // namespace components::operators
