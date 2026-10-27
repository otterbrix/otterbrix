#include "operator_remote.hpp"

#include "dml_util.hpp"
#include "scan/guard_chunk.hpp"

#include <services/disk/storage_contract.hpp>

#include <algorithm>

namespace components::operators {

    namespace {

        std::pmr::string remote_query_label(std::pmr::memory_resource* resource,
                                            const table::storage_address_t& address) {
            std::pmr::string label{"Remote Query on ", resource};
            label += address.connection();
            return label;
        }

        // The page holds the block's columns, of the types otterbrix planned the taken node with.
        core::error_t check_block_page(std::pmr::memory_resource* resource,
                                       const table::storage_address_t& address,
                                       const std::pmr::vector<types::complex_logical_type>& types,
                                       const vector::data_chunk_t& page) {
            bool fits = page.column_count() == types.size();
            for (uint64_t column = 0; fits && column < types.size(); ++column) {
                fits = page.data[column].type().type() == types[column].type();
            }
            if (fits) {
                return core::error_t::no_error();
            }
            std::pmr::string what{"the backend of connection \"", resource};
            what += address.connection();
            what += "\" answered a block with a page of other columns";
            return core::error_t{core::error_code_t::invalid_parameter, std::move(what)};
        }

    } // namespace

    operator_remote_scan::operator_remote_scan(std::pmr::memory_resource* resource,
                                               log_t log,
                                               table::storage_address_t address,
                                               std::pmr::string request,
                                               std::pmr::vector<std::pmr::string> explain,
                                               std::pmr::vector<types::complex_logical_type> output_types,
                                               std::pmr::vector<catalog::oid_t> tables)
        : read_only_operator_t(resource, std::move(log), operator_type::remote_scan)
        , address_(std::move(address))
        , request_(std::move(request), resource)
        , explain_(std::move(explain), resource)
        , output_types_(std::move(output_types), resource)
        , tables_(std::move(tables), resource) {}

    std::pmr::string operator_remote_scan::explain_label_impl() const {
        return remote_query_label(resource_, address_);
    }

    std::pmr::vector<std::pmr::string> operator_remote_scan::explain_details_impl() const {
        return std::pmr::vector<std::pmr::string>{explain_.begin(), explain_.end(), resource_};
    }

    actor_zeta::unique_future<core::error_t> operator_remote_scan::open_impl(pipeline::context_t* ctx) {
        const components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->execution_context.timezone_offset};
        for (const auto table : tables_) {
            ctx->join_transaction(address_, table, false);
        }
        auto [_r, rf] = actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_run_block>(
            address_,
            exec_ctx,
            std::pmr::string{request_, address_.actor().resource()},
            uint64_t{0},
            ctx->parameters.clone(address_.actor().resource()));
        auto reply = co_await std::move(rf);
        if (reply.has_error()) {
            set_error(reply.error());
            mark_failed();
            co_return reply.error();
        }
        cursor_id_ = reply.value().cursor_id;
        page_ = std::move(reply.value().batch);
        if (cursor_id_ == 0 && page_ && page_->size() != 0) {
            std::pmr::string what{"the backend of connection \"", resource_};
            what += address_.connection();
            what += "\" answered a page without a cursor";
            core::error_t refused{core::error_code_t::invalid_parameter, std::move(what)};
            set_error(refused);
            mark_failed();
            co_return refused;
        }
        page_offset_ = 0;
        co_return core::error_t::no_error();
    }

    core::result_wrapper_t<std::optional<vector::data_chunk_t>> operator_remote_scan::take_from_page() {
        if (!page_ || page_->size() == 0) {
            page_.reset();
            drained_ = true;
            if (!emitted_any_) {
                emitted_any_ = true;
                return make_guard_chunk(resource_, output_types_, {});
            }
            return std::optional<vector::data_chunk_t>{};
        }
        if (page_offset_ == 0) {
            if (auto error = check_block_page(resource_, address_, output_types_, *page_); error.contains_error()) {
                set_error(error);
                mark_failed();
                return error;
            }
        }
        const uint64_t rows = std::min<uint64_t>(page_->size() - page_offset_, vector::DEFAULT_VECTOR_CAPACITY);
        vector::data_chunk_t batch(resource_, page_->types(), std::max<uint64_t>(rows, 1));
        const vector::indexing_vector_t these_rows(resource_, page_offset_, rows);
        page_->copy(batch, these_rows, rows, 0);
        batch.set_cardinality(rows);
        page_offset_ += rows;
        if (page_offset_ == page_->size()) {
            page_.reset();
            page_offset_ = 0;
        }
        emitted_any_ = true;
        return std::optional<vector::data_chunk_t>{std::move(batch)};
    }

    actor_zeta::unique_future<core::result_wrapper_t<std::optional<vector::data_chunk_t>>>
    operator_remote_scan::source_next(pipeline::context_t* ctx) {
        if (page_) {
            co_return take_from_page();
        }
        if (drained_) {
            co_return std::optional<vector::data_chunk_t>{};
        }
        const components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->execution_context.timezone_offset};
        auto [_r, rf] = actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_run_block>(
            address_,
            exec_ctx,
            std::pmr::string{request_, address_.actor().resource()},
            cursor_id_,
            ctx->parameters.clone(address_.actor().resource()));
        auto reply = co_await std::move(rf);
        if (reply.has_error()) {
            set_error(reply.error());
            mark_failed();
            co_return reply.error();
        }
        page_ = std::move(reply.value().batch);
        page_offset_ = 0;
        co_return take_from_page();
    }

    actor_zeta::unique_future<void> operator_remote_scan::release_cursor(pipeline::context_t* ctx) {
        if (cursor_id_ == 0 || drained_) {
            co_return;
        }
        const uint64_t id = cursor_id_;
        cursor_id_ = 0;
        drained_ = true;
        const components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->execution_context.timezone_offset};
        auto [_c, cf] =
            actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_close_block>(address_, exec_ctx, id);
        co_await std::move(cf);
        co_return;
    }

    operator_remote_write::operator_remote_write(std::pmr::memory_resource* resource,
                                                 log_t log,
                                                 table::storage_address_t address,
                                                 std::pmr::string request,
                                                 std::pmr::vector<std::pmr::string> explain,
                                                 std::pmr::vector<projected_column_t> returning,
                                                 std::pmr::vector<types::complex_logical_type> table_types,
                                                 std::vector<size_t> scan_columns,
                                                 std::pmr::vector<catalog::oid_t> tables)
        : read_write_operator_t(resource, std::move(log), operator_type::remote_write)
        , address_(std::move(address))
        , request_(std::move(request), resource)
        , explain_(std::move(explain), resource)
        , returning_(std::move(returning))
        , table_types_(std::move(table_types), resource)
        , scan_columns_(std::move(scan_columns))
        , tables_(std::move(tables), resource) {}

    std::pmr::string operator_remote_write::explain_label_impl() const {
        return remote_query_label(resource_, address_);
    }

    std::pmr::vector<std::pmr::string> operator_remote_write::explain_details_impl() const {
        return std::pmr::vector<std::pmr::string>{explain_.begin(), explain_.end(), resource_};
    }

    actor_zeta::unique_future<void> operator_remote_write::await_async_and_resume(pipeline::context_t* ctx) {
        if (auto refused = dml_detail::check_constant_returning(resource_, returning_, *ctx);
            refused.contains_error()) {
            set_error(refused);
            mark_failed();
            co_return;
        }
        const components::execution_context_t exec_ctx{ctx->session, ctx->txn, ctx->execution_context.timezone_offset};
        for (const auto table : tables_) {
            ctx->join_transaction(address_, table, true);
        }
        auto [_w, wf] = actor_zeta::otterbrix::send<&services::disk::storage_contract::storage_run_write>(
            address_,
            exec_ctx,
            std::pmr::string{request_, address_.actor().resource()},
            ctx->parameters.clone(address_.actor().resource()),
            !returning_.empty());
        auto reply = co_await std::move(wf);
        if (reply.has_error()) {
            set_error(reply.error());
            mark_failed();
            co_return;
        }
        auto& receipt = reply.value();
        written_ = receipt.count;
        if (returning_.empty()) {
            set_output(nullptr);
            mark_executed();
            co_return;
        }
        chunks_vector_t rows_returned{resource_};
        for (auto& returned : receipt.returned) {
            if (returned.size() == 0) {
                continue;
            }
            auto rows =
                dml_detail::as_scanned(resource_, vector::adopted(resource_, std::move(returned)), scan_columns_);
            auto projected = evaluate_projection(resource_,
                                                 returning_,
                                                 &rows,
                                                 ctx->parameters,
                                                 ctx->execution_context,
                                                 &returning_graph_);
            if (projected.has_error()) {
                set_error(projected.error());
                mark_failed();
                co_return;
            }
            rows_returned.emplace_back(std::move(projected.value()));
        }
        if (rows_returned.empty()) {
            // Nothing changed: the columns are still typed.
            vector::data_chunk_t empty(resource_, table_types_, 0);
            auto projected = evaluate_projection(resource_,
                                                 returning_,
                                                 &empty,
                                                 ctx->parameters,
                                                 ctx->execution_context,
                                                 &returning_graph_);
            if (projected.has_error()) {
                set_error(projected.error());
                mark_failed();
                co_return;
            }
            rows_returned.emplace_back(std::move(projected.value()));
        }
        set_output(make_operator_data(resource_, std::move(rows_returned)));
        mark_executed();
    }

} // namespace components::operators
