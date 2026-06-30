#include "operator_external_sort.hpp"

#include "arithmetic_eval.hpp"

#include <components/table/storage/file_buffer.hpp>
#include <components/table/storage/spill_file.hpp>
#include <components/table/storage/unified_format.hpp>
#include <components/vector/vector_operations.hpp>

#include <algorithm>
#include <filesystem>
#include <numeric>
#include <queue>

namespace components::operators {

    namespace {
        // Derive the real MVCC snapshot horizon for a spill header. Prefer the
        // executor-populated lowest_active_start_time (GC threshold, the value a
        // spill reader must respect); fall back to the statement's own start_time.
        uint64_t spill_snapshot_horizon(const pipeline::context_t& ctx) noexcept {
            if (ctx.lowest_active_start_time != 0) {
                return ctx.lowest_active_start_time;
            }
            return ctx.txn.start_time;
        }
    } // anonymous namespace

    operator_external_sort_t::operator_external_sort_t(std::pmr::memory_resource* resource, log_t log)
        : read_only_operator_t(resource, log, operator_type::external_sort)
        , computed_keys_(resource)
        , grace_state_(resource) {}

    void operator_external_sort_t::add(size_t index, order order_) { sorter_.add(index, order_); }

    void operator_external_sort_t::add(const std::pmr::vector<size_t>& col_path, order order_) {
        sorter_.add(col_path, order_);
    }

    void operator_external_sort_t::add_computed(computed_sort_key_t&& key) { computed_keys_.push_back(std::move(key)); }

    bool operator_external_sort_t::evaluate_computed_keys_and_local_sort(
        pipeline::context_t* pipeline_context,
        std::pmr::vector<vector::data_chunk_t>& in_chunks,
        std::vector<std::vector<uint32_t>>& sorted_indices,
        std::pmr::vector<types::complex_logical_type>& out_types,
        size_t& first_computed_col) {

        first_computed_col = 0;
        out_types.clear();
        if (!in_chunks.empty()) {
            first_computed_col = in_chunks.front().data.size();
            out_types = in_chunks.front().types();
        }

        sorted_indices.clear();
        sorted_indices.reserve(in_chunks.size());

        bool computed_added = false;
        for (auto& chunk : in_chunks) {
            if (chunk.size() == 0) {
                sorted_indices.emplace_back();
                continue;
            }
            for (const auto& ck : computed_keys_) {
                auto result_vec = evaluate_arithmetic(resource_,
                                                      ck.op,
                                                      ck.operands,
                                                      chunk,
                                                      pipeline_context->parameters,
                                                      pipeline_context->session_tz);
                if (result_vec.has_error()) {
                    set_error(result_vec.error());
                    return false;
                }
                if (!computed_added) {
                    sorter_.add(chunk.data.size(), ck.order_);
                }
                chunk.data.emplace_back(std::move(result_vec.value()));
            }
            computed_added = true;

            std::vector<uint32_t> idx(chunk.size());
            std::iota(idx.begin(), idx.end(), uint32_t{0});
            sorter_.set_chunk(chunk);
            std::sort(idx.begin(), idx.end(), std::ref(sorter_));
            sorted_indices.emplace_back(std::move(idx));
        }
        return true;
    }

    void operator_external_sort_t::kway_merge_to_output(
        const std::pmr::vector<vector::data_chunk_t>& chunks,
        const std::vector<std::vector<uint32_t>>& sorted_indices,
        const std::pmr::vector<types::complex_logical_type>& out_types,
        size_t out_cols_effective,
        size_t /*first_computed_col*/,
        chunks_vector_t& out_chunks) {

        struct cursor_t {
            uint32_t chunk_idx;
            uint32_t cursor;
        };
        auto cmp = [&](const cursor_t& a, const cursor_t& b) {
            size_t ra = sorted_indices[a.chunk_idx][a.cursor];
            size_t rb = sorted_indices[b.chunk_idx][b.cursor];
            int c = sorter_.compare_cross(chunks[a.chunk_idx], ra, chunks[b.chunk_idx], rb);
            // std::priority_queue is a max-heap; reverse for min-heap behaviour.
            // Tie-break on chunk_idx then cursor for deterministic order.
            if (c != 0)
                return c > 0;
            if (a.chunk_idx != b.chunk_idx)
                return a.chunk_idx > b.chunk_idx;
            return a.cursor > b.cursor;
        };
        std::priority_queue<cursor_t, std::vector<cursor_t>, decltype(cmp)> heap(cmp);
        for (uint32_t ci = 0; ci < chunks.size(); ++ci) {
            if (!sorted_indices[ci].empty()) {
                heap.push({ci, uint32_t{0}});
            }
        }

        int64_t offset_val = limit_.offset();
        int64_t limit_val = limit_.limit();
        uint64_t skip = offset_val > 0 ? static_cast<uint64_t>(offset_val) : 0;
        uint64_t take = (limit_val >= 0) ? static_cast<uint64_t>(limit_val) : std::numeric_limits<uint64_t>::max();

        vector::data_chunk_t cur(resource_, out_types, vector::DEFAULT_VECTOR_CAPACITY);
        uint64_t cur_filled = 0;
        uint64_t produced = 0;

        auto flush_cur = [&]() {
            if (cur_filled == 0) {
                return;
            }
            cur.set_cardinality(cur_filled);
            out_chunks.emplace_back(std::move(cur));
            cur = vector::data_chunk_t(resource_, out_types, vector::DEFAULT_VECTOR_CAPACITY);
            cur_filled = 0;
        };

        while (!heap.empty() && produced < take) {
            auto top = heap.top();
            heap.pop();
            const auto& src_chunk = chunks[top.chunk_idx];
            size_t row = sorted_indices[top.chunk_idx][top.cursor];

            if (skip > 0) {
                --skip;
            } else {
                if (cur_filled == vector::DEFAULT_VECTOR_CAPACITY) {
                    flush_cur();
                }
                // vector_ops::copy arg 3 is the END index (exclusive), arg 4 is the start
                // offset in the source, arg 5 is the target offset. Copy count = end - offset.
                for (size_t c = 0; c < out_cols_effective; ++c) {
                    vector::vector_ops::copy(src_chunk.data[c], cur.data[c], row + 1, row, cur_filled);
                }
                vector::vector_ops::copy(src_chunk.row_ids, cur.row_ids, row + 1, row, cur_filled);
                ++cur_filled;
                ++produced;
            }

            ++top.cursor;
            if (top.cursor < sorted_indices[top.chunk_idx].size()) {
                heap.push(top);
            }
        }

        flush_cur();
    }

    void operator_external_sort_t::strip_computed_keys(std::pmr::vector<vector::data_chunk_t>& in_chunks,
                                                       size_t first_computed_col) const {
        if (computed_keys_.empty()) {
            return;
        }
        for (auto& chunk : in_chunks) {
            if (chunk.data.size() > first_computed_col) {
                chunk.data.erase(chunk.data.begin() + static_cast<ptrdiff_t>(first_computed_col), chunk.data.end());
            }
        }
    }

    void operator_external_sort_t::stamp_spill_context(pipeline::context_t* pipeline_context) {
        // Stamp the pipeline's real MVCC snapshot into the spill state so the
        // run headers carry a meaningful horizon.
        grace_state_.snapshot_horizon = pipeline_context ? spill_snapshot_horizon(*pipeline_context) : 0;
        // R10: resolve the spill dir from ctx->disk_config. When the pipeline
        // context is absent (unit-test paths) fall back to the OS temp dir so
        // spill_file_t still has a concrete create_directory target.
        grace_state_.spill_dir = components::table::storage::resolve_spill_dir(
            pipeline_context ? pipeline_context->disk_config : nullptr);
    }

    void operator_external_sort_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (!left_ || !left_->output()) {
            return;
        }
        auto in = left_->output();
        auto& in_chunks = in->chunks();

        // Phase 1: evaluate computed sort keys (mutating chunk) + local per-chunk sort.
        std::pmr::vector<types::complex_logical_type> out_types{resource_};
        size_t first_computed_col = 0;
        std::vector<std::vector<uint32_t>> sorted_indices;
        if (!evaluate_computed_keys_and_local_sort(pipeline_context, in_chunks, sorted_indices, out_types, first_computed_col)) {
            return; // arithmetic-eval error already stamped via set_error()
        }

        // Output column count (drop computed sort-key columns).
        size_t out_cols_effective = expected_output_count_ > 0 ? expected_output_count_ : first_computed_col;
        if (!computed_keys_.empty() && out_cols_effective > first_computed_col) {
            out_cols_effective = first_computed_col;
        }
        if (out_types.size() > out_cols_effective) {
            out_types.erase(out_types.begin() + static_cast<ptrdiff_t>(out_cols_effective), out_types.end());
        }

        // R10: stamp the real MVCC snapshot + resolve the spill dir from
        // ctx->disk_config into grace_state_ before touching the spill state.
        stamp_spill_context(pipeline_context);

        // Proactive spill (R6: no threshold gate, no in-memory fallback). This
        // operator exists because the optimizer decided spill is required, so it
        // always spills. Returns false only on hard I/O error.
        std::pmr::string spill_error(resource_);
        if (!spill_sorted_runs(in_chunks, sorted_indices, out_types, spill_error)) {
            set_error(core::error_t(core::error_code_t::io_error,
                                    std::pmr::string{"External merge sort spill failed: ", resource_} + spill_error));
            return;
        }

        chunks_vector_t out_chunks(resource_);
        std::pmr::string merge_error(resource_);
        if (!external_merge_sort(pipeline_context, out_types, out_cols_effective, out_chunks, merge_error)) {
            set_error(core::error_t(core::error_code_t::io_error,
                                    std::pmr::string{"External merge sort failed: ", resource_} + merge_error));
            return;
        }

        // Restore input chunks: strip the temporary computed-key columns.
        strip_computed_keys(in_chunks, first_computed_col);

        if (out_chunks.empty()) {
            out_chunks.emplace_back(resource_, out_types, 0);
        }
        output_ = operators::make_operator_data(in->resource(), std::move(out_chunks));
    }

    // ============================================================================
    // External Merge Sort Spill Implementation
    // ============================================================================

    bool operator_external_sort_t::spill_sorted_runs(
        const std::pmr::vector<vector::data_chunk_t>& chunks,
        const std::vector<std::vector<uint32_t>>& sorted_indices,
        const std::pmr::vector<types::complex_logical_type>& out_types,
        std::pmr::string& error_msg) {

        using namespace components::table::storage;

        grace_state_.run_files.clear();

        // R6: no memory-threshold gate, no spill_disabled fallback. This operator
        // ALWAYS spills — the optimizer/physgen selected it precisely because the
        // working set exceeds the in-memory budget. Spill each locally-sorted
        // chunk to its own run file.
        uint64_t run_id = 0;
        for (size_t ci = 0; ci < chunks.size(); ++ci) {
            const auto& chunk = chunks[ci];
            const auto& indices = sorted_indices[ci];

            if (indices.empty()) {
                continue; // Skip empty chunks
            }

            // Chunk the sorted rows to ≤ DEFAULT_VECTOR_CAPACITY. An input chunk
            // can carry more rows than DEFAULT_VECTOR_CAPACITY (e.g. a 2048-capacity
            // scan vector partially filled), and a single data_chunk_t asserts
            // capacity ≤ DEFAULT_VECTOR_CAPACITY. Each slice becomes its own sorted run.
            std::pmr::vector<types::complex_logical_type> chunk_types(out_types, resource_);
            constexpr size_t MAX_RUN = vector::DEFAULT_VECTOR_CAPACITY;
            for (size_t start = 0; start < indices.size(); start += MAX_RUN) {
                size_t n = std::min(MAX_RUN, indices.size() - start);
                vector::data_chunk_t sorted_chunk(resource_, chunk_types, n);

                // Copy rows in sorted order using vector_ops.
                for (size_t i = 0; i < n; ++i) {
                    size_t src_row = indices[start + i];
                    for (size_t col = 0; col < out_types.size(); ++col) {
                        vector::vector_ops::copy(chunk.data[col], sorted_chunk.data[col],
                                                src_row + 1, src_row, i);
                    }
                    // Copy row_ids unconditionally: it is a flat BIGINT vector
                    // always allocated by the data_chunk_t ctor. Do NOT guard on
                    // chunk.row_ids.size() — vector_t::size() asserts auxiliary_
                    // (set only for nested types), so it would abort on this flat vector.
                    vector::vector_ops::copy(chunk.row_ids, sorted_chunk.row_ids,
                                             src_row + 1, src_row, i);
                }
                sorted_chunk.set_cardinality(n);

                // Create file buffer for serialization.
                file_buffer_t buffer(resource_, components::table::storage::file_buffer_type::MANAGED_BUFFER, 0);

                // Setup unified format header
                unified_format_header header{};
                std::memcpy(header.magic, "OTSC1.0", 8);
                header.version = 1;
                // Stamp the pipeline's real MVCC snapshot.
                header.snapshot_horizon = grace_state_.snapshot_horizon;
                header.min_visible_commit_id = 0;
                header.max_visible_commit_id = 0;
                header.table_oid = 0; // Not applicable
                header.column_count = static_cast<uint32_t>(sorted_chunk.data.size());
                header.row_count = sorted_chunk.size();
                header.row_group_count = (sorted_chunk.size() + DEFAULT_ROW_GROUP_SIZE - 1) / DEFAULT_ROW_GROUP_SIZE;

                auto serialize_err = serialize_unified(sorted_chunk, buffer, header);
                if (serialize_err.contains_error()) {
                    error_msg = serialize_err.what; // carry the serializer's real reason
                    return false;
                }

                // Spill via the RAII spill_file_t — it creates the spill dir if
                // missing, retries partial writes, and removes the file on destruction
                // (every exit path). The handle is kept alive in grace_state_.run_handles
                // across the spill -> k-way-merge cycle so the file survives until the
                // merge consumes it. fs is a stateless by-value member passed by ref.
                auto dir = grace_state_.spill_dir;
                auto name = components::table::storage::make_spill_name(
                    run_id, std::string("sort_run_") + std::to_string(run_id));
                auto run = std::make_unique<components::table::storage::spill_file_t>(
                    grace_state_.fs, dir, name);
                if (!run->valid()) {
                    error_msg = std::pmr::string("failed to create sort run file: ", resource_) +
                                std::pmr::string(run->full_path().c_str(), resource_);
                    return false; // File creation failed
                }

                if (!run->write_all(buffer.internal_buffer(), buffer.size())) {
                    error_msg = std::pmr::string("failed to write sort run file: ", resource_) +
                                std::pmr::string(run->full_path().c_str(), resource_);
                    return false; // Write failed
                }
                run->sync();

                // Keep the path and the RAII handle.
                grace_state_.run_files.push_back(run->full_path());
                grace_state_.run_handles.push_back(std::move(run));

                ++run_id;
            } // end per-run slice
        }

        grace_state_.spilled = true;
        return true;
    }

    bool operator_external_sort_t::external_merge_sort(
        pipeline::context_t* /*pipeline_context*/,
        const std::pmr::vector<types::complex_logical_type>& out_types,
        size_t out_cols_effective,
        chunks_vector_t& out_chunks,
        std::pmr::string& error_msg) {

        using namespace components::table::storage;

        if (grace_state_.run_files.empty()) {
            error_msg = std::pmr::string("no sort runs to merge", resource_);
            return false; // No runs to merge
        }

        // Load all run files into memory chunks.
        std::pmr::vector<vector::data_chunk_t> run_chunks(resource_);
        std::vector<std::vector<uint32_t>> run_indices;

        for (size_t run_idx = 0; run_idx < grace_state_.run_files.size(); ++run_idx) {
            const auto& run_path = grace_state_.run_files[run_idx];

            // Open the spill run for reading through core::filesystem. The
            // underlying file is still owned by grace_state_.run_handles[run_idx]
            // (RAII) so a hard error here cannot leak it.
            core::filesystem::local_file_system_t fs;
            auto rh = components::table::storage::open_spill_for_read(
                fs, grace_state_.spill_dir,
                std::filesystem::path(run_path).filename().string());
            if (!rh) {
                error_msg = std::pmr::string("failed to open sort run for read: ", resource_) +
                            std::pmr::string(run_path.c_str(), resource_);
                return false; // File open failed
            }

            int64_t file_size = core::filesystem::file_size(fs, *rh);
            if (file_size < 0) {
                error_msg = std::pmr::string("failed to stat sort run file", resource_);
                return false;
            }

            // Allocate buffer.
            file_buffer_t buffer(resource_, components::table::storage::file_buffer_type::MANAGED_BUFFER, 0);
            buffer.resize(static_cast<uint64_t>(file_size));

            bool read_ok = rh->read(buffer.internal_buffer(), static_cast<uint64_t>(file_size));
            rh->close();
            if (!read_ok) {
                error_msg = std::pmr::string("failed to read sort run file", resource_);
                return false; // Read failed
            }

            // Deserialize chunk.
            unified_format_header header{};
            auto de = deserialize_unified(buffer, resource_, header);
            if (de.has_error()) {
                error_msg = de.error().what; // carry the deserializer's real reason
                return false;                // Deserialization failed
            }
            auto chunk = std::move(de.value());

            run_chunks.emplace_back(std::move(chunk));

            // Create sorted indices (0..N-1) for this run.
            std::vector<uint32_t> indices(header.row_count);
            std::iota(indices.begin(), indices.end(), uint32_t{0});
            run_indices.emplace_back(std::move(indices));
        }

        // K-way merge of the deserialized runs via the k-way merge heap helper.
        // first_computed_col is unused here (the run chunks already have the
        // trimmed column count), so pass out_types.size() to make
        // strip-style trimming a no-op on the run-chunks vector.
        kway_merge_to_output(run_chunks, run_indices, out_types, out_cols_effective, out_types.size(), out_chunks);

        // Drop the RAII spill handles — their destructors remove the temp
        // files on every path (this success path and any early-return error path).
        grace_state_.run_handles.clear();
        grace_state_.run_files.clear();
        grace_state_.spilled = false;

        return true;
    }

} // namespace components::operators
