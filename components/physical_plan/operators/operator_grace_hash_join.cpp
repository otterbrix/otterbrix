#include "operator_grace_hash_join.hpp"

#include "join_utils.hpp"
#include <components/table/storage/buffer_manager.hpp>

#include <core/file/local_file_system.hpp>

#include <cstring>
#include <utility>

namespace components::operators {

    using join_detail::join_builder;

    namespace {
        // Derive the real MVCC snapshot horizon for a spill header. Prefer the
        // executor-populated lowest_active_start_time (GC threshold); fall back to
        // the statement's own start_time.
        uint64_t spill_snapshot_horizon(const pipeline::context_t& ctx) noexcept {
            if (ctx.lowest_active_start_time != 0) {
                return ctx.lowest_active_start_time;
            }
            return ctx.txn.start_time;
        }

        // Partition buffer: holds rows belonging to a single partition
        struct partition_buffer_t {
            std::pmr::vector<vector::data_chunk_t> chunks;
            std::pmr::memory_resource* resource;

            explicit partition_buffer_t(std::pmr::memory_resource* res)
                : chunks(res)
                , resource(res) {}

            // Append rows from source chunk that belong to this partition
            void append_rows(const vector::data_chunk_t& src,
                             const std::pmr::vector<uint64_t>& row_indices,
                             size_t start_idx,
                             size_t count) {
                if (count == 0) return;

                // Capacity must be ≤ DEFAULT_VECTOR_CAPACITY (data_chunk_t asserts
                // it). The build-side input chunks may carry a larger capacity
                // (e.g. 2048), so stamp the default here — count is already chunked
                // to ≤ MAX_CHUNK_SIZE by the partition loop.
                chunks.emplace_back(src.resource(), src.types(),
                                    vector::DEFAULT_VECTOR_CAPACITY);
                auto& new_chunk = chunks.back();
                new_chunk.set_cardinality(count);

                for (size_t c = 0; c < src.column_count(); ++c) {
                    for (size_t i = 0; i < count; ++i) {
                        uint64_t src_row = row_indices[start_idx + i];
                        vector::vector_ops::copy(src.data[c], new_chunk.data[c],
                                                 src.size(), src_row, i);
                    }
                }
            }

            uint64_t row_count() const {
                uint64_t total = 0;
                for (const auto& c : chunks) {
                    total += c.size();
                }
                return total;
            }
        };

        // Partition the build side by hash(key) % partition_count and spill every
        // partition to disk. R6: no runtime memory check — the grace operator
        // spills unconditionally (the optimizer picked this strategy at plan time).
        // Each spilled partition is owned by grace_state.partition_handles
        // (spill_file_t RAII) so the temp files are removed on every exit path.
        bool partition_and_spill_build_side(const chunks_vector_t& right_chunks,
                                            size_t right_col,
                                            grace_hash_join_state_t& grace_state,
                                            std::pmr::memory_resource* resource,
                                            std::pmr::string& error_msg) {
            const uint32_t partition_count = grace_state.partition_count;

            // Create partition buffers
            std::pmr::vector<partition_buffer_t> partitions(resource);
            partitions.reserve(partition_count);
            for (uint32_t i = 0; i < partition_count; ++i) {
                partitions.emplace_back(resource);
            }

            // Partition rows: hash(key) % partition_count
            for (const auto& R : right_chunks) {
                if (right_col >= R.column_count()) {
                    continue;
                }
                const auto& col = R.data[right_col];

                std::pmr::vector<std::pmr::vector<uint64_t>> partition_rows(resource);
                partition_rows.reserve(partition_count);
                for (uint32_t i = 0; i < partition_count; ++i) {
                    partition_rows.emplace_back(std::pmr::vector<uint64_t>{resource});
                }

                // First pass: collect row indices for each partition
                for (uint64_t rj = 0; rj < R.size(); ++rj) {
                    if (!col.validity().row_is_valid(rj)) {
                        // NULL keys go to partition 0 (will never match)
                        partition_rows[0].push_back(rj);
                        continue;
                    }

                    auto key = col.value(rj);
                    size_t partition_id = key.hash() % partition_count;
                    partition_rows[partition_id].push_back(rj);
                }

                // Second pass: create chunks for each partition
                for (uint32_t pid = 0; pid < partition_count; ++pid) {
                    auto& rows = partition_rows[pid];
                    if (rows.empty()) continue;

                    constexpr size_t MAX_CHUNK_SIZE = 1024;
                    for (size_t start = 0; start < rows.size(); start += MAX_CHUNK_SIZE) {
                        size_t count = std::min(MAX_CHUNK_SIZE, rows.size() - start);
                        partitions[pid].append_rows(R, rows, start, count);
                    }
                }
            }

            // Spill each partition through spill_file_t (RAII). The spill dir
            // is created lazily, names are unique per query, partial writes retry on
            // EINTR, and the file is removed on destruction. Handles live in
            // grace_state.partition_handles across the whole spill -> probe cycle.
            // grace_state.fs is a by-value local_file_system_t.
            auto dir = grace_state.spill_dir;

            for (uint32_t pid = 0; pid < partition_count; ++pid) {
                auto& partition = partitions[pid];
                if (partition.row_count() == 0) {
                    continue;
                }

                auto name = components::table::storage::make_spill_name(
                    grace_state.query_id,
                    std::string("hj_partition_") + std::to_string(pid));
                auto sp = std::make_unique<components::table::storage::spill_file_t>(
                    grace_state.fs, dir, name);
                if (!sp->valid()) {
                    error_msg = "Failed to create partition file: " + sp->full_path();
                    return false;
                }

                // The header is rebuilt PER CHUNK: row_count / row_group_count
                // must reflect the rows carried by THIS chunk (≤
                // DEFAULT_VECTOR_CAPACITY), NOT the partition total — otherwise the
                // reader rebuilds a single data_chunk_t with capacity == partition
                // total, tripping the `capacity <= DEFAULT_VECTOR_CAPACITY`
                // assertion in data_chunk_t.
                const uint32_t column_count = static_cast<uint32_t>(
                    partition.chunks.empty() ? 0 : partition.chunks[0].column_count());

                for (auto& chunk : partition.chunks) {
                    components::table::storage::unified_format_header header;
                    std::memset(&header, 0, sizeof(header));
                    std::memcpy(header.magic, "OTSC1.0", 8);
                    header.version = 1;
                    // Stamp the real MVCC snapshot.
                    header.snapshot_horizon = grace_state.snapshot_horizon;
                    header.table_oid = 0;
                    header.column_count = column_count;
                    header.row_count = chunk.size();
                    header.row_group_count = (chunk.size() + 1023) / 1024;

                    components::table::storage::file_buffer_t file_buffer(resource,
                        components::table::storage::file_buffer_type::MANAGED_BUFFER, 0);

                    auto serialize_err =
                        components::table::storage::serialize_unified(chunk, file_buffer, header);
                    if (serialize_err.contains_error()) {
                        error_msg = serialize_err.what; // carry the serializer's real reason
                        return false;
                    }

                    if (!sp->write_all(file_buffer.internal_buffer(), file_buffer.size())) {
                        error_msg = "Failed to write partition chunk to " + sp->full_path();
                        return false;
                    }
                }

                sp->sync();
                grace_state.partition_handles.push_back(std::move(sp));
            }

            return true;
        }

        // Read a single chunk from buffer at offset, return chunk and bytes consumed
        struct chunk_read_result_t {
            vector::data_chunk_t chunk;
            uint64_t bytes_consumed;
            bool ok;
        };

        chunk_read_result_t read_single_chunk(const std::byte* buffer,
                                              uint64_t buffer_size,
                                              uint64_t offset,
                                              std::pmr::memory_resource* resource) {
            chunk_read_result_t result{vector::data_chunk_t(resource, {}, 0), 0, false};

            if (offset + 80 > buffer_size) {
                return result;
            }

            const std::byte* chunk_start = buffer + offset;

            if (std::memcmp(chunk_start, "OTSC1.0", 8) != 0) {
                return result;
            }

            uint32_t column_count;
            std::memcpy(&column_count, chunk_start + 48, sizeof(uint32_t));

            uint64_t row_count;
            std::memcpy(&row_count, chunk_start + 56, sizeof(uint64_t));

            if (column_count == 0 || column_count > 1000) {
                return result;
            }

            uint64_t remaining_size = buffer_size - offset;
            components::table::storage::file_buffer_t chunk_buffer(resource,
                components::table::storage::file_buffer_type::MANAGED_BUFFER, 0);
            chunk_buffer.resize(remaining_size);
            std::memcpy(chunk_buffer.internal_buffer(), chunk_start, remaining_size);

            components::table::storage::unified_format_header header;
            // read_single_chunk is a frame walker: a parse failure at this offset
            // means "no more valid frames" (result.ok stays false -> caller stops).
            // The deserializer's message is not surfaced here because a stop is the
            // expected end-of-partition signal.
            auto de = components::table::storage::deserialize_unified(chunk_buffer, resource, header);
            if (de.has_error()) {
                return result;
            }
            auto chunk = std::move(de.value());

            uint64_t chunk_size = 64;
            chunk_size += column_count * 8;
            uint64_t row_group_count = (row_count + 1023) / 1024;
            chunk_size += row_group_count * 32;
            for (uint32_t i = 0; i < column_count; ++i) {
                chunk_size += 4;
                auto type_size = header.column_count > 0 ? 8 : 1;
                chunk_size += type_size * row_count;
                chunk_size = (chunk_size + 7) & ~7ULL;
            }
            chunk_size += 16;

            result.chunk = std::move(chunk);
            result.bytes_consumed = chunk_size;
            result.ok = true;
            return result;
        }

        // Load a single partition from disk into memory
        bool load_partition_from_disk(const std::string& partition_path,
                                      std::pmr::vector<vector::data_chunk_t>& partition_chunks,
                                      std::pmr::memory_resource* resource,
                                      std::pmr::string& error_msg) {
            core::filesystem::local_file_system_t fs;
            if (!core::filesystem::file_exists(fs, partition_path)) {
                return true; // Empty partition - not an error
            }

            auto file_handle = core::filesystem::open_file(fs, partition_path,
                core::filesystem::file_flags::READ);

            if (!file_handle) {
                error_msg = "Failed to open partition file: " + partition_path;
                return false;
            }

            int64_t file_sz = core::filesystem::file_size(fs, *file_handle);
            if (file_sz <= 0) {
                return true;
            }

            // Bind the read+walk to the real file length. file_buffer_t::resize
            // rounds the allocation up to a sector and reserves a header, so
            // file_buffer.size() (= internal_size_ - header) over-reports the
            // usable bytes for a small partition file (200 B file → size_ ≈
            // 4088). Passing that to read_single_chunk lets it copy/deserialize
            // past the real data into garbage allocation. data_size is the exact
            // byte count on disk.
            const uint64_t data_size = static_cast<uint64_t>(file_sz);
            components::table::storage::file_buffer_t file_buffer(resource,
                components::table::storage::file_buffer_type::MANAGED_BUFFER, 0);
            file_buffer.resize(data_size);
            file_buffer.read(*file_handle, 0);

            uint64_t offset = 0;
            while (offset < data_size) {
                auto read_result = read_single_chunk(
                    file_buffer.internal_buffer(),
                    data_size,
                    offset,
                    resource);

                if (!read_result.ok) {
                    break;
                }

                partition_chunks.emplace_back(std::move(read_result.chunk));
                offset += read_result.bytes_consumed;

                if (read_result.bytes_consumed == 0) {
                    error_msg = "Invalid chunk size in partition file: " + partition_path;
                    return false;
                }
            }

            return true;
        }

        // Build hash table from partition chunks (in-memory, per partition).
        //
        // A multimap (not map) because equi-join keys need not be unique on the
        // right side; the value is (chunk_idx, row_idx) into the loaded partition
        // chunks.
        struct lv_hash {
            size_t operator()(const types::logical_value_t& v) const noexcept { return v.hash(); }
        };
        using right_index_t = std::unordered_multimap<types::logical_value_t,
                                                       std::pair<size_t, uint64_t>,
                                                       lv_hash,
                                                       std::equal_to<types::logical_value_t>>;

        right_index_t build_partition_hash_index(const std::pmr::vector<vector::data_chunk_t>& partition_chunks,
                                                 size_t right_col,
                                                 std::pmr::memory_resource* resource) {
            right_index_t table;

            uint64_t total_rows = 0;
            for (const auto& chunk : partition_chunks) {
                total_rows += chunk.size();
            }
            if (total_rows == 0) {
                return table;
            }
            table.reserve(total_rows);

            for (size_t ci = 0; ci < partition_chunks.size(); ++ci) {
                const auto& chunk = partition_chunks[ci];
                if (right_col >= chunk.column_count()) {
                    continue;
                }
                const auto& col = chunk.data[right_col];
                for (uint64_t rj = 0; rj < chunk.size(); ++rj) {
                    if (!col.validity().row_is_valid(rj)) {
                        continue;
                    }
                    table.emplace(col.value(rj), std::make_pair(ci, rj));
                }
            }
            return table;
        }

        // Track right-side matches for outer joins (flat buffer for pmr compatibility)
        struct right_match_tracker_t {
            std::pmr::vector<char> visited;  // Flat buffer: char instead of bool
            std::pmr::vector<size_t> chunk_offsets;

            explicit right_match_tracker_t(std::pmr::memory_resource* resource)
                : visited(resource)
                , chunk_offsets(resource) {}

            void init(const std::pmr::vector<vector::data_chunk_t>& chunks) {
                visited.clear();
                chunk_offsets.clear();
                chunk_offsets.reserve(chunks.size());

                size_t total_rows = 0;
                for (const auto& chunk : chunks) {
                    chunk_offsets.push_back(total_rows);
                    total_rows += chunk.size();
                }
                visited.assign(total_rows, 0);
            }

            bool is_visited(size_t chunk_idx, uint64_t row_idx) const {
                if (chunk_idx >= chunk_offsets.size()) return false;
                size_t offset = chunk_offsets[chunk_idx];
                size_t idx = offset + row_idx;
                return idx < visited.size() && visited[idx];
            }

            void mark_visited(size_t chunk_idx, uint64_t row_idx) {
                if (chunk_idx < chunk_offsets.size()) {
                    size_t offset = chunk_offsets[chunk_idx];
                    size_t idx = offset + row_idx;
                    if (idx < visited.size()) {
                        visited[idx] = 1;
                    }
                }
            }
        };
    } // namespace

    operator_grace_hash_join_t::operator_grace_hash_join_t(std::pmr::memory_resource* resource,
                                                           log_t log,
                                                           type join_type,
                                                           size_t left_col,
                                                           size_t right_col)
        : read_only_operator_t(resource, std::move(log), operator_type::grace_hash_join)
        , join_type_(join_type)
        , left_col_(left_col)
        , right_col_(right_col)
        , grace_state_(resource) {}

    void operator_grace_hash_join_t::on_execute_impl(pipeline::context_t* pipeline_context) {
        if (!left_ || !right_) {
            return;
        }
        if (!left_->output() || !right_->output()) {
            return;
        }

        // R10: stamp the real MVCC snapshot + resolve the spill dir from
        // ctx->disk_config before touching any spill file.
        if (pipeline_context != nullptr) {
            grace_state_.snapshot_horizon = spill_snapshot_horizon(*pipeline_context);
            grace_state_.query_id = pipeline_context->session.data();
            // R10: resolve the spill dir from ctx->disk_config.
            grace_state_.spill_dir = components::table::storage::resolve_spill_dir(
                pipeline_context->disk_config);
            // R10: read the per-query partition_count from ctx->disk_config.
            if (pipeline_context->disk_config) {
                grace_state_.partition_count = pipeline_context->disk_config->partition_count;
            }
        }

        // Guard against partition_count == 0 — the build-side partition step
        // divides by partition_count (key.hash() % partition_count), so a zero
        // would raise SIGFPE. Treat a misconfigured/zero count as a hard error
        // rather than crashing (R2: surface the failure, no exception).
        if (grace_state_.partition_count == 0) {
            set_error(core::error_t(core::error_code_t::invalid_parameter,
                                    std::pmr::string("grace hash join: partition_count is zero", resource_)));
            return;
        }

        auto left_out = left_->output();
        auto right_out = right_->output();
        auto& left_chunks = left_out->chunks();
        auto& right_chunks = right_out->chunks();

        assert(!left_chunks.empty());
        assert(!right_chunks.empty());

        std::pmr::vector<types::complex_logical_type> res_types{left_out->resource()};
        join_detail::compute_join_layout(left_chunks.front(), right_chunks.front(), res_types,
                                         indices_left_, indices_right_);

        if (log_.is_valid()) {
            trace(log(), "operator_grace_hash_join::left_size(): {}", left_out->size());
            trace(log(), "operator_grace_hash_join::right_size(): {}", right_out->size());
        }

        auto* res_resource = left_out->resource();
        chunks_vector_t out_chunks(res_resource);

        // R6: ALWAYS spill the build side (no runtime memory check). The
        // optimizer already decided this node spills; the operator trusts it.
        std::pmr::string error_msg(res_resource);
        if (!partition_and_spill_build_side(right_chunks, right_col_, grace_state_,
                                            res_resource, error_msg)) {
            if (log_.is_valid()) {
                trace(log(), "operator_grace_hash_join::spill_failed: {}", static_cast<std::string>(error_msg));
            }
            // R2: surface the disk I/O failure instead of fabricating an
            // empty "successful" join. Without set_error the executor reported a
            // spill write failure as a successful empty result (silent wrong
            // results). The sibling external_sort/external_group operators do the
            // same on their spill-failure paths.
            set_error(core::error_t(core::error_code_t::io_error,
                                    std::pmr::string{"grace hash join build-side spill failed: ", res_resource} +
                                        error_msg));
            return;
        }
        grace_state_.spilled = true;

        if (log_.is_valid()) {
            trace(log(), "operator_grace_hash_join::build_side_spilled: {}", true);
        }

        // Probe partition-by-partition from the spilled build side.
        bool probe_ok = probe_from_spilled_partitions(left_chunks, res_types, out_chunks, error_msg);
        if (!probe_ok) {
            if (log_.is_valid()) {
                trace(log(), "operator_grace_hash_join::grace_probe_failed: {}", static_cast<std::string>(error_msg));
            }
            // R2: surface the probe-time spill read failure (same reasoning
            // as the build-side path above) rather than emitting a silent empty
            // success.
            set_error(core::error_t(core::error_code_t::io_error,
                                    std::pmr::string{"grace hash join probe failed: ", res_resource} + error_msg));
            return;
        }

        if (out_chunks.empty()) {
            out_chunks.emplace_back(res_resource, res_types, 0);
        }
        output_ = operators::make_operator_data(res_resource, std::move(out_chunks));

        if (log_.is_valid()) {
            trace(log(), "operator_grace_hash_join::result_size(): {}", output_->size());
        }
    }

    bool operator_grace_hash_join_t::probe_from_spilled_partitions(
            const chunks_vector_t& left_chunks,
            const std::pmr::vector<types::complex_logical_type>& out_types,
            chunks_vector_t& out_chunks,
            std::pmr::string& error_msg) {
        auto* resource = left_->output()->resource();

        // Spill files live under grace_state_.spill_dir (resolved from
        // ctx->disk_config) with per-query unique names (see
        // partition_and_spill_build_side). Rebuild the same paths here.
        std::string dir = grace_state_.spill_dir;
        uint32_t partition_count = grace_state_.partition_count;

        right_match_tracker_t right_tracker(resource);

        // Track which left rows got matched (for left/full outer joins) - flat buffer
        std::pmr::vector<char> left_matched(resource);
        std::pmr::vector<size_t> left_chunk_offsets(resource);
        {
            size_t total_rows = 0;
            for (const auto& L : left_chunks) {
                left_chunk_offsets.push_back(total_rows);
                total_rows += L.size();
            }
            left_matched.assign(total_rows, 0);
        }

        // Process each partition
        for (uint32_t pid = 0; pid < partition_count; ++pid) {
            std::string partition_path = dir + "/" +
                components::table::storage::make_spill_name(
                    grace_state_.query_id,
                    std::string("hj_partition_") + std::to_string(pid));
            std::pmr::vector<vector::data_chunk_t> partition_chunks(resource);
            if (!load_partition_from_disk(partition_path, partition_chunks, resource, error_msg)) {
                return false;
            }

            if (partition_chunks.empty()) {
                continue;
            }

            right_tracker.init(partition_chunks);
            auto partition_table = build_partition_hash_index(partition_chunks, right_col_, resource);

            join_builder builder(resource, out_types, indices_left_, indices_right_, out_chunks);

            switch (join_type_) {
                case type::inner:
                    for (const auto& L : left_chunks) {
                        if (left_col_ >= L.column_count()) {
                            continue;
                        }
                        const auto& lcol = L.data[left_col_];
                        for (uint64_t li = 0; li < L.size(); ++li) {
                            if (!lcol.validity().row_is_valid(li)) {
                                continue;
                            }
                            auto key = lcol.value(li);
                            size_t key_partition = key.hash() % partition_count;
                            if (key_partition != pid) {
                                continue;
                            }
                            auto rng = partition_table.equal_range(key);
                            for (auto it = rng.first; it != rng.second; ++it) {
                                auto [ci, rj] = it->second;
                                builder.emit_matched(L, li, partition_chunks[ci], rj);
                                right_tracker.mark_visited(ci, rj);
                            }
                        }
                    }
                    break;

                case type::left:
                    for (const auto& L : left_chunks) {
                        if (left_col_ >= L.column_count()) {
                            for (uint64_t li = 0; li < L.size(); ++li) {
                                builder.emit_left_only(L, li);
                            }
                            continue;
                        }
                        const auto& lcol = L.data[left_col_];
                        for (uint64_t li = 0; li < L.size(); ++li) {
                            if (lcol.validity().row_is_valid(li)) {
                                auto key = lcol.value(li);
                                size_t key_partition = key.hash() % partition_count;
                                if (key_partition == pid) {
                                    auto rng = partition_table.equal_range(key);
                                    for (auto it = rng.first; it != rng.second; ++it) {
                                        auto [ci, rj] = it->second;
                                        builder.emit_matched(L, li, partition_chunks[ci], rj);
                                        right_tracker.mark_visited(ci, rj);
                                        // Mark this left row as matched so the
                                        // deferred left-only pass below does NOT
                                        // double-emit it. Without this mark every
                                        // matched left row was emitted once here as
                                        // a join row AND again as a left-only row
                                        // (left_matched stayed 0 for the LEFT case).
                                        size_t left_chunk_idx = &L - &left_chunks[0];
                                        size_t left_idx = left_chunk_offsets[left_chunk_idx] + li;
                                        if (left_idx < left_matched.size()) {
                                            left_matched[left_idx] = 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    break;

                case type::right:
                    for (const auto& L : left_chunks) {
                        if (left_col_ >= L.column_count()) {
                            continue;
                        }
                        const auto& lcol = L.data[left_col_];
                        for (uint64_t li = 0; li < L.size(); ++li) {
                            if (!lcol.validity().row_is_valid(li)) {
                                continue;
                            }
                            auto key = lcol.value(li);
                            size_t key_partition = key.hash() % partition_count;
                            if (key_partition != pid) {
                                continue;
                            }
                            auto rng = partition_table.equal_range(key);
                            for (auto it = rng.first; it != rng.second; ++it) {
                                auto [ci, rj] = it->second;
                                builder.emit_matched(L, li, partition_chunks[ci], rj);
                                right_tracker.mark_visited(ci, rj);
                            }
                        }
                    }
                    for (size_t ci = 0; ci < partition_chunks.size(); ++ci) {
                        const auto& R = partition_chunks[ci];
                        for (uint64_t rj = 0; rj < R.size(); ++rj) {
                            if (!right_tracker.is_visited(ci, rj)) {
                                builder.emit_right_only(R, rj);
                            }
                        }
                    }
                    break;

                case type::full:
                    for (const auto& L : left_chunks) {
                        if (left_col_ >= L.column_count()) {
                            continue;
                        }
                        const auto& lcol = L.data[left_col_];
                        for (uint64_t li = 0; li < L.size(); ++li) {
                            if (lcol.validity().row_is_valid(li)) {
                                auto key = lcol.value(li);
                                size_t key_partition = key.hash() % partition_count;
                                if (key_partition == pid) {
                                    auto rng = partition_table.equal_range(key);
                                    for (auto it = rng.first; it != rng.second; ++it) {
                                        auto [ci, rj] = it->second;
                                        builder.emit_matched(L, li, partition_chunks[ci], rj);
                                        right_tracker.mark_visited(ci, rj);
                                        size_t left_chunk_idx = &L - &left_chunks[0];
                                        size_t left_idx = left_chunk_offsets[left_chunk_idx] + li;
                                        if (left_idx < left_matched.size()) {
                                            left_matched[left_idx] = 1;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    for (size_t ci = 0; ci < partition_chunks.size(); ++ci) {
                        const auto& R = partition_chunks[ci];
                        for (uint64_t rj = 0; rj < R.size(); ++rj) {
                            if (!right_tracker.is_visited(ci, rj)) {
                                builder.emit_right_only(R, rj);
                            }
                        }
                    }
                    break;

                default:
                    break;
            }

            builder.flush();
        }

        // For left/full outer joins: emit unmatched left rows
        if (join_type_ == type::left || join_type_ == type::full) {
            join_builder left_only_builder(resource, out_types, indices_left_, indices_right_, out_chunks);
            for (size_t ci = 0; ci < left_chunks.size(); ++ci) {
                const auto& L = left_chunks[ci];
                for (uint64_t li = 0; li < L.size(); ++li) {
                    size_t left_idx = left_chunk_offsets[ci] + li;
                    if (!left_matched[left_idx]) {
                        left_only_builder.emit_left_only(L, li);
                    }
                }
            }
            left_only_builder.flush();
        }

        // Drop the RAII spill handles — their destructors remove the temp
        // files (success path). On an error early-return above, grace_state_ still
        // owns them and they are cleaned when the operator is destroyed.
        grace_state_.partition_handles.clear();

        return true;
    }

} // namespace components::operators
