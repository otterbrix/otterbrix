#include "operator_unique_constraint.hpp"

#include <atomic>

#include "constraint_util.hpp"

#include <components/context/context.hpp>
#include <components/types/logical_value.hpp>
#include <components/vector/cell_equal.hpp>
#include <components/vector/data_chunk.hpp>
#include <components/vector/vector_operations.hpp>
#include <services/disk/manager_disk.hpp>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

namespace components::operators {

    using constraint_detail::resolve_cursor_output;

#ifdef DEV_MODE
    namespace {
        std::atomic<uint64_t> g_unique_constraint_scan_sends{0};
    } // namespace
    uint64_t unique_constraint_scan_sends() noexcept {
        return g_unique_constraint_scan_sends.load(std::memory_order_relaxed);
    }
#endif

    namespace {

        // Column index of `name` in `chunk` by alias: DML rows carry their column names as the vector
        // type alias, like operator_check_constraint's find_col.
        constexpr uint64_t kAbsentCol = std::numeric_limits<uint64_t>::max();
        uint64_t find_col_index(const vector::data_chunk_t& chunk, const std::string& name) {
            for (uint64_t c = 0; c < chunk.column_count(); ++c) {
                if (chunk.data[c].type().alias() == name)
                    return c;
            }
            return kAbsentCol;
        }

        struct row_ref_t {
            std::size_t chunk_idx;
            uint64_t row;
        };

        bool key_is_null(const vector::data_chunk_t& keys, uint64_t row) {
            for (const auto& column : keys.data) {
                if (column.is_null(row)) {
                    return true;
                }
            }
            return false;
        }

        core::error_t build_key_chunks(std::pmr::memory_resource* resource,
                                       const chunks_vector_t& in_chunks,
                                       const std::vector<std::string>& group,
                                       std::pmr::vector<vector::data_chunk_t>* key_chunks) {
            // An empty key list must refuse rather than silently succeed, or a declared UNIQUE/PK enforces
            // nothing — same refusal as operator_fk_check_t's indices.empty().
            if (group.empty()) {
                return core::error_t{
                    core::error_code_t::invalid_constraint,
                    std::pmr::string{"UNIQUE constraint: key column list is empty — nothing to enforce", resource}};
            }

            // Rows are materialised (an omitted column expands to DEFAULT/NULL before append), so every key
            // column has a position; skipping here would silently accept a duplicate. Write-side half of the
            // resolve-side guard in operator_resolve_constraint.
            std::vector<uint64_t> sources;
            sources.reserve(group.size());
            for (const auto& col_name : group) {
                const auto col = find_col_index(in_chunks.front(), col_name);
                if (col == kAbsentCol) {
                    std::pmr::string what{"UNIQUE constraint: key column \"", resource};
                    what.append(col_name.c_str());
                    what.append("\" has no position in the written row");
                    return core::error_t{core::error_code_t::invalid_constraint, std::move(what)};
                }
                sources.push_back(col);
            }

            std::pmr::vector<types::complex_logical_type> key_types(resource);
            key_types.reserve(sources.size());
            for (const auto src : sources) {
                key_types.push_back(in_chunks.front().data[src].type());
            }
            key_chunks->reserve(in_chunks.size());
            // Every key column below is reference()d off the write-set column, so the view owns no
            // column buffer: an EMPTY projection list makes all of them placeholders
            // (components/vector/data_chunk.cpp:120-122). The plain ctor allocated a full
            // capacity-sized buffer per key column, zeroed it, and dropped it unread at the
            // reference() -- 8 KiB per BIGINT key per chunk. Same shape execution_dag.cpp:122 builds.
            const std::vector<size_t> no_owned_columns;
            for (const auto& chunk : in_chunks) {
                const uint64_t n = chunk.size();
                vector::data_chunk_t keys_chunk(resource, key_types, no_owned_columns, n == 0 ? 1 : n);
                for (std::size_t j = 0; j < sources.size(); ++j) {
                    // Every chunk is read at the front chunk's positions, so a layout/type mismatch would read
                    // past the array or the wrong column in silence. Same per-chunk guard as
                    // operator_fk_cascade_t's width check.
                    if (sources[j] >= chunk.column_count() || chunk.data[sources[j]].type().alias() != group[j] ||
                        chunk.data[sources[j]].type() != key_types[j]) {
                        std::pmr::string what{"UNIQUE constraint: key column \"", resource};
                        what.append(group[j].c_str());
                        what.append("\" is not at the same position in every chunk of the write-set — "
                                    "the chunks disagree about the table's shape");
                        return core::error_t{core::error_code_t::invalid_constraint, std::move(what)};
                    }
                    keys_chunk.data[j].reference(chunk.data[sources[j]]);
                }
                keys_chunk.set_cardinality(n);
                key_chunks->emplace_back(std::move(keys_chunk));
            }
            return core::error_t::no_error();
        }

        // Duplicates inside the write-set, in row order
        bool find_duplicates(std::pmr::memory_resource* resource,
                             std::pmr::vector<std::pmr::vector<vector::data_chunk_t>*>& groups,
                             const chunks_vector_t& rows,
                             const std::pmr::vector<std::pmr::vector<bool>>& skip,
                             bool first_only,
                             std::pmr::vector<std::pmr::vector<bool>>* duplicates,
                             std::pmr::vector<std::pmr::vector<int64_t>>* holders) {
            using seen_t = std::pmr::unordered_map<uint64_t, std::pmr::vector<row_ref_t>>;
            std::pmr::vector<seen_t> seen(groups.size(), resource);
            bool found = false;
            size_t chunk_count = groups.front()->size();
            for (size_t c = 0; c < chunk_count; ++c) {
                const uint64_t n = (*groups.front())[c].size();
                if (n == 0) {
                    continue;
                }
                std::pmr::vector<vector::vector_t> hashes(resource);
                hashes.reserve(groups.size());
                for (auto* key_chunks : groups) {
                    auto& chunk = (*key_chunks)[c];
                    vector::vector_t hash_vec(resource, types::logical_type::UBIGINT, n);
                    std::vector<uint64_t> hash_cols(chunk.column_count());
                    for (size_t j = 0; j < hash_cols.size(); ++j) {
                        hash_cols[j] = j;
                    }
                    chunk.hash(hash_cols, hash_vec);
                    if (hash_vec.get_vector_type() != vector::vector_type::FLAT) {
                        hash_vec.flatten(n);
                    }
                    hashes.push_back(std::move(hash_vec));
                }
                for (uint64_t row = 0; row < n; ++row) {
                    if (skip[c][row]) {
                        continue;
                    }
                    bool duplicate = false;
                    row_ref_t keeper{0, 0};
                    for (size_t group = 0; group < groups.size() && !duplicate; ++group) {
                        const auto& keys = (*groups[group])[c];
                        if (key_is_null(keys, row)) {
                            continue;
                        }
                        auto it = seen[group].find(hashes[group].data<uint64_t>()[row]);
                        if (it == seen[group].end()) {
                            continue;
                        }
                        for (const auto& cand : it->second) {
                            const auto& cand_keys = (*groups[group])[cand.chunk_idx];
                            bool match = true;
                            for (size_t key = 0; key < keys.column_count(); ++key) {
                                if (!vector::cells_equal(cand_keys.data[key], cand.row, keys.data[key], row)) {
                                    match = false;
                                    break;
                                }
                            }
                            if (match) {
                                duplicate = true;
                                keeper = cand;
                                break;
                            }
                        }
                    }
                    if (duplicate) {
                        found = true;
                        if (duplicates) {
                            (*duplicates)[c][row] = true;
                        }
                        if (holders) {
                            (*holders)[c][row] = rows[keeper.chunk_idx].row_ids.data<int64_t>()[keeper.row];
                        }
                        if (first_only) {
                            return true;
                        }
                        continue;
                    }
                    for (size_t group = 0; group < groups.size(); ++group) {
                        if (!key_is_null((*groups[group])[c], row)) {
                            seen[group][hashes[group].data<uint64_t>()[row]].push_back(row_ref_t{c, row});
                        }
                    }
                }
            }
            return found;
        }

        bool any_flagged(const std::pmr::vector<std::pmr::vector<bool>>& flags) {
            return std::any_of(flags.begin(), flags.end(), [](const auto& rows) {
                return std::find(rows.begin(), rows.end(), true) != rows.end();
            });
        }

    } // namespace

    operator_unique_constraint_t::operator_unique_constraint_t(std::pmr::memory_resource* resource,
                                                               log_t log,
                                                               catalog::oid_t table_oid,
                                                               std::vector<std::vector<std::string>> unique_groups,
                                                               std::vector<std::vector<std::string>> conflict_groups)
        : read_write_operator_t(resource, std::move(log), operator_type::unique_constraint)
        , table_oid_(table_oid)
        , unique_groups_(std::move(unique_groups))
        , conflict_groups_(std::move(conflict_groups)) {}

    actor_zeta::unique_future<void> operator_unique_constraint_t::await_async_and_resume(pipeline::context_t* ctx) {
        const auto& source = constraint_detail::resolve_constraint_source(left_);
        if (!source || source->size() == 0 || unique_groups_.empty()) {
            output_ = resolve_cursor_output(left_, source);
            mark_executed();
            co_return;
        }
        auto& in_chunks = source->chunks();

        std::pmr::unordered_set<int64_t> written(resource_);
        for (const auto& chunk : in_chunks) {
            const auto* ids = chunk.row_ids.data<int64_t>();
            for (uint64_t row = 0; row < chunk.size(); ++row) {
                written.insert(ids[row]);
            }
        }

        row_flags_t failed(resource_);
        row_ids_t holders(resource_);
        failed.reserve(in_chunks.size());
        holders.reserve(in_chunks.size());
        for (const auto& chunk : in_chunks) {
            failed.emplace_back(chunk.size(), false);
            holders.emplace_back(chunk.size(), int64_t{0});
        }

        auto is_conflict_group = [this](const std::vector<std::string>& group) {
            return std::find(conflict_groups_.begin(), conflict_groups_.end(), group) != conflict_groups_.end();
        };

        std::pmr::vector<std::pmr::vector<vector::data_chunk_t>> conflict_keys(resource_);
        conflict_keys.reserve(conflict_groups_.size());
        for (const auto& group : unique_groups_) {
            if (!is_conflict_group(group)) {
                continue;
            }
            conflict_keys.emplace_back();
            if (auto built = build_key_chunks(resource_, in_chunks, group, &conflict_keys.back());
                built.contains_error()) {
                set_error(built);
                co_return;
            }
            if (auto held = co_await find_held_keys_(ctx,
                                                     group,
                                                     conflict_keys.back(),
                                                     written,
                                                     failed,
                                                     false,
                                                     &failed,
                                                     &holders);
                held.contains_error()) {
                set_error(held);
                co_return;
            }
        }
        if (!conflict_keys.empty()) {
            std::pmr::vector<std::pmr::vector<vector::data_chunk_t>*> groups(resource_);
            for (auto& keys : conflict_keys) {
                groups.push_back(&keys);
            }
            find_duplicates(resource_, groups, in_chunks, failed, false, &failed, &holders);
        }

        for (const auto& group : unique_groups_) {
            if (is_conflict_group(group)) {
                continue;
            }
            std::pmr::vector<vector::data_chunk_t> key_chunks(resource_);
            if (auto built = build_key_chunks(resource_, in_chunks, group, &key_chunks); built.contains_error()) {
                set_error(built);
                co_return;
            }
            std::pmr::vector<std::pmr::vector<vector::data_chunk_t>*> groups(resource_);
            groups.push_back(&key_chunks);
            if (find_duplicates(resource_, groups, in_chunks, failed, true, nullptr, nullptr)) {
                set_error(core::error_t{
                    core::error_code_t::other_error,
                    std::pmr::string{"UNIQUE constraint violated: duplicate key within write batch", resource_}});
                co_return;
            }
            row_flags_t held(resource_);
            held.reserve(in_chunks.size());
            for (const auto& chunk : in_chunks) {
                held.emplace_back(chunk.size(), false);
            }
            if (auto lookup = co_await find_held_keys_(ctx, group, key_chunks, written, failed, true, &held, nullptr);
                lookup.contains_error()) {
                set_error(lookup);
                co_return;
            }
            if (any_flagged(held)) {
                set_error(core::error_t{core::error_code_t::other_error,
                                        std::pmr::string{"UNIQUE constraint violated: key already exists", resource_}});
                co_return;
            }
        }

        if (any_flagged(failed)) {
            conflict_rows_ = make_operator_data(resource_, chunks_vector_t{resource_});
            for (size_t c = 0; c < in_chunks.size(); ++c) {
                const auto& chunk = in_chunks[c];
                vector::indexing_vector_t selection(resource_, chunk.size() == 0 ? 1 : chunk.size());
                uint64_t count = 0;
                for (uint64_t row = 0; row < chunk.size(); ++row) {
                    if (failed[c][row]) {
                        selection.set_index(count++, row);
                        conflict_holders_.push_back(
                            conflict_holder_t{holders[c][row], written.count(holders[c][row]) != 0});
                    }
                }
                if (count == 0) {
                    continue;
                }
                vector::data_chunk_t rows(resource_, chunk.types(), count);
                chunk.copy(rows, selection, count);
                vector::vector_ops::copy(chunk.row_ids, rows.row_ids, selection, count, 0, 0);
                conflict_rows_->append_chunk(std::move(rows));
            }
        }

        output_ = resolve_cursor_output(left_, source);
        mark_executed();
    }

    actor_zeta::unique_future<core::error_t>
    operator_unique_constraint_t::find_held_keys_(pipeline::context_t* ctx,
                                                  const std::vector<std::string>& group,
                                                  std::pmr::vector<vector::data_chunk_t>& key_chunks,
                                                  const std::pmr::unordered_set<int64_t>& written,
                                                  const row_flags_t& skip,
                                                  bool first_only,
                                                  row_flags_t* held,
                                                  row_ids_t* holders) {
        if (table_oid_ == catalog::INVALID_OID) {
            co_return core::error_t{core::error_code_t::invalid_constraint,
                                    std::pmr::string{"UNIQUE constraint: the table it is declared on did not resolve "
                                                     "— stored rows cannot be checked",
                                                     resource_}};
        }
        if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
            co_return core::error_t::no_error();
        }
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        std::pmr::vector<vector::indexing_vector_t> qualifying(resource_);
        std::pmr::vector<uint64_t> counts(resource_);
        qualifying.reserve(key_chunks.size());
        counts.reserve(key_chunks.size());
        for (size_t c = 0; c < key_chunks.size(); ++c) {
            const auto& keys = key_chunks[c];
            vector::indexing_vector_t selection(resource_, keys.size() == 0 ? 1 : keys.size());
            uint64_t count = 0;
            for (uint64_t row = 0; row < keys.size(); ++row) {
                if (!skip[c][row] && !key_is_null(keys, row)) {
                    selection.set_index(count++, row);
                }
            }
            qualifying.push_back(std::move(selection));
            counts.push_back(count);
        }

        std::pmr::vector<std::string> col_names(resource_);
        col_names.reserve(group.size());
        for (const auto& name : group) {
            col_names.emplace_back(name);
        }
        auto key_types = key_chunks.front().types();

        size_t c = 0;     // current input chunk
        uint64_t off = 0; // qualifying rows of chunk c already packed
        while (c < key_chunks.size()) {
            vector::data_chunk_t keys(resource_, key_types, vector::DEFAULT_VECTOR_CAPACITY);
            std::pmr::vector<row_ref_t> packed(resource_);
            packed.reserve(vector::DEFAULT_VECTOR_CAPACITY);
            while (c < key_chunks.size() && packed.size() < vector::DEFAULT_VECTOR_CAPACITY) {
                if (off == counts[c]) { // chunk c exhausted (also skips counts[c] == 0)
                    ++c;
                    off = 0;
                    continue;
                }
                const uint64_t take =
                    std::min<uint64_t>(counts[c] - off, vector::DEFAULT_VECTOR_CAPACITY - packed.size());
                // source_count must be the full selection length (counts[c]), not `take`, so a
                // DICTIONARY source's merged indexing still covers the slice being copied.
                for (std::size_t j = 0; j < keys.column_count(); ++j) {
                    vector::vector_ops::copy(key_chunks[c].data[j],
                                             keys.data[j],
                                             qualifying[c],
                                             counts[c],
                                             off,
                                             packed.size(),
                                             take);
                }
                for (uint64_t i = 0; i < take; ++i) {
                    packed.push_back(row_ref_t{c, qualifying[c].get_index(off + i)});
                }
                off += take;
            }
            if (packed.empty()) {
                break; // only trailing exhausted chunks remained
            }
            keys.set_cardinality(packed.size());

            std::pmr::vector<std::string> names(col_names, resource_);
#ifdef DEV_MODE
            g_unique_constraint_scan_sends.fetch_add(1, std::memory_order_relaxed);
#endif
            auto [_scan, fut] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::scan_by_keys,
                                                            exec_ctx,
                                                            table_oid_,
                                                            std::move(names),
                                                            std::move(keys));
            auto matches_r = co_await std::move(fut);
            if (matches_r.has_error()) {
                // A failed unique-key read is not a miss; treating it as one lets the
                // operation proceed on data that was never read.
                co_return matches_r.error();
            }
            const auto& matches = matches_r.value();
            for (std::size_t i = 0; i < matches.size(); ++i) {
                const auto holder = std::find_if(matches[i].begin(), matches[i].end(), [&](int64_t row_id) {
                    return written.count(row_id) == 0;
                });
                if (holder == matches[i].end()) {
                    continue;
                }
                (*held)[packed[i].chunk_idx][packed[i].row] = true;
                if (holders) {
                    (*holders)[packed[i].chunk_idx][packed[i].row] = *holder;
                }
                if (first_only) {
                    co_return core::error_t::no_error();
                }
            }
        }
        co_return core::error_t::no_error();
    }

} // namespace components::operators
