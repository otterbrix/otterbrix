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

    } // namespace

    operator_unique_constraint_t::operator_unique_constraint_t(
        std::pmr::memory_resource* resource,
        log_t log,
        catalog::oid_t table_oid,
        std::vector<std::vector<std::string>> unique_groups)
        : read_write_operator_t(resource, std::move(log), operator_type::unique_constraint)
        , table_oid_(table_oid)
        , unique_groups_(std::move(unique_groups)) {}

    actor_zeta::unique_future<void> operator_unique_constraint_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Same policy as operator_fk_check_t: constraint ops stack above one DML, so the immediate left_
        // may be another (empty) constraint op — walk down the left_ spine to the DML's constraint_input()
        // snapshot (single canonical source, R6; see constraint_util.hpp).
        const auto& source = constraint_detail::resolve_constraint_source(left_);
        if (!source || source->size() == 0 || unique_groups_.empty()) {
            output_ = resolve_cursor_output(left_, source);
            mark_executed();
            co_return;
        }
        // Non-const so LAYER 1 can call the non-const data_chunk_t::hash; the operator_data pointee is
        // mutable even though `source` is a const reference to the intrusive_ptr.
        auto& in_chunks = source->chunks();
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // Each group is an independent UNIQUE/PK constraint; a violation in any one fails the whole write.
        for (const auto& group : unique_groups_) {
            // An empty key list must refuse rather than silently succeed, or a declared UNIQUE/PK enforces
            // nothing — same refusal as operator_fk_check_t's indices.empty().
            if (group.empty()) {
                set_error(core::error_t{
                    core::error_code_t::invalid_constraint,
                    std::pmr::string{"UNIQUE constraint: key column list is empty — nothing to enforce", resource_}});
                co_return;
            }

            // Rows are materialised (an omitted column expands to DEFAULT/NULL before append), so every key
            // column has a position; skipping here would silently accept a duplicate. Write-side half of the
            // resolve-side guard in operator_resolve_constraint.
            std::vector<uint64_t> sources;
            sources.reserve(group.size());
            for (const auto& col_name : group) {
                const auto col = find_col_index(in_chunks.front(), col_name);
                if (col == kAbsentCol) {
                    std::pmr::string what{"UNIQUE constraint: key column \"", resource_};
                    what.append(col_name.c_str());
                    what.append("\" has no position in the written row");
                    set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
                    co_return;
                }
                sources.push_back(col);
            }

            // Key columns are materialized once per chunk as zero-copy references; col_ids is their own
            // 0..k-1 positions, not their positions in the source chunk.
            std::pmr::vector<types::complex_logical_type> key_types(resource_);
            key_types.reserve(sources.size());
            for (const auto src : sources) {
                key_types.push_back(in_chunks.front().data[src].type());
            }
            std::pmr::vector<components::vector::data_chunk_t> key_chunks(resource_);
            key_chunks.reserve(in_chunks.size());
            // Every key column below is reference()d off the write-set column, so the view owns no
            // column buffer: an EMPTY projection list makes all of them placeholders
            // (components/vector/data_chunk.cpp:120-122). The plain ctor allocated a full
            // capacity-sized buffer per key column, zeroed it, and dropped it unread at the
            // reference() -- 8 KiB per BIGINT key per chunk. Same shape execution_dag.cpp:122 builds.
            const std::vector<size_t> no_owned_columns;
            for (auto& chunk : in_chunks) {
                const uint64_t n = chunk.size();
                components::vector::data_chunk_t keys_chunk(resource_,
                                                            key_types,
                                                            no_owned_columns,
                                                            n == 0 ? 1 : n);
                for (std::size_t j = 0; j < sources.size(); ++j) {
                    // Every chunk is read at the front chunk's positions, so a layout/type mismatch would read
                    // past the array or the wrong column in silence. Same per-chunk guard as
                    // operator_fk_cascade_t's width check.
                    if (sources[j] >= chunk.column_count() ||
                        chunk.data[sources[j]].type().alias() != group[j] ||
                        chunk.data[sources[j]].type() != key_types[j]) {
                        std::pmr::string what{"UNIQUE constraint: key column \"", resource_};
                        what.append(group[j].c_str());
                        what.append("\" is not at the same position in every chunk of the write-set — "
                                    "the chunks disagree about the table's shape");
                        set_error(core::error_t{core::error_code_t::invalid_constraint, std::move(what)});
                        co_return;
                    }
                    keys_chunk.data[j].reference(chunk.data[sources[j]]);
                }
                keys_chunk.set_cardinality(n);
                key_chunks.emplace_back(std::move(keys_chunk));
            }

            std::vector<uint64_t> col_ids(sources.size()); // data_chunk_t::hash wants std::vector
            for (std::size_t j = 0; j < sources.size(); ++j) {
                col_ids[j] = j;
            }

            // LAYER 1 — within-batch duplicate detection (typed hash + verify). A row qualifies only if
            // every key column is non-NULL (UNIQUE treats NULL as distinct); per-chunk qualifying selections
            // are recorded for reuse by LAYER 2.
            struct row_ref_t {
                std::size_t chunk_idx;
                uint64_t row;
            };
            std::pmr::unordered_map<uint64_t, std::pmr::vector<row_ref_t>> seen(resource_);
            std::pmr::vector<components::vector::indexing_vector_t> qualifying(resource_);
            std::pmr::vector<uint64_t> counts(resource_);
            qualifying.reserve(in_chunks.size());
            counts.reserve(in_chunks.size());

            for (std::size_t c = 0; c < key_chunks.size(); ++c) {
                auto& chunk = key_chunks[c];
                const uint64_t n = chunk.size();
                components::vector::indexing_vector_t selection(resource_, n == 0 ? 1 : n);
                uint64_t chunk_count = 0;

                components::vector::vector_t hash_vec(resource_, types::logical_type::UBIGINT, n == 0 ? 1 : n);
                if (n > 0) {
                    // hash() takes column_ids by non-const ref; hand it a copy.
                    std::vector<uint64_t> hash_cols = col_ids;
                    chunk.hash(hash_cols, hash_vec);
                    // hash() returns a CONSTANT vector (only element 0 written) when every key column hashed
                    // is itself CONSTANT; hashes[row] below assumes FLAT, so broadcast it.
                    if (hash_vec.get_vector_type() != components::vector::vector_type::FLAT) {
                        hash_vec.flatten(n);
                    }
                }
                const auto* hashes = hash_vec.data<uint64_t>();

                for (uint64_t row = 0; row < n; ++row) {
                    bool any_null = false;
                    for (auto k : col_ids) {
                        if (chunk.data[k].is_null(row)) {
                            any_null = true;
                            break;
                        }
                    }
                    if (any_null)
                        continue;

                    const uint64_t h = hashes[row];
                    auto it = seen.find(h);
                    if (it != seen.end()) {
                        for (const auto& cand : it->second) {
                            bool match = true;
                            for (std::size_t ki = 0; ki < col_ids.size(); ++ki) {
                                if (!vector::cells_equal(key_chunks[cand.chunk_idx].data[col_ids[ki]],
                                                         cand.row,
                                                         chunk.data[col_ids[ki]],
                                                         row)) {
                                    match = false;
                                    break;
                                }
                            }
                            if (match) {
                                set_error(core::error_t{
                                    core::error_code_t::other_error,
                                    std::pmr::string{"UNIQUE constraint violated: duplicate key within write batch",
                                                     resource_}});
                                co_return;
                            }
                        }
                    }
                    seen[h].push_back(row_ref_t{c, row});
                    selection.set_index(chunk_count, row);
                    ++chunk_count;
                }
                qualifying.emplace_back(std::move(selection));
                counts.push_back(chunk_count);
            }

            // LAYER 2 — existing-row detection: after LAYER 1, every qualifying key is unique in the batch,
            // so a match count > 1 means a pre-existing row. No disk actor is test topology, not corruption —
            // skip the layer.
            if (ctx->disk_address == actor_zeta::address_t::empty_address()) {
                continue;
            }
            // Unlike a missing disk actor, an unresolved oid is corruption, not topology — refuse rather
            // than skip the stored-row scan. Both splice sites (planner.cpp rewrite_insert / rewrite_update) get
            // their oid from catalog_resolves_t::constraints_for, which never emits groups for INVALID_OID.
            if (table_oid_ == catalog::INVALID_OID) {
                set_error(core::error_t{
                    core::error_code_t::invalid_constraint,
                    std::pmr::string{"UNIQUE constraint: the table it is declared on did not resolve — "
                                     "stored rows cannot be checked",
                                     resource_}});
                co_return;
            }

            // Repack qualifying rows into full DEFAULT_VECTOR_CAPACITY chunks and scan each once, instead of
            // one (under-filled) scan per input chunk — fewer mailbox round-trips. LAYER 1 already made every
            // qualifying key unique across the batch, so repacking cannot split a key's scan-match count.
            uint64_t total_qualifying = 0;
            for (uint64_t q : counts) {
                total_qualifying += q;
            }
            if (total_qualifying == 0)
                continue;

            // Key column names cross the mailbox per scan; build once, copy per scan.
            std::pmr::vector<std::string> col_names(resource_);
            col_names.reserve(group.size());
            for (const auto& gname : group) {
                col_names.emplace_back(gname);
            }

            std::size_t c = 0; // current input chunk
            uint64_t off = 0;  // qualifying rows of chunk c already packed
            while (c < key_chunks.size()) {
                // Filled by straddling input chunks until it holds DEFAULT_VECTOR_CAPACITY rows or input ends.
                components::vector::data_chunk_t keys(resource_,
                                                      key_types,
                                                      components::vector::DEFAULT_VECTOR_CAPACITY);
                uint64_t cur_n = 0; // rows packed into `keys` so far
                while (c < key_chunks.size() && cur_n < components::vector::DEFAULT_VECTOR_CAPACITY) {
                    if (off == counts[c]) { // chunk c exhausted (also skips counts[c] == 0)
                        ++c;
                        off = 0;
                        continue;
                    }
                    const uint64_t take =
                        std::min<uint64_t>(counts[c] - off, components::vector::DEFAULT_VECTOR_CAPACITY - cur_n);
                    // source_count must be the full selection length (counts[c]), not `take`, so a
                    // DICTIONARY source's merged indexing still covers the slice being copied.
                    for (std::size_t j = 0; j < col_ids.size(); ++j) {
                        components::vector::vector_ops::copy(key_chunks[c].data[j],
                                                             keys.data[j],
                                                             qualifying[c],
                                                             counts[c],
                                                             off,
                                                             cur_n,
                                                             take);
                    }
                    cur_n += take;
                    off += take;
                }
                if (cur_n == 0)
                    break; // only trailing exhausted chunks remained
                keys.set_cardinality(cur_n);

                std::pmr::vector<std::string> names(col_names, resource_);
#ifdef DEV_MODE
                g_unique_constraint_scan_sends.fetch_add(1, std::memory_order_relaxed);
#endif
                auto [_, fut] = actor_zeta::otterbrix::send(ctx->disk_address,
                                                            &services::disk::manager_disk_t::scan_by_keys,
                                                            exec_ctx,
                                                            table_oid_,
                                                            std::move(names),
                                                            std::move(keys));
                auto matches_r = co_await std::move(fut);
                if (matches_r.has_error()) {
                    // A failed unique-key read is not a miss; treating it as one lets the
                    // operation proceed on data that was never read.
                    set_error(matches_r.error());
                    co_return;
                }
                auto& matches = matches_r.value();

                for (std::size_t i = 0; i < matches.size(); ++i) {
                    if (matches[i].size() > 1) {
                        set_error(core::error_t{
                            core::error_code_t::other_error,
                            std::pmr::string{"UNIQUE constraint violated: key already exists", resource_}});
                        co_return;
                    }
                }
            }
        }

        output_ = resolve_cursor_output(left_, source);
        mark_executed();
    }

} // namespace components::operators
