#include "operator_unique_constraint.hpp"

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

    namespace {

        // Column index of `name` in `chunk` by alias (the DML rows carry their
        // column names as the vector type alias, like operator_check_constraint's
        // find_col). Returns absent when the column is not present.
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
        std::vector<std::vector<std::string>> unique_groups,
        std::vector<std::pair<std::string, types::logical_value_t>> column_defaults,
        bool write_set_named)
        : read_write_operator_t(resource, std::move(log), operator_type::unique_constraint)
        , table_oid_(table_oid)
        , unique_groups_(std::move(unique_groups))
        , column_defaults_(std::move(column_defaults))
        , write_set_named_(write_set_named) {}

    actor_zeta::unique_future<void> operator_unique_constraint_t::await_async_and_resume(pipeline::context_t* ctx) {
        // Resolve the rows to validate — identical policy to operator_fk_check_t: the
        // DML's constraint_input() snapshot. Constraint ops STACK above one DML, so the
        // immediate left_ may be another (empty) constraint op (e.g. a spliced
        // check/fk_check) — walk DOWN the left_ spine to the DML's snapshot (single
        // canonical source, R6; see constraint_util.hpp).
        const auto& source = constraint_detail::resolve_constraint_source(left_);
        if (!source || source->size() == 0 || unique_groups_.empty()) {
            output_ = resolve_cursor_output(left_, source);
            mark_executed();
            co_return;
        }
        // Non-const so LAYER 1 can call data_chunk_t::hash (a non-const method that
        // only reads the key columns). The operator_data pointee is mutable even
        // though `source` is a const reference to the intrusive_ptr.
        auto& in_chunks = source->chunks();
        execution_context_t exec_ctx{ctx->session, ctx->txn, {}};

        // One constraint group at a time. Each group is an independent UNIQUE/PK
        // constraint; a violation in any group fails the whole write.
        for (const auto& group : unique_groups_) {
            if (group.empty())
                continue;

            // Resolve each group column in the written-row schema. A column ABSENT
            // from the write-set stores the table DEFAULT when one exists (filled
            // agent-side at storage_append): materialize that (non-NULL) default as
            // a constant key column so omitted-column rows still participate in
            // uniqueness. Absent with NO (or NULL) default means the stored value is
            // NULL => NULLS DISTINCT voids the group (nothing to compare), matching
            // operator_fk_check_t's has_absent skip.
            struct key_source_t {
                uint64_t col{kAbsentCol};                   // present: column index in the chunk
                const types::logical_value_t* def{nullptr}; // absent: the non-NULL default
            };
            std::vector<key_source_t> sources;
            sources.reserve(group.size());
            bool group_void = false;
            for (const auto& col_name : group) {
                key_source_t src;
                src.col = find_col_index(in_chunks.front(), col_name);
                if (src.col == kAbsentCol) {
                    // Only a NAMED write-set proves the statement omitted the column
                    // (a positional insert aliases arbitrarily — legacy skip).
                    if (write_set_named_) {
                        for (const auto& [name, value] : column_defaults_) {
                            if (name == col_name && !value.is_null()) {
                                src.def = &value;
                                break;
                            }
                        }
                    }
                    if (src.def == nullptr) {
                        group_void = true;
                        break;
                    }
                }
                sources.push_back(src);
            }
            if (group_void)
                continue;

            // Materialize the group's key columns once per chunk: present columns
            // are zero-copy REFERENCED, absent-with-default columns constant-filled.
            // Every downstream layer (hash, NULL skip, verify, LAYER-2 key
            // extraction) reads these key chunks, so present and defaulted columns
            // flow through one shape; col_ids is simply 0..k-1 over them.
            std::pmr::vector<types::complex_logical_type> key_types(resource_);
            key_types.reserve(sources.size());
            for (const auto& src : sources) {
                key_types.push_back(src.col != kAbsentCol ? in_chunks.front().data[src.col].type() : src.def->type());
            }
            std::pmr::vector<components::vector::data_chunk_t> key_chunks(resource_);
            key_chunks.reserve(in_chunks.size());
            for (auto& chunk : in_chunks) {
                const uint64_t n = chunk.size();
                components::vector::data_chunk_t keys_chunk(resource_, key_types, n == 0 ? 1 : n);
                for (std::size_t j = 0; j < sources.size(); ++j) {
                    if (sources[j].col != kAbsentCol) {
                        keys_chunk.data[j].reference(chunk.data[sources[j].col]);
                    } else {
                        // Absent-with-default: reference the default as ONE CONSTANT
                        // vector rather than n per-row logical_value_t writes (LAYER-0
                        // minimize, rule 1). A CONSTANT column with cardinality n is
                        // valid — downstream hash / vector_ops::copy / cells_equal all
                        // read CONSTANT via get_value / zero-indexing.
                        keys_chunk.data[j].reference(*sources[j].def);
                    }
                }
                keys_chunk.set_cardinality(n);
                key_chunks.emplace_back(std::move(keys_chunk));
            }

            std::vector<uint64_t> col_ids(sources.size()); // data_chunk_t::hash wants std::vector
            for (std::size_t j = 0; j < sources.size(); ++j) {
                col_ids[j] = j;
            }

            // LAYER 1 — within-batch duplicate detection (typed hash + verify).
            // A row is a KEY-BEARING row only if every key column is non-NULL
            // (UNIQUE treats NULL as distinct). Per-chunk qualifying selections are
            // recorded for reuse by LAYER 2.
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

                // Batch-hash this chunk's key columns.
                components::vector::vector_t hash_vec(resource_, types::logical_type::UBIGINT, n == 0 ? 1 : n);
                if (n > 0) {
                    // hash() takes column_ids by non-const ref; hand it a copy.
                    std::vector<uint64_t> hash_cols = col_ids;
                    chunk.hash(hash_cols, hash_vec);
                    // When EVERY key column is an absent-with-default CONSTANT (LAYER 0
                    // fills those via reference()), data_chunk_t::hash returns a CONSTANT
                    // hash vector — only element 0 is written. The per-row hashes[row]
                    // read below assumes FLAT, so broadcast the constant across all n
                    // rows. A mixed present/CONSTANT key already flattens inside
                    // combine_hash; this only fires for the all-defaulted group (e.g. a
                    // lone defaulted UNIQUE column omitted by every batch row).
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
                        continue; // NULLS DISTINCT: not a key-bearing row.

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

            // LAYER 2 — existing-row detection. Skipped without a disk actor / a
            // resolved table oid (e.g. unit tests) — the within-batch guarantee
            // above is unaffected. After LAYER 1 every qualifying key is unique in
            // the batch, so the just-written row contributes exactly one row to its
            // key's scan result: a match count > 1 means a pre-existing distinct row.
            if (ctx->disk_address == actor_zeta::address_t::empty_address() || table_oid_ == catalog::INVALID_OID) {
                continue;
            }

            // STRADDLE-PACK all qualifying rows of the group (across input chunks)
            // into keys chunks of EXACTLY DEFAULT_VECTOR_CAPACITY rows, then scan
            // each packed chunk once. Total scans = ceil(total_qualifying / 1024),
            // instead of one scan per input chunk (which under-fills every chunk and
            // multiplies the mailbox round-trips on multi-chunk inserts). LAYER 1 has
            // already made every qualifying key unique across the batch, so a key's
            // scan-match count is never split by packing and the `> 1` threshold —
            // "the just-written row plus a pre-existing distinct row" — still holds.
            uint64_t total_qualifying = 0;
            for (uint64_t q : counts) {
                total_qualifying += q;
            }
            if (total_qualifying == 0)
                continue;

            // Key column names cross the mailbox per scan; build ONCE, copy per scan.
            std::pmr::vector<std::string> col_names(resource_);
            col_names.reserve(group.size());
            for (const auto& gname : group) {
                col_names.emplace_back(gname);
            }

            std::size_t c = 0; // current input chunk
            uint64_t off = 0;  // qualifying rows of chunk c already packed
            while (c < key_chunks.size()) {
                // FLAT target sized to a full vector; filled by straddling input
                // chunks until it holds DEFAULT_VECTOR_CAPACITY rows (or input ends).
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
                    // 7-arg copy: bounded partial of qualifying[c]. source_count is the
                    // FULL selection length (counts[c]) so a DICTIONARY source's merged
                    // indexing covers the slice; source_offset walks the selection and
                    // target_offset appends into the packed chunk.
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
                auto [_, fut] = actor_zeta::send(ctx->disk_address,
                                                 &services::disk::manager_disk_t::scan_by_keys,
                                                 exec_ctx,
                                                 table_oid_,
                                                 std::move(names),
                                                 std::move(keys));
                auto matches = co_await std::move(fut);

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
